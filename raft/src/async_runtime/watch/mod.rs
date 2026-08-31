//! Watch channel with lock-free/low-contention version tracking and exact `borrow_and_update` semantics.

mod watch_error;

pub use watch_error::RecvError;
pub use watch_error::SendError;

use rapidhash::{HashMapExt, RapidHashMap as HashMap};
use std::fmt;
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock, RwLockReadGuard};
use std::task::{Context, Poll, Waker};

struct Shared<T> {
    value: RwLock<T>,
    version: AtomicU64,
    closed: AtomicBool,
    tx_count: AtomicUsize,
    rx_count: AtomicUsize,
    wakers: StdMutex<HashMap<usize, Waker>>,
    next_waker_id: AtomicUsize,
}

pub struct Ref<'a, T> {
    inner: RwLockReadGuard<'a, T>,
}

impl<T> Deref for Ref<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T: fmt::Debug> fmt::Debug for Ref<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T: fmt::Display> fmt::Display for Ref<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

/// Creates a new watch channel, returning the sender and receiver handles.
pub fn channel<T: Send + Sync + 'static>(init: T) -> (WatchSender<T>, WatchReceiver<T>) {
    let shared = Arc::new(Shared {
        value: RwLock::new(init),
        version: AtomicU64::new(1),
        closed: AtomicBool::new(false),
        tx_count: AtomicUsize::new(1),
        rx_count: AtomicUsize::new(1),
        wakers: StdMutex::new(HashMap::new()),
        next_waker_id: AtomicUsize::new(1),
    });

    let tx = WatchSender {
        shared: shared.clone(),
    };
    let rx = WatchReceiver {
        shared,
        seen_version: 1,
        waker_id: 0,
    };

    (tx, rx)
}

pub struct WatchSender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for WatchSender<T> {
    fn clone(&self) -> Self {
        self.shared.tx_count.fetch_add(1, Ordering::Relaxed);
        Self {
            shared: self.shared.clone(),
        }
    }
}

#[inline]
fn wake_all_wakers(wakers_lock: &StdMutex<HashMap<usize, Waker>>) {
    let wakers: smallvec::SmallVec<[Waker; 4]> = {
        let mut guard = wakers_lock.lock().unwrap();
        guard.drain().map(|(_, w)| w).collect()
    };
    for waker in wakers {
        waker.wake();
    }
}

impl<T> WatchSender<T> {
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        if self.shared.rx_count.load(Ordering::Relaxed) == 0 {
            return Err(SendError(value));
        }

        {
            let mut guard = self.shared.value.write().unwrap();
            *guard = value;
            self.shared.version.fetch_add(1, Ordering::Release);
        }

        wake_all_wakers(&self.shared.wakers);

        Ok(())
    }

    pub fn send_if_modified<F>(&self, modify: F) -> bool
    where
        F: FnOnce(&mut T) -> bool,
    {
        let modified = {
            let mut guard = self.shared.value.write().unwrap();
            if modify(&mut *guard) {
                self.shared.version.fetch_add(1, Ordering::Release);
                true
            } else {
                false
            }
        };

        if modified {
            wake_all_wakers(&self.shared.wakers);
            true
        } else {
            false
        }
    }

    pub fn borrow_watched(&self) -> Ref<'_, T> {
        Ref {
            inner: self.shared.value.read().unwrap(),
        }
    }

    pub fn subscribe(&self) -> WatchReceiver<T> {
        self.shared.rx_count.fetch_add(1, Ordering::Relaxed);
        let waker_id = self.shared.next_waker_id.fetch_add(1, Ordering::Relaxed);
        let current_version = self.shared.version.load(Ordering::Acquire);
        WatchReceiver {
            shared: self.shared.clone(),
            seen_version: current_version,
            waker_id,
        }
    }

    pub fn send_if_different(&self, value: T) -> bool
    where
        T: PartialEq,
    {
        self.send_if_modified(|current| {
            if *current != value {
                *current = value;
                true
            } else {
                false
            }
        })
    }

    pub fn send_if_greater(&self, value: T) -> bool
    where
        T: PartialOrd,
    {
        self.send_if_modified(|current| {
            if value > *current {
                *current = value;
                true
            } else {
                false
            }
        })
    }
}

impl<T> Drop for WatchSender<T> {
    fn drop(&mut self) {
        if self.shared.tx_count.fetch_sub(1, Ordering::Release) == 1 {
            self.shared.closed.store(true, Ordering::Release);
            wake_all_wakers(&self.shared.wakers);
        }
    }
}

pub struct WatchReceiver<T> {
    shared: Arc<Shared<T>>,
    seen_version: u64,
    waker_id: usize,
}

impl<T> Clone for WatchReceiver<T> {
    fn clone(&self) -> Self {
        self.shared.rx_count.fetch_add(1, Ordering::Relaxed);
        let waker_id = self.shared.next_waker_id.fetch_add(1, Ordering::Relaxed);
        Self {
            shared: self.shared.clone(),
            seen_version: self.seen_version,
            waker_id,
        }
    }
}

impl<T> Drop for WatchReceiver<T> {
    fn drop(&mut self) {
        let mut wakers = self.shared.wakers.lock().unwrap();
        wakers.remove(&self.waker_id);
        drop(wakers);
        self.shared.rx_count.fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct Changed<'a, T> {
    rx: &'a mut WatchReceiver<T>,
}

impl<T> Future for Changed<'_, T> {
    type Output = Result<(), RecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let rx = &mut self.rx;
        let current_version = rx.shared.version.load(Ordering::Acquire);
        if rx.seen_version < current_version {
            rx.seen_version = current_version;
            return Poll::Ready(Ok(()));
        }

        if rx.shared.closed.load(Ordering::Acquire) {
            return Poll::Ready(Err(RecvError(())));
        }

        let mut wakers = rx.shared.wakers.lock().unwrap();
        let current_version = rx.shared.version.load(Ordering::Acquire);
        if rx.seen_version < current_version {
            rx.seen_version = current_version;
            return Poll::Ready(Ok(()));
        }
        if rx.shared.closed.load(Ordering::Acquire) {
            return Poll::Ready(Err(RecvError(())));
        }
        wakers.insert(rx.waker_id, cx.waker().clone());
        Poll::Pending
    }
}

impl<T> Drop for Changed<'_, T> {
    fn drop(&mut self) {
        let mut wakers = self.rx.shared.wakers.lock().unwrap();
        wakers.remove(&self.rx.waker_id);
    }
}

impl<T> WatchReceiver<T> {
    pub fn changed(&mut self) -> Changed<'_, T> {
        Changed { rx: self }
    }

    pub fn borrow_watched(&self) -> Ref<'_, T> {
        Ref {
            inner: self.shared.value.read().unwrap(),
        }
    }

    pub fn borrow_and_update(&mut self) -> Ref<'_, T> {
        self.seen_version = self.shared.version.load(Ordering::Acquire);
        Ref {
            inner: self.shared.value.read().unwrap(),
        }
    }

    pub async fn wait_until_ge(&mut self, value: &T) -> Result<T, RecvError>
    where
        T: PartialOrd + Clone,
    {
        loop {
            {
                let current = self.borrow_watched();
                if &*current >= value {
                    return Ok(current.clone());
                }
            }
            self.changed().await?;
        }
    }

    pub async fn wait_until<F>(&mut self, condition: F) -> Result<T, RecvError>
    where
        T: Clone,
        F: Fn(&T) -> bool,
    {
        loop {
            {
                let current = self.borrow_watched();
                if condition(&*current) {
                    return Ok(current.clone());
                }
            }
            self.changed().await?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::task::noop_waker;

    #[compio::test]
    async fn test_watch_borrow_and_update() {
        let (tx, mut rx) = channel(10);
        assert_eq!(*rx.borrow_watched(), 10);

        tx.send(20).unwrap();
        let val = *rx.borrow_and_update();
        assert_eq!(val, 20);

        // After borrow_and_update, changed() should NOT immediately return
        let mut changed_fut = rx.changed();
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(Pin::new(&mut changed_fut).poll(&mut cx).is_pending());
        drop(changed_fut);

        // Sending new value should wake and update
        tx.send(30).unwrap();
        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow_watched(), 30);
    }
}
