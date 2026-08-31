//! Async runtime components for Zenoh Raft based on compio and crossfire.

#[macro_use]
pub mod task_local;
pub mod deterministic_rng;
pub mod watch;

use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::future::Future;
use std::io::Error;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};

use crossfire::mpsc::{Array, bounded_async};
use crossfire::oneshot::RxOneshot;
use crossfire::{AsyncRx, MAsyncTx};

// ── Oneshot ──

pub use crossfire::oneshot::RxOneshot as OneshotReceiver;
pub use crossfire::oneshot::TxOneshot as OneshotSender;

// ── JoinHandle ──

pub struct JoinHandle<T> {
    rx: RxOneshot<T>,
}

impl<T> Debug for JoinHandle<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("JoinHandle").finish()
    }
}

impl<T> JoinHandle<T> {
    pub fn new(rx: RxOneshot<T>) -> Self {
        Self { rx }
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.rx)
            .poll(cx)
            .map(|r| r.map_err(|_| Error::other("task canceled")))
    }
}

// ── MPSC ──

pub use crossfire::SendError;
pub use crossfire::TryRecvError;

type MpscTx<T> = MAsyncTx<Array<T>>;
type MpscRx<T> = AsyncRx<Array<T>>;

pub fn mpsc_channel<T: 'static>(buffer: usize) -> (MpscSender<T>, MpscReceiver<T>) {
    let (tx, rx) = bounded_async::<T>(buffer);
    (MpscSender::new(tx), MpscReceiver::new(rx))
}

/// Arc 包装的 MPSC 发送端，支持 downgrade/upgrade
pub struct MpscSender<T: 'static> {
    tx: Arc<MpscTx<T>>,
}

impl<T: 'static> Clone for MpscSender<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<T: 'static> MpscSender<T> {
    pub fn new(tx: MpscTx<T>) -> Self {
        Self { tx: Arc::new(tx) }
    }

    pub fn from_arc(tx: Arc<MpscTx<T>>) -> Self {
        Self { tx }
    }

    pub async fn send(&self, msg: T) -> Result<(), SendError<T>>
    where
        T: Send + Unpin,
    {
        self.tx.send(msg).await
    }

    pub fn downgrade(&self) -> MpscWeakSender<T> {
        MpscWeakSender {
            tx: Arc::downgrade(&self.tx),
        }
    }
}

pub struct MpscReceiver<T: 'static> {
    rx: MpscRx<T>,
}

impl<T: 'static> MpscReceiver<T> {
    pub fn new(rx: MpscRx<T>) -> Self {
        Self { rx }
    }

    pub async fn recv(&mut self) -> Option<T> {
        self.rx.recv().await.ok()
    }

    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.rx.try_recv()
    }
}

pub struct MpscWeakSender<T: 'static> {
    tx: Weak<MpscTx<T>>,
}

impl<T: 'static> Clone for MpscWeakSender<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<T: 'static> MpscWeakSender<T> {
    pub fn upgrade(&self) -> Option<MpscSender<T>> {
        self.tx.upgrade().map(|tx| MpscSender::<T> { tx })
    }
}

// ── Mutex ──

pub use futures_util::lock::Mutex;
pub use futures_util::lock::MutexGuard as OwnedGuard;

// ── Misc ──

pub use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("deadline has elapsed")]
pub struct Elapsed;

pub use threaded::BoxAny;
pub use threaded::BoxAsyncOnceMut;
pub use threaded::BoxFuture;
pub use threaded::BoxIterator;
pub use threaded::BoxMaybeAsyncOnceMut;
pub use threaded::BoxOnce;
pub use threaded::BoxStream;
pub use threaded::OptionalSend;
pub use threaded::OptionalSync;
pub use watch::RecvError;
pub use watch::WatchReceiver;
pub use watch::WatchSender;

pub mod instant {
    pub use std::time::Instant;
}

mod threaded {
    use std::any::Any;
    use std::future::Future;
    use std::pin::Pin;

    use futures_util::Stream;

    /// A trait that extends `Send`.
    pub trait OptionalSend: Send {}
    impl<T: Send + ?Sized> OptionalSend for T {}

    /// A trait that extends `Sync`.
    pub trait OptionalSync: Sync {}
    impl<T: Sync + ?Sized> OptionalSync for T {}

    pub type BoxIterator<'a, T> = Box<dyn Iterator<Item = T> + Send + 'a>;
    pub type BoxFuture<'a, T = ()> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
    pub type BoxStream<'a, T> = Pin<Box<dyn Stream<Item = T> + Send + 'a>>;
    pub type BoxAsyncOnceMut<'a, A, T = ()> = Box<dyn FnOnce(&mut A) -> BoxFuture<T> + Send + 'a>;
    pub type BoxMaybeAsyncOnceMut<'a, A, T = ()> =
        Box<dyn FnOnce(&mut A) -> Option<BoxFuture<T>> + Send + 'a>;
    pub type BoxOnce<'a, A, T = ()> = Box<dyn FnOnce(&A) -> T + Send + 'a>;
    pub type BoxAny = Box<dyn Any + Send>;
}
