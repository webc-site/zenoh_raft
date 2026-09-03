use std::{
  error::Error,
  future::Future,
  io,
  panic::AssertUnwindSafe,
  pin::Pin,
  task::{Context, Poll},
  thread,
  time::{Duration, Instant},
};

use compio::runtime::{Runtime, spawn};
use crossfire::oneshot::{RxOneshot, TxOneshot, oneshot as oneshot_channel};
use futures_util::{
  FutureExt, Stream,
  future::{Either, select},
  stream::unfold,
};

use crate::{
  OptionalSend, OptionalSync, RaftTypeConfig,
  async_runtime::{
    Elapsed, JoinHandle, MpscReceiver, MpscSender, Mutex, WatchReceiver, WatchSender, mpsc_channel,
    watch,
  },
  errors::ErrorSource,
  type_config::alias::ErrorSourceOf,
};

struct YieldNow(bool);

impl Future for YieldNow {
  type Output = ();

  fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    if self.0 {
      Poll::Ready(())
    } else {
      self.0 = true;
      cx.waker().wake_by_ref();
      Poll::Pending
    }
  }
}

/// Collection of utility methods for `RaftTypeConfig`.
pub trait TypeConfigExt: RaftTypeConfig {
  // Time related methods

  /// Returns the current time.
  #[track_caller]
  fn now() -> Instant {
    Instant::now()
  }

  /// Wait until `duration` has elapsed.
  #[track_caller]
  fn sleep(duration: Duration) -> impl Future<Output = ()> + Send {
    futures_timer::Delay::new(duration)
  }

  /// Yield control back to the async runtime cooperatively.
  #[track_caller]
  fn yield_now() -> impl Future<Output = ()> + Send {
    YieldNow(false)
  }

  /// Wait until `deadline` is reached.
  #[track_caller]
  fn sleep_until(deadline: Instant) -> impl Future<Output = ()> + Send {
    futures_timer::Delay::new(deadline.saturating_duration_since(Instant::now()))
  }

  /// Require a [`Future`] to complete before the specified duration has elapsed.
  #[track_caller]
  fn timeout<R, F: Future<Output = R> + OptionalSend>(
    duration: Duration,
    future: F,
  ) -> impl Future<Output = Result<R, Elapsed>> + Send
  where
    R: OptionalSend,
  {
    async move {
      let delay = futures_timer::Delay::new(duration);
      futures_util::pin_mut!(future);
      futures_util::pin_mut!(delay);
      match select(future, delay).await {
        Either::Left((res, _)) => Ok(res),
        Either::Right(_) => Err(Elapsed),
      }
    }
  }

  /// Require a [`Future`] to complete before the specified instant in time.
  #[track_caller]
  fn timeout_at<R, F: Future<Output = R> + OptionalSend>(
    deadline: Instant,
    future: F,
  ) -> impl Future<Output = Result<R, Elapsed>> + Send
  where
    R: OptionalSend,
  {
    Self::timeout(deadline.saturating_duration_since(Instant::now()), future)
  }

  // Synchronization methods

  /// Creates a new one-shot channel for sending single values.
  #[track_caller]
  fn oneshot<T>() -> (TxOneshot<T>, RxOneshot<T>)
  where
    T: OptionalSend,
  {
    oneshot_channel()
  }

  /// Creates a bounded mpsc channel backed by crossfire.
  #[track_caller]
  fn mpsc<T>(buffer: usize) -> (MpscSender<T>, MpscReceiver<T>)
  where
    T: OptionalSend + 'static,
  {
    mpsc_channel::<T>(buffer)
  }

  /// Converts an mpsc receiver into a [`Stream`].
  fn mpsc_to_stream<T>(rx: MpscReceiver<T>) -> impl Stream<Item = T>
  where
    T: OptionalSend + 'static,
  {
    unfold(rx, |mut rx| async move {
      let item = rx.recv().await?;
      Some((item, rx))
    })
  }

  /// Creates a watch channel for watching changes.
  #[track_caller]
  fn watch_channel<T>(init: T) -> (WatchSender<T>, WatchReceiver<T>)
  where
    T: OptionalSend + OptionalSync + Clone + 'static,
  {
    watch::channel(init)
  }

  /// Creates a Mutex lock.
  #[track_caller]
  fn mutex<T>(value: T) -> Mutex<T>
  where
    T: OptionalSend,
  {
    Mutex::new(value)
  }

  // Task methods

  /// Spawn a new task on the current compio runtime.
  #[track_caller]
  fn spawn<T>(future: T) -> JoinHandle<T::Output>
  where
    T: Future + OptionalSend + 'static,
    T::Output: OptionalSend + 'static,
  {
    if Runtime::try_current().is_some() {
      let (tx, rx) = oneshot_channel();
      spawn(async move {
        if let Ok(res) = AssertUnwindSafe(future).catch_unwind().await {
          tx.send(res);
        }
      })
      .detach();
      JoinHandle::new(rx)
    } else {
      panic!("spawn called outside of an active compio runtime context");
    }
  }

  /// Run a blocking function on a separate thread.
  #[track_caller]
  fn spawn_blocking<F, T>(f: F) -> impl Future<Output = Result<T, io::Error>> + Send
  where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
  {
    let (tx, rx) = oneshot_channel();
    thread::spawn(move || {
      let res = f();
      tx.send(res);
    });
    async move {
      rx.await
        .map_err(|_| io::Error::other("blocking task canceled"))
    }
  }

  // Error creation methods

  /// Create an error source for a storage error.
  #[track_caller]
  fn storage_error<E>(e: E) -> ErrorSourceOf<Self>
  where
    E: Error + OptionalSend + OptionalSync + 'static,
  {
    <Self::ErrorSource as ErrorSource>::from_error(&e)
  }

  /// Create an error source for a network error.
  #[track_caller]
  fn network_error<E>(e: E) -> ErrorSourceOf<Self>
  where
    E: Error + OptionalSend + OptionalSync + 'static,
  {
    <Self::ErrorSource as ErrorSource>::from_error(&e)
  }

  /// Create an error source from an error reference.
  #[track_caller]
  fn err_from_error<E>(e: &E) -> ErrorSourceOf<Self>
  where
    E: Error + OptionalSend + OptionalSync + 'static,
  {
    <Self::ErrorSource as ErrorSource>::from_error(e)
  }

  /// Create an error source from a string message.
  #[track_caller]
  fn err_from_string(msg: impl ToString) -> ErrorSourceOf<Self> {
    <Self::ErrorSource as ErrorSource>::from_string(msg)
  }

  /// Block on a future to completion.
  fn block_on<F: Future>(future: F) -> F::Output {
    Runtime::new().unwrap().block_on(future)
  }

  /// Run a future to completion (alias for block_on).
  fn run<F: Future>(future: F) -> F::Output {
    Runtime::new().unwrap().block_on(future)
  }
}

impl<C: RaftTypeConfig> TypeConfigExt for C {}

#[cfg(test)]
mod tests {
  use std::time::Duration;

  use crate::{
    async_runtime::{MpscReceiver, MpscSender},
    engine::testing::UTConfig,
    type_config::TypeConfigExt,
  };

  type C = UTConfig;

  #[compio::test]
  async fn test_sleep() {
    let start = C::now();
    C::sleep(Duration::from_millis(50)).await;
    assert!(C::now() - start >= Duration::from_millis(40));
  }

  #[compio::test]
  async fn test_yield_now() {
    C::yield_now().await;
  }

  #[compio::test]
  async fn test_sleep_until() {
    let start = C::now();
    let deadline = start + Duration::from_millis(50);
    C::sleep_until(deadline).await;
    assert!(C::now() >= deadline - Duration::from_millis(10));
  }

  #[compio::test]
  async fn test_timeout_success() {
    let fut = async {
      C::sleep(Duration::from_millis(10)).await;
      42
    };
    let result = C::timeout(Duration::from_millis(100), fut).await;
    assert_eq!(result, Ok(42));
  }

  #[compio::test]
  async fn test_timeout_elapsed() {
    let fut = async {
      C::sleep(Duration::from_millis(100)).await;
      42
    };
    let result = C::timeout(Duration::from_millis(10), fut).await;
    assert!(result.is_err());
  }

  #[compio::test]
  async fn test_oneshot() {
    let (tx, rx) = C::oneshot::<i32>();
    tx.send(42);
    assert_eq!(rx.await, Ok(42));
  }

  #[compio::test]
  async fn test_mpsc() {
    let (tx, mut rx) = C::mpsc::<i32>(10);
    MpscSender::send(&tx, 42).await.unwrap();
    assert_eq!(MpscReceiver::recv(&mut rx).await, Some(42));
  }

  #[compio::test]
  async fn test_watch_channel() {
    let (tx, mut rx) = C::watch_channel::<i32>(0);
    assert_eq!(*rx.borrow_watched(), 0);

    tx.send(42).unwrap();
    rx.changed().await.unwrap();
    assert_eq!(*rx.borrow_watched(), 42);
  }

  #[compio::test]
  async fn test_mutex() {
    let m = C::mutex(42);
    {
      let mut guard = m.lock().await;
      *guard += 1;
    }
    let guard = m.lock().await;
    assert_eq!(*guard, 43);
  }

  #[compio::test]
  async fn test_spawn() {
    let handle = C::spawn(async { 42 });
    let res = handle.await;
    assert_eq!(res.unwrap(), 42);
  }
}
