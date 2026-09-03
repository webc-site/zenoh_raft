//! Async runtime components for Zenoh Raft based on compio and crossfire
//! 基于 compio 和 crossfire 的 Zenoh Raft 异步运行时组件

#[macro_use]
pub mod task_local;
pub mod deterministic_rng;
pub mod watch;

use std::{
  fmt::{Debug, Formatter, Result as FmtResult},
  future::Future,
  io::Error,
  pin::Pin,
  sync::{Arc, Weak},
  task::{Context, Poll},
};

// ── Oneshot ──
pub use crossfire::oneshot::RxOneshot as OneshotReceiver;
pub use crossfire::oneshot::TxOneshot as OneshotSender;
use crossfire::{
  AsyncRx, MAsyncTx,
  mpsc::{Array, bounded_async},
  oneshot::RxOneshot,
};

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

pub use crossfire::{SendError, TryRecvError};

type MpscTx<T> = MAsyncTx<Array<T>>;
type MpscRx<T> = AsyncRx<Array<T>>;

pub fn mpsc_channel<T: 'static>(buffer: usize) -> (MpscSender<T>, MpscReceiver<T>) {
  let (tx, rx) = bounded_async::<T>(buffer);
  (MpscSender::new(tx), MpscReceiver::new(rx))
}

/// Arc-wrapped MPSC sender supporting downgrade/upgrade
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

// ── Misc ──
pub use std::time::Instant;

pub use futures_util::lock::{Mutex, MutexGuard as OwnedGuard};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("deadline has elapsed")]
pub struct Elapsed;

pub use threaded::{
  BoxAny, BoxAsyncOnceMut, BoxFuture, BoxIterator, BoxMaybeAsyncOnceMut, BoxOnce, BoxStream,
  OptionalSend, OptionalSync,
};
pub use watch::{RecvError, WatchReceiver, WatchSender};

pub mod instant {
  pub use std::time::Instant;
}

mod threaded {
  use std::{any::Any, future::Future, pin::Pin};

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
