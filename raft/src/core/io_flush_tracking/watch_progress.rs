use std::marker::PhantomData;

use super::flush_point::FlushPoint;
use crate::{
  OptionalSend, OptionalSync, RaftTypeConfig, Vote,
  async_runtime::RecvError,
  type_config::alias::{LeaderIdOf, LogIdOf, WatchReceiverOf},
};

pub type VoteProgress<C> = WatchProgress<C, Option<Vote<LeaderIdOf<C>>>>;
pub type LogProgress<C> = WatchProgress<C, Option<FlushPoint<C>>>;
pub type CommitProgress<C> = WatchProgress<C, Option<LogIdOf<C>>>;
pub type AppliedProgress<C> = WatchProgress<C, Option<LogIdOf<C>>>;
pub type SnapshotProgress<C> = WatchProgress<C, Option<LogIdOf<C>>>;

/// A handle for watching I/O flush progress with monotonic progress guarantees.
#[derive(Clone)]
pub struct WatchProgress<C, T>
where
  C: RaftTypeConfig,
  T: OptionalSend + OptionalSync + PartialOrd + Clone + 'static,
{
  inner: WatchReceiverOf<C, T>,
  _p: PhantomData<C>,
}

impl<C, T> WatchProgress<C, T>
where
  C: RaftTypeConfig,
  T: OptionalSend + OptionalSync + PartialOrd + Clone + 'static,
{
  pub(crate) fn new(inner: WatchReceiverOf<C, T>) -> Self {
    Self {
      inner,
      _p: PhantomData,
    }
  }

  /// Wait until the flushed I/O progress becomes greater than or equal to the target value.
  pub async fn wait_until_ge(&mut self, target: &T) -> Result<T, RecvError> {
    self.inner.wait_until_ge(target).await
  }

  /// Wait until the flushed I/O progress satisfies the given condition.
  pub async fn wait_until<F>(&mut self, condition: F) -> Result<T, RecvError>
  where
    F: Fn(&T) -> bool + OptionalSend,
  {
    self.inner.wait_until(condition).await
  }

  /// Get the current flushed I/O progress state immediately without waiting.
  pub fn get(&self) -> T {
    self.inner.borrow_watched().clone()
  }

  /// Wait for a value change notification.
  pub async fn changed(&mut self) -> Result<(), RecvError> {
    self.inner.changed().await
  }

  /// Wait for and return the next changed value.
  pub async fn next(&mut self) -> Result<T, RecvError> {
    self.changed().await?;
    Ok(self.get())
  }
}

#[cfg(test)]
mod tests {
  use std::time::Duration;

  use super::*;
  use crate::{
    RaftTypeConfig,
    impls::{InlineBatch, OneshotResponder, Vote, leader_id_adv::LeaderId},
    type_config::TypeConfigExt,
    vote::RaftLeaderId,
  };

  #[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd)]
  struct TestConfig;

  impl RaftTypeConfig for TestConfig {
    type D = u64;
    type R = ();
    type NodeId = u64;
    type Node = ();
    type Term = u64;
    type LeaderId = LeaderId<u64, u64>;
    type Vote = Vote<Self::LeaderId>;
    type Payload = crate::EntryPayload<Self::D, Self::NodeId, Self::Node>;
    type Entry = crate::Entry<<Self::LeaderId as RaftLeaderId>::Committed, Self::Payload>;
    type Responder<T>
      = OneshotResponder<Self, T>
    where
      T: OptionalSend + 'static;
    type Batch<T>
      = InlineBatch<T>
    where
      T: OptionalSend + 'static;
    type ErrorSource = anyerror::AnyError;
  }

  #[compio::test]
  async fn test_wait_until_ge() {
    let (tx, rx) = TestConfig::watch_channel(0u64);
    let mut progress = WatchProgress::<TestConfig, u64>::new(rx);

    assert_eq!(progress.get(), 0);

    drop(TestConfig::spawn(async move {
      TestConfig::sleep(Duration::from_millis(10)).await;
      tx.send(5).unwrap();
      TestConfig::sleep(Duration::from_millis(10)).await;
      tx.send(10).unwrap();
    }));

    let result = progress.wait_until_ge(&8).await.unwrap();
    assert!(result >= 8);
    assert_eq!(result, 10);
  }

  #[compio::test]
  async fn test_wait_until_ge_immediate() {
    let (tx, rx) = TestConfig::watch_channel(10u64);
    let mut progress = WatchProgress::<TestConfig, u64>::new(rx);

    let result = progress.wait_until_ge(&5).await.unwrap();
    assert_eq!(result, 10);

    drop(tx);
  }

  #[compio::test]
  async fn test_wait_until_custom_condition() {
    let (tx, rx) = TestConfig::watch_channel(1u64);
    let mut progress = WatchProgress::<TestConfig, u64>::new(rx);

    drop(TestConfig::spawn(async move {
      for i in 2..=10 {
        TestConfig::sleep(Duration::from_millis(5)).await;
        tx.send(i).unwrap();
      }
    }));

    let result = progress.wait_until(|v| v % 2 == 0).await.unwrap();
    assert_eq!(result % 2, 0);
    assert_eq!(result, 2);
  }

  #[compio::test]
  async fn test_wait_until_immediate() {
    let (tx, rx) = TestConfig::watch_channel(10u64);
    let mut progress = WatchProgress::<TestConfig, u64>::new(rx);

    let result = progress.wait_until(|v| v >= &5).await.unwrap();
    assert_eq!(result, 10);

    drop(tx);
  }

  #[compio::test]
  async fn test_changed_waits_for_notification() {
    let (tx, rx) = TestConfig::watch_channel(0u64);
    let mut progress = WatchProgress::<TestConfig, u64>::new(rx);

    // Initial value is 0
    assert_eq!(progress.get(), 0);

    // Spawn a task that sends a new value after a delay
    drop(TestConfig::spawn(async move {
      TestConfig::sleep(Duration::from_millis(10)).await;
      tx.send(5).unwrap();
    }));

    // Wait for change
    progress.changed().await.unwrap();

    // Value should have changed
    assert_eq!(progress.get(), 5);
  }

  #[compio::test]
  async fn test_changed_returns_immediately_if_unseen_value() {
    let (tx, rx) = TestConfig::watch_channel(0u64);
    let mut progress = WatchProgress::<TestConfig, u64>::new(rx);

    // Send a value before calling changed()
    tx.send(5).unwrap();

    // changed() should return immediately since value hasn't been marked seen
    progress.changed().await.unwrap();
    assert_eq!(progress.get(), 5);
  }

  #[compio::test]
  async fn test_changed_returns_error_when_sender_dropped() {
    let (tx, rx) = TestConfig::watch_channel(0u64);
    let mut progress = WatchProgress::<TestConfig, u64>::new(rx);

    // Drop sender
    drop(tx);

    // changed() should return error
    let result = progress.changed().await;
    assert!(result.is_err());
  }

  #[compio::test]
  async fn test_next_returns_changed_value() {
    let (tx, rx) = TestConfig::watch_channel(0u64);
    let mut progress = WatchProgress::<TestConfig, u64>::new(rx);

    // Spawn a task that sends values
    drop(TestConfig::spawn(async move {
      TestConfig::sleep(Duration::from_millis(10)).await;
      tx.send(5).unwrap();
      TestConfig::sleep(Duration::from_millis(10)).await;
      tx.send(10).unwrap();
    }));

    // First next() should return 5
    let value = progress.next().await.unwrap();
    assert_eq!(value, 5);

    // Second next() should return 10
    let value = progress.next().await.unwrap();
    assert_eq!(value, 10);
  }

  #[compio::test]
  async fn test_next_returns_immediate_if_unseen_value() {
    let (tx, rx) = TestConfig::watch_channel(0u64);
    let mut progress = WatchProgress::<TestConfig, u64>::new(rx);

    // Send value before calling next()
    tx.send(42).unwrap();

    // next() should return immediately with the new value
    let value = progress.next().await.unwrap();
    assert_eq!(value, 42);
  }

  #[compio::test]
  async fn test_next_returns_error_when_sender_dropped() {
    let (tx, rx) = TestConfig::watch_channel(0u64);
    let mut progress = WatchProgress::<TestConfig, u64>::new(rx);

    // Drop sender
    drop(tx);

    // next() should return error
    let result = progress.next().await;
    assert!(result.is_err());
  }
}
