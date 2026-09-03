use core::time::Duration;
use std::collections::BTreeSet;

use futures_util::FutureExt;

use crate::{
  LogIdOptionExt, OptionalSend, RaftTypeConfig,
  core::ServerState,
  metrics::{Condition, Metric, RaftMetrics},
  type_config::{
    TypeConfigExt,
    alias::{InstantOf, LogIdOf, SerdeInstantOf, VoteOf, WatchReceiverOf},
  },
};

/// Error variants related to waiting for metrics conditions.
#[derive(Debug, PartialEq, Eq, thiserror::Error, bitcode::Encode, bitcode::Decode)]
pub enum WaitError {
  /// Timeout occurred while waiting for a condition.
  #[error("timeout after {0:?} when {1}")]
  Timeout(Duration, String),

  /// Raft node is shutting down.
  #[error("raft is shutting down")]
  ShuttingDown,
}

/// Wait is a wrapper of RaftMetrics channel that impls several utils to wait for metrics to satisfy
/// some condition.
pub struct Wait<C: RaftTypeConfig> {
  /// The timeout duration for waiting operations.
  pub timeout: Duration,
  /// The metrics receiver channel.
  pub rx: WatchReceiverOf<C, RaftMetrics<C>>,
}

impl<C> Wait<C>
where
  C: RaftTypeConfig,
{
  /// Wait for metrics to satisfy some condition or timeout.
  pub async fn metrics<T>(&self, func: T, msg: impl ToString) -> Result<RaftMetrics<C>, WaitError>
  where
    T: Fn(&RaftMetrics<C>) -> bool + OptionalSend,
  {
    let timeout_at = C::now() + self.timeout;

    let mut rx = self.rx.clone();
    loop {
      let latest = rx.borrow_watched().clone();
      let latest_id = latest.id.clone();
      let msg_str = msg.to_string();

      log::debug!("id={latest_id} wait {msg_str} latest: {latest}");

      if func(&latest) {
        log::debug!("id={latest_id} done wait {msg_str} latest: {latest}");
        return Ok(latest);
      }

      let now = C::now();
      if now >= timeout_at {
        return Err(WaitError::Timeout(
          self.timeout,
          format!("{msg_str} latest: {latest}"),
        ));
      }

      let sleep_time = timeout_at - now;
      log::debug!("wait timeout: {sleep_time:?}");
      let delay = C::sleep(sleep_time);

      futures_util::select_biased! {
          _ = delay.fuse() => {
              log::debug!("id={latest_id} timeout wait {msg_str} latest: {latest}");
              return Err(WaitError::Timeout(self.timeout, format!("{msg_str} latest: {latest}")));
          }
          changed = rx.changed().fuse() => {
              match changed {
                  Ok(_) => {
                      // metrics changed, continue the waiting loop
                  },
                  Err(err) => {
                      log::debug!("id={latest_id} error: {err:?}; wait {msg_str} latest: {latest:?}");

                      return Err(WaitError::ShuttingDown);
                  }
              }
          }
      }
    }
  }

  /// Wait for `vote` to become `want` or timeout.
  pub async fn vote(
    &self,
    want: VoteOf<C>,
    msg: impl ToString,
  ) -> Result<RaftMetrics<C>, WaitError> {
    self.eq(Metric::Vote(want), msg).await
  }

  /// Wait for `current_leader` to become `Some(leader_id)` until timeout.
  pub async fn current_leader(
    &self,
    leader_id: C::NodeId,
    msg: impl ToString,
  ) -> Result<RaftMetrics<C>, WaitError> {
    let msg = msg.to_string();
    self
      .metrics(
        |m| m.current_leader.as_ref() == Some(&leader_id),
        &format!("{msg} .current_leader == {leader_id}"),
      )
      .await
  }

  /// Block until the last log index becomes exactly `index` (inclusive) or timeout.
  pub async fn log_index(
    &self,
    index: Option<u64>,
    msg: impl ToString,
  ) -> Result<RaftMetrics<C>, WaitError> {
    self.eq(Metric::LastLogIndex(index), msg).await
  }

  /// Block until the last log index becomes at least `index` (inclusive) or timeout.
  pub async fn log_index_at_least(
    &self,
    index: Option<u64>,
    msg: impl ToString,
  ) -> Result<RaftMetrics<C>, WaitError> {
    self.ge(Metric::LastLogIndex(index), msg).await
  }

  /// Block until the applied index becomes exactly `index` (inclusive) or timeout.
  pub async fn applied_index(
    &self,
    index: Option<u64>,
    msg: impl ToString,
  ) -> Result<RaftMetrics<C>, WaitError> {
    self.eq(Metric::AppliedIndex(index), msg).await
  }

  /// Block until the last applied log index become at least `index` (inclusive) or timeout.
  /// Note that this also implies `last_log_id >= index`.
  pub async fn applied_index_at_least(
    &self,
    index: Option<u64>,
    msg: impl ToString,
  ) -> Result<RaftMetrics<C>, WaitError> {
    self.ge(Metric::AppliedIndex(index), msg).await
  }

  /// Wait for `state` to become `want_state` or timeout.
  pub async fn state(
    &self,
    want_state: ServerState,
    msg: impl ToString,
  ) -> Result<RaftMetrics<C>, WaitError> {
    let msg = msg.to_string();
    self
      .metrics(
        |m| m.state == want_state,
        &format!("{msg} .state == {want_state:?}"),
      )
      .await
  }

  /// Wait until this node is a leader with a quorum-acknowledged timestamp.
  ///
  /// If `at_least` is `Some`, the timestamp must be greater than or equal to it.
  /// If `at_least` is `None`, any quorum-acknowledged timestamp is accepted.
  pub async fn leader_with_quorum_acked(
    &self,
    at_least: Option<InstantOf<C>>,
    msg: impl ToString,
  ) -> Result<RaftMetrics<C>, WaitError> {
    self
      .metrics(
        |m| {
          m.state == ServerState::Leader
            && m
              .last_quorum_acked
              .map(|a: SerdeInstantOf<C>| a.into_inner())
              .is_some_and(|acked| at_least.is_none_or(|want| acked >= want))
        },
        &{
          let msg = msg.to_string();
          format!("{msg} .leader_with_quorum_acked({at_least:?})")
        },
      )
      .await
  }

  /// Block until membership contains exact the expected `voter_ids` or timeout.
  pub async fn voter_ids(
    &self,
    voter_ids: impl IntoIterator<Item = C::NodeId>,
    msg: impl ToString,
  ) -> Result<RaftMetrics<C>, WaitError> {
    let want = voter_ids.into_iter().collect::<BTreeSet<_>>();

    log::debug!("block until voter_ids == {want:?}");

    let msg = msg.to_string();
    self
      .metrics(
        |m| {
          let got = m.membership_config.membership().voter_ids().collect();
          want == got
        },
        &format!("{msg} .members == {want:?}"),
      )
      .await
  }

  /// Wait for `snapshot` to become `snapshot_last_log_id` or timeout.
  pub async fn snapshot(
    &self,
    snapshot_last_log_id: LogIdOf<C>,
    msg: impl ToString,
  ) -> Result<RaftMetrics<C>, WaitError> {
    self
      .eq(Metric::Snapshot(Some(snapshot_last_log_id)), msg)
      .await
  }

  /// Block until the committed index becomes exactly `index` or timeout.
  pub async fn committed_index(
    &self,
    index: Option<u64>,
    msg: impl ToString,
  ) -> Result<RaftMetrics<C>, WaitError> {
    let msg = msg.to_string();
    self
      .metrics(
        |m| m.local_committed.index() == index,
        &format!("{msg} .committed_index == {index:?}"),
      )
      .await
  }

  /// Block until the committed index becomes at least `index` (inclusive) or timeout.
  pub async fn committed_index_at_least(
    &self,
    index: Option<u64>,
    msg: impl ToString,
  ) -> Result<RaftMetrics<C>, WaitError> {
    let msg = msg.to_string();
    self
      .metrics(
        |m| m.local_committed.index() >= index,
        &format!("{msg} .committed_index >= {index:?}"),
      )
      .await
  }

  /// Wait for `purged` to become `want` or timeout.
  pub async fn purged(
    &self,
    want: Option<LogIdOf<C>>,
    msg: impl ToString,
  ) -> Result<RaftMetrics<C>, WaitError> {
    self.eq(Metric::Purged(want), msg).await
  }

  /// Block until a metric becomes greater than or equal the specified value or timeout.
  ///
  /// For example, to await until the term becomes 2 or greater:
  /// ```ignore
  /// my_raft.wait(None).ge(Metric::Term(2), "become term 2").await?;
  /// ```
  pub async fn ge(
    &self,
    metric: Metric<C>,
    msg: impl ToString,
  ) -> Result<RaftMetrics<C>, WaitError> {
    self.until(Condition::ge(metric), msg).await
  }

  /// Block until a metric becomes equal to the specified value or timeout.
  ///
  /// For example, to await until the term becomes exact 2:
  /// ```ignore
  /// my_raft.wait(None).eq(Metric::Term(2), "become term 2").await?;
  /// ```
  pub async fn eq(
    &self,
    metric: Metric<C>,
    msg: impl ToString,
  ) -> Result<RaftMetrics<C>, WaitError> {
    self.until(Condition::eq(metric), msg).await
  }

  /// Block until a metric satisfies the specified condition or timeout.
  pub(crate) async fn until(
    &self,
    cond: Condition<C>,
    msg: impl ToString,
  ) -> Result<RaftMetrics<C>, WaitError> {
    let msg = msg.to_string();
    self
      .metrics(
        |raft_metrics| match &cond {
          Condition::GE(expect) => raft_metrics >= expect,
          Condition::EQ(expect) => raft_metrics == expect,
        },
        &format!("{msg} .{cond}"),
      )
      .await
  }
}

#[cfg(test)]
mod tests {

  #[test]
  fn test_wait_error_serde() {
    use super::*;

    // Test Timeout variant
    {
      let err = WaitError::Timeout(Duration::from_millis(500), "waiting for leader".to_string());
      let serialized = bitcode::encode(&err);
      let deserialized: WaitError = bitcode::decode(&serialized).unwrap();
      assert_eq!(err, deserialized);
    }

    // Test ShuttingDown variant
    {
      let err = WaitError::ShuttingDown;
      let serialized = bitcode::encode(&err);
      let deserialized: WaitError = bitcode::decode(&serialized).unwrap();
      assert_eq!(err, deserialized);
    }
  }
}
