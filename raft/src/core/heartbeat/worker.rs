use std::{fmt, sync::Arc, time::Duration};

use futures_util::{FutureExt, StreamExt, stream};

use crate::{
  Config, RaftTypeConfig,
  core::{
    heartbeat::{
      errors::{RaftCoreClosed, Stopped},
      event::HeartbeatEvent,
    },
    notification::Notification,
  },
  network::{NetStreamAppend, RPCOption},
  progress::stream_id::StreamId,
  raft::{AppendEntriesRequest, StreamAppendError, StreamAppendResult},
  replication::{Progress, response::ReplicationResult},
  type_config::{
    TypeConfigExt,
    alias::{CommittedVoteOf, MpscSenderOf, OneshotReceiverOf, WatchReceiverOf},
  },
};

/// A dedicated worker sending heartbeat to a specific follower.
pub struct HeartbeatWorker<C, N>
where
  C: RaftTypeConfig,
  N: NetStreamAppend<C>,
{
  pub(crate) id: C::NodeId,

  /// The leader this heartbeat worker works for
  pub(crate) leader_vote: CommittedVoteOf<C>,

  /// A unique stream.
  pub(crate) stream_id: StreamId,

  /// The receiver will be changed when a new heartbeat is needed to be sent.
  pub(crate) rx: WatchReceiverOf<C, Option<HeartbeatEvent<C>>>,

  pub(crate) network: N,

  pub(crate) target: C::NodeId,

  pub(crate) config: Arc<Config>,

  /// For sending back result to the [`RaftCore`].
  ///
  /// [`RaftCore`]: crate::core::RaftCore
  pub(crate) tx_notification: MpscSenderOf<C, Notification<C>>,
}

impl<C, N> fmt::Display for HeartbeatWorker<C, N>
where
  C: RaftTypeConfig,
  N: NetStreamAppend<C>,
{
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "HeartbeatWorker(id={}, target={})", self.id, self.target)
  }
}

impl<C, N> HeartbeatWorker<C, N>
where
  C: RaftTypeConfig,
  N: NetStreamAppend<C>,
{
  pub(crate) async fn run(self, rx_shutdown: OneshotReceiverOf<C, ()>) {
    let res = self.do_run(rx_shutdown).await;
    log::info!("HeartbeatWorker finished with result: {:?}", res);
  }

  pub(crate) async fn do_run(
    mut self,
    mut rx_shutdown: OneshotReceiverOf<C, ()>,
  ) -> Result<(), Stopped> {
    loop {
      log::debug!("{} is waiting for a new heartbeat event.", self);

      futures_util::select! {
          _ = (&mut rx_shutdown).fuse() => {
              log::info!("{} is shutdown.", self);
              return Err(Stopped::ReceivedShutdown);
          },
          _ = self.rx.changed().fuse() => {},
      }

      let heartbeat: Option<HeartbeatEvent<C>> = self.rx.borrow_watched().clone();

      // None is the initial value of the WatchReceiver, ignore it.
      let Some(heartbeat) = heartbeat else {
        continue;
      };

      let timeout = Duration::from_millis(self.config.heartbeat_interval);
      let option = RPCOption::new(timeout);

      let payload = AppendEntriesRequest {
        vote: self.leader_vote.clone().into_vote(),
        // Use last known matching log id as prev_log_id to detect follower state reversion.
        // prev_log_id == None does not conflict.
        //
        // Fail test `t99_issue_1500_heartbeat_cause_reversion_panic` by changing the
        // following line to `prev_log_id = heartbeat.cluster_committed.clone()`.
        prev_log_id: heartbeat.matching.clone(),
        leader_commit: heartbeat.cluster_committed.clone(),
        entries: vec![],
      };

      let input_stream = Box::pin(stream::once(async { payload }));

      let res = C::timeout(timeout, async {
        let mut output = self.network.stream_append(input_stream, option).await?;
        output.next().await.transpose()
      })
      .await;

      log::debug!(
        "{} sent a heartbeat: {}, result: {:?}",
        self,
        heartbeat,
        res
      );

      match res {
        Ok(Ok(Some(stream_result))) => {
          self.handle_stream_result(stream_result, &heartbeat).await?;
        }
        Ok(Ok(None)) => {
          // Stream returned no response - treat as network error
          log::warn!("{} heartbeat stream returned no response", self);
        }
        _ => {
          log::warn!("{} failed to send a heartbeat: {:?}", self, res);
        }
      };
    }
  }

  /// Handle the stream append result, send appropriate notifications.
  async fn handle_stream_result(
    &self,
    result: StreamAppendResult<C>,
    heartbeat: &HeartbeatEvent<C>,
  ) -> Result<(), RaftCoreClosed> {
    match result {
      Ok(_) => {
        self.send_heartbeat_progress(heartbeat).await?;
      }
      Err(StreamAppendError::HigherVote(vote)) => {
        log::debug!(
          "seen a higher vote({vote}) from {}; when:(sending heartbeat)",
          self.target
        );

        let noti = Notification::HigherVote {
          target: self.target.clone(),
          higher: vote,
          leader_vote: self.leader_vote.clone(),
        };

        self.send_notification(noti, "Seeing higher Vote").await?;
        // Higher vote means leadership is not granted, don't send HeartbeatProgress
      }
      Err(StreamAppendError::Conflict(_conflict_log_id)) => {
        // The follower does not have `matching` log id.
        // Use `matching` (which may be None) as the conflict point.
        //
        // Safe unwrap(): a None never conflict
        let conflict_log_id = heartbeat.matching.clone().unwrap();

        let noti = Notification::ReplicationProgress {
          stream_id: self.stream_id,
          progress: Progress {
            target: self.target.clone(),
            result: Ok(ReplicationResult(Err(conflict_log_id))),
          },
          inflight_id: None,
        };

        self.send_notification(noti, "Seeing conflict").await?;
        self.send_heartbeat_progress(heartbeat).await?;
      }
    }
    Ok(())
  }

  async fn send_heartbeat_progress(
    &self,
    heartbeat: &HeartbeatEvent<C>,
  ) -> Result<(), RaftCoreClosed> {
    let noti = Notification::HeartbeatProgress {
      stream_id: self.stream_id,
      sending_time: heartbeat.time,
      target: self.target.clone(),
    };
    self.send_notification(noti, "send HeartbeatProgress").await
  }

  async fn send_notification(
    &self,
    notification: Notification<C>,
    when: impl fmt::Display,
  ) -> Result<(), RaftCoreClosed> {
    let res = self.tx_notification.send(notification).await;

    if let Err(e) = res {
      let notification = e.0;
      log::error!("{self} failed to send {notification} to RaftCore; when:({when})");
      return Err(RaftCoreClosed);
    }
    Ok(())
  }
}
