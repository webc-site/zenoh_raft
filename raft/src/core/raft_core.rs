use std::{
  collections::BTreeMap,
  fmt,
  fmt::Debug,
  future::Future,
  mem,
  sync::{Arc, atomic::Ordering},
  time::Duration,
};

use display_more::{DisplayOptionExt, DisplaySliceExt};
use futures_util::FutureExt;
use log::Level::Debug as LogDebug;
use sm::Response::{Apply, BuildSnapshotDone, InstallSnapshot};

use crate::{
  ChangeMembers, Membership, OptionalSend, RaftState, RaftTypeConfig, StorageError,
  async_runtime::TryRecvError,
  batch::Batch,
  config::{Config, RuntimeConfig},
  core::{
    ClientResponderQueue, IoBroadcast, MetricsChannels, PendingRead, PendingReadDeadlineNotifier,
    PendingReadQueue, ServerState,
    balancer::Balancer,
    core_state::CoreState,
    heartbeat::{event::HeartbeatEvent, handle::HeartbeatWorkersHandle},
    merged_raft_msg_receiver::BatchRaftMsgReceiver,
    notification::Notification,
    raft_msg::{
      AppendEntriesTx, ClientReadTx, RaftMsg, ResultSender, VoteTx,
      external_command::ExternalCommand, install_full_snapshot_request::InstallFullSnapshotRequest,
      membership_payloads::MembershipPayloads,
    },
    sm,
    sm::handle::{Handle, SnapshotReader},
  },
  display_ext::DisplayInstantExt,
  engine::{
    Command, Condition, Engine, Respond, TargetProgress, handler::leader_handler::LeaderHandler,
    leader_log_ids::LeaderLogIds,
  },
  entry::{RaftEntry, RaftPayload},
  errors::{
    AllowNextRevertError, ClientWriteError, Fatal, ForwardToLeader, Infallible, InitializeError,
    LinearizableReadError, PreconditionFailed, StorageIOResult, Timeout, UncommittedLeaderLog,
  },
  impls::ProgressResponder,
  log_id::option_raft_log_id_ext::OptionRaftLogIdExt,
  metrics::{
    HeartbeatMetrics, MetricsRecorder, RaftDataMetrics, RaftMetrics, RaftServerMetrics,
    ReplicationMetrics, SerdeInstant, forward_metrics,
  },
  network::{NetSnapshot, NetTransferLeader, NetVote, RPCOption, RPCTypes, RaftNetworkFactory},
  progress::{VecProgressEntry, inflight_id::InflightId, stream_id::StreamId},
  proposer::{Leader, LeaderQuorumSet},
  raft::{
    AppendEntriesRequest, ClientWriteResult, LogSegment, Precondition, VoteRequest, VoteResponse,
    linearizable_read::{Linearizer, LinearizerOption},
    message::TransferLeaderRequest,
    responder::{Responder, core_responder::CoreResponder},
  },
  raft_state::{
    LogStateReader,
    io_state::{io_id::IOId, log_io_id::LogIOId},
  },
  replication::{
    ReplicationCore, ReplicationSessionId, event_watcher::EventWatcher, replicate::Replicate,
    replication_context::ReplicationContext, replication_handle::ReplicationHandle,
    replication_progress, snapshot_transmitter::SnapshotTransmitter,
  },
  runtime::RaftRuntime,
  storage::{IOFlushed, RaftLogStorage, RaftStateMachine},
  type_config::{
    TypeConfigExt,
    alias::{
      BatchOf, ChangeMembershipErrorOf, CommittedLeaderIdOf, CommittedVoteOf, InstantOf, LogIdOf,
      MembershipStateOf, MpscReceiverOf, MpscSenderOf, MutexOf, OneshotReceiverOf, PayloadOf,
      VoteOf, WatchReceiverOf, WatchSenderOf,
    },
  },
  vote::{RaftLeaderId, RaftVote, raft_vote::RaftVoteExt, vote_status::VoteStatus},
};

/// The result of applying log entries to state machine.
pub(crate) struct ApplyResult<C: RaftTypeConfig> {
  pub(crate) since: u64,
  pub(crate) end: u64,
  pub(crate) last_applied: LogIdOf<C>,
}

impl<C: RaftTypeConfig> Debug for ApplyResult<C> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("ApplyResult")
      .field("since", &self.since)
      .field("end", &self.end)
      .field("last_applied", &self.last_applied)
      .finish()
  }
}

impl<C: RaftTypeConfig> fmt::Display for ApplyResult<C> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "ApplyResult([{}, {}), last_applied={})",
      self.since, self.end, self.last_applied,
    )
  }
}

fn apply_membership_to_payload<C>(
  membership_state: &MembershipStateOf<C>,
  changes: ChangeMembers<C::NodeId, C::Node>,
  retain: bool,
  payloads: MembershipPayloads<C>,
) -> Result<C::Payload, ChangeMembershipErrorOf<C>>
where
  C: RaftTypeConfig,
{
  let membership = membership_state.next_membership(changes, retain)?;
  let payload = payloads.select(membership);
  Ok(payload)
}

/// The core type implementing the Raft protocol.
pub struct RaftCore<C, NF, LS, SM>
where
  C: RaftTypeConfig,
  NF: RaftNetworkFactory<C>,
  NF::Network: NetSnapshot<C, SnapshotData = SM::SnapshotData>,
  LS: RaftLogStorage<C>,
  SM: RaftStateMachine<C>,
{
  /// This node's ID.
  pub(crate) id: C::NodeId,

  /// This node's runtime config.
  pub(crate) config: Arc<Config>,

  pub(crate) runtime_config: Arc<RuntimeConfig>,

  /// Additional state that does not directly affect the consensus.
  pub(crate) core_state: CoreState<C>,

  /// The `RaftNetworkFactory` implementation.
  pub(crate) network_factory: Arc<MutexOf<C, NF>>,

  /// The [`RaftLogStorage`] implementation.
  pub(crate) log_store: LS,

  /// A controlling handle to the [`RaftStateMachine`] worker.
  ///
  /// [`RaftStateMachine`]: `crate::storage::RaftStateMachine`
  pub(crate) sm_handle: Handle<C, SM>,

  pub(crate) engine: Engine<C, SM>,

  /// Responders to send result back to client when logs are applied.
  pub(crate) client_responders: ClientResponderQueue<CoreResponder<C>>,

  /// Linearizable reads waiting for the quorum acknowledgement clock to exceed their thresholds.
  pub(crate) pending_reads: PendingReadQueue<C>,

  /// Wakes this core when a pending linearizable read reaches its deadline.
  pub(crate) pending_read_deadline_notifier: PendingReadDeadlineNotifier<C>,

  /// A mapping of node IDs the replication state of the target node.
  pub(crate) replications: BTreeMap<C::NodeId, ReplicationHandle<C>>,

  pub(crate) heartbeat_handle: HeartbeatWorkersHandle<C>,

  pub(crate) rx_api: BatchRaftMsgReceiver<C>,

  /// Keepalive sender to keep `rx_api` channel open
  /// When application drops last `Raft` handle, this sender keeps channel open,
  /// so core shuts down only via `rx_shutdown`, avoiding truncating active `stream_append`
  /// 保持 `rx_api` 通道活跃的 keepalive sender
  /// 当应用层最后一个 `Raft` handle drop 后，此 sender 仍保持通道打开，
  /// 使 core 仅通过 `rx_shutdown` 关闭，避免截断正在进行的 `stream_append`
  #[expect(dead_code)]
  pub(crate) tx_api: MpscSenderOf<C, RaftMsg<C>>,

  /// Receiver of the dedicated channel that delivers a full snapshot to install
  ///
  /// The snapshot data type is defined by the state machine, thus it does not go through
  /// `rx_api`, which is independent of the state machine type
  /// 用于接收待安装完整快照的专用通道接收端
  ///
  /// 快照数据类型由状态机定义，因此不经过与状态机类型解耦的 `rx_api` 通道
  pub(crate) rx_install_snapshot: MpscReceiverOf<C, InstallFullSnapshotRequest<C, SM>>,

  /// Keepalive sender to keep `rx_install_snapshot` channel open
  /// 保持 `rx_install_snapshot` 通道活跃的 keepalive sender
  #[expect(dead_code)]
  pub(crate) tx_install_snapshot: MpscSenderOf<C, InstallFullSnapshotRequest<C, SM>>,

  /// A Sender to send callback by other components to [`RaftCore`], when an action is finished,
  /// such as flushing log to disk, or applying log entries to state machine.
  pub(crate) tx_notification: MpscSenderOf<C, Notification<C>>,

  /// A Receiver to receive callback from other components.
  pub(crate) rx_notification: MpscReceiverOf<C, Notification<C>>,

  /// The watch channels that broadcast local IO progress to storage callbacks and replication.
  pub(crate) io_broadcast: IoBroadcast<C>,

  /// The watch channels that publish this node's state for observers.
  pub(crate) metrics: MetricsChannels<C>,

  /// Runtime statistics for Raft operations.
  ///
  /// Owned directly by RaftCore for lock-free access to most stats.
  /// Only `replicate_batch` is shared with replication tasks via `shared_replicate_batch`.

  /// Shared histogram for replication batch sizes.
  ///
  /// This is the only stats field that needs to be shared with replication tasks.
  /// All other stats are updated only by RaftCore.

  /// External metrics recorder for exporting metrics to custom backends.
  ///
  /// Defaults to `None`. Applications can install a custom recorder
  /// via [`Raft::set_metrics_recorder`] to collect metrics.
  ///
  /// [`Raft::set_metrics_recorder`]: crate::Raft::set_metrics_recorder
  pub(crate) metrics_recorder: Option<Arc<dyn MetricsRecorder>>,
}

/// Selects whether [`RaftCore::spawn_parallel_vote_requests`] sends real Vote or Pre-Vote RPCs.
#[derive(Clone, Copy)]
enum VoteRequestKind {
  Vote,
  PreVote,
}

impl VoteRequestKind {
  /// Lowercase label used in log messages: `"vote"` or `"pre-vote"`.
  fn as_str(self) -> &'static str {
    match self {
      VoteRequestKind::Vote => "vote",
      VoteRequestKind::PreVote => "pre-vote",
    }
  }
}

impl<C, NF, LS, SM> RaftCore<C, NF, LS, SM>
where
  C: RaftTypeConfig,
  NF: RaftNetworkFactory<C>,
  NF::Network: NetSnapshot<C, SnapshotData = SM::SnapshotData>,
  LS: RaftLogStorage<C>,
  SM: RaftStateMachine<C>,
{
  /// The main loop of the Raft protocol.
  pub(crate) async fn main(
    mut self,
    rx_shutdown: OneshotReceiverOf<C, ()>,
  ) -> Result<Infallible, Fatal<C>> {
    let res = self.do_main(rx_shutdown).await;

    // Flush buffered metrics
    self.flush_metrics();

    // Safe unwrap: res is Result<Infallible, _>
    let err = res.unwrap_err();
    match err {
      Fatal::Stopped => { /* Normal quit */ }
      _ => {
        log::error!("RaftCore::main error: {}", err);
      }
    }

    log::debug!("update metrics for shutdown");
    {
      let mut curr = self.metrics.all.borrow_watched().clone();
      curr.state = ServerState::Shutdown;
      curr.running_state = Err(err.clone());

      self.metrics.all.send(curr).ok();
    }

    log::info!("RaftCore shutdown complete");

    Err(err)
  }

  async fn do_main(
    &mut self,
    rx_shutdown: OneshotReceiverOf<C, ()>,
  ) -> Result<Infallible, Fatal<C>> {
    log::debug!("raft node is initializing");

    self.engine.startup();
    // It may not finish running all the commands, if there is a command waiting for a callback.
    self.run_engine_commands().await?;

    // Initialize metrics.
    self.flush_metrics();

    self.runtime_loop(rx_shutdown).await
  }

  fn ensure_preconditions_satisfied(
    &self,
    preconditions: BatchOf<C, Precondition<C>>,
  ) -> Result<(), PreconditionFailed<C>> {
    for c in preconditions {
      c.ensure_satisfied(&self.engine.state)?;
    }

    Ok(())
  }

  /// Handle `is_leader` requests.
  ///
  /// Send heartbeat to all voters. We respond once we have
  /// a quorum of agreement.
  ///
  /// Why:
  /// To ensure linearizability, a read request proposed at time `T1` confirms this node's
  /// leadership to guarantee that all the committed entries proposed before `T1` are present in
  /// this node.
  ///
  /// Both the fast path and the queue require `now < acked + age`; an acknowledgement at the
  /// exact threshold is not fresh enough.
  ///
  /// Broadcasting a heartbeat per read does not amplify RPCs: heartbeat events reach each target
  /// through a watch channel, so a burst coalesces to the latest event, and a heartbeat sent
  /// later also satisfies the thresholds of the reads queued before it.
  pub(super) fn handle_get_linearizer(
    &mut self,
    linearizer_option: LinearizerOption,
    tx: ClientReadTx<C>,
  ) {
    // Setup sentinel values to track when we've received majority confirmation of leadership.

    let now = C::now();
    let leader_lease = self.engine.config.timer_config.leader_lease;
    let max_quorum_ack_age = linearizer_option.effective_max_quorum_ack_age(leader_lease);
    let applied = self.engine.state.io_applied().cloned();
    let id = self.id.clone();

    let mut lh = match self.ensure_leader_handler() {
      Ok(leading_handler) => leading_handler,
      Err(forward) => {
        tx.send(Err(forward.into()));
        return;
      }
    };

    let read_log_id = lh.get_read_log_id();

    let linearizer = Linearizer::new(id, read_log_id, applied);

    if lh.leader.is_self_quorum() {
      tx.send(Ok(linearizer));
      return;
    }

    let last_quorum_acked_at = lh.leader.last_quorum_acked_time();

    // This comparison must remain strict. A zero `max_quorum_ack_age` selects ReadIndex,
    // which requires a fresh heartbeat round to be acknowledged by a quorum. It must not reuse
    // a recorded acknowledgement, even when its timestamp equals `now`; using `<=` would take
    // this fast path and skip the new quorum confirmation.
    if last_quorum_acked_at
      .is_some_and(|last_quorum_acked_at| now < last_quorum_acked_at + max_quorum_ack_age)
    {
      tx.send(Ok(linearizer));
      return;
    }

    let min_quorum_acked_at = now - max_quorum_ack_age;
    let wait_timeout = linearizer_option.effective_wait_timeout(leader_lease);

    // A read that will not wait cannot benefit from a newer quorum acknowledgement.
    if wait_timeout.is_zero() {
      let quorum_not_enough = lh.leader.clock_quorum_not_enough(min_quorum_acked_at);
      let err = LinearizableReadError::QuorumNotEnough(quorum_not_enough);
      tx.send(Err(err));
      return;
    }

    if linearizer_option.heartbeat_if_quorum_ack_stale {
      lh.send_heartbeat(true);
    }

    let deadline = now + wait_timeout;
    let pending_read = PendingRead::new(deadline, linearizer, tx);
    self.pending_reads.push(min_quorum_acked_at, pending_read);
    self.reschedule_pending_read_check();
  }

  /// Submit change-membership by writing a Membership log entry.
  ///
  /// If `retain` is `true`, removed `voter` will becomes `learner`. Otherwise they will
  /// be just removed.
  ///
  /// Changing membership includes changing voters config or adding/removing learners:
  ///
  /// - To change voters config, it will build a new **joint** config. If it already a joint
  ///   config, it returns the final uniform config.
  /// - Adding a learner does not affect election, thus it does not need to enter joint consensus.
  ///   But it still has to wait for the previous membership to commit. Otherwise a second
  ///   proposed membership implies the previous one is committed.
  // ---
  // TODO: This limit can be removed if membership_state is replaced by a list of membership logs.
  //       Because allowing this requires the engine to be able to store more than 2
  //       membership logs. And it does not need to wait for the previous membership log to commit
  //       to propose the new membership log.
  pub(super) fn change_membership(
    &mut self,
    changes: ChangeMembers<C::NodeId, C::Node>,
    retain: bool,
    preconditions: BatchOf<C, Precondition<C>>,
    payloads: MembershipPayloads<C>,
    tx: ProgressResponder<C, ClientWriteResult<C>>,
  ) {
    if let Err(e) = self.ensure_leader_handler() {
      tx.on_complete(Err(ClientWriteError::ForwardToLeader(e)));
      return;
    }

    if let Err(e) = self.ensure_preconditions_satisfied(preconditions) {
      tx.on_complete(Err(e.into()));
      return;
    }

    let res = apply_membership_to_payload::<C>(
      &self.engine.state.membership_state,
      changes,
      retain,
      payloads,
    );
    let payload = match res {
      Ok(x) => x,
      Err(e) => {
        tx.on_complete(Err(ClientWriteError::ChangeMembershipError(e)));
        return;
      }
    };

    self.write_entries(
      Batch::of([payload]),
      Batch::of([Some(CoreResponder::Progress(tx))]),
    );
  }

  /// Append a caller-built membership as one log entry, with no intermediate joint membership.
  ///
  /// `payload` already carries the proposed membership, bound by
  /// [`RaftPayload::with_membership()`]. That payload is the only source of truth here: the
  /// membership this method validates is read back out of it, and the entry written to the log
  /// is that same payload.
  ///
  /// [`RaftPayload::with_membership()`]: crate::entry::RaftPayload::with_membership
  pub(super) fn append_membership(
    &mut self,
    payload: C::Payload,
    preconditions: BatchOf<C, Precondition<C>>,
    tx: ProgressResponder<C, ClientWriteResult<C>>,
  ) {
    let lh = match self.ensure_writable_leader_handler() {
      Ok(lh) => lh,
      Err(forward) => {
        tx.on_complete(Err(ClientWriteError::ForwardToLeader(forward)));
        return;
      }
    };

    // Read the barrier while the leader handler is still borrowed, but report it only after
    // the preconditions: every `Precondition` is checked before anything else is validated,
    // so a caller holding a stale membership log id learns that instead of being told to
    // retry.
    let leader_log_committed = Self::ensure_leader_log_committed(lh.state, lh.leader);

    if let Err(e) = self.ensure_preconditions_satisfied(preconditions) {
      tx.on_complete(Err(e.into()));
      return;
    }

    if let Err(e) = leader_log_committed {
      tx.on_complete(Err(ClientWriteError::ChangeMembershipError(e.into())));
      return;
    }

    // Safe unwrap(): the caller bound `membership` into this payload with
    // `RaftPayload::with_membership()`, whose contract is that `get_membership()` returns it.
    let membership = payload.get_membership().unwrap();

    let membership_state = &self.engine.state.membership_state;
    if let Err(e) = membership_state.validate_append_membership(&membership) {
      tx.on_complete(Err(ClientWriteError::ChangeMembershipError(e)));
      return;
    }

    self.write_entries(
      Batch::of([payload]),
      Batch::of([Some(CoreResponder::Progress(tx))]),
    );
  }

  /// Ensure the leader has committed a log entry proposed in its own term.
  ///
  /// A direct membership append needs this barrier on top of a valid leader lease. Without it,
  /// two configurations proposed in different terms from the same committed parent can both
  /// become committed, even when their quorums do not intersect. See [`UncommittedLeaderLog`]
  /// for a run that loses a committed membership this way.
  ///
  /// A valid lease does not imply the barrier. The lease reads the quorum acknowledgement clock,
  /// which the heartbeat worker advances even when a follower reports a log conflict, so the
  /// lease proves a quorum still sees this leader but not that a quorum stored the blank log.
  ///
  /// [`Raft::change_membership()`] deliberately does not take this barrier: its two proposals
  /// share an exact constituent voter set, so every pair of quorums intersects through the two
  /// strict majorities of that set.
  ///
  /// [`Raft::change_membership()`]: crate::raft::Raft::change_membership
  fn ensure_leader_log_committed(
    state: &RaftState<C>,
    leader: &Leader<C, LeaderQuorumSet<C>>,
  ) -> Result<(), UncommittedLeaderLog<CommittedLeaderIdOf<C>>> {
    let noop_log_id = leader.noop_log_id();
    let cluster_committed = state.cluster_committed();

    if cluster_committed >= Some(noop_log_id) {
      return Ok(());
    }

    Err(UncommittedLeaderLog {
      committed: cluster_committed.cloned(),
      leader_log_id: noop_log_id.clone(),
    })
  }

  /// Ensure this node is the leader and is not transferring leadership.
  ///
  /// Returns `Err(ForwardToLeader)` if:
  /// - This node is not the leader
  /// - The leader is transferring leadership to another node
  fn ensure_leader_handler(&mut self) -> Result<LeaderHandler<'_, C, SM>, ForwardToLeader<C>> {
    let lh = self.engine.try_leader_handler()?;

    // If the leader is transferring leadership, forward requests to the new leader.
    if let Some(to) = lh.leader.get_transfer_to() {
      return Err(lh.state.new_forward_to_leader(to.clone()));
    }

    Ok(lh)
  }

  /// Ensure this node has a valid leader lease and may accept new writes.
  fn ensure_writable_leader_handler(
    &mut self,
  ) -> Result<LeaderHandler<'_, C, SM>, ForwardToLeader<C>> {
    let lh = self.ensure_leader_handler()?;

    if !lh.is_lease_valid() {
      return Err(ForwardToLeader::empty());
    }

    Ok(lh)
  }

  /// Write log entries to the cluster through raft protocol.
  ///
  /// I.e.: append the log entries to local store, forward them to a quorum(including the
  /// leader), waiting for them to be committed and applied.
  ///
  /// Returns the log IDs assigned to the entries, or `None` if the entries could not be
  /// written (e.g., this node is not the leader).
  ///
  /// The result of applying each entry to state machine is sent to its corresponding responder,
  /// if provided. The calling side may not receive a result if raft is shut down.
  ///
  /// The responder is either Responder type of [`RaftTypeConfig::Responder`]
  /// (application-defined) or [`ProgressResponder`] (general-purpose); the former is for
  /// application-defined entries like user data, the latter is for membership configuration
  /// changes.
  pub(crate) fn write_entries(
    &mut self,
    payloads: BatchOf<C, PayloadOf<C>>,
    responders: BatchOf<C, Option<CoreResponder<C>>>,
  ) -> Option<LeaderLogIds<CommittedLeaderIdOf<C>>> {
    debug_assert_eq!(
      payloads.len(),
      responders.len(),
      "payloads and responders must have same length"
    );

    log::debug!("write {} entries", payloads.len());

    let mut lh = match self.ensure_writable_leader_handler() {
      Ok(lh) => lh,
      Err(forward_err) => {
        let err = ClientWriteError::ForwardToLeader(forward_err);
        for tx in responders.into_iter().flatten() {
          tx.on_complete(Err(err.clone()))
        }
        return None;
      }
    };

    let entry_count = payloads.len() as u64;
    let log_ids = lh.leader_append_entries(payloads)?;

    // Record write batch size to external metrics recorder
    if let Some(r) = &self.metrics_recorder {
      r.record_write_batch(entry_count);
    }

    for (log_id, resp_tx) in log_ids.clone().into_iter().zip(responders) {
      if let Some(tx) = resp_tx {
        let index = log_id.index();
        log::debug!("write entries: push tx to responders, log_id: {}", log_id);
        self.client_responders.push(index, tx);
      }
    }

    Some(log_ids)
  }

  /// Send a heartbeat message to every follower/learners.
  pub(crate) fn send_heartbeat(&mut self, emitter: impl fmt::Display) -> bool {
    log::debug!("send heartbeat, now: {}", C::now().display());

    let Some(mut lh) = self.engine.try_leader_handler().ok() else {
      log::debug!(
        "{} failed to send heartbeat, not a Leader: now: {}",
        emitter,
        C::now().display()
      );
      return false;
    };

    if lh.leader.get_transfer_to().is_some() {
      log::debug!(
        "{} is transferring leadership, skip sending heartbeat: now: {}",
        emitter,
        C::now().display()
      );
      return false;
    }

    lh.send_heartbeat(false);

    // Record heartbeat to external metrics recorder
    if let Some(r) = &self.metrics_recorder {
      r.increment_heartbeat();
    }

    log::debug!("{} triggered sending heartbeat", emitter);
    true
  }

  pub fn flush_metrics(&mut self) {
    let io_state = self.engine.state.io_state();
    self
      .metrics
      .progress
      .send_log_progress(io_state.log_progress.flushed().cloned());
    self
      .metrics
      .progress
      .send_commit_progress(io_state.apply_progress.accepted().cloned());
    self
      .metrics
      .progress
      .send_apply_progress(io_state.apply_progress.flushed().cloned());
    self
      .metrics
      .progress
      .send_snapshot_progress(io_state.snapshot.flushed().cloned());

    let (replication, heartbeat) = if let Some(leader) = self.engine.leader.as_ref() {
      let replication_prog = &leader.progress;
      let replication = Some(replication_prog.collect_mapped(|item| item.id_progress_owned()));

      let clock_prog = &leader.clock_progress;
      let heartbeat =
        Some(clock_prog.collect_mapped(|item| (item.id.clone(), item.val.map(SerdeInstant::new))));

      (replication, heartbeat)
    } else {
      (None, None)
    };

    self.report_metrics(replication, heartbeat);
  }

  /// Report a metrics payload on the current state of the Raft node.
  pub(crate) fn report_metrics(
    &mut self,
    replication: Option<ReplicationMetrics<C>>,
    heartbeat: Option<HeartbeatMetrics<C>>,
  ) {
    let last_quorum_acked = self.last_quorum_acked_time();
    let st = &self.engine.state;

    // Get the last flushed vote, or use initial vote (term=0, node_id=self.id)
    // if no IO has been flushed yet (e.g., during startup).
    let vote = st
      .log_progress()
      .flushed()
      .map(|io_id| io_id.to_app_vote())
      .unwrap_or_else(|| VoteOf::<C>::new_with_default_term(self.id.clone()));

    let data_metrics = RaftDataMetrics {
      last_log: st.last_log_id().cloned(),
      local_committed: st.local_committed().cloned(),
      cluster_committed: st.cluster_committed().cloned(),
      last_applied: st.io_applied().cloned(),
      snapshot: st.io_snapshot_last_log_id().cloned(),
      purged: st.io_purged().cloned(),

      last_quorum_acked: last_quorum_acked.map(SerdeInstant::new),
      replication,
      heartbeat,
    };

    let server_metrics = RaftServerMetrics::<C> {
      id: self.id.clone(),
      vote,
      state: st.server_state,
      current_leader: self.current_leader(),
      membership_config: st.membership_state.effective().clone(),
      committed_membership_config: st.membership_state.committed().clone(),
    };

    // `RaftMetrics` is the union of the two above, plus the running state and the term.
    // It is assembled from them rather than re-read from the state, so that every field has
    // exactly one place where it is derived.
    let m = RaftMetrics {
      running_state: Ok(()),
      id: server_metrics.id.clone(),

      // --- data ---
      current_term: st.vote_ref().term(),
      vote: server_metrics.vote.clone(),
      last_log_index: data_metrics.last_log.index(),
      local_committed: data_metrics.local_committed.clone(),
      cluster_committed: data_metrics.cluster_committed.clone(),
      last_applied: data_metrics.last_applied.clone(),
      snapshot: data_metrics.snapshot.clone(),
      purged: data_metrics.purged.clone(),

      // --- cluster ---
      state: server_metrics.state,
      current_leader: server_metrics.current_leader.clone(),
      last_quorum_acked: data_metrics.last_quorum_acked,
      membership_config: server_metrics.membership_config.clone(),
      committed_membership_config: server_metrics.committed_membership_config.clone(),
      heartbeat: data_metrics.heartbeat.clone(),

      // --- replication ---
      replication: data_metrics.replication.clone(),
    };

    // Record to external metrics recorder
    if let Some(r) = &self.metrics_recorder {
      forward_metrics(&m, r.as_ref());
    }

    // Start to send metrics
    // `RaftMetrics` is sent last, because `Wait` only examines `RaftMetrics`
    // but not `RaftDataMetrics` and `RaftServerMetrics`.
    // Thus if `RaftMetrics` change is perceived, the other two should have been updated.

    self.metrics.data.send_if_modified(|metrix| {
      if data_metrics.ne(metrix) {
        *metrix = data_metrics.clone();
        return true;
      }
      false
    });

    self.metrics.server.send_if_modified(|metrix| {
      if server_metrics.ne(metrix) {
        *metrix = server_metrics.clone();
        return true;
      }
      false
    });

    log::debug!("report metrics: {}", m);
    let res = self.metrics.all.send(m);

    if let Err(err) = res {
      log::error!("failed to report metrics, error: {}, id: {}", err, self.id);
    }
  }

  /// Handle the admin command `initialize`.
  ///
  /// It is allowed to initialize only when `last_log_id.is_none()` and `vote==(0,0)`.
  /// See: [Conditions for initialization][precondition]
  ///
  /// [precondition]: crate::docs::cluster_control::cluster_formation#preconditions-for-initialization
  pub(crate) fn handle_initialize(
    &mut self,
    member_nodes: BTreeMap<C::NodeId, C::Node>,
    tx: ResultSender<C, (), InitializeError<C>>,
  ) {
    log::debug!("{}: member_nodes: {:?}", func_name!(), member_nodes);

    let membership = Membership::from(member_nodes);

    let res = self.engine.initialize(membership);

    let has_error = res.is_err();

    // If there is an error, respond at once.
    // Otherwise, wait for the initialization log to be applied to state machine.
    let condition = if has_error {
      None
    } else {
      // Wait for the initialization log to be flushed, not applied.
      //
      // Because committing a log entry requires a leader, the first leader may or may not be able to
      // established, for example, there are already other nodes in the cluster with more logs.
      //
      // Thus, initialization should never wait for to apply of the initialization log.
      //
      // When adding new learners after initialization, we should not wait for the log to be applied.
      // But this introduces an issue, if the client send a change membership at once after
      // initialization, it may receive a InProgress error:
      // `"InProgress": { "committed": null, "membership_log_id": { "leader_id": { "term": 0,
      //             "node_id": 0 }, "index": 0 } }`
      // TODO: change-membership should check leadership or wait for leader to establish?

      // Wait for the generated IO to be flushed before respond.
      let accepted = self
        .engine
        .state
        .io_state()
        .log_progress
        .accepted()
        .cloned();
      accepted.map(|io_id| Condition::IOFlushed { io_id })
    };
    self.engine.output.push_command(Command::Respond {
      when: condition,
      resp: Respond::new(res, tx),
    });

    if !has_error {
      // With the new config, start to elect to become leader
      self.engine.elect();
    }
  }

  /// Trigger a snapshot building(log compaction) job if there is no pending building job.
  ///
  /// Returns `true` if a build was queued, `false` if one is already in progress and the request
  /// was dropped.
  pub(crate) fn trigger_snapshot(&mut self) -> bool {
    log::debug!("{}", func_name!());
    self.engine.snapshot_handler().trigger_snapshot()
  }

  /// Trigger routine actions that need to be checked after processing messages.
  ///
  /// This is called in the main event loop after processing messages and running engine commands.
  /// It performs routine checks and triggers corresponding actions:
  /// - Snapshot building based on `SnapshotPolicy`
  /// - Initiate replication if the replication stream is idle (for leader)
  ///
  /// Unlike tick-based triggers, this runs after every message batch, making it independent of
  /// the tick configuration and more responsive to state changes.
  pub(crate) fn trigger_routine_actions(&mut self) {
    // Check snapshot policy and trigger snapshot if needed
    if let Some(at) = self.config.snapshot_policy.should_snapshot(
      &self.engine.state,
      self.core_state.snapshot_tried_at.as_ref(),
    ) {
      log::debug!("snapshot policy triggered at: {}", at);
      // Only record the attempt if a build was actually queued. A trigger dropped because a
      // build is already in flight must not advance `snapshot_tried_at`; otherwise the phantom
      // attempt suppresses `should_snapshot` and the snapshot never re-arms once the in-flight
      // build completes. See https://github.com/databendlabs/openraft/issues/1829
      if self.trigger_snapshot() {
        self.core_state.snapshot_tried_at = Some(at);
      }
    }

    // Keep replicating to a target if the replication stream to it is idle
    if let Ok(mut lh) = self.engine.try_leader_handler() {
      lh.replication_handler().initiate_replication();
    }

    // Broadcast I/O progress so replication tasks can read submitted logs.
    if let Some(submitted) = self.engine.state.log_progress().submitted().cloned() {
      self.io_broadcast.submitted.send_if_greater(submitted);
    }

    self.process_pending_reads();
  }

  fn process_pending_reads(&mut self) {
    let now = C::now();
    let applied = self.engine.state.io_applied().cloned();

    let lh_res = self.engine.try_leader_handler();
    match lh_res {
      Ok(lh) => {
        // Quorum satisfaction takes precedence over a delayed timeout wake-up.
        let quorum_acked_at = lh.leader.last_quorum_acked_time();
        if let Some(quorum_acked_at) = quorum_acked_at {
          self.pending_reads.drain_satisfied(quorum_acked_at, applied);
        }

        let leader = &*lh.leader;
        let make_error = |min_quorum_acked_at| {
          let quorum_not_enough = leader.clock_quorum_not_enough(min_quorum_acked_at);
          LinearizableReadError::QuorumNotEnough(quorum_not_enough)
        };

        self.pending_reads.drain_expired(now, make_error);
      }
      Err(forward) => {
        let err = LinearizableReadError::ForwardToLeader(forward);
        self.pending_reads.drain_all_with_error(err);
      }
    }

    self.reschedule_pending_read_check();
  }

  fn fail_pending_reads(&mut self) {
    let forward = self.engine.state.forward_to_leader();
    let err = LinearizableReadError::ForwardToLeader(forward);
    self.pending_reads.drain_all_with_error(err);
    self.reschedule_pending_read_check();
  }

  fn reschedule_pending_read_check(&self) {
    let deadline = self.pending_reads.earliest_deadline();
    self.pending_read_deadline_notifier.set_deadline(deadline);
  }

  /// Return the current leader node ID based on the committed vote.
  ///
  /// In OpenRaft, a leader does not have to be a voter — it can be a learner
  /// or even a node outside the membership. Leadership is determined solely by
  /// a committed vote (i.e., a vote granted by a quorum), following Paxos
  /// semantics. Therefore, this method does not check voter or membership
  /// status.
  ///
  /// Currently, this situation arises when a membership change removes the
  /// leader from the voter set (or from the membership entirely). The leader
  /// continues to operate and commit logs until it steps down or a new leader
  /// is elected. In the future, OpenRaft will also allow a node that was never
  /// in the membership to become a leader.
  pub(crate) fn current_leader(&self) -> Option<C::NodeId> {
    log::debug!(
      "get current_leader: self_id: {}, vote: {}",
      self.id,
      self.engine.state.vote_ref()
    );

    let vote = self.engine.state.vote_ref();

    if !vote.is_committed() {
      return None;
    }

    Some(vote.to_leader_id().node_id().clone())
  }

  /// Retrieves the most recent timestamp that is acknowledged by a quorum.
  ///
  /// This function returns the latest known time at which the leader received acknowledgment
  /// from a quorum of followers, indicating its leadership is current and recognized.
  /// If the node is not a leader or no acknowledgment has been received, `None` is returned.
  fn last_quorum_acked_time(&self) -> Option<InstantOf<C>> {
    let leading = self.engine.leader.as_ref();
    leading.and_then(|l| l.last_quorum_acked_time())
  }

  pub(crate) fn get_leader_node(&self, leader_id: Option<C::NodeId>) -> Option<C::Node> {
    let leader_id = leader_id?;

    self
      .engine
      .state
      .membership_state
      .effective()
      .get_node(&leader_id)
      .cloned()
  }

  /// Apply log entries to the state machine, from the `first`(inclusive) to `last`(inclusive).
  pub(crate) async fn apply_to_state_machine(
    &mut self,
    first: LogIdOf<C>,
    last: LogIdOf<C>,
  ) -> Result<(), StorageError<C>> {
    log::debug!("{}: {}..={}", func_name!(), first, last);

    debug_assert!(
      first.index() <= last.index(),
      "first.index {} should <= last.index {}",
      first.index(),
      last.index()
    );

    #[cfg(debug_assertions)]
    if let Some(first_idx) = self.client_responders.first_index() {
      debug_assert!(
        first.index() <= first_idx,
        "first.index {} should <= client_resp_channels.first index {}",
        first.index(),
        first_idx,
      );
    }

    // Drain responders up to last.index
    let mut responders = self.client_responders.drain_upto(last.index());

    let entry_count = last.index() + 1 - first.index();

    // Record to external metrics recorder
    if let Some(r) = &self.metrics_recorder {
      r.record_apply_batch(entry_count);
    }

    // Call on_commit on each responder
    for (index, responder) in responders.iter_mut() {
      let log_id = self.engine.state.get_log_id(*index).unwrap();
      responder.on_commit(log_id);
    }

    let cmd = sm::Command::apply(first, last.clone(), responders);
    self
      .sm_handle
      .send(cmd)
      .await
      .map_err(|e| StorageError::apply(last, C::err_from_string(e)))?;

    Ok(())
  }

  /// Spawn a new replication stream returning its replication state handle.
  pub(crate) async fn spawn_replication_stream(
    &mut self,
    leader_vote: CommittedVoteOf<C>,
    prog: &TargetProgress<C>,
  ) -> ReplicationHandle<C> {
    let network = {
      let mut factory = self.network_factory.lock().await;
      factory
        .new_client(prog.target.clone(), &prog.target_node)
        .await
    };

    let (replicate_tx, replicate_rx) = C::watch_channel(Replicate::default());

    let event_watcher = self.new_event_watcher(replicate_rx);

    let (mut replication_handle, replication_context) =
      self.new_replication(leader_vote, prog, replicate_tx);

    let progress = replication_progress::ReplicationProgress {
      local_committed: self.engine.state.local_committed().cloned(),
      remote_matched: prog.progress.matching.clone(),
    };

    let join_handle = ReplicationCore::<C, NF, LS>::spawn(
      replication_context,
      progress,
      network,
      self.log_store.get_log_reader().await,
      event_watcher,
    );

    replication_handle.join_handle = Some(join_handle);

    replication_handle
  }

  fn new_replication(
    &self,
    leader_vote: CommittedVoteOf<C>,
    prog: &TargetProgress<C>,
    replicate_tx: WatchSenderOf<C, Replicate<C>>,
  ) -> (ReplicationHandle<C>, ReplicationContext<C>) {
    let (cancel_tx, cancel_rx) = C::watch_channel(());

    let context = self.new_replication_context(leader_vote, prog, cancel_rx);

    let handle = ReplicationHandle::new(prog.progress.data.stream_id, replicate_tx, cancel_tx);

    (handle, context)
  }

  fn new_replication_context(
    &self,
    leader_vote: CommittedVoteOf<C>,
    prog: &TargetProgress<C>,
    cancel_rx: WatchReceiverOf<C, ()>,
  ) -> ReplicationContext<C> {
    let id = self.id.clone();

    ReplicationContext {
      id,
      target: prog.target.clone(),
      leader_vote,
      stream_id: prog.progress.data.stream_id,
      config: self.config.clone(),
      tx_notify: self.tx_notification.clone(),
      cancel_rx,
    }
  }

  fn new_event_watcher(&self, replicate_rx: WatchReceiverOf<C, Replicate<C>>) -> EventWatcher<C> {
    EventWatcher {
      replicate_rx,
      committed_rx: self.io_broadcast.committed.subscribe(),
      io_accepted_rx: self.io_broadcast.accepted.subscribe(),
      io_submitted_rx: self.io_broadcast.submitted.subscribe(),
    }
  }

  /// Run as many commands as possible.
  ///
  /// If there is a command that waits for a callback, just return and wait for
  /// next RaftMsg.
  pub(crate) async fn run_engine_commands(&mut self) -> Result<(), StorageError<C>> {
    if log::log_enabled!(LogDebug) {
      log::debug!("queued commands: start...");
      for c in self.engine.output.iter_commands() {
        log::debug!("queued commands: {:?}", c);
      }
      log::debug!("queued commands: end...");
    }

    self.send_satisfied_responds();

    loop {
      // Batch commands for better I/O performance (e.g., merge consecutive AppendEntries)
      self.engine.output.sched_commands(&self.config);

      let Some(cmd) = self.engine.output.pop_command() else {
        break;
      };

      let res = self.run_command(cmd).await?;

      let Some(cmd) = res else {
        // cmd executed. Process next
        continue;
      };

      // cmd is returned, means it can not be executed now, postpone it.

      log::debug!(
        "RAFT_stats id={:<2}    cmd: postpone command: {}, pending: {}",
        self.id,
        cmd,
        self.engine.output.len()
      );

      if self.engine.output.postpone_command(cmd).is_ok() {
        continue;
      }

      // cmd is put back to the front of the queue. quit the loop

      if log::log_enabled!(LogDebug) {
        for c in self.engine.output.iter_commands().take(8) {
          log::debug!("postponed, first 8 queued commands: {:?}", c);
        }
      }

      // A command must be postponed, but progress driven command may be ready to run.
      // Thus, we do not return, but find progress driven commands to run.
      break;
    }

    // Progress driven commands run at last because some command may generate progress changes.
    self.run_progress_driven_command().await?;

    Ok(())
  }

  /// Run all commands that are automatically generated by progress changes.
  async fn run_progress_driven_command(&mut self) -> Result<(), StorageError<C>> {
    while let Some(cmd) = self.engine.next_progress_driven_command() {
      log::debug!(
        "RAFT_event id={:<2}    progress_driven cmd: {}",
        self.id,
        cmd
      );

      // IO progress generated command is always ready to run. no need to postpone.
      let res: Option<Command<C, SM>> = self.run_command(cmd).await?;
      debug_assert!(
        res.is_none(),
        "progress driven command should always be executed"
      );
    }

    Ok(())
  }

  /// Send responds whose waiting conditions are satisfied.
  ///
  /// Responds are queued when their waiting conditions (log flushed, applied, snapshot built)
  /// are not yet met. This method drains all responds whose conditions are now satisfied.
  pub(crate) fn send_satisfied_responds(&mut self) {
    let io_state = self.engine.state.io_state();

    log::debug!(
      "RAFT_stats id={:<2}    cmd: try send satisfied responds: log_io: {}, apply: {}, snapshot: {}",
      self.id,
      io_state.log_progress.flushed().display(),
      io_state.apply_progress.flushed().display(),
      io_state.snapshot.flushed().display(),
    );

    for (phase, respond) in self
      .engine
      .output
      .pending_responds
      .drain_satisfied(io_state)
    {
      log::debug!(
        "RAFT_stats id={:<2}    cmd: send respond waiting for {}: {}",
        self.id,
        phase,
        respond
      );
      respond.send();
    }
  }

  /// Run an event handling loop
  ///
  /// It always returns a [`Fatal`] error upon returning.
  async fn runtime_loop(
    &mut self,
    mut rx_shutdown: OneshotReceiverOf<C, ()>,
  ) -> Result<Infallible, Fatal<C>> {
    // Ratio control the ratio of number of RaftMsg to process to number of Notification to process.
    let mut balancer = Balancer::new(10_000);

    loop {
      self.flush_metrics();

      log::debug!(
        "RAFT_stats id={:<2} log_io: {}",
        self.id,
        self.engine.state.log_progress()
      );

      // In each loop, it does not have to check rx_shutdown and flush metrics for every RaftMsg
      // processed.
      // In each loop, the first step is blocking waiting for any message from any channel.
      // Then if there is any message, process as many as possible to maximize throughput.

      // Check shutdown in each loop first so that a message flood in `tx_api` won't block shutting down.
      // `select!` without `biased` provides a random fairness.
      // We want to check shutdown prior to other channels.
      // See: https://docs.rs/tokio/latest/tokio/macro.select.html#fairness
      futures_util::select_biased! {
          _ = (&mut rx_shutdown).fuse() => {
              log::info!("recv from rx_shutdown");
              return Err(Fatal::Stopped);
          }

          notify_res = self.rx_notification.recv().fuse() => {
              match notify_res {
                  Some(notify) => self.handle_notification(notify)?,
                  None => {
                      log::error!("all rx_notify senders are dropped");
                      return Err(Fatal::Stopped);
                  }
              };
          }

          install_res = self.rx_install_snapshot.recv().fuse() => {
              match install_res {
                  Some(req) => self.handle_install_full_snapshot_request(req),
                  None => {
                      log::error!("all rx_install_snapshot senders are dropped");
                      return Err(Fatal::Stopped);
                  }
              };
          }

          msg_res = self.rx_api.ensure_buffered().fuse() => {
              msg_res?;
          }
      };

      self.run_engine_commands().await?;

      // There is a message waking up the loop, process channels one by one.

      let raft_msg_processed = self.process_raft_msg(balancer.raft_msg()).await?;
      let notify_processed = self.process_notification(balancer.notification()).await?;

      // If one of the channel consumed all its budget, re-balance the budget ratio.

      if notify_processed == balancer.notification() {
        log::info!("there may be more Notification to process, increase Notification ratio");
        balancer.increase_notification();
      } else if raft_msg_processed == balancer.raft_msg() {
        log::info!("there may be more RaftMsg to process, increase RaftMsg ratio");
        balancer.increase_raft_msg();
      }

      // Trigger routine actions after processing all messages
      self.trigger_routine_actions();

      self.run_engine_commands().await?;
    }
  }

  /// Process RaftMsg as many as possible.
  ///
  /// It returns the number of processed message.
  /// If the input channel is closed, it returns `Fatal::Stopped`.
  async fn process_raft_msg(&mut self, at_most: u64) -> Result<u64, Fatal<C>> {
    let mut total = 0u64;
    // Accumulate log entries before batch executing commands to reduce run_engine_commands calls
    // 累积一定数量的日志条目后再批量执行命令，减少 run_engine_commands 调用频率
    let run_command_threshold = 64u64;
    let mut last_log_index = 0;

    for _i in 0..at_most {
      let res = self.rx_api.try_recv().await?;
      let Some(msg) = res else {
        break;
      };

      self.handle_api_msg(msg);
      total += 1;

      let index = self.engine.state.last_log_id().next_index();

      if index.saturating_sub(last_log_index) >= run_command_threshold {
        // Batch execute commands after accumulating enough logs
        // 累积了足够多的日志后，批量执行命令
        self.run_engine_commands().await?;

        last_log_index = index;
      }
    }

    // Execute remaining commands after processing all inputs
    // 处理完所有输入后，执行剩余的命令
    self.run_engine_commands().await?;

    if total == at_most {
      log::debug!(
        "at_most({}) reached, there are more queued RaftMsg to process",
        at_most
      );
    }

    Ok(total)
  }

  /// Process Notification as many as possible.
  ///
  /// It returns the number of processed notifications.
  /// If the input channel is closed, it returns `Fatal::Stopped`.
  async fn process_notification(&mut self, at_most: u64) -> Result<u64, Fatal<C>> {
    let mut processed = 0u64;
    // Batch process notifications before executing commands to reduce run_engine_commands calls
    // 批量处理通知后再执行命令，减少 run_engine_commands 调用次数
    let batch_flush_threshold = 16u64;
    let mut since_last_flush = 0u64;

    for _i in 0..at_most {
      let res = self.rx_notification.try_recv();
      let notify = match res {
        Ok(msg) => msg,
        Err(e) => match e {
          TryRecvError::Empty => {
            log::debug!("all Notification are processed, wait for more");
            break;
          }
          TryRecvError::Disconnected => {
            log::error!("rx_notify is disconnected, quit");
            return Err(Fatal::Stopped);
          }
        },
      };

      self.handle_notification(notify)?;
      processed += 1;
      since_last_flush += 1;

      if since_last_flush >= batch_flush_threshold {
        self.run_engine_commands().await?;
        since_last_flush = 0;
      }
    }

    // Execute remaining commands after processing all notifications
    // 处理完所有通知后，执行剩余的命令
    if since_last_flush > 0 {
      self.run_engine_commands().await?;
    }

    if processed == at_most {
      log::debug!(
        "at_most({}) reached, there are more queued Notification to process",
        at_most
      );
    }

    Ok(processed)
  }

  /// Spawn parallel Vote or Pre-Vote requests to all other cluster members, selected by `kind`.
  ///
  /// For a Pre-Vote, only an affirmative `Ok(granted)` response counts toward the quorum: a
  /// transport error — including [`Unreachable`](crate::error::Unreachable) from a genuinely
  /// partitioned peer — is **not** a grant, otherwise a fully isolated node would synthesize a
  /// quorum and inflate its term. A network that has not implemented `pre_vote` returns
  /// `Ok(granted)` from the default impl, keeping Pre-Vote a no-op during a rolling upgrade.
  async fn spawn_parallel_vote_requests(
    &mut self,
    vote_req: &VoteRequest<C>,
    kind: VoteRequestKind,
  ) {
    let vote = vote_req.vote.clone();
    let id = self.id.clone();
    let tx = self.tx_notification.clone();
    let ttl = Duration::from_millis(self.config.election_timeout_min);

    self
      .broadcast_to_voters(ttl, |target, mut client, option| {
        let req = vote_req.clone();
        let vote = vote.clone();
        let id = id.clone();
        let tx = tx.clone();

        async move {
          let tm_res = match kind {
            VoteRequestKind::Vote => C::timeout(ttl, client.vote(req, option)).await,
            VoteRequestKind::PreVote => C::timeout(ttl, client.pre_vote(req, option)).await,
          };
          let res = match tm_res {
            Ok(res) => res,

            Err(_timeout) => {
              let timeout_err = Timeout::<C> {
                action: RPCTypes::Vote,
                id,
                target: target.clone(),
                timeout: ttl,
              };
              log::error!(
                "timeout while requesting {}: {}",
                kind.as_str(),
                timeout_err
              );
              return;
            }
          };

          match res {
            Ok(resp) => {
              let candidate_vote = vote.to_non_committed();
              let notification = match kind {
                VoteRequestKind::Vote => Notification::VoteResponse {
                  target,
                  resp,
                  candidate_vote,
                },
                VoteRequestKind::PreVote => Notification::PreVoteResponse {
                  target,
                  resp,
                  candidate_vote,
                },
              };
              tx.send(notification).await.ok();
            }
            // A transport failure is not a grant: a partitioned peer must not count
            // toward the (Pre-)Vote quorum. A network that has not implemented
            // `pre_vote` returns `Ok(granted)` from the default impl, so Pre-Vote
            // degrades to a no-op rather than relying on this.
            Err(err) => {
              log::error!(
                "while requesting {}, error: {}, target: {}",
                kind.as_str(),
                err,
                target
              )
            }
          }
        }
      })
      .await;
  }

  /// Tell every other voter to accept a new leader.
  async fn broadcast_transfer_leader(&mut self, req: TransferLeaderRequest<C>) {
    let ttl = Duration::from_millis(self.config.election_timeout_min);

    self
      .broadcast_to_voters(ttl, |target, mut client, option| {
        let r = req.clone();

        async move {
          let tm_res = C::timeout(ttl, client.transfer_leader(r, option)).await;
          let res = match tm_res {
            Ok(res) => res,
            Err(timeout) => {
              log::error!(
                "timeout sending transfer_leader: {}, target: {}",
                timeout,
                target
              );
              return;
            }
          };

          match res {
            Err(e) => {
              log::error!("error sending transfer_leader: {}, target: {}", e, target);
            }
            Ok(resp) => {
              log::info!("Done transfer_leader sent to {}, resp: {:?}", target, resp);
            }
          }
        }
      })
      .await;
  }

  /// Send an RPC to every voter except this node, each in its own detached task.
  ///
  /// `make_rpc` is handed a fresh client for one target and the shared [`RPCOption`], and returns
  /// the future that talks to it. Nothing is joined, so each future has to report its own
  /// outcome; `ttl` is the timeout the caller is expected to enforce inside that future.
  async fn broadcast_to_voters<F, Fut>(&mut self, ttl: Duration, make_rpc: F)
  where
    F: Fn(C::NodeId, NF::Network, RPCOption) -> Fut,
    Fut: Future<Output = ()> + OptionalSend + 'static,
  {
    let voter_ids = self.engine.state.membership_state.effective().voter_ids();

    // Collect target nodes and their information for broadcasting
    // 收集需要广播的目标节点及其信息
    let targets: smallvec::SmallVec<[(C::NodeId, C::Node); 8]> = voter_ids
      .filter(|target| *target != self.id)
      .map(|target| {
        // Safe unwrap(): target must be in membership
        let target_node = self
          .engine
          .state
          .membership_state
          .effective()
          .get_node(&target)
          .unwrap()
          .clone();
        (target, target_node)
      })
      .collect();

    // Batch create all network clients under a single lock acquisition to reduce lock contention
    // 一次持锁批量创建所有 network client，减少锁竞争
    let clients: smallvec::SmallVec<[(C::NodeId, NF::Network); 8]> = {
      let mut factory = self.network_factory.lock().await;
      let mut clients = smallvec::SmallVec::with_capacity(targets.len());
      for (target, target_node) in &targets {
        let client = factory.new_client(target.clone(), target_node).await;
        clients.push((target.clone(), client));
      }
      clients
    };

    for (target, client) in clients {
      let fut = make_rpc(target, client, RPCOption::new(ttl));
      drop(C::spawn(fut));
    }
  }

  pub(super) fn handle_vote_request(&mut self, req: VoteRequest<C>, tx: VoteTx<C>) {
    log::info!("{}: req: {}", func_name!(), req);

    let resp = self.engine.handle_vote_req(req);

    // Record vote to external metrics recorder
    if let Some(r) = &self.metrics_recorder {
      r.increment_vote();
    }

    let condition = Some(Condition::IOFlushed {
      io_id: IOId::new(self.engine.state.vote_ref()),
    });
    self.engine.output.push_command(Command::Respond {
      when: condition,
      resp: Respond::new(resp, tx),
    });
  }

  pub(super) fn handle_pre_vote_request(&mut self, req: VoteRequest<C>, tx: VoteTx<C>) {
    log::info!("{}: req: {}", func_name!(), req);

    let resp = self.engine.handle_pre_vote_req(req);

    // A Pre-Vote persists nothing, so there is no vote IO to wait for: respond at once.
    self.engine.output.push_command(Command::Respond {
      when: None,
      resp: Respond::new(resp, tx),
    });
  }

  pub(super) fn handle_append_entries_request(
    &mut self,
    req: AppendEntriesRequest<C>,
    tx: AppendEntriesTx<C>,
  ) {
    log::debug!("{}: req: {}", func_name!(), req);

    let segment = LogSegment::new(req.prev_log_id, req.entries);
    self.engine.handle_append_entries(&req.vote, segment, tx);

    // Record append entries to external metrics recorder
    if let Some(r) = &self.metrics_recorder {
      r.increment_append();
    }

    let committed = LogIOId::new(req.vote.to_committed(), req.leader_commit);
    self.engine.state.update_committed(committed);
  }

  /// Handle a full snapshot received from the dedicated install-snapshot channel.
  pub(crate) fn handle_install_full_snapshot_request(
    &mut self,
    req: InstallFullSnapshotRequest<C, SM>,
  ) {
    log::debug!("RAFT_event id={:<2}  input: {}", self.id, req);

    self
      .engine
      .handle_install_full_snapshot(req.vote, req.snapshot, req.tx);
  }

  pub(crate) fn handle_api_msg(&mut self, msg: RaftMsg<C>) {
    log::debug!("RAFT_event id={:<2}  input: {}", self.id, msg);

    match msg {
      RaftMsg::AppendEntries { rpc, tx } => {
        self.handle_append_entries_request(rpc, tx);
      }
      RaftMsg::RequestVote { rpc, tx } => {
        let now = C::now();
        log::info!(
          "received RaftMsg::RequestVote: {}, now: {}, vote_request: {}",
          func_name!(),
          now.display(),
          rpc
        );

        self.handle_vote_request(rpc, tx);
      }
      RaftMsg::RequestPreVote { rpc, tx } => {
        log::info!("received RaftMsg::RequestPreVote: vote_request: {}", rpc);

        self.handle_pre_vote_request(rpc, tx);
      }
      RaftMsg::GetLinearizer {
        linearizer_option,
        tx,
      } => {
        self.handle_get_linearizer(linearizer_option, tx);
      }
      RaftMsg::ClientWrite {
        payloads,
        responders,
        expected_leader,
        ..
      } => {
        // Check if expected leader matches current leader
        if let Some(expected) = expected_leader {
          let vote = self.engine.state.vote_ref();

          let committed_leader_id = vote.try_to_committed_leader_id();

          if committed_leader_id.as_ref() != Some(&expected) {
            // Leader has changed, return ForwardToLeader error to all responders
            let forward_err = self.engine.state.forward_to_leader();
            for r in responders.into_iter().flatten() {
              let err = ClientWriteError::ForwardToLeader(forward_err.clone());
              r.on_complete(Err(err));
            }
            return;
          }
        }
        self.write_entries(payloads, responders);
      }
      RaftMsg::Initialize { members, tx } => {
        log::info!(
          "received RaftMsg::Initialize: {}, members: {:?}",
          func_name!(),
          members
        );

        self.handle_initialize(members, tx);
      }
      RaftMsg::ChangeMembership {
        changes,
        payloads,
        retain,
        preconditions,
        tx,
      } => {
        log::info!(
          "received RaftMsg::ChangeMembership: {}, members: {:?}, retain: {:?}, preconditions: {}",
          func_name!(),
          changes,
          retain,
          preconditions.as_ref().display()
        );

        self.change_membership(changes, retain, preconditions, payloads, tx);
      }
      RaftMsg::AppendMembership {
        payload,
        preconditions,
        tx,
      } => {
        log::info!(
          "received RaftMsg::AppendMembership: {}, payload: {}, preconditions: {}",
          func_name!(),
          payload,
          preconditions.as_ref().display()
        );

        self.append_membership(payload, preconditions, tx);
      }
      RaftMsg::WithRaftState { req } => {
        req(&self.engine.state);
      }
      RaftMsg::HandleTransferLeader {
        from: current_leader_vote,
        to,
        last_log_id,
      } => {
        if self.engine.state.vote_ref() == &current_leader_vote {
          log::info!("Transfer Leader from: {}, to {}", current_leader_vote, to);

          self.engine.state.vote.disable_lease();
          if self.id == to {
            if last_log_id.as_ref() > self.engine.state.last_log_id() {
              log::info!(
                "ignore transfer Leader: local log is not up to date; expected: {}, local: {}",
                last_log_id.display(),
                self.engine.state.last_log_id().display()
              );
              return;
            }

            self.engine.elect_by_leadership_transfer();
          }
        }
      }
      RaftMsg::ExternalCommand { cmd } => {
        log::info!(
          "{}: received RaftMsg::ExternalCommand, cmd: {:?}",
          func_name!(),
          cmd
        );

        self.handle_external_command(cmd);
      }
    };
  }

  /// Handle an [`ExternalCommand`], a request from the application that bypasses the Raft
  /// protocol, such as triggering an election or a snapshot.
  fn handle_external_command(&mut self, cmd: ExternalCommand<C>) {
    match cmd {
      ExternalCommand::Elect { pre_vote } => {
        if self.engine.leader.is_some() {
          // Leader cannot initiate election: heartbeats refresh followers' leader lease,
          // and unexpired leases will reject vote requests
          // Leader 不能发起竞选：自身心跳会刷新 follower 的 leader lease，
          // 未过期的 lease 会拒绝 vote 请求
          log::info!("ExternalCommand: already a Leader, ignore election trigger");
        } else if self
          .engine
          .state
          .membership_state
          .effective()
          .is_voter(&self.id)
        {
          if pre_vote {
            self.engine.pre_elect();
          } else {
            self.engine.elect();
          }
          log::debug!("ExternalCommand: triggered election, pre_vote: {pre_vote}");
        }
      }
      ExternalCommand::Heartbeat => {
        self.send_heartbeat("ExternalCommand");
      }
      ExternalCommand::Snapshot => {
        self.trigger_snapshot();
      }
      ExternalCommand::PurgeLog { upto } => {
        self.engine.trigger_purge_log(upto);
      }
      ExternalCommand::TriggerTransferLeader { to } => {
        self.engine.trigger_transfer_leader(to);
      }
      ExternalCommand::AllowNextRevert { to, allow, tx } => {
        //
        let res = match self.engine.try_leader_handler() {
          Ok(mut l) => {
            let res = l.replication_handler().allow_next_revert(to, allow);
            res.map_err(AllowNextRevertError::from)
          }
          Err(e) => {
            log::warn!("AllowNextRevert: current node is not a Leader");
            Err(AllowNextRevertError::from(e))
          }
        };
        tx.send(res);
      }
      ExternalCommand::SetMetricsRecorder { recorder } => {
        log::info!("setting metrics recorder");
        self.metrics_recorder = recorder;
      }
      ExternalCommand::RefreshServerState {
        vote,
        membership_log_id,
      } => {
        // The condition to refresh, e.g., the membership config that removes the
        // Leader being committed, is checked by the sender. Refresh only if the
        // vote and the effective membership config log id still match what the
        // sender observed, so that a delayed command can not cause an unexpected
        // server state refresh. A `None` skips the corresponding check.
        let st = &self.engine.state;
        let vote_unchanged = vote.as_ref().is_none_or(|v| st.vote_ref() == v);
        let membership_unchanged = membership_log_id
          .as_ref()
          .is_none_or(|log_id| st.membership_state.effective().log_id().as_ref() == Some(log_id));

        if vote_unchanged && membership_unchanged {
          self.engine.refresh_server_state();
        } else {
          log::info!(
            "RefreshServerState is dropped: expected vote: {}, membership log id: {}; current vote: {}, membership log id: {}",
            vote.display(),
            membership_log_id.display(),
            self.engine.state.vote_ref(),
            self
              .engine
              .state
              .membership_state
              .effective()
              .log_id()
              .display(),
          );
        }
      }
    }
  }

  pub(crate) fn handle_notification(&mut self, notify: Notification<C>) -> Result<(), Fatal<C>> {
    log::debug!("RAFT_event id={:<2} notify: {}", self.id, notify);

    match notify {
      Notification::VoteResponse {
        target,
        resp,
        candidate_vote,
      } => {
        let now = C::now();

        log::info!(
          "received Notification::VoteResponse: {}, now: {}, resp: {}",
          func_name!(),
          now.display(),
          resp
        );

        if self.engine.candidate.is_some() {
          let my_vote = self.engine.candidate_ref().map(|x| x.vote_ref());
          if Self::does_vote_match("Candidate", &candidate_vote, my_vote, "VoteResponse") {
            self.engine.handle_vote_resp(target, resp);
          }
        }
      }

      Notification::PreVoteResponse {
        target,
        resp,
        candidate_vote,
      } => {
        log::info!(
          "received Notification::PreVoteResponse: target: {}, resp: {}",
          target,
          resp
        );

        if self.engine.pre_candidate.is_some() {
          let my_vote = self.engine.pre_candidate_ref().map(|x| x.vote_ref());
          if Self::does_vote_match("Pre-Candidate", &candidate_vote, my_vote, "PreVoteResponse") {
            self.engine.handle_pre_vote_resp(target, resp);
          }
        }
      }

      Notification::HigherVote {
        target,
        higher,
        leader_vote,
      } => {
        log::info!(
          "{}: received Notification::HigherVote, target: {}, higher_vote: {}, sending_vote: {}",
          func_name!(),
          target,
          higher,
          leader_vote
        );

        let my_vote = self.engine.leader.as_ref().map(|x| &x.committed_vote);
        if Self::does_vote_match("Leader", &leader_vote, my_vote, "HigherVote") {
          // Rejected vote change is ok.
          self.engine.vote_handler().update_vote(&higher).ok();
        }
      }

      Notification::Tick { i } => self.handle_tick(i),
      Notification::StorageError { error } => {
        log::error!("RaftCore received Notification::StorageError: {}", error);
        return Err(Fatal::StorageError(error));
      }

      Notification::LocalIO { io_id } => self.handle_local_io(io_id),
      Notification::ReplicationProgress {
        stream_id,
        progress,
        inflight_id,
      } => {
        log::debug!(
          "recv Notification::ReplicationProgress: progress: {}",
          progress
        );

        // Clean up handle after snapshot transfer finishes to avoid memory leaks
        // 快照传输完成后清理 handle，避免内存泄漏
        if inflight_id.is_some()
          && let Some(node) = self.replications.get_mut(&progress.target)
          && node.snapshot_transmit_handle.is_some()
        {
          log::debug!(
            "clearing snapshot_transmit_handle for target: {}",
            progress.target
          );
          node.snapshot_transmit_handle.take();
        }

        if let Some(mut rh) = self.engine.try_replication_handler() {
          rh.update_progress(progress.target, stream_id, progress.result, inflight_id);
        }
      }

      Notification::HeartbeatProgress {
        stream_id,
        sending_time,
        target,
      } => {
        if let Some(mut rh) = self.engine.try_replication_handler() {
          rh.try_update_leader_clock(stream_id, target, sending_time);
        }
      }

      Notification::StateMachine { command_result } => {
        self.handle_state_machine_result(command_result)?
      }
      Notification::PendingReadDeadlineReached => self.process_pending_reads(),
    };
    Ok(())
  }

  /// Handle [`Notification::Tick`]: check the election timer and, if leading, the heartbeat
  /// timer.
  fn handle_tick(&mut self, i: u64) {
    // check every timer

    let now = C::now();
    log::debug!("received tick: {}, now: {}", i, now.display());

    self.handle_tick_election();

    // Leader send heartbeat
    let heartbeat_at = self.engine.leader_ref().map(|l| l.next_heartbeat);
    if let Some(t) = heartbeat_at
      && now >= t
    {
      if self.runtime_config.enable_heartbeat.load(Ordering::Relaxed) {
        self.send_heartbeat("tick");
      }

      // Install next heartbeat
      if let Some(l) = self.engine.leader_mut() {
        l.next_heartbeat = C::now() + Duration::from_millis(self.config.heartbeat_interval);
      }
    }
  }

  /// Handle [`Notification::LocalIO`]: a local log or vote write reached the disk.
  fn handle_local_io(&mut self, io_id: IOId<C>) {
    self
      .engine
      .state
      .log_progress_mut()
      .try_flush(io_id.clone());

    match io_id {
      IOId::Log(log_io_id) => {
        // No need to check membership change: local log will not revert due to membership changes
        // 无需检查成员变更：本地日志不会因成员变更而回退
        if let Some(leader) = self.engine.leader.as_ref()
          && Self::does_vote_match(
            "Leader",
            &log_io_id.committed_vote,
            Some(&leader.committed_vote),
            "LocalIO Notification",
          )
        {
          self
            .engine
            .replication_handler()
            .update_local_progress(log_io_id.log_id);
        }
      }
      IOId::Vote(_) => {}
    }
  }

  /// Handle [`Notification::StateMachine`]: the state machine worker finished a command.
  fn handle_state_machine_result(
    &mut self,
    command_result: sm::CommandResult<C>,
  ) -> Result<(), Fatal<C>> {
    log::debug!("sm::StateMachine command result: {:?}", command_result);

    let res = command_result.result?;

    match res {
      BuildSnapshotDone(meta) => {
        log::info!(
          "sm::StateMachine command done: BuildSnapshotDone: {}: {}",
          meta.display(),
          func_name!()
        );

        self.engine.on_building_snapshot_done(meta);
      }
      InstallSnapshot((log_io_id, meta)) => {
        log::info!(
          "sm::StateMachine command done: InstallSnapshot: {}, log_io_id: {}: {}",
          meta.display(),
          log_io_id,
          func_name!()
        );

        self
          .engine
          .state
          .log_progress_mut()
          .try_flush(IOId::Log(log_io_id));

        if let Some(meta) = meta {
          let st = self.engine.state.io_state_mut();
          if let Some(last) = &meta.last_log_id {
            st.apply_progress.try_flush(last.clone());
            st.snapshot.try_flush(last.clone());
          }
        }
      }
      Apply(res) => {
        self
          .engine
          .state
          .apply_progress_mut()
          .try_flush(res.last_applied);
      }
    }

    Ok(())
  }

  fn handle_tick_election(&mut self) {
    let now = C::now();

    log::debug!("try to trigger election, now: {}", now.display());

    if self.engine.state.server_state == ServerState::Leader {
      log::debug!("skip election, already a leader");
      return;
    }

    if !self
      .engine
      .state
      .membership_state
      .effective()
      .is_voter(&self.id)
    {
      log::debug!("skip election, not a voter");
      return;
    }

    if !self.runtime_config.enable_elect.load(Ordering::Relaxed) {
      log::debug!("skip election, election disabled");
      return;
    }

    let mut election_timeout = self.engine.config.timer_config.election_timeout;
    if self.engine.is_there_greater_log() {
      election_timeout += self.engine.config.timer_config.smaller_log_timeout;
    }

    let voter_count = self
      .engine
      .state
      .membership_state
      .effective()
      .voter_ids()
      .count();

    if voter_count == 1 {
      // When a node restart, it may stay in any state but the in progress election(engine.candidate) is
      // empty.
      if self.engine.candidate_ref().is_some() {
        log::debug!("skip election, single voter already has an active election in progress");
        return;
      }
      log::debug!("single voter, elect immediately");
    } else {
      log::debug!("multiple voters, check election timeout");

      let local_vote = &self.engine.state.vote;
      log::debug!(
        "local vote: {}, election_timeout: {:?}",
        local_vote,
        election_timeout,
      );

      if local_vote.is_expired(now, election_timeout) {
        log::info!("election timeout expired, triggering election");
      } else {
        log::debug!("election timeout not yet expired");
        return;
      }
    }

    // Pre-Vote (multi-voter only): probe a quorum before incrementing the term.
    // A single voter always wins its own Pre-Vote, so it elects directly.
    let pre_vote = self.runtime_config.enable_pre_vote.load(Ordering::Relaxed) && voter_count > 1;

    if pre_vote {
      // A Pre-Vote does not advance `vote.last_update_time`, so without this guard a node
      // would re-issue a Pre-Vote on every tick. Skip while a fresh round is still in flight;
      // restart once it has been pending longer than `election_timeout`.
      if let Some(started) = self.engine.pre_candidate_ref().map(|pc| pc.starting_time())
        && now < started + election_timeout
      {
        log::debug!("skip pre-vote, a pre-vote round is already in flight");
        return;
      }
    }

    // Every time elect, reset this flag.
    self.engine.reset_greater_log();

    if pre_vote {
      log::info!("trigger pre-vote");
      self.engine.pre_elect();
    } else {
      log::info!("trigger election");
      self.engine.elect();
    }
  }

  /// If a message is sent under a vote that this node no longer holds, it is a stale message
  /// and should be just ignored.
  ///
  /// `role` names the role the vote belongs to, for the log message, e.g. `"Candidate"`.
  /// `my_vote` is the vote this node currently holds in that role, `None` if it left the role
  /// (a Candidate that finished voting has no vote).
  ///
  /// The two votes may have different types: the vote a message was sent under is an
  /// `UncommittedVote`/`CommittedVote`, while the vote this node holds may be the
  /// application's `C::Vote`. They are therefore compared by leader id.
  fn does_vote_match<V, W>(
    role: &str,
    sent_vote: &V,
    my_vote: Option<&W>,
    msg: impl fmt::Display,
  ) -> bool
  where
    V: RaftVote,
    W: RaftVote<LeaderId = V::LeaderId>,
  {
    let Some(my_vote) = my_vote else {
      log::warn!(
        "A message will be ignored because this node is no longer {}: \
                 msg sent by vote: {}; when ({})",
        role,
        sent_vote,
        msg
      );
      return false;
    };

    if sent_vote.leader_id() != my_vote.leader_id() {
      log::warn!(
        "A message will be ignored because {} vote changed: \
                msg sent by vote: {}; current my vote: {}; when ({})",
        role,
        sent_vote,
        my_vote,
        msg
      );
      return false;
    }

    true
  }

  /// Broadcast heartbeat to all followers with per-follower matching log ids.
  ///
  /// This method validates the session and sends heartbeat events only if the current
  /// session matches the requested session (no leader change or membership change).
  fn broadcast_heartbeat(
    &mut self,
    session_id: ReplicationSessionId<C>,
    bypass_min_interval: bool,
  ) {
    // Lazy get the progress data for heartbeat. If the leader changes or replication
    // config changes, no need to send heartbeat.
    let Ok(lh) = self.engine.try_leader_handler() else {
      // No longer a leader
      return;
    };

    let committed_vote = lh.leader.committed_vote.clone();
    let membership_log_id = lh.state.membership_state.effective().log_id();
    let current_session_id = ReplicationSessionId::new(committed_vote, membership_log_id.clone());

    if current_session_id != session_id {
      // Session changed, skip heartbeat
      return;
    }

    let cluster_committed = lh.state.cluster_committed().cloned();
    let now = C::now();
    let min_interval = Duration::from_millis(self.config.heartbeat_min_interval());
    let leader = &*lh.leader;
    let events = leader
      .progress
      .iter()
      .filter(|progress_entry| progress_entry.id != self.id)
      .filter(|progress_entry| {
        bypass_min_interval || leader.need_heartbeat(&progress_entry.id, now, min_interval)
      })
      .map(|progress_entry| {
        (
          progress_entry.id.clone(),
          HeartbeatEvent {
            time: now,
            matching: progress_entry.matching.clone(),
            cluster_committed: cluster_committed.clone(),
          },
        )
      });

    self.heartbeat_handle.broadcast(events);
  }

  /// Creates a new replication context and its associated cancellation channel.
  ///
  /// Returns the context for the replication task and the sender half of the
  /// cancellation channel. Dropping the sender signals the task to stop.
  pub(crate) fn new_replication_task_context(
    &self,
    leader_vote: CommittedVoteOf<C>,
    stream_id: StreamId,
    target: C::NodeId,
  ) -> (ReplicationContext<C>, WatchSenderOf<C, ()>) {
    let (cancel_tx, cancel_rx) = C::watch_channel(());
    let ctx = ReplicationContext {
      id: self.id.clone(),
      target,
      leader_vote,
      stream_id,
      config: self.config.clone(),
      tx_notify: self.tx_notification.clone(),
      cancel_rx,
    };
    (ctx, cancel_tx)
  }

  fn close_replication(target: &C::NodeId, mut s: ReplicationHandle<C>) {
    let Some(handle) = s.join_handle.take() else {
      return;
    };

    // Drop sender to notify the task to shutdown
    drop(s.replicate_tx);
    drop(s.cancel_tx);

    let target = target.clone();
    drop(C::spawn(async move {
      log::debug!("joining removed replication: {}", target);
      let _ = handle.await;
      log::info!("done joining removed replication: {}", target);
    }));
  }

  /// Run [`Command::UpdateIOProgress`].
  async fn run_update_io_progress(&mut self, io_id: IOId<C>) {
    // Notify that I/O is about to be submitted.
    self.io_broadcast.accepted.send_if_greater(io_id.clone());

    self.engine.state.log_progress_mut().submit(io_id.clone());

    let notify = Notification::LocalIO {
      io_id: io_id.clone(),
    };

    self.tx_notification.send(notify).await.ok();
  }

  /// Run [`Command::AppendEntries`].
  async fn run_append_entries(
    &mut self,
    committed_vote: CommittedVoteOf<C>,
    entries: BatchOf<C, C::Entry>,
  ) -> Result<(), StorageError<C>> {
    let last_log_id = entries.last().unwrap().log_id();
    log::debug!("AppendEntries: {}", entries.as_ref().display_n(10));

    let entry_count = entries.len() as u64;

    // Record to internal histogram

    // Record to external metrics recorder
    if let Some(r) = &self.metrics_recorder {
      r.record_append_batch(entry_count);
    }

    let io_id = IOId::new_log_io(committed_vote, Some(last_log_id));
    let callback = IOFlushed::new(io_id.clone(), self.io_broadcast.completed.clone());

    // Notify that I/O is about to be submitted.
    self.io_broadcast.accepted.send_if_greater(io_id.clone());

    // Mark this IO request as submitted,
    // other commands relying on it can then be processed.
    // For example,
    // `Replicate` command cannot run until this IO request is submitted(no need to be flushed),
    // because it needs to read the log entry from the log store.
    //
    // The `submit` state must be updated before calling `append()`,
    // because `append()` may call the callback before returning.
    self.engine.state.log_progress_mut().submit(io_id.clone());

    // Submit IO request, do not wait for the response.
    self
      .log_store
      .append(entries, callback)
      .await
      .sto_write_logs()?;

    Ok(())
  }

  /// Run [`Command::SaveVote`].
  async fn run_save_vote(&mut self, vote: VoteOf<C>) -> Result<(), StorageError<C>> {
    let io_id = IOId::new(&vote);

    // Notify that vote I/O is about to be submitted.
    self.io_broadcast.accepted.send_if_greater(io_id.clone());

    self.engine.state.log_progress_mut().submit(io_id.clone());
    self.log_store.save_vote(&vote).await.sto_write_vote()?;

    self
      .tx_notification
      .send(Notification::<C>::LocalIO { io_id })
      .await
      .ok();

    // If a non-committed vote is saved,
    // there may be a candidate waiting for the response.
    if let VoteStatus::Pending(non_committed) = vote.clone().into_vote_status() {
      self
        .tx_notification
        .send(Notification::<C>::VoteResponse {
          target: self.id.clone(),
          // last_log_id is not used when sending VoteRequest to local node
          resp: VoteResponse::new(vote, None, true),
          candidate_vote: non_committed,
        })
        .await
        .ok();
    }

    Ok(())
  }

  /// Run [`Command::PurgeLog`].
  fn fail_responders(
    forward_err: &ForwardToLeader<C>,
    drained: impl IntoIterator<Item = (u64, CoreResponder<C>)>,
    reason: &str,
  ) {
    for (log_index, tx) in drained {
      tx.on_complete(Err(ClientWriteError::ForwardToLeader(forward_err.clone())));
      log::debug!("sent ForwardToLeader for {reason} log_index: {log_index}");
    }
  }

  async fn run_purge_log(&mut self, upto: LogIdOf<C>) -> Result<(), StorageError<C>> {
    self.log_store.purge(upto.clone()).await.sto_write_logs()?;

    // A responder may still be pending for a log covered by this purge, e.g. a former
    // leader's uncommitted log superseded by a snapshot install. That log is gone, so
    // fail the responder with `ForwardToLeader` instead of leaving it stranded below the
    let leader_id = self.current_leader();
    let forward_err = ForwardToLeader {
      leader_node: self.get_leader_node(leader_id.clone()),
      leader_id,
    };
    Self::fail_responders(
      &forward_err,
      self.client_responders.drain_upto(upto.index()),
      "purged",
    );

    self.engine.state.io_state_mut().update_purged(Some(upto));

    Ok(())
  }

  /// Run [`Command::TruncateLog`].
  async fn run_truncate_log(&mut self, after: Option<LogIdOf<C>>) -> Result<(), StorageError<C>> {
    self
      .log_store
      .truncate_after(after.clone())
      .await
      .sto_write_logs()?;

    // Inform clients waiting for logs to be applied.
    let leader_id = self.current_leader();
    let forward_err = ForwardToLeader {
      leader_node: self.get_leader_node(leader_id.clone()),
      leader_id,
    };
    Self::fail_responders(
      &forward_err,
      self.client_responders.drain_from(after.next_index()),
      "truncated",
    );

    Ok(())
  }

  /// Run [`Command::SaveCommittedAndApply`].
  async fn run_save_committed_and_apply(
    &mut self,
    already_applied: Option<LogIdOf<C>>,
    upto: LogIdOf<C>,
  ) -> Result<(), StorageError<C>> {
    self.engine.state.apply_progress_mut().submit(upto.clone());

    self
      .log_store
      .save_committed(Some(upto.clone()))
      .await
      .sto_write()?;

    let first = self
      .engine
      .state
      .get_log_id(already_applied.next_index())
      .unwrap();
    self.apply_to_state_machine(first, upto).await?;

    Ok(())
  }

  /// Run [`Command::ReplicateSnapshot`].
  async fn run_replicate_snapshot(
    &mut self,
    leader_vote: CommittedVoteOf<C>,
    target: C::NodeId,
    inflight_id: InflightId,
  ) {
    let node = self
      .replications
      .get(&target)
      .expect("replication to target node exists");

    let snapshot_reader: SnapshotReader<C, SM> =
      Handle::<C, SM>::new_snapshot_reader(&self.sm_handle);
    let stream_id = node.stream_id;
    let (replication_task_context, cancel_tx) =
      self.new_replication_task_context(leader_vote, stream_id, target.clone());

    let target_node = self
      .engine
      .state
      .membership_state
      .effective()
      .get_node(&target)
      .unwrap();
    let snapshot_network = {
      let mut factory = self.network_factory.lock().await;
      factory
        .new_snapshot_client(target.clone(), target_node)
        .await
    };

    let handle = SnapshotTransmitter::<C, NF, SM>::spawn(
      replication_task_context,
      snapshot_network,
      snapshot_reader,
      inflight_id,
      cancel_tx,
    );

    let node = self
      .replications
      .get_mut(&target)
      .expect("replication to target node exists");
    // TODO: it is not cleaned when snapshot transmission is done.
    node.snapshot_transmit_handle = Some(handle);
  }

  /// Run [`Command::CloseReplicationStreams`].
  fn run_close_replication_streams(&mut self) {
    self.heartbeat_handle.close_workers();

    let left = mem::take(&mut self.replications);
    for (target, s) in left {
      Self::close_replication(&target, s);
    }
  }

  /// Run [`Command::RebuildReplicationStreams`].
  async fn run_rebuild_replication_streams(
    &mut self,
    leader_vote: CommittedVoteOf<C>,
    targets: Vec<TargetProgress<C>>,
    close_old_streams: bool,
  ) {
    {
      let mut factory = self.network_factory.lock().await;
      self
        .heartbeat_handle
        .spawn_workers::<NF>(
          leader_vote.clone(),
          &mut *factory,
          &self.tx_notification,
          &targets,
          close_old_streams,
        )
        .await;
    }

    let mut new_replications = BTreeMap::new();

    for prog in targets.iter() {
      let handle = match self.replications.remove(&prog.target) {
        Some(existing) if !close_old_streams => existing,
        Some(existing) => {
          Self::close_replication(&prog.target, existing);
          self
            .spawn_replication_stream(leader_vote.clone(), prog)
            .await
        }
        None => {
          self
            .spawn_replication_stream(leader_vote.clone(), prog)
            .await
        }
      };

      new_replications.insert(prog.target.clone(), handle);
    }

    log::debug!("removing unused replications");

    let left = mem::replace(&mut self.replications, new_replications);

    for (target, s) in left {
      Self::close_replication(&target, s);
    }
  }

  /// Run [`Command::StateMachine`], forwarding the inner command to the state machine worker.
  async fn run_state_machine(
    &mut self,
    command: sm::Command<C, SM>,
  ) -> Result<(), StorageError<C>> {
    let io_id = command.get_log_progress();

    if let Some(io_id) = io_id {
      self.engine.state.log_progress_mut().submit(io_id);
    }

    // If this command update the last-applied log id, mark it as submitted(to state machine).
    if let Some(log_id) = command.get_apply_progress() {
      self.engine.state.apply_progress_mut().submit(log_id);
    }

    if let Some(log_id) = command.get_snapshot_progress() {
      self.engine.state.snapshot_progress_mut().submit(log_id);
    }

    // Just forward a state machine command to the worker.
    self.sm_handle.send(command).await.map_err(|_e| {
      StorageError::write_state_machine(C::err_from_string("cannot send to sm::Worker"))
    })?;

    Ok(())
  }
}

impl<C, N, LS, SM> RaftRuntime<C, SM> for RaftCore<C, N, LS, SM>
where
  C: RaftTypeConfig,
  N: RaftNetworkFactory<C>,
  N::Network: NetSnapshot<C, SnapshotData = SM::SnapshotData>,
  LS: RaftLogStorage<C>,
  SM: RaftStateMachine<C>,
{
  async fn run_command(
    &mut self,
    cmd: Command<C, SM>,
  ) -> Result<Option<Command<C, SM>>, StorageError<C>> {
    // log::debug!("RAFT_event id={:<2} trycmd: {}", self.id, cmd);

    let condition = cmd.condition();
    log::debug!("condition: {:?}", condition);

    if let Some(condition) = condition {
      if condition.is_met(&self.engine.state.io_state) {
        // continue run the command
      } else {
        log::debug!("{} is not yet met, postpone cmd: {}", condition, cmd);
        return Ok(Some(cmd));
      }
    }

    log::debug!("RAFT_event id={:<2}    cmd: {}", self.id, cmd);

    // Record command execution

    match cmd {
      Command::UpdateIOProgress { io_id, .. } => self.run_update_io_progress(io_id).await,
      Command::AppendEntries {
        committed_vote,
        entries,
      } => self.run_append_entries(committed_vote, entries).await?,
      Command::SaveVote { vote } => self.run_save_vote(vote).await?,
      Command::PurgeLog { upto } => self.run_purge_log(upto).await?,
      Command::TruncateLog { after } => self.run_truncate_log(after).await?,
      Command::SendVote { vote_req } => {
        self
          .spawn_parallel_vote_requests(&vote_req, VoteRequestKind::Vote)
          .await;
      }
      Command::SendPreVote { vote_req } => {
        self
          .spawn_parallel_vote_requests(&vote_req, VoteRequestKind::PreVote)
          .await;
      }
      Command::ReplicateCommitted { committed } => {
        self.io_broadcast.committed.send_if_greater(committed);
      }
      Command::BroadcastHeartbeat {
        session_id,
        bypass_min_interval,
      } => self.broadcast_heartbeat(session_id, bypass_min_interval),
      Command::SaveCommittedAndApply {
        already_applied,
        upto,
      } => {
        self
          .run_save_committed_and_apply(already_applied, upto)
          .await?
      }
      Command::Replicate { req, target } => {
        let node = self
          .replications
          .get(&target)
          .expect("replication to target node exists");
        node.replicate_tx.send(req).ok();
      }
      Command::ReplicateSnapshot {
        leader_vote,
        target,
        inflight_id,
      } => {
        self
          .run_replicate_snapshot(leader_vote, target, inflight_id)
          .await
      }
      Command::BroadcastTransferLeader { req } => self.broadcast_transfer_leader(req).await,
      Command::CloseReplicationStreams => self.run_close_replication_streams(),
      Command::FailPendingReads => self.fail_pending_reads(),
      Command::RebuildReplicationStreams {
        leader_vote,
        targets,
        close_old_streams,
      } => {
        self
          .run_rebuild_replication_streams(leader_vote, targets, close_old_streams)
          .await;
      }
      Command::StateMachine { command } => self.run_state_machine(command).await?,
      Command::Respond { resp, .. } => resp.send(),
    }

    Ok(None)
  }
}

#[cfg(test)]
mod tests {
  use std::{fmt, sync::Arc};

  use maplit::{btreemap, btreeset};

  use super::apply_membership_to_payload;
  use crate::{
    ChangeMembers, Membership,
    core::raft_msg::membership_payloads::MembershipPayloads,
    entry::RaftPayload,
    type_config::{
      TypeConfigExt,
      alias::{MembershipStateOf, StoredMembershipOf},
    },
  };

  #[derive(Debug, PartialEq)]
  struct TestPayload {
    normal: Option<u64>,
    membership: Option<Membership<u64, ()>>,
  }

  impl fmt::Display for TestPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
      write!(
        f,
        "normal={:?}, membership={:?}",
        self.normal, self.membership
      )
    }
  }

  impl RaftPayload for TestPayload {
    type D = u64;
    type NodeId = u64;
    type Node = ();

    fn blank() -> Self {
      Self {
        normal: None,
        membership: None,
      }
    }

    fn with_normal(mut self, data: u64) -> Self {
      self.normal = Some(data);
      self
    }

    fn with_membership(mut self, membership: Membership<u64, ()>) -> Self {
      self.membership = Some(membership);
      self
    }

    fn get_membership(&self) -> Option<Membership<u64, ()>> {
      self.membership.clone()
    }
  }

  crate::declare_raft_types!(
      TestConfig:
          D = u64,
          R = (),
          Node = (),
          Payload = TestPayload,
  );

  #[test]
  fn membership_change_replaces_payload_membership() {
    let current = Membership::new_with_defaults(vec![btreeset! {1}], []);
    let stored = Arc::new(StoredMembershipOf::<TestConfig>::new(None, current));
    let membership_state = MembershipStateOf::<TestConfig>::new(stored.clone(), stored);

    let input_membership = Membership::new_with_defaults(vec![btreeset! {9}], []);
    let payload = TestPayload {
      normal: Some(7),
      membership: Some(input_membership),
    };

    let payloads = MembershipPayloads::<TestConfig>::Uniform(payload);
    let changes = ChangeMembers::AddNodes(btreemap! {2 => ()});
    let result =
      apply_membership_to_payload::<TestConfig>(&membership_state, changes, false, payloads);
    let actual = result.unwrap();

    let expected_membership = Membership::new_with_defaults(vec![btreeset! {1}], btreeset! {2});
    let expected = TestPayload {
      normal: Some(7),
      membership: Some(expected_membership),
    };
    assert_eq!(expected, actual);
  }

  #[test]
  fn membership_change_from_blank_payload_creates_membership_payload() {
    let current = Membership::new_with_defaults(vec![btreeset! {1}], []);
    let stored = Arc::new(StoredMembershipOf::<TestConfig>::new(None, current));
    let membership_state = MembershipStateOf::<TestConfig>::new(stored.clone(), stored);

    let payloads = MembershipPayloads::<TestConfig>::Uniform(TestPayload::blank());
    let changes = ChangeMembers::AddNodes(btreemap! {2 => ()});
    let result =
      apply_membership_to_payload::<TestConfig>(&membership_state, changes, false, payloads);
    let actual = result.unwrap();

    let expected_membership = Membership::new_with_defaults(vec![btreeset! {1}], btreeset! {2});
    let expected = TestPayload {
      normal: None,
      membership: Some(expected_membership),
    };
    assert_eq!(expected, actual);
  }

  /// Replacing the voters computes a joint membership, so the joint payload carries it and the
  /// uniform payload goes back to the caller.
  #[test]
  fn joint_membership_selects_joint_payload() {
    let current = Membership::new_with_defaults(vec![btreeset! {1, 2, 3}], btreeset! {4, 5});
    let stored = Arc::new(StoredMembershipOf::<TestConfig>::new(None, current));
    let membership_state = MembershipStateOf::<TestConfig>::new(stored.clone(), stored);

    let (unused_tx, mut unused_rx) = TestConfig::oneshot();
    let payloads = MembershipPayloads::<TestConfig>::JointOrUniform {
      joint: TestPayload::normal(1),
      uniform: TestPayload::normal(2),
      unused_tx,
    };

    let changes = ChangeMembers::ReplaceAllVoters(btreeset! {3, 4, 5});
    let result =
      apply_membership_to_payload::<TestConfig>(&membership_state, changes, false, payloads);
    let actual = result.unwrap();

    let joint_membership =
      Membership::new_with_defaults(vec![btreeset! {1, 2, 3}, btreeset! {3, 4, 5}], []);
    let expected = TestPayload {
      normal: Some(1),
      membership: Some(joint_membership),
    };
    assert_eq!(expected, actual);

    let returned = unused_rx.try_recv().unwrap();
    assert_eq!(TestPayload::normal(2), returned);
  }

  /// Adding a node moves no voter, so the computed membership is uniform and the uniform
  /// payload carries it, sending the joint payload back to the caller.
  #[test]
  fn uniform_membership_selects_uniform_payload() {
    let current = Membership::new_with_defaults(vec![btreeset! {1}], []);
    let stored = Arc::new(StoredMembershipOf::<TestConfig>::new(None, current));
    let membership_state = MembershipStateOf::<TestConfig>::new(stored.clone(), stored);

    let (unused_tx, mut unused_rx) = TestConfig::oneshot();
    let payloads = MembershipPayloads::<TestConfig>::JointOrUniform {
      joint: TestPayload::normal(1),
      uniform: TestPayload::normal(2),
      unused_tx,
    };

    let changes = ChangeMembers::AddNodes(btreemap! {2 => ()});
    let result =
      apply_membership_to_payload::<TestConfig>(&membership_state, changes, false, payloads);
    let actual = result.unwrap();

    let expected_membership = Membership::new_with_defaults(vec![btreeset! {1}], btreeset! {2});
    let expected = TestPayload {
      normal: Some(2),
      membership: Some(expected_membership),
    };
    assert_eq!(expected, actual);

    let returned = unused_rx.try_recv().unwrap();
    assert_eq!(TestPayload::normal(1), returned);
  }
}
