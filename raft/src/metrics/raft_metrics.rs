use std::fmt;
use std::sync::Arc;

use display_more::DisplayOptionExt;

use crate::RaftTypeConfig;
use crate::core::ServerState;
use crate::display_ext::DisplayBTreeMapOptValue;
use crate::errors::Fatal;
use crate::metrics::HeartbeatMetrics;
use crate::metrics::ReplicationMetrics;
use crate::metrics::SerdeInstant;
use crate::type_config::alias::InstantOf;
use crate::type_config::alias::LogIdOf;
use crate::type_config::alias::SerdeInstantOf;
use crate::type_config::alias::StoredMembershipOf;
use crate::type_config::alias::VoteOf;
use crate::vote::raft_vote::RaftVoteExt;

/// Comprehensive metrics describing the current state of a Raft node.
///
/// `RaftMetrics` provides real-time observability into a Raft node's operation, including its
/// role, log state, cluster membership, and replication progress.
///
/// # Structure
///
/// Metrics are organized into logical groups:
///
/// - **Node State**: `id`, `state`, `current_leader`, `running_state`
/// - **Log State**: `last_log_index`, `last_applied`, `snapshot`, `purged`
/// - **Voting State**: `current_term`, `vote`
/// - **Leader Metrics** (only when leader): `heartbeat`, `replication`, `last_quorum_acked`
/// - **Cluster Config**: `membership_config`
///
/// # Usage
///
/// Access metrics through the watch channel returned by [`Raft::metrics`]:
///
/// ```ignore
/// let metrics_rx = raft.metrics();
///
/// // Read current metrics
/// let metrics = metrics_rx.borrow_watched();
/// println!("Node state: {:?}", metrics.state);
/// println!("Current leader: {:?}", metrics.current_leader);
///
/// // Wait for specific conditions
/// raft.wait(None).state(State::Leader, "become leader").await?;
/// raft.wait(Some(timeout)).applied_index_at_least(Some(10), "applied-10").await?;
/// ```
///
/// # Leader-Specific Metrics
///
/// When this node is the leader, `heartbeat` and `replication` fields contain detailed information
/// about follower/learner connectivity and replication progress:
///
/// - `heartbeat`: Last acknowledged time for each node (for detecting offline nodes)
/// - `replication`: Replication state including `matched` log index for each node
///
/// These fields are `None` when the node is a follower or candidate.
///
/// # See Also
///
/// - [`Raft::metrics`](crate::Raft::metrics) for obtaining the metrics channel
/// - [`Wait`](crate::metrics::Wait) for waiting on specific metric conditions
/// - [`RaftDataMetrics`] for additional data-plane metrics
/// - [`RaftServerMetrics`] for server operational metrics
///
/// [`Raft::metrics`]: crate::Raft::metrics
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftMetrics<C: RaftTypeConfig> {
    /// The running state of the Raft node, or a fatal error if the node has stopped.
    pub running_state: Result<(), Fatal<C>>,

    /// The ID of the Raft node.
    pub id: C::NodeId,

    // ---
    // --- data ---
    // ---
    /// The current term of the Raft node.
    pub current_term: C::Term,

    /// The last flushed vote.
    pub vote: VoteOf<C>,

    /// The last log index has been appended to this Raft node's log.
    pub last_log_index: Option<u64>,

    /// The last log ID known to this node as committed, i.e., safe to apply to the local state
    /// machine.
    ///
    /// This is the **local committed** value, which may lag behind the
    /// [**cluster-committed**](Self::cluster_committed) value (the actual quorum-acknowledged
    /// frontier) due to network delays or out-of-order RPC delivery. See [`commit`] for the full
    /// explanation of the distinction.
    ///
    /// [`commit`]: crate::docs::protocol::commit
    pub local_committed: Option<LogIdOf<C>>,

    /// The last log ID known to be committed by a quorum of the cluster, as last reported by the
    /// leader.
    ///
    /// This is the **cluster-committed** value. It may lead
    /// [`local_committed`](Self::local_committed) on a node that has not yet received the
    /// corresponding log entries. See [`commit`] for the full explanation of the distinction.
    ///
    /// [`commit`]: crate::docs::protocol::commit
    pub cluster_committed: Option<LogIdOf<C>>,

    /// The last log index has been applied to this Raft node's state machine.
    pub last_applied: Option<LogIdOf<C>>,

    /// The id of the last log included in snapshot.
    /// If there is no snapshot, it is (0,0).
    pub snapshot: Option<LogIdOf<C>>,

    /// The last log id that has purged from storage, inclusive.
    ///
    /// `purged` is also the first log id Openraft knows, although the corresponding log entry has
    /// already been deleted.
    pub purged: Option<LogIdOf<C>>,

    // ---
    // --- cluster ---
    // ---
    /// The state of the Raft node.
    pub state: ServerState,

    /// The current cluster leader.
    pub current_leader: Option<C::NodeId>,

    /// For a leader, it is the most recently acknowledged timestamp by a quorum.
    ///
    /// It is `None` if this node is not leader, or the leader is not yet acknowledged by a quorum.
    /// Being acknowledged means receiving a reply of
    /// `AppendEntries`(`AppendEntriesRequest.vote.committed == true`).
    /// Receiving a reply of `RequestVote`(`RequestVote.vote.committed == false`) does not count
    /// because a node will not maintain a lease for a vote with `committed == false`.
    ///
    /// This timestamp can be used by the application to assess the likelihood that the leader has
    /// lost synchronization with the cluster.
    /// An older value may suggest a higher probability of the leader being partitioned from the
    /// cluster.
    pub last_quorum_acked: Option<SerdeInstantOf<C>>,

    /// The current membership config of the cluster.
    pub membership_config: Arc<StoredMembershipOf<C>>,

    /// The last committed membership config.
    ///
    /// It lags behind [`membership_config`](Self::membership_config) until the effective
    /// membership config is committed: when the two are equal, the last membership log entry is
    /// committed, i.e., a membership change is fully completed.
    pub committed_membership_config: Arc<StoredMembershipOf<C>>,

    /// Heartbeat metrics. It is Some() only when this node is leader.
    ///
    /// This field records a mapping between a node's ID and the time of the
    /// last acknowledged heartbeat or replication to this node.
    ///
    /// This duration since the recorded time can be used by applications to
    /// guess if a follower/learner node is offline, longer duration suggests
    /// a higher possibility of that.
    pub heartbeat: Option<HeartbeatMetrics<C>>,

    // ---
    // --- replication ---
    // ---
    /// The replication states. It is Some() only when this node is leader.
    pub replication: Option<ReplicationMetrics<C>>,
}

impl<C> fmt::Display for RaftMetrics<C>
where
    C: RaftTypeConfig,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Metrics{{")?;

        let id = &self.id;
        let state = &self.state;
        let term = &self.current_term;
        let vote = &self.vote;
        let last_log = self.last_log_index.display();
        let local_committed = self.local_committed.display();
        let cluster_committed = self.cluster_committed.display();
        let last_applied = self.last_applied.display();
        let leader = self.current_leader.display();

        write!(
            f,
            "id:{id}, {state:?}, term:{term}, vote:{vote}, last_log:{last_log}, local_committed:{local_committed}, cluster_committed:{cluster_committed}, last_applied:{last_applied}, leader:{leader}"
        )?;

        if let Some(quorum_acked) = &self.last_quorum_acked {
            let elapsed = quorum_acked.elapsed();
            write!(f, "(quorum_acked_time:{quorum_acked}, {elapsed:?} ago)")?;
        } else {
            write!(f, "(quorum_acked_time:None)")?;
        }

        let membership = &self.membership_config;
        let committed_membership = &self.committed_membership_config;
        let snapshot = self.snapshot.display();
        let purged = self.purged.display();
        let replication = self.replication.as_ref().map(DisplayBTreeMapOptValue);
        let replication_disp = replication.display();
        let heartbeat = self.heartbeat.as_ref().map(DisplayBTreeMapOptValue);
        let heartbeat_disp = heartbeat.display();

        write!(
            f,
            ", membership:{membership}, committed_membership:{committed_membership}, snapshot:{snapshot}, purged:{purged}, replication:{{{replication_disp}}}, heartbeat:{{{heartbeat_disp}}}"
        )?;

        write!(f, "}}")?;
        Ok(())
    }
}

impl<C> RaftMetrics<C>
where
    C: RaftTypeConfig,
{
    /// Create initial metrics for a new Raft node with the given ID.
    pub fn new_initial(id: C::NodeId) -> Self {
        let vote = VoteOf::<C>::new_with_default_term(id.clone());
        Self {
            running_state: Ok(()),
            id,

            current_term: Default::default(),
            vote,
            last_log_index: None,
            local_committed: None,
            cluster_committed: None,
            last_applied: None,
            snapshot: None,
            purged: None,

            state: ServerState::Follower,
            current_leader: None,
            last_quorum_acked: None,
            membership_config: Arc::new(StoredMembershipOf::<C>::default()),
            committed_membership_config: Arc::new(StoredMembershipOf::<C>::default()),
            replication: None,
            heartbeat: None,
        }
    }
}

/// Subset of RaftMetrics, only include data-related metrics
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RaftDataMetrics<C: RaftTypeConfig> {
    /// The last log ID known to this node as committed, i.e., safe to apply to the local state
    /// machine.
    ///
    /// This is the **local committed** value, which may lag behind the
    /// [**cluster-committed**](Self::cluster_committed) value (the actual quorum-acknowledged
    /// frontier) due to network delays or out-of-order RPC delivery. See [`commit`] for the full
    /// explanation of the distinction.
    ///
    /// [`commit`]: crate::docs::protocol::commit
    pub local_committed: Option<LogIdOf<C>>,

    /// The latest log ID that has been acknowledged by a quorum, as perceived by this node.
    ///
    /// This is the highest log ID known to have been safely replicated to a majority of the
    /// cluster, making it permanently committed and safe from truncation by future leaders.
    ///
    /// This value is monotonic: it only advances and never regresses, even across leadership
    /// changes.
    ///
    /// It can be greater than [`local_committed`](Self::local_committed):
    /// - On a follower receiving `AppendEntries` containing a commit index that covers logs the
    ///   follower has not yet fetched.
    /// - On a leader that has received quorum acks for an entry but has not yet updated its local
    ///   commit pointer.
    pub cluster_committed: Option<LogIdOf<C>>,

    /// The last log index has been appended to this Raft node's log.
    pub last_log: Option<LogIdOf<C>>,

    /// The last log ID applied to this Raft node's state machine.
    pub last_applied: Option<LogIdOf<C>>,

    /// The log ID of the last log entry included in the current snapshot, if any.
    pub snapshot: Option<LogIdOf<C>>,

    /// The log ID of the last purged log entry, if any.
    pub purged: Option<LogIdOf<C>>,

    /// The latest time when a quorum has acknowledged a leader's lease.
    ///
    /// This is only `Some` when this node is currently the leader.
    pub last_quorum_acked: Option<SerdeInstant<InstantOf<C>>>,

    /// Replication metrics for each remote node.
    ///
    /// This is only `Some` when this node is currently the leader.
    pub replication: Option<ReplicationMetrics<C>>,

    /// The heartbeat intervals for each node.
    ///
    /// This is only `Some` when this node is currently the leader.
    ///
    /// A heartbeat interval is the time elapsed since the last time a heartbeat was acknowledged by
    /// a remote node.
    ///
    /// If the node has not received any response from a remote node yet, the heartbeat interval is
    /// `None`.
    ///
    /// The longer the interval, the less likely the remote node is alive or reachable, and there is
    /// a higher possibility of that.
    pub heartbeat: Option<HeartbeatMetrics<C>>,
}

impl<C> fmt::Display for RaftDataMetrics<C>
where
    C: RaftTypeConfig,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DataMetrics{{")?;

        let last_log = self.last_log.display();
        let local_committed = self.local_committed.display();
        let cluster_committed = self.cluster_committed.display();
        let last_applied = self.last_applied.display();
        let snapshot = self.snapshot.display();
        let purged = self.purged.display();

        write!(
            f,
            "last_log:{last_log}, local_committed:{local_committed}, cluster_committed:{cluster_committed}, last_applied:{last_applied}, snapshot:{snapshot}, purged:{purged}"
        )?;

        if let Some(quorum_acked) = &self.last_quorum_acked {
            let elapsed = quorum_acked.elapsed();
            write!(f, ", quorum_acked_time:({quorum_acked}, {elapsed:?} ago)")?;
        } else {
            write!(f, ", quorum_acked_time:None")?;
        }

        let replication = self.replication.as_ref().map(DisplayBTreeMapOptValue);
        let replication_disp = replication.display();
        let heartbeat = self.heartbeat.as_ref().map(DisplayBTreeMapOptValue);
        let heartbeat_disp = heartbeat.display();

        write!(
            f,
            ", replication:{{{replication_disp}}}, heartbeat:{{{heartbeat_disp}}}"
        )?;

        write!(f, "}}")?;
        Ok(())
    }
}

/// Subset of RaftMetrics, only include server-related metrics
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftServerMetrics<C: RaftTypeConfig> {
    /// The ID of this Raft node.
    pub id: C::NodeId,
    /// The current vote state.
    pub vote: VoteOf<C>,
    /// The current server state (Leader, Follower, Candidate, etc.).
    pub state: ServerState,
    /// The ID of the current leader, if known.
    pub current_leader: Option<C::NodeId>,

    /// The current membership configuration.
    pub membership_config: Arc<StoredMembershipOf<C>>,

    /// The last committed membership config.
    ///
    /// It lags behind [`membership_config`](Self::membership_config) until the effective
    /// membership config is committed: when the two are equal, the last membership log entry is
    /// committed, i.e., a membership change is fully completed.
    pub committed_membership_config: Arc<StoredMembershipOf<C>>,
}

impl<C> fmt::Display for RaftServerMetrics<C>
where
    C: RaftTypeConfig,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ServerMetrics{{")?;

        let id = &self.id;
        let state = &self.state;
        let vote = &self.vote;
        let leader = self.current_leader.display();
        let membership = &self.membership_config;
        let committed_membership = &self.committed_membership_config;

        write!(
            f,
            "id:{id}, {state:?}, vote:{vote}, leader:{leader}, membership:{membership}, committed_membership:{committed_membership}"
        )?;

        write!(f, "}}")?;
        Ok(())
    }
}

impl<C> RaftServerMetrics<C>
where
    C: RaftTypeConfig,
{
    /// Create initial server metrics for a new Raft node.
    ///
    /// The vote is initialized with the default term (0) and the given node id,
    /// representing the initial state before any leader election has occurred.
    pub(crate) fn new_initial(id: C::NodeId) -> Self {
        let vote = VoteOf::<C>::new_with_default_term(id.clone());
        Self {
            id,
            vote,
            state: Default::default(),
            current_leader: None,
            membership_config: Arc::new(Default::default()),
            committed_membership_config: Arc::new(Default::default()),
        }
    }
}
