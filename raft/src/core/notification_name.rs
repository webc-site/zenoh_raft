use zenoh_raft_macros::VariantName;

/// Enum naming each notification type for logging, metrics, and debugging.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, VariantName)]
pub enum NotificationName {
    VoteResponse,
    PreVoteResponse,
    HigherVote,
    StorageError,
    LocalIO,
    ReplicationProgress,
    HeartbeatProgress,
    StateMachine,
    Tick,
    PendingReadDeadlineReached,
}
