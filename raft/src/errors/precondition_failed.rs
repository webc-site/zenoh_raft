use display_more::DisplayOptionExt;

use crate::RaftTypeConfig;
use crate::type_config::alias::CommittedLeaderIdOf;
use crate::type_config::alias::LogIdOf;

/// A [`Precondition`] is not satisfied by the current Raft state.
///
/// [`Precondition`]: `crate::raft::Precondition`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PreconditionFailed<C>
where
    C: RaftTypeConfig,
{
    /// The established leader is not the one [`Precondition::CommittedLeaderId`] required.
    ///
    /// `actual` is `None` when this node's vote is not committed, i.e. it has no established
    /// leader.
    ///
    /// [`Precondition::CommittedLeaderId`]: `crate::raft::Precondition::CommittedLeaderId`
    #[error("committed leader id mismatch: expected: {expected}, actual: {}", actual.display())]
    CommittedLeaderIdMismatch {
        expected: CommittedLeaderIdOf<C>,
        actual: Option<CommittedLeaderIdOf<C>>,
    },

    /// The last log id is not the one [`Precondition::LastLogId`] required.
    ///
    /// [`Precondition::LastLogId`]: `crate::raft::Precondition::LastLogId`
    #[error("last log id mismatch: expected: {}, actual: {}", expected.display(), actual.display())]
    LastLogIdMismatch {
        expected: Option<LogIdOf<C>>,
        actual: Option<LogIdOf<C>>,
    },

    /// The effective membership is not the one [`Precondition::LastMembershipLogId`] required,
    /// meaning another membership change was proposed in between.
    ///
    /// [`Precondition::LastMembershipLogId`]: `crate::raft::Precondition::LastMembershipLogId`
    #[error("last membership log id mismatch: expected: {}, actual: {}", expected.display(), actual.display())]
    LastMembershipLogIdMismatch {
        expected: Option<LogIdOf<C>>,
        actual: Option<LogIdOf<C>>,
    },
}
