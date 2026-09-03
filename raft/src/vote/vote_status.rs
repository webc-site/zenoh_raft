use crate::vote::{RaftLeaderId, committed::CommittedVote, non_committed::UncommittedVote};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum VoteStatus<LID>
where
  LID: RaftLeaderId,
{
  Committed(CommittedVote<LID>),
  Pending(UncommittedVote<LID>),
}
