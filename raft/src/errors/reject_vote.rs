use crate::RaftTypeConfig;
use crate::type_config::alias::VoteOf;

/// The local vote is greater than the received vote.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("reject vote: local vote {higher} is >= the received vote")]
pub struct RejectVote<C: RaftTypeConfig> {
    pub higher: VoteOf<C>,
}
