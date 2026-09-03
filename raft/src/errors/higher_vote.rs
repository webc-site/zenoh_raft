use crate::{RaftTypeConfig, type_config::alias::VoteOf};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("seen a higher vote: {higher} GT mine: {sender_vote}")]
pub(crate) struct HigherVote<C: RaftTypeConfig> {
  pub(crate) higher: VoteOf<C>,
  pub(crate) sender_vote: VoteOf<C>,
}
