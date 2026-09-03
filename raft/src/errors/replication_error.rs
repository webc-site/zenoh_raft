use crate::{
  RaftTypeConfig, StorageError,
  errors::{RPCError, higher_vote::HigherVote, replication_closed::ReplicationClosed},
};

/// Error variants related to the Replication.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ReplicationError<C>
where
  C: RaftTypeConfig,
{
  #[error(transparent)]
  HigherVote(#[from] HigherVote<C>),

  #[error(transparent)]
  Closed(#[from] ReplicationClosed),

  #[error(transparent)]
  StorageError(#[from] StorageError<C>),

  #[error(transparent)]
  RPCError(#[from] RPCError<C>),
}
