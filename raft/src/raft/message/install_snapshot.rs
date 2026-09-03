use crate::{RaftTypeConfig, type_config::alias::VoteOf};

/// The response to `Raft::install_full_snapshot` API.
#[derive(Debug, Clone, PartialEq, Eq, derive_more::Display)]
#[display("SnapshotResponse{{vote:{}}}", vote)]
pub struct SnapshotResponse<C: RaftTypeConfig> {
  /// The responder's current vote.
  pub vote: VoteOf<C>,
}

impl<C: RaftTypeConfig> SnapshotResponse<C> {
  /// Create a new snapshot response with the given vote.
  pub fn new(vote: VoteOf<C>) -> Self {
    Self { vote }
  }
}
