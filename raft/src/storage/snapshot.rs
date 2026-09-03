use std::fmt;

use crate::{
  base::OptionalSend,
  node::{Node, NodeId},
  storage::SnapshotMeta,
  vote::RaftCommittedLeaderId,
};

/// The data associated with the current snapshot.
#[derive(Debug, Clone)]
pub struct Snapshot<CLID, NID, N, SD>
where
  CLID: RaftCommittedLeaderId,
  NID: NodeId,
  N: Node,
  SD: OptionalSend + 'static,
{
  /// metadata of a snapshot
  pub meta: SnapshotMeta<CLID, NID, N>,

  /// A read handle to the associated snapshot.
  pub snapshot: SD,
}

impl<CLID, NID, N, SD> fmt::Display for Snapshot<CLID, NID, N, SD>
where
  CLID: RaftCommittedLeaderId,
  NID: NodeId,
  N: Node,
  SD: OptionalSend + 'static,
{
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "Snapshot{{meta: {}}}", self.meta)
  }
}
