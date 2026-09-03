use std::fmt::{Display, Formatter, Result as FmtResult};

use display_more::DisplayOptionExt;

use crate::{
  RaftTypeConfig,
  type_config::alias::{CommittedVoteOf, LogIdOf},
};

/// Uniquely identifies a replication session.
///
/// A replication session represents a set of replication streams from a leader to its followers.
/// For example, in a cluster of 3 nodes where node-1 is the leader, it maintains replication
/// streams to nodes {2,3}.
///
/// A replication session is uniquely identified by the leader's vote and the membership
/// configuration. When either changes, a new replication session is created.
///
/// See: [ReplicationSession](crate::docs::data::replication_session)
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplicationSessionId<C>
where
  C: RaftTypeConfig,
{
  /// The Leader or Candidate this replication belongs to.
  pub(crate) leader_vote: CommittedVoteOf<C>,

  /// The log id of the membership log this replication works for.
  pub(crate) membership_log_id: Option<LogIdOf<C>>,
}

impl<C> Display for ReplicationSessionId<C>
where
  C: RaftTypeConfig,
{
  fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
    write!(
      f,
      "(leader_vote:{}, membership_log_id:{})",
      self.leader_vote,
      self.membership_log_id.display()
    )
  }
}

impl<C> ReplicationSessionId<C>
where
  C: RaftTypeConfig,
{
  pub(crate) fn new(vote: CommittedVoteOf<C>, membership_log_id: Option<LogIdOf<C>>) -> Self {
    Self {
      leader_vote: vote,
      membership_log_id,
    }
  }
}
