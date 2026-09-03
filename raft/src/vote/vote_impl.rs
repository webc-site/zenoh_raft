use std::{
  cmp::Ordering,
  fmt::{Display, Formatter, Result as FmtResult},
};

use crate::vote::{RaftLeaderId, RaftVote, ref_vote::RefVote};

/// `Vote` represent the privilege of a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, bitcode::Encode, bitcode::Decode)]
pub struct Vote<LID>
where
  LID: RaftLeaderId,
{
  /// The id of the node that tries to become the leader.
  pub leader_id: LID,

  /// Whether this vote has been committed (granted by a quorum).
  pub committed: bool,
}

impl<LID> PartialOrd for Vote<LID>
where
  LID: RaftLeaderId,
{
  #[inline]
  fn partial_cmp(&self, other: &Vote<LID>) -> Option<Ordering> {
    let self_ref = RefVote::new(&self.leader_id, self.committed);
    let other_ref = RefVote::new(&other.leader_id, other.committed);
    PartialOrd::partial_cmp(&self_ref, &other_ref)
  }
}

impl<LID> Display for Vote<LID>
where
  LID: RaftLeaderId,
{
  fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
    let ref_vote = RefVote::new(&self.leader_id, self.committed);
    ref_vote.fmt(f)
  }
}

impl<LID> RaftVote for Vote<LID>
where
  LID: RaftLeaderId,
{
  type LeaderId = LID;

  fn from_leader_id(leader_id: LID, committed: bool) -> Self {
    Self {
      leader_id,
      committed,
    }
  }

  fn leader_id(&self) -> &LID {
    &self.leader_id
  }

  fn is_committed(&self) -> bool {
    self.committed
  }
}

impl<LID> Vote<LID>
where
  LID: RaftLeaderId,
{
  /// Create a new uncommitted vote for the given term and node.
  pub fn new(term: LID::Term, node_id: LID::NodeId) -> Self {
    Self {
      leader_id: LID::new(term, node_id),
      committed: false,
    }
  }

  /// Create a new committed vote for the given term and node.
  pub fn new_committed(term: LID::Term, node_id: LID::NodeId) -> Self {
    Self {
      leader_id: LID::new(term, node_id),
      committed: true,
    }
  }

  /// Check if this vote has been committed.
  pub fn is_committed(&self) -> bool {
    self.committed
  }

  /// Return the `LeaderId` this vote represents for.
  ///
  /// The leader may or may not be granted by a quorum.
  pub fn leader_id(&self) -> &LID {
    &self.leader_id
  }
}

#[cfg(test)]
mod tests {
  mod feature_no_single_term_leader {
    use crate::{
      Vote,
      engine::testing::{UTConfig, UTLeaderId},
    };

    #[test]
    fn test_vote_serde() -> anyhow::Result<()> {
      let v = Vote::new(1, 2);
      let bytes = bitcode::encode(&v);
      let v2: Vote<UTLeaderId> = bitcode::decode(&bytes)?;
      assert_eq!(v, v2);

      Ok(())
    }

    #[test]
    fn test_vote_total_order() -> anyhow::Result<()> {
      let vote = |term, node_id| Vote::<UTLeaderId>::new(term, node_id);

      let committed = |term, node_id| Vote::<UTLeaderId>::new_committed(term, node_id);

      // Compare term first
      assert!(vote(2, 2) > vote(1, 2));
      assert!(vote(1, 2) < vote(2, 2));

      // Equal term
      assert!(vote(2, 2) > vote(2, 1));
      assert!(vote(2, 1) < vote(2, 2));

      // Equal term, node_id
      assert!(vote(2, 2) == vote(2, 2));
      assert!(vote(2, 2) >= vote(2, 2));
      assert!(vote(2, 2) <= vote(2, 2));

      assert!(committed(2, 2) > vote(2, 2));
      assert!(vote(2, 2) < committed(2, 2));
      Ok(())
    }

    #[test]
    fn test_to_committed_leader_id() -> anyhow::Result<()> {
      use crate::{
        type_config::alias::LeaderIdOf,
        vote::{RaftLeaderId, raft_vote::RaftVoteExt},
      };

      let vote = Vote::<UTLeaderId>::new(1, 2);
      assert_eq!(None, vote.try_to_committed_leader_id());

      let committed = Vote::<UTLeaderId>::new_committed(1, 2);
      let leader_id = committed.try_to_committed_leader_id();
      let expected = LeaderIdOf::<UTConfig>::new(1, 2).to_committed();
      assert_eq!(Some(expected), leader_id);

      Ok(())
    }
  }

  mod feature_single_term_leader {
    use std::cmp::Ordering;

    use crate::{Vote, vote::leader_id_std::LeaderId};

    type TCLeaderId = LeaderId<u64, u64>;

    #[test]
    fn test_vote_serde() -> anyhow::Result<()> {
      let v = Vote::<TCLeaderId>::new(1, 2);
      let bytes = bitcode::encode(&v);
      let v2: Vote<TCLeaderId> = bitcode::decode(&bytes)?;
      assert_eq!(v, v2);

      Ok(())
    }

    #[test]
    fn test_vote_partial_order() -> anyhow::Result<()> {
      let vote = |term, node_id| Vote::<TCLeaderId>::new(term, node_id);

      let committed = |term, node_id| Vote::<TCLeaderId>::new_committed(term, node_id);

      // Compare term first
      assert!(vote(2, 2) > vote(1, 2));
      assert!(vote(2, 2) >= vote(1, 2));
      assert!(vote(1, 2) < vote(2, 2));
      assert!(vote(1, 2) <= vote(2, 2));

      // Committed greater than non-committed if leader_id is incomparable
      assert!(committed(2, 2) > vote(2, 2));
      assert!(committed(2, 2) >= vote(2, 2));
      assert!(committed(2, 1) > vote(2, 2));
      assert!(committed(2, 1) >= vote(2, 2));

      // Lower term committed is not greater
      assert_eq!(
        committed(1, 1).partial_cmp(&vote(2, 1)),
        Some(Ordering::Less)
      );

      // Compare to itself
      assert!(committed(1, 1) >= committed(1, 1));
      assert!(committed(1, 1) <= committed(1, 1));
      assert_eq!(committed(1, 1), committed(1, 1));

      // Equal
      assert_eq!(vote(2, 2), vote(2, 2));
      assert!(vote(2, 2) >= vote(2, 2));
      assert!(vote(2, 2) <= vote(2, 2));

      // Incomparable
      assert_eq!(vote(2, 2).partial_cmp(&vote(2, 3)), None);
      assert_ne!(vote(2, 2), vote(2, 3));

      // Incomparable committed: returns None, not panic
      assert_eq!(committed(2, 2).partial_cmp(&committed(2, 3)), None);
      assert_ne!(committed(2, 2), committed(2, 3));

      Ok(())
    }

    #[test]
    fn test_to_committed_leader_id() -> anyhow::Result<()> {
      use crate::vote::{RaftLeaderId, raft_vote::RaftVoteExt};

      let vote = Vote::<TCLeaderId>::new(1, 2);
      assert_eq!(None, vote.try_to_committed_leader_id());

      let committed = Vote::<TCLeaderId>::new_committed(1, 2);
      let leader_id = committed.try_to_committed_leader_id();
      let expected = LeaderId {
        term: 1,
        voted_for: 2,
      }
      .to_committed();
      assert_eq!(Some(expected), leader_id);

      Ok(())
    }
  }
}
