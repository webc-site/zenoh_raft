use std::{cmp::Ordering, fmt};

use crate::vote::RaftLeaderId;

/// Similar to [`Vote`] but with a reference to the `LeaderId`,
/// and provide ordering and display implementation.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RefVote<'a, LID>
where
  LID: RaftLeaderId,
{
  pub(crate) leader_id: &'a LID,
  pub(crate) committed: bool,
}

impl<'a, LID> RefVote<'a, LID>
where
  LID: RaftLeaderId,
{
  pub(crate) fn new(leader_id: &'a LID, committed: bool) -> Self {
    Self {
      leader_id,
      committed,
    }
  }

  pub(crate) fn is_committed(&self) -> bool {
    self.committed
  }
}

impl<LID> Eq for RefVote<'_, LID> where LID: RaftLeaderId {}

impl<'a, 'b, LID> PartialEq<RefVote<'b, LID>> for RefVote<'a, LID>
where
  LID: RaftLeaderId,
{
  fn eq(&self, other: &RefVote<'b, LID>) -> bool {
    self.leader_id == other.leader_id && self.committed == other.committed
  }
}

// Commit votes have a total order relation with all other votes
impl<'a, 'b, LID> PartialOrd<RefVote<'b, LID>> for RefVote<'a, LID>
where
  LID: RaftLeaderId,
{
  #[inline]
  fn partial_cmp(&self, other: &RefVote<'b, LID>) -> Option<Ordering> {
    match PartialOrd::partial_cmp(&self.leader_id, &other.leader_id) {
      Some(Ordering::Equal) => PartialOrd::partial_cmp(&self.committed, &other.committed),
      None => {
        // If two leader_id are not comparable, they won't both be granted(committed).
        // Therefore use `committed` to determine greatness to minimize election conflict.
        match (self.committed, other.committed) {
          (false, false) => None,
          (true, false) => Some(Ordering::Greater),
          (false, true) => Some(Ordering::Less),
          (true, true) => None,
        }
      }
      // Some(non-equal)
      cmp => cmp,
    }
  }
}

impl<LID> fmt::Display for RefVote<'_, LID>
where
  LID: RaftLeaderId,
{
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let q = if self.is_committed() { "Q" } else { "-" };
    let leader_id = self.leader_id;
    write!(f, "<{leader_id}:{q}>")
  }
}
