use std::cmp::Ordering;
use std::fmt;

use crate::vote::RaftLeaderId;
use crate::vote::RaftVote;
use crate::vote::Vote;
use crate::vote::ref_vote::RefVote;

/// Represents a committed Vote that has been accepted by a quorum.
///
/// The inner `Vote`'s attribute `committed` is always set to `true`
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommittedVote<LID>
where
    LID: RaftLeaderId,
{
    leader_id: LID,
}

impl<LID> Default for CommittedVote<LID>
where
    LID: RaftLeaderId,
    LID::NodeId: Default,
{
    fn default() -> Self {
        Self {
            leader_id: LID::new_with_default_term(LID::NodeId::default()),
        }
    }
}

/// The `CommittedVote` is totally ordered.
///
/// Because:
/// - any two quorums have common elements,
/// - and the `CommittedVote` is accepted by a quorum,
/// - and a `Vote` is granted if it is greater than the old one.
impl<LID> Ord for CommittedVote<LID>
where
    LID: RaftLeaderId,
{
    fn cmp(&self, other: &Self) -> Ordering {
        let self_ref = RefVote::new(&self.leader_id, true);
        let other_ref = RefVote::new(&other.leader_id, true);
        self_ref.partial_cmp(&other_ref).unwrap()
    }
}

impl<LID> PartialOrd for CommittedVote<LID>
where
    LID: RaftLeaderId,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<LID> CommittedVote<LID>
where
    LID: RaftLeaderId,
{
    pub(crate) fn new(leader_id: LID) -> Self {
        Self { leader_id }
    }

    pub(crate) fn committed_leader_id(&self) -> LID::Committed {
        self.leader_id().to_committed()
    }

    /// Convert to the user-facing vote type.
    pub(crate) fn into_vote<V: RaftVote<LeaderId = LID>>(self) -> V {
        V::from_leader_id(self.leader_id, true)
    }

    pub(crate) fn into_internal_vote(self) -> Vote<LID> {
        Vote::from_leader_id(self.leader_id, true)
    }
}

impl<LID> fmt::Display for CommittedVote<LID>
where
    LID: RaftLeaderId,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ref_vote = RefVote::new(&self.leader_id, true);
        ref_vote.fmt(f)
    }
}

impl<LID> RaftVote for CommittedVote<LID>
where
    LID: RaftLeaderId,
{
    type LeaderId = LID;

    fn from_leader_id(_leader_id: LID, _committed: bool) -> Self {
        unreachable!("CommittedVote should only be built from a Vote")
    }

    fn leader_id(&self) -> &LID {
        &self.leader_id
    }

    fn is_committed(&self) -> bool {
        true
    }
}
