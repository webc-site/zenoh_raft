use crate::{
  ChangeMembers, RaftTypeConfig, batch::Batch, raft::Precondition, type_config::alias::BatchOf,
};

/// Parameters for a membership change that may carry an application-defined payload.
pub struct ChangeMembershipRequest<C>
where
  C: RaftTypeConfig,
{
  members: ChangeMembers<C::NodeId, C::Node>,
  retain_removed_as_learners: bool,
  preconditions: BatchOf<C, Precondition<C>>,
  payload: Option<(C::Payload, C::Payload)>,
}

impl<C> ChangeMembershipRequest<C>
where
  C: RaftTypeConfig,
{
  /// Create a request that uses a new blank payload for each membership entry and has no
  /// preconditions.
  pub fn new(members: impl Into<ChangeMembers<C::NodeId, C::Node>>, retain: bool) -> Self {
    let members = members.into();
    Self {
      members,
      retain_removed_as_learners: retain,
      preconditions: BatchOf::<C, _>::of([]),
      payload: None,
    }
  }

  /// Use separate application-defined payloads for the membership-change steps.
  ///
  /// Each payload is tied to the shape of the membership it carries, not to the order of the
  /// steps. `joint_payload` is used only for a joint membership entry, which the change writes
  /// only when it moves voters. `uniform_payload` is used for the uniform membership entry,
  /// which every completed change writes. A change that needs no joint entry therefore drops
  /// `joint_payload` and writes its single entry with `uniform_payload`.
  pub fn with_payload(mut self, joint_payload: C::Payload, uniform_payload: C::Payload) -> Self {
    self.payload = Some((joint_payload, uniform_payload));
    self
  }

  /// Guard the first membership proposal with the given preconditions.
  pub fn with_preconditions(
    mut self,
    preconditions: impl IntoIterator<Item = Precondition<C>>,
  ) -> Self {
    let preconditions = BatchOf::<C, _>::of(preconditions);
    self.preconditions = preconditions;
    self
  }

  pub(crate) fn into_parts(self) -> ChangeMembershipParts<C> {
    (
      self.members,
      self.retain_removed_as_learners,
      self.preconditions,
      self.payload,
    )
  }
}

pub(crate) type ChangeMembershipParts<C> = (
  ChangeMembers<<C as RaftTypeConfig>::NodeId, <C as RaftTypeConfig>::Node>,
  bool,
  BatchOf<C, Precondition<C>>,
  Option<(
    <C as RaftTypeConfig>::Payload,
    <C as RaftTypeConfig>::Payload,
  )>,
);
