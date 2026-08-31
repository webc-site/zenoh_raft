use std::fmt::Debug;
use std::fmt::Display;

use crate::AppData;
use crate::Membership;
use crate::base::OptionalFeatures;
use crate::node::Node;
use crate::node::NodeId;

/// Defines operations for constructing and inspecting an entry payload.
pub trait RaftPayload
where
    Self: OptionalFeatures + Debug + Display + Sized + 'static,
{
    /// Application-specific data stored in the payload.
    type D: AppData;

    /// The node ID type used in memberships.
    type NodeId: NodeId;

    /// The node type used in memberships.
    type Node: Node;

    /// Create a blank payload.
    ///
    /// OpenRaft uses a new blank payload as the base of a membership-change entry when the
    /// application does not supply a payload.
    fn blank() -> Self;

    /// Replace the normal application data in this payload.
    fn with_normal(self, data: Self::D) -> Self;

    /// Replace the membership in this payload.
    ///
    /// OpenRaft calls this method for every physical membership-change entry. An implementation
    /// may preserve other application fields.
    fn with_membership(self, membership: Membership<Self::NodeId, Self::Node>) -> Self;

    /// Return `Some(Membership)` if the entry payload contains a membership payload.
    fn get_membership(&self) -> Option<Membership<Self::NodeId, Self::Node>>;

    /// Create a payload containing normal application data.
    fn normal(data: Self::D) -> Self {
        let payload = Self::blank();
        payload.with_normal(data)
    }

    /// Create a payload containing a membership.
    fn membership(membership: Membership<Self::NodeId, Self::Node>) -> Self {
        let payload = Self::blank();
        payload.with_membership(membership)
    }
}
