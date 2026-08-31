use crate::errors::Operation;
use crate::node::NodeId;

/// Error indicating a node was not found in the cluster.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("Node {node_id} not found when: ({operation})")]
pub struct NodeNotFound<NID>
where
    NID: NodeId,
{
    /// The node ID that was not found.
    pub node_id: NID,
    /// The operation that was being attempted when the node was not found.
    pub operation: Operation,
}

impl<NID> NodeNotFound<NID>
where
    NID: NodeId,
{
    /// Create a new NodeNotFound error.
    pub fn new(node_id: NID, operation: Operation) -> Self {
        Self { node_id, operation }
    }
}
