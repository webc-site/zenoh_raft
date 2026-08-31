use std::fmt;

use crate::RaftTypeConfig;
use crate::raft::ClientWriteResponse;

/// Responses from the physical log entries of a membership change.
pub struct ChangeMembershipOutcome<C>
where
    C: RaftTypeConfig,
{
    /// The response from the joint membership entry, if the change needed one.
    pub joint: Option<ClientWriteResponse<C>>,

    /// The response from the uniform membership entry, which every completed change writes.
    pub uniform: ClientWriteResponse<C>,
}

impl<C> fmt::Debug for ChangeMembershipOutcome<C>
where
    C: RaftTypeConfig,
    C::R: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChangeMembershipOutcome")
            .field("joint", &self.joint)
            .field("uniform", &self.uniform)
            .finish()
    }
}
