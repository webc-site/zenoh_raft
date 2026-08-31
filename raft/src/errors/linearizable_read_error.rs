use crate::RaftTypeConfig;
use crate::errors::ForwardToLeader;
use crate::errors::ForwardToLeaderRef;
use crate::errors::QuorumNotEnough;

/// An error related to an is_leader request.
#[derive(Debug, Clone, thiserror::Error, derive_more::TryInto)]
pub enum LinearizableReadError<C>
where
    C: RaftTypeConfig,
{
    /// This node is not the leader; request should be forwarded to the leader.
    #[error(transparent)]
    ForwardToLeader(#[from] ForwardToLeader<C>),

    /// Cannot finish a request, such as elect or replicate, because a quorum is not available.
    #[error(transparent)]
    QuorumNotEnough(#[from] QuorumNotEnough<C>),
}

impl<C> ForwardToLeaderRef<C> for LinearizableReadError<C>
where
    C: RaftTypeConfig,
{
    fn forward_to_leader(&self) -> Option<&ForwardToLeader<C>> {
        match self {
            Self::ForwardToLeader(f) => Some(f),
            _ => None,
        }
    }
}

impl<C> LinearizableReadError<C>
where
    C: RaftTypeConfig,
{
    pub fn forward_to_leader(&self) -> Option<&ForwardToLeader<C>> {
        match self {
            Self::ForwardToLeader(f) => Some(f),
            _ => None,
        }
    }
}
