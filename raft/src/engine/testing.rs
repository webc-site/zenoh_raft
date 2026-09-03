use std::marker::PhantomData;

use crate::{
  Node, OptionalSend, RaftTypeConfig,
  entry::Entry,
  impls::{InlineBatch, OneshotResponder, Vote, leader_id_adv::LeaderId},
  type_config::alias::{LeaderIdOf, LogIdOf, NodeIdOf},
  vote::RaftLeaderId,
};

/// Trivial Raft type config for Engine-related unit tests,
/// with an optional custom node type `N` for the Node type.
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct UTConfig<N = ()> {
  _p: PhantomData<N>,
}

impl<N> Default for UTConfig<N> {
  fn default() -> Self {
    Self { _p: PhantomData }
  }
}

impl<N> Clone for UTConfig<N> {
  fn clone(&self) -> Self {
    *self
  }
}

impl<N> Copy for UTConfig<N> {}

impl<N> RaftTypeConfig for UTConfig<N>
where
  N: Node + Ord,
{
  type D = u64;
  type R = ();
  type NodeId = u64;
  type Node = N;
  type Term = u64;
  type LeaderId = LeaderId<u64, u64>;
  type Vote = Vote<Self::LeaderId>;
  type Payload = crate::EntryPayload<Self::D, Self::NodeId, Self::Node>;
  type Entry = Entry<<Self::LeaderId as RaftLeaderId>::Committed, Self::Payload>;
  type Responder<T>
    = OneshotResponder<Self, T>
  where
    T: OptionalSend + 'static;
  type Batch<T>
    = InlineBatch<T>
  where
    T: OptionalSend + 'static;
  type ErrorSource = anyerror::AnyError;
}

/// Type alias for the LeaderId used in unit tests.
#[cfg(test)]
pub(crate) type UTLeaderId = LeaderId<u64, u64>;

/// Type alias for the CommittedLeaderId used in unit tests.
///
/// For `leader_id_adv`, `Committed = Self`, so this is the same as `UTLeaderId`.
#[cfg(test)]
pub(crate) type UtClid = UTLeaderId;

/// Builds a log id, for testing purposes.
pub(crate) fn log_id(term: u64, node_id: NodeIdOf<UTConfig>, index: u64) -> LogIdOf<UTConfig> {
  LogIdOf::<UTConfig>::new(LeaderIdOf::<UTConfig>::new_committed(term, node_id), index)
}
