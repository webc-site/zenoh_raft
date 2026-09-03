use std::sync::Arc;

use crate::{
  Membership,
  proposer::{Candidate, Leader},
  type_config::alias::{NodeIdOf, NodeOf},
};

/// The quorum set type used by `Leader`.
pub(crate) type LeaderQuorumSet<C> = Arc<Membership<NodeIdOf<C>, NodeOf<C>>>;

pub(crate) type LeaderState<C> = Option<Box<Leader<C, LeaderQuorumSet<C>>>>;
pub(crate) type CandidateState<C> = Option<Candidate<C, LeaderQuorumSet<C>>>;
