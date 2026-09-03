//! Defines the [`NetVote`] trait for Vote RPC.

use zenoh_raft_macros::add_async_trait;

use crate::{
  OptionalSend, OptionalSync, RaftTypeConfig,
  errors::RPCError,
  network::RPCOption,
  raft::{VoteRequest, VoteResponse},
};

/// Sends Vote RPCs to a target node.
///
/// **For most applications, implement [`RaftNetwork`] instead.** This trait is
/// automatically derived from `RaftNetwork` via blanket implementation.
///
/// Direct implementation is an advanced option for fine-grained control.
///
/// [`RaftNetwork`]: crate::network::RaftNetwork
#[add_async_trait]
pub trait NetVote<C>: OptionalSend + OptionalSync + 'static
where
  C: RaftTypeConfig,
{
  /// Send a RequestVote RPC to the target.
  ///
  /// The network implementation is responsible for enforcing `option.soft_ttl()`.
  async fn vote(
    &mut self,
    rpc: VoteRequest<C>,
    option: RPCOption,
  ) -> Result<VoteResponse<C>, RPCError<C>>;

  /// Send a Pre-Vote RPC to the target.
  ///
  /// The default implementation synthesizes a **granting** response, so a network that has not
  /// implemented `pre_vote` makes Pre-Vote a no-op and elections proceed as before. A transport
  /// failure must instead be returned as `Err` (it is not counted as a grant). See
  /// [`RaftNetwork::pre_vote`](crate::network::RaftNetwork::pre_vote).
  ///
  /// Implementations that send a real Pre-Vote RPC are responsible for enforcing
  /// `option.soft_ttl()`.
  async fn pre_vote(
    &mut self,
    rpc: VoteRequest<C>,
    _option: RPCOption,
  ) -> Result<VoteResponse<C>, RPCError<C>> {
    Ok(VoteResponse::new(rpc.vote, None, true))
  }
}
