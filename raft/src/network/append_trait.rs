//! Defines the [`NetAppend`] trait for AppendEntries RPC.

use zenoh_raft_macros::add_async_trait;

use crate::{
  OptionalSend, OptionalSync, RaftTypeConfig,
  errors::RPCError,
  network::RPCOption,
  raft::{AppendEntriesRequest, AppendEntriesResponse},
};

/// Sends AppendEntries RPCs to a target node.
///
/// **For most applications, implement [`RaftNetwork`] instead.** This trait is
/// automatically derived from `RaftNetwork` via blanket implementation.
///
/// Direct implementation is an advanced option for fine-grained control.
///
/// [`RaftNetwork`]: crate::network::RaftNetwork
#[add_async_trait]
pub trait NetAppend<C>: OptionalSend + OptionalSync + 'static
where
  C: RaftTypeConfig,
{
  /// Send an AppendEntries RPC to the target.
  ///
  /// The network implementation is responsible for enforcing `option.soft_ttl()`.
  async fn append_entries(
    &mut self,
    rpc: AppendEntriesRequest<C>,
    option: RPCOption,
  ) -> Result<AppendEntriesResponse<C>, RPCError<C>>;
}
