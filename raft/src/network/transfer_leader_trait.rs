//! Defines the [`NetTransferLeader`] trait for leader transfer.

use zenoh_raft_macros::add_async_trait;

use crate::OptionalSend;
use crate::OptionalSync;
use crate::RaftTypeConfig;
use crate::errors::RPCError;
use crate::network::RPCOption;
use crate::raft::message::TransferLeaderRequest;
use crate::raft::message::TransferLeaderResponse;

/// Sends TransferLeader messages to a target node.
///
/// **For most applications, implement [`RaftNetwork`] instead.** This trait is
/// automatically derived from `RaftNetwork` via blanket implementation.
///
/// Direct implementation is an advanced option for fine-grained control.
///
/// [`RaftNetwork`]: crate::network::RaftNetwork
#[add_async_trait]
pub trait NetTransferLeader<C>: OptionalSend + OptionalSync + 'static
where
    C: RaftTypeConfig,
{
    /// Send TransferLeader message to the target node.
    ///
    /// The node received this message should pass it to [`Raft::handle_transfer_leader()`].
    ///
    /// [`Raft::handle_transfer_leader()`]: crate::raft::Raft::handle_transfer_leader
    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<C>,
        option: RPCOption,
    ) -> Result<TransferLeaderResponse<C>, RPCError<C>>;
}
