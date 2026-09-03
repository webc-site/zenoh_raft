//! Defines the [`NetSnapshot`] trait for snapshot transmission.

use std::future::Future;

use zenoh_raft_macros::add_async_trait;

use crate::{
  OptionalSend, OptionalSync, RaftTypeConfig,
  errors::{ReplicationClosed, StreamingError},
  network::RPCOption,
  raft::SnapshotResponse,
  type_config::alias::{SnapshotOf, VoteOf},
};

/// Sends full snapshots to a target node.
///
/// **For most applications, implement [`RaftNetwork`] instead.** This trait is
/// automatically derived from `RaftNetwork` via blanket implementation.
///
/// Direct implementation is an advanced option for fine-grained control.
///
/// [`RaftNetwork`]: crate::network::RaftNetwork
#[add_async_trait]
pub trait NetSnapshot<C>: OptionalSend + OptionalSync + 'static
where
  C: RaftTypeConfig,
{
  /// Snapshot data this network implementation can transmit.
  type SnapshotData: OptionalSend + 'static;

  /// Send a complete Snapshot to the target.
  ///
  /// This method is responsible for fragmenting the snapshot and sending it to the target node.
  /// Before returning from this method, the snapshot should be completely transmitted and
  /// installed on the target node or rejected because of `vote` being smaller than the
  /// remote one.
  ///
  /// The `vote` is the leader vote used to check if the leader is still valid by a
  /// follower.
  /// When the follower finished receiving the snapshot, it calls
  /// [`Raft::install_full_snapshot()`] with this vote.
  ///
  /// `cancel` gets `Ready` when the caller decides to cancel this snapshot transmission.
  /// The network implementation is also responsible for enforcing `option.soft_ttl()`.
  ///
  /// [`Raft::install_full_snapshot()`]: crate::raft::Raft::install_full_snapshot
  async fn full_snapshot(
    &mut self,
    vote: VoteOf<C>,
    snapshot: SnapshotOf<C, Self::SnapshotData>,
    cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
    option: RPCOption,
  ) -> Result<SnapshotResponse<C>, StreamingError<C>>;
}
