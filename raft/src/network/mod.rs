//! The Raft network interface.
//!
//! This module defines traits for implementing network communication between Raft nodes:
//!
//! ## Network Traits
//!
//! - [`RaftNetwork`] - Protocol for sending Raft RPCs (AppendEntries, Vote, Snapshot)
//! - [`RaftNetworkFactory`] - Factory for creating network connections to target nodes
//!
//! ## Key Types
//!
//! - [`Backoff`] - Backoff strategy for retrying failed network operations
//! - [`RPCOption`] - Options for configuring RPC behavior
//! - [`RPCTypes`] - Type definitions for RPC requests and responses
//!
//! ## Usage
//!
//! Applications implement [`RaftNetworkFactory`] to create [`RaftNetwork`] instances
//! for communicating with each remote Raft node. The factory is passed to
//! [`Raft::new()`](crate::Raft::new) when creating a Raft instance.
//!
//! See the [Getting Started Guide](crate::docs::getting_started) for implementation
//! details and examples.

mod append_trait;
mod backoff;
mod backoff_trait;
mod factory;
mod raft_network;
mod rpc_option;
mod rpc_type;
mod snapshot_trait;
mod stream_append_trait;
mod transfer_leader_trait;
mod vote_trait;
pub mod zenoh;

pub use append_trait::NetAppend;
pub use backoff::Backoff;
pub use backoff_trait::NetBackoff;
pub use factory::RaftNetworkFactory;
pub use raft_network::RaftNetwork;
pub use rpc_option::RPCOption;
pub use rpc_type::RPCTypes;
pub use snapshot_trait::NetSnapshot;
pub use stream_append_trait::AppendResponseStream;
pub use stream_append_trait::NetStreamAppend;
pub use stream_append_trait::StreamAppendFuture;
pub use stream_append_trait::stream_append_sequential;
pub use transfer_leader_trait::NetTransferLeader;
pub use vote_trait::NetVote;
pub use zenoh::ZenohNetwork;
pub use zenoh::ZenohNetworkConfig;
pub use zenoh::ZenohNetworkFactory;
pub use zenoh::ZenohRaftServer;
pub use zenoh::ZenohSessionBuilder;
pub use zenoh::ZenohTlsConfig;
