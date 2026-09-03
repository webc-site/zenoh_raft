//! Raft protocol messages and types.
//!
//! Request and response types for an application to talk to the Raft,
//! and are also used by network layer to talk to other Raft nodes.

mod append_entries_request;
mod append_entries_response;
mod change_membership;
mod change_membership_request;
mod install_snapshot;
mod log_segment;
mod precondition;
mod stream_append_error;
mod transfer_leader;
mod vote;
mod write;

mod client_write;
mod write_request;

pub use append_entries_request::AppendEntriesRequest;
pub use append_entries_response::AppendEntriesResponse;
pub use change_membership::ChangeMembershipOutcome;
pub use change_membership_request::ChangeMembershipRequest;
pub use client_write::{ClientWriteResponse, ClientWriteResult};
pub use install_snapshot::SnapshotResponse;
pub use log_segment::LogSegment;
pub use precondition::Precondition;
pub use stream_append_error::StreamAppendError;
pub use transfer_leader::{TransferLeaderError, TransferLeaderRequest, TransferLeaderResponse};
pub use vote::{VoteRequest, VoteResponse};
pub(crate) use write::into_write_result;
pub use write::{WriteResponse, WriteResult};
pub use write_request::WriteRequest;
