//! The Raft storage interface and data types.
//!
//! This module defines traits and types for implementing Raft log storage and state machine:
//!
//! ## Core Traits
//!
//! - [`RaftLogStorage`] - Persistent log storage for Raft entries and vote state
//! - [`RaftStateMachine`] - Application state machine that applies committed log entries
//! - [`RaftLogReader`] - Reader interface for accessing stored log entries
//! - [`RaftSnapshotBuilder`] - Builder interface for creating snapshots
//!
//! ## Key Types
//!
//! - [`LogState`] - Current state of log storage (first/last log IDs)
//! - [`Snapshot`] - Container for snapshot data and metadata
//! - [`SnapshotMeta`] - Snapshot metadata (last log ID, membership)
//!
//! ## Usage
//!
//! Applications implement [`RaftLogStorage`] and [`RaftStateMachine`] to provide
//! persistence for Raft. These implementations are passed to [`Raft::new()`](crate::Raft::new)
//! to create a Raft node.
//!
//! See the [Getting Started Guide](crate::docs::getting_started) and
//! [State Machine Component](crate::docs::components::state_machine) documentation
//! for implementation details and examples.

mod apply_responder;
mod apply_responder_inner;
mod callback;
pub(crate) mod entry_responder;
mod helper;
mod log_reader_ext;
mod log_state;
mod raft_log_reader;
mod raft_log_storage;
mod raft_log_storage_ext;
mod raft_snapshot_builder;
mod raft_state_machine;
mod snapshot;
mod snapshot_meta;
mod snapshot_signature;

pub use self::apply_responder::ApplyResponder;
pub use self::callback::IOFlushed;
pub use self::entry_responder::EntryResponder;
pub use self::helper::StorageHelper;
pub use self::log_reader_ext::RaftLogReaderExt;
pub use self::log_state::LogState;
pub use self::raft_log_reader::LeaderBoundedStreamError;
pub use self::raft_log_reader::LeaderBoundedStreamResult;
pub use self::raft_log_reader::RaftLogReader;
pub use self::raft_log_storage::RaftLogStorage;
pub use self::raft_log_storage_ext::RaftLogStorageExt;
pub use self::raft_snapshot_builder::RaftSnapshotBuilder;
pub use self::raft_state_machine::RaftStateMachine;
pub use self::snapshot::Snapshot;
pub use self::snapshot_meta::SnapshotMeta;
pub use self::snapshot_signature::SnapshotSignature;
