#![recursion_limit = "256"]

// Rust Edition 2024 - zenoh_raft

#[cfg(test)]
#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

macro_rules! func_name {
  () => {{
    // Internal helper function to obtain type name containing function name at compile time
    // 内部辅助函数，用于在编译期获取包含函数名的类型名
    fn f() {}
    fn type_name_of<T>(_: T) -> &'static str {
      std::any::type_name::<T>()
    }
    let name = type_name_of(f);
    // Return &'static str to avoid runtime allocation
    // 返回 &'static str 避免运行时分配
    name.strip_suffix("::f").unwrap_or(name)
  }};
}

pub mod async_runtime;
pub mod base;
pub mod batch;
pub mod change_members;
pub mod config;
pub mod core;
pub mod display_ext;
pub mod engine;
pub mod entry;
pub mod errors;
pub mod extensions;
pub mod impls;
pub mod log_id;
pub mod log_id_range;
pub mod membership;
pub mod metrics;
pub mod network;
pub mod node;
pub mod progress;
pub mod proposer;
pub mod quorum;
pub mod raft;
pub mod raft_state;
pub mod raft_types;
pub mod replication;
pub mod runtime;
pub mod storage;
pub mod summary;
pub mod testing;
pub mod try_as_ref;
pub mod type_config;
pub mod utime;
pub mod vote;

pub use errors as error;
pub use zenoh_raft_macros as macros;
pub mod alias {
  pub use crate::type_config::alias::*;
}

use std::fmt;

pub use anyerror::{self, AnyError};
pub use errors::storage_error::{ErrorSubject, ErrorVerb, StorageError, ToStorageResult};
pub use zenoh_raft_macros::add_async_trait;

pub use self::storage::{
  EntryResponder, IOFlushed, LogState, RaftLogReader, RaftLogStorage, RaftSnapshotBuilder,
  RaftStateMachine, Snapshot, SnapshotMeta, StorageHelper,
};
pub use crate::{
  async_runtime as rt,
  async_runtime::{mpsc_channel, task_local, task_local::TaskLocalFuture, watch, *},
  base::{OptionalFeatures, OptionalSend, OptionalSync},
  change_members::ChangeMembers,
  config::{Config, ConfigError, SnapshotPolicy, StepDownPolicy},
  core::{ServerState, io_flush_tracking::FlushPoint},
  entry::{Entry, EntryPayload, RaftEntry, RaftPayload},
  errors::ForwardToLeaderRef,
  extensions::Extensions,
  log_id::{LogId, LogIdOptionExt, LogIndexOptionExt},
  membership::{Membership, StoredMembership},
  metrics::RaftMetrics,
  network::{
    RPCOption, RPCTypes, RaftNetwork, RaftNetworkFactory, ZenohNetwork, ZenohNetworkConfig,
    ZenohNetworkFactory, ZenohRaftServer, ZenohSessionBuilder, ZenohTlsConfig,
  },
  node::{BasicNode, EmptyNode, Node, NodeId, NodeInfo},
  raft::{Precondition, Raft, ReadPolicy, WatchChangeHandle, linearizable_read::LinearizerOption},
  raft_state::{MembershipState, RaftState},
  raft_types::SnapshotId,
  summary::MessageSummary,
  try_as_ref::TryAsRef,
  type_config::{RaftTypeConfig, TypeConfigExt},
  vote::Vote,
};

/// Application data payload trait
/// 应用层数据 Trait
pub trait AppData: OptionalFeatures + fmt::Debug + fmt::Display + 'static {}
impl<T> AppData for T where T: OptionalFeatures + fmt::Debug + fmt::Display + 'static {}

/// Application response data payload trait
/// 应用层响应数据 Trait
pub trait AppDataResponse: OptionalFeatures + 'static {}
impl<T> AppDataResponse for T where T: OptionalFeatures + 'static {}
