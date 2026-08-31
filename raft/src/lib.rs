#![recursion_limit = "256"]

// Rust Edition 2024 - zenoh_raft

#[cfg(test)]
#[ctor::ctor(unsafe)]
fn _log_init() {
    log_init::init();
}

macro_rules! func_name {
    () => {{
        // 内部辅助函数，用于在编译期获取包含函数名的类型名
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);
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

pub use anyerror;
pub use anyerror::AnyError;
pub use errors::storage_error::ErrorSubject;
pub use errors::storage_error::ErrorVerb;
pub use errors::storage_error::StorageError;
pub use errors::storage_error::ToStorageResult;
pub use zenoh_raft_macros::add_async_trait;

pub use crate::async_runtime as rt;
pub use crate::async_runtime::mpsc_channel;
pub use crate::async_runtime::task_local;
pub use crate::async_runtime::task_local::TaskLocalFuture;
pub use crate::async_runtime::watch;
pub use crate::async_runtime::*;

pub use self::storage::EntryResponder;
pub use self::storage::IOFlushed;
pub use self::storage::LogState;
pub use self::storage::RaftLogReader;
pub use self::storage::RaftLogStorage;
pub use self::storage::RaftSnapshotBuilder;
pub use self::storage::RaftStateMachine;
pub use self::storage::Snapshot;
pub use self::storage::SnapshotMeta;
pub use self::storage::StorageHelper;

pub use crate::base::OptionalFeatures;
pub use crate::base::OptionalSend;
pub use crate::base::OptionalSync;
pub use crate::change_members::ChangeMembers;
pub use crate::config::Config;
pub use crate::config::ConfigError;
pub use crate::config::SnapshotPolicy;
pub use crate::config::StepDownPolicy;
pub use crate::core::ServerState;
pub use crate::core::io_flush_tracking::FlushPoint;
pub use crate::entry::Entry;
pub use crate::entry::EntryPayload;
pub use crate::entry::RaftEntry;
pub use crate::entry::RaftPayload;
pub use crate::errors::ForwardToLeaderRef;
pub use crate::extensions::Extensions;
pub use crate::log_id::LogId;
pub use crate::log_id::LogIdOptionExt;
pub use crate::log_id::LogIndexOptionExt;
pub use crate::membership::Membership;
pub use crate::membership::StoredMembership;
pub use crate::metrics::RaftMetrics;
pub use crate::network::RPCOption;
pub use crate::network::RPCTypes;
pub use crate::network::RaftNetwork;
pub use crate::network::RaftNetworkFactory;
pub use crate::network::ZenohNetwork;
pub use crate::network::ZenohNetworkConfig;
pub use crate::network::ZenohNetworkFactory;
pub use crate::network::ZenohRaftServer;
pub use crate::network::ZenohSessionBuilder;
pub use crate::network::ZenohTlsConfig;
pub use crate::node::BasicNode;
pub use crate::node::EmptyNode;
pub use crate::node::Node;
pub use crate::node::NodeId;
pub use crate::node::NodeInfo;
pub use crate::raft::Precondition;
pub use crate::raft::Raft;
pub use crate::raft::ReadPolicy;
pub use crate::raft::WatchChangeHandle;
pub use crate::raft::linearizable_read::LinearizerOption;
pub use crate::raft_state::MembershipState;
pub use crate::raft_state::RaftState;
pub use crate::raft_types::SnapshotId;
pub use crate::summary::MessageSummary;
pub use crate::try_as_ref::TryAsRef;
pub use crate::type_config::RaftTypeConfig;
pub use crate::type_config::TypeConfigExt;
pub use crate::vote::Vote;

use std::fmt;

/// 应用层数据 Trait
pub trait AppData: OptionalFeatures + fmt::Debug + fmt::Display + 'static {}
impl<T> AppData for T where T: OptionalFeatures + fmt::Debug + fmt::Display + 'static {}

/// 应用层响应数据 Trait
pub trait AppDataResponse: OptionalFeatures + 'static {}
impl<T> AppDataResponse for T where T: OptionalFeatures + 'static {}
