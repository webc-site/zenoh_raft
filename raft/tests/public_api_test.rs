//! Compile-time test to verify all public API paths remain accessible from external crates.
//!
//! This test ensures that refactoring or restructuring of modules
//! does not break any existing public API paths.

#![allow(unused_imports)]
#![allow(dead_code)]

use std::time::Duration;

use zenoh_raft::{
  AnyError, AppData, AppDataResponse, BasicNode, ChangeMembers, Config, ConfigError, EmptyNode,
  Entry, EntryPayload, ErrorSubject, ErrorVerb, FlushPoint, ForwardToLeaderRef, IOFlushed,
  LinearizerOption, LogId, LogIdOptionExt, LogIndexOptionExt, LogState, Membership,
  MembershipState, MessageSummary, Node, NodeId, NodeInfo, OptionalFeatures, OptionalSend,
  OptionalSync, Precondition, RPCOption, RPCTypes, Raft, RaftEntry, RaftLogReader, RaftLogStorage,
  RaftMetrics, RaftNetwork, RaftNetworkFactory, RaftPayload, RaftSnapshotBuilder, RaftState,
  RaftStateMachine, RaftTypeConfig, ReadPolicy, ServerState, Snapshot, SnapshotId, SnapshotMeta,
  SnapshotPolicy, StepDownPolicy, StorageError, StorageHelper, StoredMembership, ToStorageResult,
  TryAsRef, TypeConfigExt, Vote, WatchChangeHandle, ZenohNetwork, ZenohNetworkConfig,
  ZenohNetworkFactory, ZenohRaftServer, ZenohSessionBuilder, ZenohTlsConfig, add_async_trait,
  anyerror,
  errors::{
    AllowNextRevertError, ChangeMembershipError, ClientWriteError, EmptyMembership, Fatal,
    ForwardToLeader, InProgress, Infallible, InitializeError, LeaderChanged, LearnerNotFound,
    LinearizableReadError, MembershipError, NetworkError, NodeMetadataChanged, NodeNotFound,
    NotInMembers, Operation, QuorumNotEnough, RPCError, RaftError, ReplicationClosed,
    StreamingError, Timeout, UncommittedLeaderLog, Unreachable, UnsupportedMembershipTransition,
  },
  macros,
  network::zenoh::{
    RPC_APPEND_ENTRIES, RPC_PRE_VOTE, RPC_SNAPSHOT, RPC_TRANSFER_LEADER, RPC_VOTE,
    ZenohNetwork as ZenohNet, ZenohNetworkConfig as ZenohNetCfg,
    ZenohNetworkFactory as ZenohNetFactory, ZenohRaftServer as ZenohServer,
    ZenohSessionBuilder as ZenohBuilder, ZenohTlsConfig as ZenohTls,
  },
  raft::{
    AppendEntriesRequest, AppendEntriesResponse, ClientWriteResponse, ClientWriteResult,
    SnapshotResponse, StreamAppendError, StreamAppendResult, TransferLeaderRequest,
    TransferLeaderResponse, VoteRequest, VoteResponse, WriteRequest, WriteResponse, WriteResult,
  },
  rt,
};

#[test]
fn test_public_api_accessible() {
  let age = Duration::from_millis(100);
  let _linearizer_option = LinearizerOption::new(Some(age), true);

  let net_config = ZenohNetworkConfig::default();
  assert!(net_config.validate().is_ok());
  assert_eq!(RPC_APPEND_ENTRIES, "append_entries");
  assert_eq!(RPC_VOTE, "vote");
  assert_eq!(RPC_PRE_VOTE, "pre_vote");
  assert_eq!(RPC_SNAPSHOT, "snapshot");
  assert_eq!(RPC_TRANSFER_LEADER, "transfer_leader");
}
