//! 测试框架中的 RPC 响应枚举

use std::fmt;

use zenoh_raft::{
  RPCTypes, RaftTypeConfig,
  raft::{AppendEntriesResponse, SnapshotResponse, TransferLeaderResponse, VoteResponse},
};

/// 统一的 RPC 响应类型
#[derive(Debug, derive_more::From, derive_more::TryInto)]
pub enum RpcResponse<C: RaftTypeConfig> {
  AppendEntries(AppendEntriesResponse<C>),
  InstallFullSnapshot(SnapshotResponse<C>),
  Vote(VoteResponse<C>),
  TransferLeader(TransferLeaderResponse<C>),
}

impl<C: RaftTypeConfig> RpcResponse<C> {
  pub fn get_type(&self) -> RPCTypes {
    match self {
      RpcResponse::AppendEntries(_) => RPCTypes::AppendEntries,
      RpcResponse::InstallFullSnapshot(_) => RPCTypes::InstallSnapshot,
      RpcResponse::Vote(_) => RPCTypes::Vote,
      RpcResponse::TransferLeader(_) => RPCTypes::TransferLeader,
    }
  }
}

impl<C: RaftTypeConfig> fmt::Display for RpcResponse<C> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      RpcResponse::AppendEntries(resp) => write!(f, "AppendEntries({resp:?})"),
      RpcResponse::InstallFullSnapshot(resp) => write!(f, "InstallFullSnapshot({resp})"),
      RpcResponse::Vote(resp) => write!(f, "Vote({resp})"),
      RpcResponse::TransferLeader(resp) => write!(f, "TransferLeader({resp:?})"),
    }
  }
}
