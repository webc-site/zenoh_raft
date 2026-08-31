//! 测试框架中的 RPC 请求枚举

use std::fmt;

use zenoh_raft::OptionalSend;
use zenoh_raft::RPCTypes;
use zenoh_raft::RaftTypeConfig;
use zenoh_raft::alias::SnapshotOf;
use zenoh_raft::raft::AppendEntriesRequest;
use zenoh_raft::raft::TransferLeaderRequest;
use zenoh_raft::raft::VoteRequest;

/// 统一的 RPC 请求类型
#[derive(Debug, derive_more::From, derive_more::TryInto)]
pub enum RpcRequest<C, SD = ()>
where
    C: RaftTypeConfig,
    SD: fmt::Debug + OptionalSend + 'static,
{
    AppendEntries(AppendEntriesRequest<C>),
    InstallFullSnapshot(SnapshotOf<C, SD>),
    Vote(VoteRequest<C>),
    TransferLeader(TransferLeaderRequest<C>),
}

impl<C, SD> RpcRequest<C, SD>
where
    C: RaftTypeConfig,
    SD: fmt::Debug + OptionalSend + 'static,
{
    pub fn get_type(&self) -> RPCTypes {
        match self {
            RpcRequest::AppendEntries(_) => RPCTypes::AppendEntries,
            RpcRequest::InstallFullSnapshot(_) => RPCTypes::InstallSnapshot,
            RpcRequest::Vote(_) => RPCTypes::Vote,
            RpcRequest::TransferLeader(_) => RPCTypes::TransferLeader,
        }
    }
}

impl<C, SD> fmt::Display for RpcRequest<C, SD>
where
    C: RaftTypeConfig,
    SD: fmt::Debug + OptionalSend + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RpcRequest::AppendEntries(req) => write!(f, "AppendEntries({req})"),
            RpcRequest::InstallFullSnapshot(req) => write!(f, "InstallFullSnapshot({})", req.meta),
            RpcRequest::Vote(req) => write!(f, "Vote({req})"),
            RpcRequest::TransferLeader(req) => write!(f, "TransferLeader({})", req.from_leader()),
        }
    }
}
