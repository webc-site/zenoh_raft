//! Zenoh 传输层 RPC 消息格式定义

use bitcode::Decode;
use bitcode::Encode;

/// AppendEntries RPC 路由后缀
pub const RPC_APPEND_ENTRIES: &str = "append_entries";
/// Vote RPC 路由后缀
pub const RPC_VOTE: &str = "vote";
/// PreVote RPC 路由后缀
pub const RPC_PRE_VOTE: &str = "pre_vote";
/// Snapshot RPC 路由后缀
pub const RPC_SNAPSHOT: &str = "snapshot";
/// TransferLeader RPC 路由后缀
pub const RPC_TRANSFER_LEADER: &str = "transfer_leader";

/// AppendEntries RPC 请求线格式
#[derive(Encode, Decode)]
pub struct WireAppendEntriesReq<V, L, E> {
    pub vote: V,
    pub prev_log_id: Option<L>,
    pub entries: Vec<E>,
    pub leader_commit: Option<L>,
}

/// AppendEntries RPC 响应线格式
#[derive(Encode, Decode)]
pub enum WireAppendEntriesResp<V, L> {
    Success,
    PartialSuccess(Option<L>),
    Conflict,
    HigherVote(V),
}

/// Vote RPC 请求线格式
#[derive(Encode, Decode)]
pub struct WireVoteReq<V, L> {
    pub vote: V,
    pub last_log_id: Option<L>,
    pub leadership_transfer: bool,
    pub is_pre_vote: bool,
}

/// Vote RPC 响应线格式
#[derive(Encode, Decode)]
pub struct WireVoteResp<V, L> {
    pub vote: V,
    pub vote_granted: bool,
    pub last_log_id: Option<L>,
}

/// Snapshot 安装响应线格式
#[derive(Encode, Decode)]
pub struct WireSnapshotResp<V> {
    pub vote: V,
}

/// Leader 转移请求线格式
#[derive(Encode, Decode)]
pub struct WireTransferLeaderReq<V, N, L> {
    pub from_leader: V,
    pub to_node_id: N,
    pub last_log_id: Option<L>,
}

/// Leader 转移错误响应线格式
#[derive(Encode, Decode)]
pub enum WireTransferLeaderErr<V, L> {
    VoteChanged {
        expected: V,
        actual: V,
    },
    LogNotFlushed {
        expected: Option<L>,
        actual: Option<L>,
    },
}

/// 快照安装请求负载线格式
#[derive(Encode, Decode)]
pub struct WireSnapshotPayload<V, M> {
    pub vote: V,
    pub meta: M,
    pub data: Vec<u8>,
}
