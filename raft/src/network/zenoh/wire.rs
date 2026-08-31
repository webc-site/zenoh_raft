//! Zenoh 传输层 RPC 消息格式定义

use std::io::Cursor;

use bitcode::Decode;
use bitcode::Encode;

use crate::RaftTypeConfig;
use crate::alias::EntryOf;
use crate::alias::LogIdOf;
use crate::alias::VoteOf;
use crate::node::Node;
use crate::node::NodeId;
use crate::raft::AppendEntriesRequest;
use crate::raft::AppendEntriesResponse;
use crate::raft::SnapshotResponse;
use crate::raft::TransferLeaderError;
use crate::raft::TransferLeaderRequest;
use crate::raft::VoteRequest;
use crate::raft::VoteResponse;
use crate::storage::Snapshot;
use crate::storage::SnapshotMeta;
use crate::vote::RaftCommittedLeaderId;

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

impl<C: RaftTypeConfig> From<AppendEntriesRequest<C>>
    for WireAppendEntriesReq<VoteOf<C>, LogIdOf<C>, EntryOf<C>>
{
    #[inline]
    fn from(req: AppendEntriesRequest<C>) -> Self {
        Self {
            vote: req.vote,
            prev_log_id: req.prev_log_id,
            entries: req.entries,
            leader_commit: req.leader_commit,
        }
    }
}

impl<C: RaftTypeConfig> From<WireAppendEntriesReq<VoteOf<C>, LogIdOf<C>, EntryOf<C>>>
    for AppendEntriesRequest<C>
{
    #[inline]
    fn from(req: WireAppendEntriesReq<VoteOf<C>, LogIdOf<C>, EntryOf<C>>) -> Self {
        Self {
            vote: req.vote,
            prev_log_id: req.prev_log_id,
            entries: req.entries,
            leader_commit: req.leader_commit,
        }
    }
}

/// AppendEntries RPC 响应线格式
#[derive(Encode, Decode)]
pub enum WireAppendEntriesResp<V, L> {
    Success,
    PartialSuccess(Option<L>),
    Conflict,
    HigherVote(V),
}

impl<C: RaftTypeConfig> From<AppendEntriesResponse<C>>
    for WireAppendEntriesResp<VoteOf<C>, LogIdOf<C>>
{
    #[inline]
    fn from(resp: AppendEntriesResponse<C>) -> Self {
        match resp {
            AppendEntriesResponse::Success => Self::Success,
            AppendEntriesResponse::PartialSuccess(log_id) => Self::PartialSuccess(log_id),
            AppendEntriesResponse::Conflict => Self::Conflict,
            AppendEntriesResponse::HigherVote(vote) => Self::HigherVote(vote),
        }
    }
}

impl<C: RaftTypeConfig> From<WireAppendEntriesResp<VoteOf<C>, LogIdOf<C>>>
    for AppendEntriesResponse<C>
{
    #[inline]
    fn from(resp: WireAppendEntriesResp<VoteOf<C>, LogIdOf<C>>) -> Self {
        match resp {
            WireAppendEntriesResp::Success => Self::Success,
            WireAppendEntriesResp::PartialSuccess(log_id) => Self::PartialSuccess(log_id),
            WireAppendEntriesResp::Conflict => Self::Conflict,
            WireAppendEntriesResp::HigherVote(vote) => Self::HigherVote(vote),
        }
    }
}

/// Vote RPC 请求线格式
#[derive(Encode, Decode)]
pub struct WireVoteReq<V, L> {
    pub vote: V,
    pub last_log_id: Option<L>,
    pub leadership_transfer: bool,
    pub is_pre_vote: bool,
}

impl<C: RaftTypeConfig> From<VoteRequest<C>> for WireVoteReq<VoteOf<C>, LogIdOf<C>> {
    #[inline]
    fn from(req: VoteRequest<C>) -> Self {
        Self {
            vote: req.vote,
            last_log_id: req.last_log_id,
            leadership_transfer: req.leadership_transfer,
            is_pre_vote: req.is_pre_vote,
        }
    }
}

impl<V, L> WireVoteReq<V, L> {
    #[inline]
    pub fn into_vote_request<C: RaftTypeConfig>(self, is_pre_vote: bool) -> VoteRequest<C>
    where
        VoteOf<C>: From<V>,
        LogIdOf<C>: From<L>,
    {
        VoteRequest {
            vote: self.vote.into(),
            last_log_id: self.last_log_id.map(Into::into),
            leadership_transfer: self.leadership_transfer,
            is_pre_vote,
        }
    }
}

/// Vote RPC 响应线格式
#[derive(Encode, Decode)]
pub struct WireVoteResp<V, L> {
    pub vote: V,
    pub vote_granted: bool,
    pub last_log_id: Option<L>,
}

impl<C: RaftTypeConfig> From<VoteResponse<C>> for WireVoteResp<VoteOf<C>, LogIdOf<C>> {
    #[inline]
    fn from(resp: VoteResponse<C>) -> Self {
        Self {
            vote: resp.vote,
            vote_granted: resp.vote_granted,
            last_log_id: resp.last_log_id,
        }
    }
}

impl<C: RaftTypeConfig> From<WireVoteResp<VoteOf<C>, LogIdOf<C>>> for VoteResponse<C> {
    #[inline]
    fn from(resp: WireVoteResp<VoteOf<C>, LogIdOf<C>>) -> Self {
        Self {
            vote: resp.vote,
            vote_granted: resp.vote_granted,
            last_log_id: resp.last_log_id,
        }
    }
}

/// Snapshot 安装响应线格式
#[derive(Encode, Decode)]
pub struct WireSnapshotResp<V> {
    pub vote: V,
}

impl<C: RaftTypeConfig> From<SnapshotResponse<C>> for WireSnapshotResp<VoteOf<C>> {
    #[inline]
    fn from(resp: SnapshotResponse<C>) -> Self {
        Self { vote: resp.vote }
    }
}

impl<C: RaftTypeConfig> From<WireSnapshotResp<VoteOf<C>>> for SnapshotResponse<C> {
    #[inline]
    fn from(resp: WireSnapshotResp<VoteOf<C>>) -> Self {
        Self { vote: resp.vote }
    }
}

/// Leader 转移请求线格式
#[derive(Encode, Decode)]
pub struct WireTransferLeaderReq<V, N, L> {
    pub from_leader: V,
    pub to_node_id: N,
    pub last_log_id: Option<L>,
}

impl<C: RaftTypeConfig> From<TransferLeaderRequest<C>>
    for WireTransferLeaderReq<VoteOf<C>, C::NodeId, LogIdOf<C>>
{
    #[inline]
    fn from(req: TransferLeaderRequest<C>) -> Self {
        Self {
            from_leader: req.from_leader,
            to_node_id: req.to_node_id,
            last_log_id: req.last_log_id,
        }
    }
}

impl<C: RaftTypeConfig> From<WireTransferLeaderReq<VoteOf<C>, C::NodeId, LogIdOf<C>>>
    for TransferLeaderRequest<C>
{
    #[inline]
    fn from(req: WireTransferLeaderReq<VoteOf<C>, C::NodeId, LogIdOf<C>>) -> Self {
        TransferLeaderRequest::new(req.from_leader, req.to_node_id, req.last_log_id)
    }
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

impl<C: RaftTypeConfig> From<TransferLeaderError<C>>
    for WireTransferLeaderErr<VoteOf<C>, LogIdOf<C>>
{
    #[inline]
    fn from(err: TransferLeaderError<C>) -> Self {
        match err {
            TransferLeaderError::VoteChanged { expected, actual } => {
                Self::VoteChanged { expected, actual }
            }
            TransferLeaderError::LogNotFlushed { expected, actual } => {
                Self::LogNotFlushed { expected, actual }
            }
        }
    }
}

impl<C: RaftTypeConfig> From<WireTransferLeaderErr<VoteOf<C>, LogIdOf<C>>>
    for TransferLeaderError<C>
{
    #[inline]
    fn from(err: WireTransferLeaderErr<VoteOf<C>, LogIdOf<C>>) -> Self {
        match err {
            WireTransferLeaderErr::VoteChanged { expected, actual } => {
                Self::VoteChanged { expected, actual }
            }
            WireTransferLeaderErr::LogNotFlushed { expected, actual } => {
                Self::LogNotFlushed { expected, actual }
            }
        }
    }
}

/// 快照安装请求负载线格式
#[derive(Encode, Decode)]
pub struct WireSnapshotPayload<V, M> {
    pub vote: V,
    pub meta: M,
    pub data: Vec<u8>,
}

/// 基于内存 Cursor 的快照线类型别名
pub type WireSnapshot<CLID, NID, N> = Snapshot<CLID, NID, N, Cursor<Vec<u8>>>;

impl<V, M> WireSnapshotPayload<V, M> {
    #[inline]
    pub fn into_snapshot<CLID, NID, N>(self) -> (V, WireSnapshot<CLID, NID, N>)
    where
        CLID: RaftCommittedLeaderId,
        NID: NodeId,
        N: Node,
        SnapshotMeta<CLID, NID, N>: From<M>,
    {
        (
            self.vote,
            Snapshot {
                meta: self.meta.into(),
                snapshot: Cursor::new(self.data),
            },
        )
    }
}
