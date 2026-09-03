//! Zenoh 传输层 RPC 消息格式定义

use std::io::Cursor;

use bitcode::{Decode, Encode};

use crate::{
  RaftTypeConfig,
  alias::{EntryOf, LogIdOf, VoteOf},
  node::{Node, NodeId},
  raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, TransferLeaderError,
    TransferLeaderRequest, VoteRequest, VoteResponse,
  },
  storage::{Snapshot, SnapshotMeta},
  vote::RaftCommittedLeaderId,
};

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

impl<C: RaftTypeConfig> From<WireVoteReq<VoteOf<C>, LogIdOf<C>>> for VoteRequest<C> {
  #[inline]
  fn from(req: WireVoteReq<VoteOf<C>, LogIdOf<C>>) -> Self {
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

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_wire_append_entries_roundtrip() {
    let req = WireAppendEntriesReq {
      vote: 10u64,
      prev_log_id: Some(5u64),
      entries: vec![1u32, 2, 3],
      leader_commit: Some(4u64),
    };
    let encoded = bitcode::encode(&req);
    let decoded: WireAppendEntriesReq<u64, u64, u32> =
      bitcode::decode(&encoded).expect("decode wire req");
    assert_eq!(decoded.vote, req.vote);
    assert_eq!(decoded.prev_log_id, req.prev_log_id);
    assert_eq!(decoded.entries, req.entries);
    assert_eq!(decoded.leader_commit, req.leader_commit);

    let resp = WireAppendEntriesResp::<u64, u64>::PartialSuccess(Some(3));
    let encoded = bitcode::encode(&resp);
    let decoded: WireAppendEntriesResp<u64, u64> =
      bitcode::decode(&encoded).expect("decode wire resp");
    match decoded {
      WireAppendEntriesResp::PartialSuccess(Some(3)) => {}
      _ => panic!("unexpected decoded response"),
    }
  }

  #[test]
  fn test_wire_vote_roundtrip() {
    let req = WireVoteReq {
      vote: 42u64,
      last_log_id: Some(100u64),
      leadership_transfer: true,
      is_pre_vote: false,
    };
    let encoded = bitcode::encode(&req);
    let decoded: WireVoteReq<u64, u64> = bitcode::decode(&encoded).expect("decode wire vote");
    assert_eq!(decoded.vote, req.vote);
    assert_eq!(decoded.last_log_id, req.last_log_id);
    assert_eq!(decoded.leadership_transfer, req.leadership_transfer);
    assert_eq!(decoded.is_pre_vote, req.is_pre_vote);

    let resp = WireVoteResp {
      vote: 42u64,
      vote_granted: true,
      last_log_id: Some(100u64),
    };
    let encoded = bitcode::encode(&resp);
    let decoded: WireVoteResp<u64, u64> = bitcode::decode(&encoded).expect("decode wire vote resp");
    assert_eq!(decoded.vote, resp.vote);
    assert!(decoded.vote_granted);
    assert_eq!(decoded.last_log_id, resp.last_log_id);
  }

  #[test]
  fn test_wire_snapshot_and_transfer_leader_roundtrip() {
    let payload = WireSnapshotPayload {
      vote: 1u64,
      meta: 2u64,
      data: b"snapshot_payload_bytes".to_vec(),
    };
    let encoded = bitcode::encode(&payload);
    let decoded: WireSnapshotPayload<u64, u64> =
      bitcode::decode(&encoded).expect("decode wire snapshot");
    assert_eq!(decoded.vote, payload.vote);
    assert_eq!(decoded.meta, payload.meta);
    assert_eq!(decoded.data, payload.data);

    let resp = WireSnapshotResp { vote: 1u64 };
    let encoded = bitcode::encode(&resp);
    let decoded: WireSnapshotResp<u64> =
      bitcode::decode(&encoded).expect("decode wire snapshot resp");
    assert_eq!(decoded.vote, resp.vote);

    let req = WireTransferLeaderReq {
      from_leader: 10u64,
      to_node_id: 20u64,
      last_log_id: Some(30u64),
    };
    let encoded = bitcode::encode(&req);
    let decoded: WireTransferLeaderReq<u64, u64, u64> =
      bitcode::decode(&encoded).expect("decode transfer req");
    assert_eq!(decoded.from_leader, req.from_leader);
    assert_eq!(decoded.to_node_id, req.to_node_id);
    assert_eq!(decoded.last_log_id, req.last_log_id);

    let err = WireTransferLeaderErr::<u64, u64>::VoteChanged {
      expected: 10,
      actual: 20,
    };
    let encoded = bitcode::encode(&err);
    let decoded: WireTransferLeaderErr<u64, u64> =
      bitcode::decode(&encoded).expect("decode transfer err");
    match decoded {
      WireTransferLeaderErr::VoteChanged { expected, actual } => {
        assert_eq!(expected, 10);
        assert_eq!(actual, 20);
      }
      _ => panic!("unexpected transfer error"),
    }
  }
}
