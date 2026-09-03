//! Zenoh Raft server implementation for listening to and dispatching remote RPCs
//! 基于 Zenoh 的 Raft 服务端实现，用于监听并分发远端 RPC 请求

use std::{fmt::Display, io::Cursor};

use anyerror::AnyError;
use compio::runtime::spawn;
use zenoh::query::{Query, Queryable};

use super::wire::{
  RPC_APPEND_ENTRIES, RPC_PRE_VOTE, RPC_SNAPSHOT, RPC_TRANSFER_LEADER, RPC_VOTE,
  WireAppendEntriesReq, WireAppendEntriesResp, WireSnapshotPayload, WireSnapshotResp,
  WireTransferLeaderErr, WireTransferLeaderReq, WireVoteReq, WireVoteResp,
};
use crate::{
  OptionalSend, OptionalSync, Raft, RaftTypeConfig,
  alias::{EntryOf, LogIdOf, SnapshotMetaOf, VoteOf},
  raft::TransferLeaderRequest,
  storage::RaftStateMachine,
};

const ERR_UNKNOWN_ROUTE: &str = "unknown RPC route";

/// Zenoh Raft server for listening to remote RPC requests and dispatching them
/// Zenoh Raft 服务端，用于监听远端 RPC 请求并分发处理
pub struct ZenohRaftServer {
  _queryable: Queryable<()>,
}

#[inline]
async fn decode_or_reply_err<T: for<'de> bitcode::Decode<'de>>(query: &Query) -> Option<T> {
  let res = match query.payload() {
    Some(p) => bitcode::decode(&p.to_bytes()),
    None => bitcode::decode(&[]),
  };
  match res {
    Ok(val) => Some(val),
    Err(e) => {
      reply_err(query, format!("decode error: {e}")).await;
      None
    }
  }
}

#[inline]
async fn reply_ok<T: bitcode::Encode>(query: &Query, resp: &T) {
  let resp_bytes = bitcode::encode(resp);
  let _ = query.reply(query.key_expr(), resp_bytes).await;
}

#[inline]
async fn reply_err(query: &Query, err: impl AsRef<str>) {
  let _ = query.reply_err(err.as_ref()).await;
}

#[inline]
async fn reply_rpc_err(query: &Query, rpc: &str, err: impl Display) {
  reply_err(query, format!("{rpc} error: {err}")).await;
}

impl ZenohRaftServer {
  /// Start the Zenoh Raft server
  /// 启动 Zenoh Raft 服务端
  pub async fn start<C, SM>(
    session: &zenoh::Session,
    raft: Raft<C, SM>,
    key_prefix: &str,
    node_id: C::NodeId,
  ) -> Result<Self, AnyError>
  where
    C: RaftTypeConfig,
    SM: RaftStateMachine<C, SnapshotData = Cursor<Vec<u8>>> + OptionalSend + OptionalSync + 'static,
    C::NodeId: Display + bitcode::Encode + for<'a> bitcode::Decode<'a>,
    EntryOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
    VoteOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
    LogIdOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
    SnapshotMetaOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
  {
    let selector = format!("{key_prefix}/{node_id}/**");
    let queryable = session
      .declare_queryable(&selector)
      .callback(move |query| {
        let raft = raft.clone();
        spawn(async move {
          Self::dispatch_query(&raft, query).await;
        })
        .detach();
      })
      .await
      .map_err(|e| AnyError::error(e.to_string()))?;

    Ok(Self {
      _queryable: queryable,
    })
  }

  async fn dispatch_query<C, SM>(raft: &Raft<C, SM>, query: Query)
  where
    C: RaftTypeConfig,
    SM: RaftStateMachine<C, SnapshotData = Cursor<Vec<u8>>> + OptionalSend + OptionalSync + 'static,
    C::NodeId: Display + bitcode::Encode + for<'a> bitcode::Decode<'a>,
    EntryOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
    VoteOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
    LogIdOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
    SnapshotMetaOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
  {
    let rpc = query.key_expr().as_str().rsplit_once('/').map(|(_, r)| r);
    match rpc {
      Some(RPC_APPEND_ENTRIES) => {
        let Some(wire_req) =
          decode_or_reply_err::<WireAppendEntriesReq<VoteOf<C>, LogIdOf<C>, EntryOf<C>>>(&query)
            .await
        else {
          return;
        };
        match raft.append_entries(wire_req.into()).await {
          Ok(resp) => reply_ok(&query, &WireAppendEntriesResp::from(resp)).await,
          Err(e) => reply_rpc_err(&query, RPC_APPEND_ENTRIES, e).await,
        }
      }
      Some(RPC_VOTE) => {
        let Some(wire_req) =
          decode_or_reply_err::<WireVoteReq<VoteOf<C>, LogIdOf<C>>>(&query).await
        else {
          return;
        };
        match raft.vote(wire_req.into()).await {
          Ok(resp) => reply_ok(&query, &WireVoteResp::from(resp)).await,
          Err(e) => reply_rpc_err(&query, RPC_VOTE, e).await,
        }
      }
      Some(RPC_PRE_VOTE) => {
        let Some(wire_req) =
          decode_or_reply_err::<WireVoteReq<VoteOf<C>, LogIdOf<C>>>(&query).await
        else {
          return;
        };
        let req = wire_req.into_vote_request(true);
        match raft.pre_vote(req).await {
          Ok(resp) => reply_ok(&query, &WireVoteResp::from(resp)).await,
          Err(e) => reply_rpc_err(&query, RPC_PRE_VOTE, e).await,
        }
      }
      Some(RPC_SNAPSHOT) => {
        let Some(wire_snap) =
          decode_or_reply_err::<WireSnapshotPayload<VoteOf<C>, SnapshotMetaOf<C>>>(&query).await
        else {
          return;
        };
        let (vote, snapshot) = wire_snap.into_snapshot();
        match raft.install_full_snapshot(vote, snapshot).await {
          Ok(resp) => reply_ok(&query, &WireSnapshotResp::from(resp)).await,
          Err(e) => reply_rpc_err(&query, RPC_SNAPSHOT, e).await,
        }
      }
      Some(RPC_TRANSFER_LEADER) => {
        let Some(wire_req) =
          decode_or_reply_err::<WireTransferLeaderReq<VoteOf<C>, C::NodeId, LogIdOf<C>>>(&query)
            .await
        else {
          return;
        };
        let req = TransferLeaderRequest::from(wire_req);
        match raft.handle_transfer_leader(req).await {
          Ok(res) => {
            let wire_res = res.map_err(WireTransferLeaderErr::from);
            reply_ok(&query, &wire_res).await;
          }
          Err(e) => reply_rpc_err(&query, RPC_TRANSFER_LEADER, e).await,
        }
      }
      _ => {
        reply_err(&query, ERR_UNKNOWN_ROUTE).await;
      }
    }
  }
}
