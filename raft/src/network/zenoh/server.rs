use std::fmt::Display;
use std::io::Cursor;

use anyerror::AnyError;
use compio::runtime::spawn;
use zenoh::query::Query;
use zenoh::query::Queryable;

use crate::OptionalSend;
use crate::OptionalSync;
use crate::Raft;
use crate::RaftTypeConfig;
use crate::alias::EntryOf;
use crate::alias::LogIdOf;
use crate::alias::SnapshotMetaOf;
use crate::alias::VoteOf;
use crate::network::zenoh::wire::RPC_APPEND_ENTRIES;
use crate::network::zenoh::wire::RPC_PRE_VOTE;
use crate::network::zenoh::wire::RPC_SNAPSHOT;
use crate::network::zenoh::wire::RPC_TRANSFER_LEADER;
use crate::network::zenoh::wire::RPC_VOTE;
use crate::network::zenoh::wire::WireAppendEntriesReq;
use crate::network::zenoh::wire::WireAppendEntriesResp;
use crate::network::zenoh::wire::WireSnapshotPayload;
use crate::network::zenoh::wire::WireSnapshotResp;
use crate::network::zenoh::wire::WireTransferLeaderErr;
use crate::network::zenoh::wire::WireTransferLeaderReq;
use crate::network::zenoh::wire::WireVoteReq;
use crate::network::zenoh::wire::WireVoteResp;
use crate::raft::AppendEntriesRequest;
use crate::raft::TransferLeaderRequest;
use crate::storage::RaftStateMachine;

/// Zenoh Raft 服务端，用于监听远端 RPC 请求并分发处理
pub struct ZenohRaftServer {
    _queryable: Queryable<()>,
}

#[inline]
async fn decode_or_reply_err<T: for<'de> bitcode::Decode<'de>>(query: &Query) -> Option<T> {
    let res = match query.payload() {
        Some(p) => {
            let bytes = p.to_bytes();
            bitcode::decode(&bytes)
        }
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

impl ZenohRaftServer {
    /// 启动 Zenoh Raft 服务端
    pub async fn start<C, SM>(
        session: &zenoh::Session,
        raft: Raft<C, SM>,
        key_prefix: &str,
        node_id: C::NodeId,
    ) -> Result<Self, AnyError>
    where
        C: RaftTypeConfig,
        SM: RaftStateMachine<C, SnapshotData = Cursor<Vec<u8>>>
            + OptionalSend
            + OptionalSync
            + 'static,
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
        SM: RaftStateMachine<C, SnapshotData = Cursor<Vec<u8>>>
            + OptionalSend
            + OptionalSync
            + 'static,
        C::NodeId: Display + bitcode::Encode + for<'a> bitcode::Decode<'a>,
        EntryOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
        VoteOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
        LogIdOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
        SnapshotMetaOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
    {
        let rpc = query.key_expr().as_str().rsplit_once('/').map(|(_, r)| r);
        match rpc {
            Some(RPC_APPEND_ENTRIES) => Self::handle_append_entries(raft, query).await,
            Some(RPC_VOTE) => Self::handle_vote(raft, query, false).await,
            Some(RPC_PRE_VOTE) => Self::handle_vote(raft, query, true).await,
            Some(RPC_SNAPSHOT) => Self::handle_snapshot(raft, query).await,
            Some(RPC_TRANSFER_LEADER) => Self::handle_transfer_leader(raft, query).await,
            _ => {
                reply_err(&query, "unknown RPC route").await;
            }
        }
    }

    async fn handle_append_entries<C, SM>(raft: &Raft<C, SM>, query: Query)
    where
        C: RaftTypeConfig,
        SM: RaftStateMachine<C, SnapshotData = Cursor<Vec<u8>>>
            + OptionalSend
            + OptionalSync
            + 'static,
        C::NodeId: Display + bitcode::Encode + for<'a> bitcode::Decode<'a>,
        EntryOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
        VoteOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
        LogIdOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
        SnapshotMetaOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
    {
        let Some(wire_req) =
            decode_or_reply_err::<WireAppendEntriesReq<VoteOf<C>, LogIdOf<C>, EntryOf<C>>>(&query)
                .await
        else {
            return;
        };

        let req = AppendEntriesRequest::from(wire_req);

        match raft.append_entries(req).await {
            Ok(resp) => {
                let wire_resp = WireAppendEntriesResp::from(resp);
                reply_ok(&query, &wire_resp).await;
            }
            Err(e) => {
                reply_err(&query, format!("append_entries error: {e}")).await;
            }
        }
    }

    async fn handle_vote<C, SM>(raft: &Raft<C, SM>, query: Query, is_pre_vote: bool)
    where
        C: RaftTypeConfig,
        SM: RaftStateMachine<C, SnapshotData = Cursor<Vec<u8>>>
            + OptionalSend
            + OptionalSync
            + 'static,
        C::NodeId: Display + bitcode::Encode + for<'a> bitcode::Decode<'a>,
        EntryOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
        VoteOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
        LogIdOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
        SnapshotMetaOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
    {
        let Some(wire_req) =
            decode_or_reply_err::<WireVoteReq<VoteOf<C>, LogIdOf<C>>>(&query).await
        else {
            return;
        };

        let req = wire_req.into_vote_request(is_pre_vote);

        let res = if is_pre_vote {
            raft.pre_vote(req).await
        } else {
            raft.vote(req).await
        };

        match res {
            Ok(resp) => {
                let wire_resp = WireVoteResp::from(resp);
                reply_ok(&query, &wire_resp).await;
            }
            Err(e) => {
                reply_err(&query, format!("vote error: {e}")).await;
            }
        }
    }

    async fn handle_snapshot<C, SM>(raft: &Raft<C, SM>, query: Query)
    where
        C: RaftTypeConfig,
        SM: RaftStateMachine<C, SnapshotData = Cursor<Vec<u8>>>
            + OptionalSend
            + OptionalSync
            + 'static,
        C::NodeId: Display + bitcode::Encode + for<'a> bitcode::Decode<'a>,
        EntryOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
        VoteOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
        LogIdOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
        SnapshotMetaOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
    {
        let Some(wire_snap) =
            decode_or_reply_err::<WireSnapshotPayload<VoteOf<C>, SnapshotMetaOf<C>>>(&query).await
        else {
            return;
        };

        let (vote, snapshot) = wire_snap.into_snapshot();

        match raft.install_full_snapshot(vote, snapshot).await {
            Ok(resp) => {
                let wire_resp = WireSnapshotResp::from(resp);
                reply_ok(&query, &wire_resp).await;
            }
            Err(e) => {
                reply_err(&query, format!("snapshot error: {e}")).await;
            }
        }
    }

    async fn handle_transfer_leader<C, SM>(raft: &Raft<C, SM>, query: Query)
    where
        C: RaftTypeConfig,
        SM: RaftStateMachine<C, SnapshotData = Cursor<Vec<u8>>>
            + OptionalSend
            + OptionalSync
            + 'static,
        C::NodeId: Display + bitcode::Encode + for<'a> bitcode::Decode<'a>,
        EntryOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
        VoteOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
        LogIdOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
        SnapshotMetaOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
    {
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
            Err(e) => {
                reply_err(&query, format!("transfer_leader error: {e}")).await;
            }
        }
    }
}
