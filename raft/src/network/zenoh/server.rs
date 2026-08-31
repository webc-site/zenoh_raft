//! 基于 Zenoh 的 Raft 服务端实现

use std::fmt::Display;
use std::io::Cursor;

use anyerror::AnyError;
use compio::runtime::spawn;
use crossfire::mpsc::unbounded_async;
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
use crate::raft::AppendEntriesResponse;
use crate::raft::TransferLeaderError;
use crate::raft::TransferLeaderRequest;
use crate::raft::VoteRequest;
use crate::storage::RaftStateMachine;
use crate::storage::Snapshot;

/// Zenoh Raft 服务端，用于监听远端 RPC 请求并分发处理
pub struct ZenohRaftServer {
    _queryable: Queryable<()>,
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
        let (tx, rx) = unbounded_async::<Query>();
        let raft_clone = raft.clone();

        spawn(async move {
            while let Ok(query) = rx.recv().await {
                let raft = raft_clone.clone();
                spawn(async move {
                    Self::dispatch_query(raft, query).await;
                })
                .detach();
            }
        })
        .detach();

        let queryable = session
            .declare_queryable(&selector)
            .callback(move |query| {
                let _ = tx.send(query);
            })
            .await
            .map_err(|e| AnyError::error(e.to_string()))?;

        Ok(Self {
            _queryable: queryable,
        })
    }

    async fn dispatch_query<C, SM>(raft: Raft<C, SM>, query: Query)
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
            _ => {}
        }
    }

    async fn handle_append_entries<C, SM>(raft: Raft<C, SM>, query: Query)
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
        let payload_bytes = query.payload().map(|p| p.to_bytes());
        let payload: &[u8] = payload_bytes.as_deref().unwrap_or(&[]);

        if let Ok(wire_req) =
            bitcode::decode::<WireAppendEntriesReq<VoteOf<C>, LogIdOf<C>, EntryOf<C>>>(payload)
        {
            let req = AppendEntriesRequest {
                vote: wire_req.vote,
                prev_log_id: wire_req.prev_log_id,
                entries: wire_req.entries,
                leader_commit: wire_req.leader_commit,
            };
            if let Ok(resp) = raft.append_entries(req).await {
                let wire_resp = match resp {
                    AppendEntriesResponse::Success => WireAppendEntriesResp::Success,
                    AppendEntriesResponse::PartialSuccess(log_id) => {
                        WireAppendEntriesResp::PartialSuccess(log_id)
                    }
                    AppendEntriesResponse::Conflict => WireAppendEntriesResp::Conflict,
                    AppendEntriesResponse::HigherVote(v) => WireAppendEntriesResp::HigherVote(v),
                };
                let resp_bytes = bitcode::encode(&wire_resp);
                let _ = query.reply(query.key_expr().clone(), resp_bytes).await;
            }
        }
    }

    async fn handle_vote<C, SM>(raft: Raft<C, SM>, query: Query, is_pre_vote: bool)
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
        let payload_bytes = query.payload().map(|p| p.to_bytes());
        let payload: &[u8] = payload_bytes.as_deref().unwrap_or(&[]);

        if let Ok(wire_req) = bitcode::decode::<WireVoteReq<VoteOf<C>, LogIdOf<C>>>(payload) {
            let req = VoteRequest {
                vote: wire_req.vote,
                last_log_id: wire_req.last_log_id,
                leadership_transfer: wire_req.leadership_transfer,
                is_pre_vote,
            };
            let res = if is_pre_vote {
                raft.pre_vote(req).await
            } else {
                raft.vote(req).await
            };
            if let Ok(resp) = res {
                let wire_resp = WireVoteResp {
                    vote: resp.vote,
                    vote_granted: resp.vote_granted,
                    last_log_id: resp.last_log_id,
                };
                let resp_bytes = bitcode::encode(&wire_resp);
                let _ = query.reply(query.key_expr().clone(), resp_bytes).await;
            }
        }
    }

    async fn handle_snapshot<C, SM>(raft: Raft<C, SM>, query: Query)
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
        let payload_bytes = query.payload().map(|p| p.to_bytes());
        let payload: &[u8] = payload_bytes.as_deref().unwrap_or(&[]);

        if let Ok(wire_snap) =
            bitcode::decode::<WireSnapshotPayload<VoteOf<C>, SnapshotMetaOf<C>>>(payload)
        {
            let snapshot = Snapshot {
                meta: wire_snap.meta,
                snapshot: Cursor::new(wire_snap.data),
            };
            if let Ok(resp) = raft.install_full_snapshot(wire_snap.vote, snapshot).await {
                let wire_resp = WireSnapshotResp { vote: resp.vote };
                let resp_bytes = bitcode::encode(&wire_resp);
                let _ = query.reply(query.key_expr().clone(), resp_bytes).await;
            }
        }
    }

    async fn handle_transfer_leader<C, SM>(raft: Raft<C, SM>, query: Query)
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
        let payload_bytes = query.payload().map(|p| p.to_bytes());
        let payload: &[u8] = payload_bytes.as_deref().unwrap_or(&[]);

        if let Ok(wire_req) =
            bitcode::decode::<WireTransferLeaderReq<VoteOf<C>, C::NodeId, LogIdOf<C>>>(payload)
        {
            let req = TransferLeaderRequest::new(
                wire_req.from_leader,
                wire_req.to_node_id,
                wire_req.last_log_id,
            );
            if let Ok(res) = raft.handle_transfer_leader(req).await {
                let wire_res = match res {
                    Ok(()) => Ok(()),
                    Err(TransferLeaderError::VoteChanged { expected, actual }) => {
                        Err(WireTransferLeaderErr::VoteChanged { expected, actual })
                    }
                    Err(TransferLeaderError::LogNotFlushed { expected, actual }) => {
                        Err(WireTransferLeaderErr::LogNotFlushed { expected, actual })
                    }
                };
                let resp_bytes = bitcode::encode(&wire_res);
                let _ = query.reply(query.key_expr().clone(), resp_bytes).await;
            }
        }
    }
}
