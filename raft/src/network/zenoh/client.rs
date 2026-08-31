//! 基于 Zenoh 的 Raft 客户端网络实现

use std::fmt::Display;
use std::future::Future;
use std::io::Cursor;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use crate::OptionalSend;
use crate::RPCTypes;
use crate::RaftTypeConfig;
use crate::alias::EntryOf;
use crate::alias::LogIdOf;
use crate::alias::SnapshotMetaOf;
use crate::alias::SnapshotOf;
use crate::alias::VoteOf;
use crate::errors::NetworkError;
use crate::errors::RPCError;
use crate::errors::ReplicationClosed;
use crate::errors::StreamingError;
use crate::errors::Timeout;
use crate::errors::Unreachable;
use crate::network::RPCOption;
use crate::network::RaftNetwork;
use crate::network::RaftNetworkFactory;
use crate::network::zenoh::config::ZenohNetworkConfig;
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
use crate::raft::SnapshotResponse;
use crate::raft::TransferLeaderRequest;
use crate::raft::TransferLeaderResponse;
use crate::raft::VoteRequest;
use crate::raft::VoteResponse;

enum RawRpcError<N> {
    Network(String),
    Unreachable(String),
    Timeout {
        action: RPCTypes,
        target: N,
        timeout: Duration,
    },
}

impl<N: Clone> RawRpcError<N> {
    fn into_rpc_error<C: RaftTypeConfig<NodeId = N>>(self) -> RPCError<C> {
        match self {
            Self::Network(msg) => RPCError::Network(NetworkError::from_string(msg)),
            Self::Unreachable(msg) => RPCError::Unreachable(Unreachable::from_string(msg)),
            Self::Timeout {
                action,
                target,
                timeout,
            } => RPCError::Timeout(Timeout {
                action,
                id: target.clone(),
                target,
                timeout,
            }),
        }
    }

    fn into_streaming_error<C: RaftTypeConfig<NodeId = N>>(self) -> StreamingError<C> {
        match self {
            Self::Network(msg) => StreamingError::Network(NetworkError::from_string(msg)),
            Self::Unreachable(msg) => StreamingError::Unreachable(Unreachable::from_string(msg)),
            Self::Timeout {
                action,
                target,
                timeout,
            } => StreamingError::Timeout(Timeout {
                action,
                id: target.clone(),
                target,
                timeout,
            }),
        }
    }
}

/// 预计算的 Zenoh RPC 路由键，避免在热路径上进行字符串格式化与堆内存分配
#[derive(Clone, Debug)]
pub struct ZenohRpcKeys {
    pub append_entries: String,
    pub vote: String,
    pub pre_vote: String,
    pub snapshot: String,
    pub transfer_leader: String,
}

impl ZenohRpcKeys {
    pub fn new(key_prefix: &str, target: &impl Display) -> Self {
        let base_key = format!("{key_prefix}/{target}");
        Self {
            append_entries: format!("{base_key}/{RPC_APPEND_ENTRIES}"),
            vote: format!("{base_key}/{RPC_VOTE}"),
            pre_vote: format!("{base_key}/{RPC_PRE_VOTE}"),
            snapshot: format!("{base_key}/{RPC_SNAPSHOT}"),
            transfer_leader: format!("{base_key}/{RPC_TRANSFER_LEADER}"),
        }
    }
}

/// 基于 Zenoh 的 Raft 网络工厂
#[derive(Clone)]
pub struct ZenohNetworkFactory<C: RaftTypeConfig> {
    session: Arc<zenoh::Session>,
    config: ZenohNetworkConfig,
    _marker: PhantomData<C>,
}

impl<C: RaftTypeConfig> ZenohNetworkFactory<C> {
    /// 创建新的 Zenoh 网络工厂
    pub fn new(session: Arc<zenoh::Session>, config: ZenohNetworkConfig) -> Self {
        Self {
            session,
            config,
            _marker: PhantomData,
        }
    }

    /// 获取底层 Zenoh 会话引用
    pub fn session(&self) -> &Arc<zenoh::Session> {
        &self.session
    }

    /// 获取网络配置引用
    pub fn config(&self) -> &ZenohNetworkConfig {
        &self.config
    }
}

impl<C: RaftTypeConfig> RaftNetworkFactory<C> for ZenohNetworkFactory<C>
where
    C::NodeId: Display + bitcode::Encode + for<'a> bitcode::Decode<'a>,
    EntryOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
    VoteOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
    LogIdOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
    SnapshotMetaOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
{
    type Network = ZenohNetwork<C>;

    async fn new_client(&mut self, target: C::NodeId, _node: &C::Node) -> Self::Network {
        let keys = ZenohRpcKeys::new(&self.config.key_prefix, &target);
        ZenohNetwork {
            target,
            keys,
            session: self.session.clone(),
            config: self.config.clone(),
            _marker: PhantomData,
        }
    }
}

/// 基于 Zenoh 的目标节点 Raft 网络客户端
pub struct ZenohNetwork<C: RaftTypeConfig> {
    target: C::NodeId,
    keys: ZenohRpcKeys,
    session: Arc<zenoh::Session>,
    config: ZenohNetworkConfig,
    _marker: PhantomData<C>,
}

impl<C: RaftTypeConfig> ZenohNetwork<C>
where
    C::NodeId: Display,
{
    async fn send_raw_query<Req, Resp>(
        &self,
        key: &str,
        action: RPCTypes,
        option: &RPCOption,
        req: &Req,
    ) -> Result<Resp, RawRpcError<C::NodeId>>
    where
        Req: bitcode::Encode,
        Resp: for<'a> bitcode::Decode<'a>,
    {
        let timeout = option.soft_ttl();
        let payload = bitcode::encode(req);

        let replies = self
            .session
            .get(key)
            .payload(payload)
            .target(self.config.query_target)
            .timeout(timeout)
            .await
            .map_err(|e| RawRpcError::Unreachable(e.to_string()))?;

        if let Ok(reply) = replies.recv_async().await {
            match reply.result() {
                Ok(sample) => {
                    let bytes = sample.payload().to_bytes();
                    let resp: Resp =
                        bitcode::decode(&bytes).map_err(|e| RawRpcError::Network(e.to_string()))?;
                    return Ok(resp);
                }
                Err(err) => {
                    let err_str = err
                        .payload()
                        .try_to_string()
                        .map(|s| s.into_owned())
                        .unwrap_or_else(|_| "Zenoh query error".to_string());
                    return Err(RawRpcError::Unreachable(err_str));
                }
            }
        }

        Err(RawRpcError::Timeout {
            action,
            target: self.target.clone(),
            timeout,
        })
    }

    async fn send_query<Req, Resp>(
        &self,
        key: &str,
        action: RPCTypes,
        option: &RPCOption,
        req: &Req,
    ) -> Result<Resp, RPCError<C>>
    where
        Req: bitcode::Encode,
        Resp: for<'a> bitcode::Decode<'a>,
    {
        self.send_raw_query(key, action, option, req)
            .await
            .map_err(|e| e.into_rpc_error())
    }

    async fn send_streaming_query<Req, Resp>(
        &self,
        key: &str,
        action: RPCTypes,
        option: &RPCOption,
        req: &Req,
    ) -> Result<Resp, StreamingError<C>>
    where
        Req: bitcode::Encode,
        Resp: for<'a> bitcode::Decode<'a>,
    {
        self.send_raw_query(key, action, option, req)
            .await
            .map_err(|e| e.into_streaming_error())
    }
}

impl<C: RaftTypeConfig> RaftNetwork<C> for ZenohNetwork<C>
where
    C::NodeId: Display + bitcode::Encode + for<'a> bitcode::Decode<'a>,
    EntryOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
    VoteOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
    LogIdOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
    SnapshotMetaOf<C>: bitcode::Encode + for<'a> bitcode::Decode<'a>,
{
    type SnapshotData = Cursor<Vec<u8>>;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<C>, RPCError<C>> {
        let wire_req = WireAppendEntriesReq::from(rpc);
        let wire_resp: WireAppendEntriesResp<VoteOf<C>, LogIdOf<C>> = self
            .send_query(
                &self.keys.append_entries,
                RPCTypes::AppendEntries,
                &option,
                &wire_req,
            )
            .await?;
        Ok(wire_resp.into())
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<C>,
        option: RPCOption,
    ) -> Result<VoteResponse<C>, RPCError<C>> {
        let wire_req = WireVoteReq::from(rpc);
        let wire_resp: WireVoteResp<VoteOf<C>, LogIdOf<C>> = self
            .send_query(&self.keys.vote, RPCTypes::Vote, &option, &wire_req)
            .await?;
        Ok(wire_resp.into())
    }

    async fn pre_vote(
        &mut self,
        mut rpc: VoteRequest<C>,
        option: RPCOption,
    ) -> Result<VoteResponse<C>, RPCError<C>> {
        rpc.is_pre_vote = true;
        let wire_req = WireVoteReq::from(rpc);
        let wire_resp: WireVoteResp<VoteOf<C>, LogIdOf<C>> = self
            .send_query(&self.keys.pre_vote, RPCTypes::Vote, &option, &wire_req)
            .await?;
        Ok(wire_resp.into())
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<C>,
        snapshot: SnapshotOf<C, Self::SnapshotData>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<C>, StreamingError<C>> {
        let wire_payload = WireSnapshotPayload {
            vote,
            meta: snapshot.meta,
            data: snapshot.snapshot.into_inner(),
        };

        let wire_resp: WireSnapshotResp<VoteOf<C>> = self
            .send_streaming_query(
                &self.keys.snapshot,
                RPCTypes::InstallSnapshot,
                &option,
                &wire_payload,
            )
            .await?;

        Ok(wire_resp.into())
    }

    async fn transfer_leader(
        &mut self,
        req: TransferLeaderRequest<C>,
        option: RPCOption,
    ) -> Result<TransferLeaderResponse<C>, RPCError<C>> {
        let wire_req = WireTransferLeaderReq::from(req);
        let wire_res: Result<(), WireTransferLeaderErr<VoteOf<C>, LogIdOf<C>>> = self
            .send_query(
                &self.keys.transfer_leader,
                RPCTypes::TransferLeader,
                &option,
                &wire_req,
            )
            .await?;

        Ok(wire_res.map_err(Into::into))
    }
}
