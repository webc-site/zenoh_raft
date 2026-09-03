//! Raft client network implementation based on Zenoh
//! 基于 Zenoh 的 Raft 客户端网络实现

use std::{
  fmt::Display,
  future::Future,
  io::Cursor,
  marker::PhantomData,
  sync::Arc,
  time::{Duration, Instant},
};

use anyerror::AnyError;
use futures_util::FutureExt;
use zenoh::key_expr::KeyExpr;

use super::{
  config::ZenohNetworkConfig,
  wire::{
    RPC_APPEND_ENTRIES, RPC_PRE_VOTE, RPC_SNAPSHOT, RPC_TRANSFER_LEADER, RPC_VOTE,
    WireAppendEntriesReq, WireAppendEntriesResp, WireSnapshotPayload, WireSnapshotResp,
    WireTransferLeaderErr, WireTransferLeaderReq, WireVoteReq, WireVoteResp,
  },
};
use crate::{
  OptionalSend, RPCTypes, RaftTypeConfig,
  alias::{EntryOf, LogIdOf, SnapshotMetaOf, SnapshotOf, VoteOf},
  errors::{NetworkError, RPCError, ReplicationClosed, StreamingError, Timeout, Unreachable},
  network::{RPCOption, RaftNetwork, RaftNetworkFactory},
  raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, TransferLeaderRequest,
    TransferLeaderResponse, VoteRequest, VoteResponse,
  },
};

const TIMEOUT_TOLERANCE: Duration = Duration::from_millis(15);
const ERR_ZENOH_QUERY: &str = "Zenoh query error";

/// Precomputed and compiled Zenoh RPC route keys to avoid hot-path string formatting and heap allocations
/// 预计算并编译的 Zenoh RPC 路由键，避免在热路径上进行字符串格式化、语法校验与堆内存分配
#[derive(Clone, Debug)]
pub struct ZenohRpcKeys {
  pub append_entries: KeyExpr<'static>,
  pub vote: KeyExpr<'static>,
  pub pre_vote: KeyExpr<'static>,
  pub snapshot: KeyExpr<'static>,
  pub transfer_leader: KeyExpr<'static>,
}

impl ZenohRpcKeys {
  /// Attempt to build precomputed Zenoh RPC route keys from key prefix and target node
  /// 尝试根据键前缀与目标节点构建预计算的 Zenoh RPC 路由键
  pub fn try_new(key_prefix: &str, target: &impl Display) -> Result<Self, AnyError> {
    let base_key = format!("{key_prefix}/{target}");
    Ok(Self {
      append_entries: KeyExpr::try_from(format!("{base_key}/{RPC_APPEND_ENTRIES}"))
        .map_err(|e| AnyError::error(format!("invalid key_expr for append_entries: {e}")))?,
      vote: KeyExpr::try_from(format!("{base_key}/{RPC_VOTE}"))
        .map_err(|e| AnyError::error(format!("invalid key_expr for vote: {e}")))?,
      pre_vote: KeyExpr::try_from(format!("{base_key}/{RPC_PRE_VOTE}"))
        .map_err(|e| AnyError::error(format!("invalid key_expr for pre_vote: {e}")))?,
      snapshot: KeyExpr::try_from(format!("{base_key}/{RPC_SNAPSHOT}"))
        .map_err(|e| AnyError::error(format!("invalid key_expr for snapshot: {e}")))?,
      transfer_leader: KeyExpr::try_from(format!("{base_key}/{RPC_TRANSFER_LEADER}"))
        .map_err(|e| AnyError::error(format!("invalid key_expr for transfer_leader: {e}")))?,
    })
  }

  /// Build precomputed Zenoh RPC route keys from key prefix and target node
  /// 根据键前缀与目标节点构建预计算的 Zenoh RPC 路由键
  #[inline]
  pub fn new(key_prefix: &str, target: &impl Display) -> Self {
    Self::try_new(key_prefix, target).expect("valid key_expr for ZenohRpcKeys")
  }
}

/// Raft network factory based on Zenoh
/// 基于 Zenoh 的 Raft 网络工厂
#[derive(Clone)]
pub struct ZenohNetworkFactory<C: RaftTypeConfig> {
  session: Arc<zenoh::Session>,
  config: ZenohNetworkConfig,
  _marker: PhantomData<C>,
}

impl<C: RaftTypeConfig> ZenohNetworkFactory<C> {
  /// Create a new Zenoh network factory
  /// 创建新的 Zenoh 网络工厂
  pub fn new(session: Arc<zenoh::Session>, config: ZenohNetworkConfig) -> Self {
    Self {
      session,
      config,
      _marker: PhantomData,
    }
  }

  /// Get reference to the underlying Zenoh session
  /// 获取底层 Zenoh 会话引用
  pub fn session(&self) -> &Arc<zenoh::Session> {
    &self.session
  }

  /// Get reference to the network configuration
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

/// Target node Raft network client based on Zenoh
/// 基于 Zenoh 的目标节点 Raft 网络客户端
pub struct ZenohNetwork<C: RaftTypeConfig> {
  target: C::NodeId,
  keys: ZenohRpcKeys,
  session: Arc<zenoh::Session>,
  config: ZenohNetworkConfig,
  _marker: PhantomData<C>,
}

impl<C: RaftTypeConfig> ZenohNetwork<C> {
  async fn send_query<Req, Resp>(
    &self,
    key: &KeyExpr<'_>,
    action: RPCTypes,
    option: &RPCOption,
    req: &Req,
  ) -> Result<Resp, RPCError<C>>
  where
    Req: bitcode::Encode,
    Resp: for<'a> bitcode::Decode<'a>,
  {
    let timeout = option.soft_ttl();
    let payload = bitcode::encode(req);
    let start = Instant::now();

    let replies = self
      .session
      .get(key)
      .payload(payload)
      .target(self.config.query_target)
      .timeout(timeout)
      .await
      .map_err(|e| RPCError::Unreachable(Unreachable::from_string(e.to_string())))?;

    if let Ok(reply) = replies.recv_async().await {
      match reply.result() {
        Ok(sample) => {
          let bytes = sample.payload().to_bytes();
          let resp: Resp = bitcode::decode(&bytes)
            .map_err(|e| RPCError::Network(NetworkError::from_string(e.to_string())))?;
          return Ok(resp);
        }
        Err(err) => {
          let err_str = err.payload().try_to_string().map_or_else(
            |_| ERR_ZENOH_QUERY.to_string(),
            std::borrow::Cow::into_owned,
          );
          return Err(RPCError::Unreachable(Unreachable::from_string(err_str)));
        }
      }
    }

    let elapsed = start.elapsed();
    // Rigorously differentiate timeout and unreachable: if channel closes well before timeout,
    // target node is offline or queryable unregistered; return Unreachable to trigger backoff.
    // 严谨区分超时与不可达：若通道在超时时间前关闭说明目标离线或未注册，返回 Unreachable 触发 Backoff。
    if elapsed + TIMEOUT_TOLERANCE >= timeout {
      Err(RPCError::Timeout(Timeout {
        action,
        id: self.target.clone(),
        target: self.target.clone(),
        timeout,
      }))
    } else {
      Err(RPCError::Unreachable(Unreachable::from_string(format!(
        "target node {} unreachable: query channel closed after {elapsed:?}",
        self.target
      ))))
    }
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
    cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
    option: RPCOption,
  ) -> Result<SnapshotResponse<C>, StreamingError<C>> {
    let wire_payload = WireSnapshotPayload {
      vote,
      meta: snapshot.meta,
      data: snapshot.snapshot.into_inner(),
    };

    let send_fut = self.send_query(
      &self.keys.snapshot,
      RPCTypes::InstallSnapshot,
      &option,
      &wire_payload,
    );

    futures_util::pin_mut!(cancel);
    futures_util::pin_mut!(send_fut);

    futures_util::select! {
      closed = cancel.fuse() => Err(StreamingError::Closed(closed)),
      res = send_fut.fuse() => {
        let wire_resp: WireSnapshotResp<VoteOf<C>> = res?;
        Ok(wire_resp.into())
      }
    }
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

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_zenoh_rpc_keys_generation() {
    let keys = ZenohRpcKeys::try_new("test_cluster", &10).expect("generate keys");
    assert_eq!(
      keys.append_entries.as_str(),
      "test_cluster/10/append_entries"
    );
    assert_eq!(keys.vote.as_str(), "test_cluster/10/vote");
    assert_eq!(keys.pre_vote.as_str(), "test_cluster/10/pre_vote");
    assert_eq!(keys.snapshot.as_str(), "test_cluster/10/snapshot");
    assert_eq!(
      keys.transfer_leader.as_str(),
      "test_cluster/10/transfer_leader"
    );

    let keys_new = ZenohRpcKeys::new("test_cluster", &10);
    assert_eq!(
      keys_new.append_entries.as_str(),
      keys.append_entries.as_str()
    );
  }

  #[test]
  fn test_zenoh_rpc_keys_invalid() {
    // Key expressions cannot contain '$' as non-wildcard
    let err = ZenohRpcKeys::try_new("test$cluster", &10);
    assert!(err.is_err());
  }
}
