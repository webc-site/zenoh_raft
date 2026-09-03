//! Raft 测试脚手架与路由网络模拟器

#![allow(dead_code)]

use std::{
  collections::{BTreeMap, BTreeSet},
  fmt,
  future::Future,
  io::Cursor,
  sync::{
    Arc, Mutex,
    atomic::{AtomicU16, AtomicU64, Ordering},
  },
  time::Duration,
};

use anyhow::Context;
use futures_util::lock::Mutex as AsyncMutex;
use rapidhash::{HashMapExt, RapidHashMap as HashMap};
use zenoh_raft::{
  Config, OptionalSend, RPCTypes, Raft, RaftMetrics, RaftState, RaftTypeConfig, ReadPolicy,
  ServerState, Vote,
  alias::{LogIdOf, SnapshotOf, VoteOf},
  entry::RaftEntry,
  errors::{
    ClientWriteError, Fatal, LinearizableReadError, NetworkError, RPCError, RaftError,
    ReplicationClosed, StreamingError, Unreachable,
  },
  metrics::Wait,
  network::{RPCOption, RaftNetwork, RaftNetworkFactory},
  raft::{
    AppendEntriesRequest, AppendEntriesResponse, ClientWriteResponse, SnapshotResponse,
    TransferLeaderRequest, TransferLeaderResponse, VoteRequest, VoteResponse,
    linearizable_read::ReadLogId,
  },
  testing::memstore::{
    ClientRequest, ClientResponse, IntoMemClientRequest, MemLogStore as LogStoreInner, MemNodeId,
    MemStateMachine as SMInner, TypeConfig, TypeConfig as MemConfig, new_mem_store,
  },
  type_config::TypeConfigExt,
  vote::RaftLeaderId,
};

pub mod post_hook;
pub mod pre_hook;
pub mod rpc_error_type;
pub mod rpc_request;
pub mod rpc_response;

use post_hook::{PostHook, PostHookResult};
use pre_hook::{PreHook, PreHookResult};
use rpc_error_type::RpcErrorType;
use rpc_request::RpcRequest;
use rpc_response::RpcResponse;

pub type MemSnapshotData = Cursor<Vec<u8>>;
pub type MemRpcRequest = RpcRequest<TypeConfig, MemSnapshotData>;
pub type MemRpcResponse = RpcResponse<TypeConfig>;

pub type MemLogStore = Arc<LogStoreInner>;
pub type MemStateMachine = Arc<SMInner>;
pub type MemRaft = Raft<MemConfig, MemStateMachine>;

pub fn log_id(term: u64, node_id: u64, index: u64) -> LogIdOf<TypeConfig> {
  LogIdOf::<TypeConfig>::new(
    <TypeConfig as RaftTypeConfig>::LeaderId::new_committed(term, node_id),
    index,
  )
}

pub fn timeout() -> Option<Duration> {
  Some(Duration::from_millis(5_000))
}

pub fn get_available_port() -> u16 {
  static PORT_COUNTER: AtomicU16 = AtomicU16::new(0);
  let offset = PORT_COUNTER.fetch_add(1, Ordering::Relaxed);
  fastrand::u16(32000..58000).wrapping_add(offset)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
  NetSend,
  NetRecv,
}

impl fmt::Display for Direction {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Direction::NetSend => write!(f, "sending from"),
      Direction::NetRecv => write!(f, "receiving by"),
    }
  }
}

use Direction::{NetRecv, NetSend};

pub type MemNodeMap = BTreeMap<MemNodeId, (MemRaft, MemLogStore, MemStateMachine)>;

/// 路由与网络模拟器
#[derive(Clone)]
pub struct TypedRaftRouter {
  config: Arc<Config>,
  nodes: Arc<Mutex<MemNodeMap>>,
  pub enable_saving_committed: bool,
  fail_rpc: Arc<Mutex<HashMap<(MemNodeId, Direction), RpcErrorType>>>,
  send_delay: Arc<AtomicU64>,
  append_entries_quota: Arc<Mutex<Option<u64>>>,
  rpc_count: Arc<Mutex<HashMap<RPCTypes, u64>>>,
  rpc_pre_hook: Arc<AsyncMutex<HashMap<RPCTypes, PreHook>>>,
  rpc_post_hook: Arc<AsyncMutex<HashMap<RPCTypes, PostHook>>>,
}

pub type RaftRouter = TypedRaftRouter;

pub struct Builder {
  config: Arc<Config>,
  send_delay: u64,
}

impl Builder {
  pub fn send_delay(mut self, ms: u64) -> Self {
    self.send_delay = ms;
    self
  }

  pub fn build(self) -> TypedRaftRouter {
    let send_delay = self.send_delay;
    TypedRaftRouter {
      config: self.config,
      nodes: Default::default(),
      enable_saving_committed: true,
      fail_rpc: Default::default(),
      send_delay: Arc::new(AtomicU64::new(send_delay)),
      append_entries_quota: Arc::new(Mutex::new(None)),
      rpc_count: Default::default(),
      rpc_pre_hook: Arc::new(AsyncMutex::new(HashMap::new())),
      rpc_post_hook: Arc::new(AsyncMutex::new(HashMap::new())),
    }
  }
}

impl TypedRaftRouter {
  pub fn builder(config: Arc<Config>) -> Builder {
    Builder {
      config,
      send_delay: 0,
    }
  }

  pub fn new(config: Arc<Config>) -> Self {
    Self::builder(config).build()
  }

  pub fn network_send_delay(&mut self, ms: u64) {
    self.send_delay.store(ms, Ordering::Relaxed);
  }

  async fn rand_send_delay(&self) {
    let send_delay = self.send_delay.load(Ordering::Relaxed);
    if send_delay == 0 {
      return;
    }

    let r = rand::random::<u64>() % send_delay;
    let timeout = Duration::from_millis(r);
    TypeConfig::sleep(timeout).await;
  }

  pub fn set_append_entries_quota(&mut self, quota: Option<u64>) {
    let mut append_entries_quota = self.append_entries_quota.lock().unwrap();
    *append_entries_quota = quota;
  }

  fn count_rpc(&self, rpc_type: RPCTypes) {
    let mut rpc_count = self.rpc_count.lock().unwrap();
    let count = rpc_count.entry(rpc_type).or_insert(0);
    *count += 1;
  }

  pub fn get_rpc_count(&self) -> HashMap<RPCTypes, u64> {
    self.rpc_count.lock().unwrap().clone()
  }

  /// 创建集群：0 是初始 leader，其他是 voters 和 learners
  pub async fn new_cluster(
    &mut self,
    voter_ids: BTreeSet<MemNodeId>,
    learners: BTreeSet<MemNodeId>,
  ) -> anyhow::Result<u64> {
    let leader_id = MemNodeId::default();
    assert!(voter_ids.contains(&leader_id));

    self.new_raft_node(leader_id).await;

    for node in [0] {
      self
        .external_request(node, |s| {
          assert_eq!(s.server_state, ServerState::Learner);
        })
        .await?;
    }
    self
      .wait(&leader_id, timeout())
      .applied_index(None, "empty")
      .await?;

    self.initialize(leader_id).await?;
    let mut log_index = 1;

    self
      .wait(&leader_id, timeout())
      .applied_index(Some(log_index), "init")
      .await?;
    self
      .wait(&leader_id, timeout())
      .vote(VoteOf::<MemConfig>::new_committed(1, 0), "init vote")
      .await?;

    for id in voter_ids.iter() {
      if *id == leader_id {
        continue;
      }

      self.new_raft_node(*id).await;
      self.add_learner(leader_id, *id).await?;
      log_index += 1;

      self
        .wait(id, timeout())
        .state(ServerState::Learner, "empty node")
        .await?;
    }

    for id in voter_ids.iter() {
      self
        .wait(id, timeout())
        .applied_index(Some(log_index), &format!("learners of {voter_ids:?}"))
        .await?;
    }

    if voter_ids.len() > 1 {
      let node = self.get_raft_handle(&MemNodeId::default())?;
      node.change_membership(voter_ids.clone(), false).await?;
      log_index += 2;

      for id in voter_ids.iter() {
        self
          .wait(id, timeout())
          .applied_index(Some(log_index), &format!("cluster of {voter_ids:?}"))
          .await?;
      }
    }

    for id in learners.clone() {
      self.new_raft_node(id).await;
      self.add_learner(MemNodeId::default(), id).await?;
      log_index += 1;
    }
    for id in learners.iter() {
      self
        .wait(id, timeout())
        .applied_index(Some(log_index), &format!("learners of {learners:?}"))
        .await?;
    }

    Ok(log_index)
  }

  pub async fn new_raft_node(&mut self, id: MemNodeId) {
    let (log_store, sm) = self.new_store();
    self.new_raft_node_with_sto(id, log_store, sm).await
  }

  pub fn new_store(&mut self) -> (MemLogStore, MemStateMachine) {
    let (log, sm) = new_mem_store();
    log
      .enable_saving_committed
      .store(self.enable_saving_committed, Ordering::Relaxed);
    (log, sm)
  }

  pub async fn new_raft_node_with_sto(
    &mut self,
    id: MemNodeId,
    log_store: MemLogStore,
    sm: MemStateMachine,
  ) {
    let node = Raft::new(
      id,
      self.config.clone(),
      self.clone(),
      log_store.clone(),
      sm.clone(),
    )
    .await
    .unwrap();
    let mut rt = self.nodes.lock().unwrap();
    rt.insert(id, (node, log_store, sm));
  }

  pub fn remove_node(&mut self, id: MemNodeId) -> Option<(MemRaft, MemLogStore, MemStateMachine)> {
    let opt_handles = {
      let mut rt = self.nodes.lock().unwrap();
      rt.remove(&id)
    };

    self.set_network_error(id, false);
    self.set_unreachable(id, false);

    opt_handles
  }

  pub async fn initialize(&self, node_id: MemNodeId) -> anyhow::Result<()> {
    let members: BTreeSet<MemNodeId> = {
      let rt = self.nodes.lock().unwrap();
      rt.keys().cloned().collect()
    };

    let node = self.get_raft_handle(&node_id)?;
    node.initialize(members.clone()).await?;
    Ok(())
  }

  pub fn set_network_error(&self, id: MemNodeId, emit_failure: bool) {
    let v = if emit_failure {
      Some(RpcErrorType::NetworkError)
    } else {
      None
    };

    self.set_rpc_failure(id, NetRecv, v);
    self.set_rpc_failure(id, NetSend, v);
  }

  pub fn set_unreachable(&self, id: MemNodeId, unreachable: bool) {
    let v = if unreachable {
      Some(RpcErrorType::Unreachable)
    } else {
      None
    };
    self.set_rpc_failure(id, NetRecv, v);
    self.set_rpc_failure(id, NetSend, v);
  }

  pub fn set_rpc_failure(
    &self,
    id: MemNodeId,
    dir: Direction,
    rpc_error_type: Option<RpcErrorType>,
  ) {
    let mut fails = self.fail_rpc.lock().unwrap();
    if let Some(rpc_error_type) = rpc_error_type {
      fails.insert((id, dir), rpc_error_type);
    } else {
      fails.remove(&(id, dir));
    }
  }

  pub async fn set_rpc_pre_hook<F>(&self, rpc_type: RPCTypes, hook: F)
  where
    F: Fn(&TypedRaftRouter, MemRpcRequest, MemNodeId, MemNodeId) -> PreHookResult + Send + 'static,
  {
    self.rpc_pre_hook(rpc_type, Some(Box::new(hook))).await;
  }

  pub async fn set_rpc_post_hook<F>(&self, rpc_type: RPCTypes, hook: F)
  where
    F: Fn(&TypedRaftRouter, MemRpcRequest, MemRpcResponse, MemNodeId, MemNodeId) -> PostHookResult
      + Send
      + 'static,
  {
    self.rpc_post_hook(rpc_type, Some(Box::new(hook))).await;
  }

  pub async fn rpc_pre_hook(&self, rpc_type: RPCTypes, hook: Option<PreHook>) {
    let mut rpc_pre_hook = self.rpc_pre_hook.lock().await;
    if let Some(hook) = hook {
      rpc_pre_hook.insert(rpc_type, hook);
    } else {
      rpc_pre_hook.remove(&rpc_type);
    }
  }

  pub async fn rpc_post_hook(&self, rpc_type: RPCTypes, hook: Option<PostHook>) {
    let mut post_hook = self.rpc_post_hook.lock().await;
    if let Some(hook) = hook {
      post_hook.insert(rpc_type, hook);
    } else {
      post_hook.remove(&rpc_type);
    }
  }

  async fn call_rpc_pre_hook(
    &self,
    request: impl Into<MemRpcRequest>,
    from: MemNodeId,
    to: MemNodeId,
  ) -> Result<(), RPCError<MemConfig>> {
    let request = request.into();
    let typ = request.get_type();

    let fu = {
      let rpc_pre_hook = self.rpc_pre_hook.lock().await;
      let Some(hook) = rpc_pre_hook.get(&typ) else {
        return Ok(());
      };
      hook(self, request, from, to)
    };

    let res = fu.await;
    match res {
      Ok(()) => Ok(()),
      Err(err) => {
        let rpc_err = match err {
          RPCError::Timeout(e) => e.into(),
          RPCError::Unreachable(e) => e.into(),
          RPCError::Network(e) => e.into(),
          RPCError::RemoteError(e) => {
            unreachable!("unexpected RemoteError: {:?}", e);
          }
        };
        Err(rpc_err)
      }
    }
  }

  async fn call_rpc_post_hook(
    &self,
    request: impl Into<MemRpcRequest>,
    response: impl Into<MemRpcResponse>,
    from: MemNodeId,
    to: MemNodeId,
  ) -> Result<(), RPCError<MemConfig>> {
    let request = request.into();
    let response = response.into();
    let typ = request.get_type();

    let fu = {
      let rpc_post_hook = self.rpc_post_hook.lock().await;
      let Some(hook) = rpc_post_hook.get(&typ) else {
        return Ok(());
      };
      hook(self, request, response, from, to)
    };

    let res = fu.await;
    match res {
      Ok(()) => Ok(()),
      Err(err) => {
        let rpc_err = match err {
          RPCError::Timeout(e) => e.into(),
          RPCError::Unreachable(e) => e.into(),
          RPCError::Network(e) => e.into(),
          RPCError::RemoteError(e) => {
            unreachable!("unexpected RemoteError: {:?}", e);
          }
        };
        Err(rpc_err)
      }
    }
  }

  pub fn latest_metrics(&self) -> Vec<RaftMetrics<MemConfig>> {
    let rt = self.nodes.lock().unwrap();
    let mut metrics = vec![];
    for node in rt.values() {
      let m = node.0.metrics().borrow_watched().clone();
      metrics.push(m);
    }
    metrics
  }

  pub fn get_metrics(&self, node_id: &MemNodeId) -> anyhow::Result<RaftMetrics<MemConfig>> {
    let node = self.get_raft_handle(node_id)?;
    let metrics = node.metrics().borrow_watched().clone();
    Ok(metrics)
  }

  pub fn get_raft_handle(&self, node_id: &MemNodeId) -> Result<MemRaft, NetworkError<MemConfig>> {
    let rt = self.nodes.lock().unwrap();
    let raft_and_sto = rt
      .get(node_id)
      .ok_or_else(|| NetworkError::<MemConfig>::from_string(format!("node {node_id} not found")))?;
    Ok(raft_and_sto.0.clone())
  }

  pub fn get_storage_handle(
    &self,
    node_id: &MemNodeId,
  ) -> anyhow::Result<(MemLogStore, MemStateMachine)> {
    let rt = self.nodes.lock().unwrap();
    let addr = rt
      .get(node_id)
      .with_context(|| format!("could not find node {node_id} in routing table"))?;
    Ok((addr.1.clone(), addr.2.clone()))
  }

  pub fn set_return_empty_limited_get(
    &self,
    node_id: &MemNodeId,
    value: bool,
  ) -> anyhow::Result<()> {
    let (log_store, _) = self.get_storage_handle(node_id)?;
    log_store.set_return_empty_limited_get(value);
    Ok(())
  }

  pub fn set_fail_next_limited_get(&self, node_id: &MemNodeId, value: bool) -> anyhow::Result<()> {
    let (log_store, _) = self.get_storage_handle(node_id)?;
    log_store.set_fail_next_limited_get(value);
    Ok(())
  }

  pub fn wait(&self, node_id: &MemNodeId, timeout: Option<Duration>) -> Wait<MemConfig> {
    let node = {
      let rt = self.nodes.lock().unwrap();
      rt.get(node_id)
        .expect("target node not found in routing table")
        .clone()
        .0
    };

    node.wait(timeout)
  }

  pub fn leader(&self) -> Option<MemNodeId> {
    self.latest_metrics().into_iter().find_map(|node| {
      if node.current_leader == Some(node.id) {
        Some(node.id)
      } else {
        None
      }
    })
  }

  pub async fn add_learner(
    &self,
    leader: MemNodeId,
    target: MemNodeId,
  ) -> Result<ClientWriteResponse<MemConfig>, ClientWriteError<MemConfig>> {
    let node = self.get_raft_handle(&leader).unwrap();
    node
      .add_learner(target, (), true)
      .await
      .map_err(|e| e.into_api_error().unwrap())
  }

  pub async fn ensure_linearizable(
    &self,
    target: MemNodeId,
    read_policy: ReadPolicy,
  ) -> anyhow::Result<()> {
    let n = self.get_raft_handle(&target)?;
    let linearizer = n.get_read_linearizer(read_policy).await?;
    linearizer.await_ready(&n).await?;
    Ok(())
  }

  pub async fn get_read_log_id(
    &self,
    target: MemNodeId,
    read_policy: ReadPolicy,
  ) -> Result<(ReadLogId<MemConfig>, Option<LogIdOf<MemConfig>>), LinearizableReadError<MemConfig>>
  {
    let n = self.get_raft_handle(&target).unwrap();
    n.get_read_log_id(read_policy)
      .await
      .map_err(|e| e.into_api_error().unwrap())
  }

  pub async fn client_request(
    &self,
    mut target: MemNodeId,
    client_id: &str,
    serial: u64,
  ) -> Result<(), RaftError<MemConfig, ClientWriteError<MemConfig>>> {
    for _ in 0..3 {
      let req = ClientRequest::make_request(client_id, serial);
      if let Err(err) = self.send_client_request(target, req).await {
        if let RaftError::APIError(ClientWriteError::ForwardToLeader(e)) = &err
          && let Some(l) = e.leader_id
        {
          target = l;
          continue;
        }
        return Err(err);
      } else {
        return Ok(());
      }
    }

    panic!("Max retry times exceeded: target={target}, client_id={client_id}, serial={serial}")
  }

  pub async fn with_raft_state<V, F>(
    &self,
    target: MemNodeId,
    func: F,
  ) -> Result<V, Fatal<MemConfig>>
  where
    F: FnOnce(&RaftState<MemConfig>) -> V + Send + 'static,
    V: Send + 'static,
  {
    let r = self.get_raft_handle(&target).unwrap();
    r.with_raft_state(func).await
  }

  pub async fn external_request<F: FnOnce(&RaftState<MemConfig>) + Send + 'static>(
    &self,
    target: MemNodeId,
    req: F,
  ) -> Result<(), Fatal<MemConfig>> {
    let r = self.get_raft_handle(&target).unwrap();
    r.external_request(req).await
  }

  pub async fn current_leader(&self, target: MemNodeId) -> Option<MemNodeId> {
    let node = self.get_raft_handle(&target).unwrap();
    node.current_leader().await
  }

  pub async fn client_request_many(
    &self,
    target: MemNodeId,
    client_id: &str,
    count: usize,
  ) -> Result<u64, RaftError<MemConfig, ClientWriteError<MemConfig>>> {
    for idx in 0..count {
      self.client_request(target, client_id, idx as u64).await?;
    }
    Ok(count as u64)
  }

  pub async fn send_client_request(
    &self,
    target: MemNodeId,
    req: ClientRequest,
  ) -> Result<ClientResponse, RaftError<MemConfig, ClientWriteError<MemConfig>>> {
    let node = {
      let rt = self.nodes.lock().unwrap();
      rt.get(&target)
        .unwrap_or_else(|| panic!("node '{target}' does not exist in routing table"))
        .clone()
    };
    node.0.client_write(req).await.map(|res| res.data)
  }

  pub fn emit_rpc_error(
    &self,
    id: MemNodeId,
    target: MemNodeId,
  ) -> Result<(), RPCError<MemConfig>> {
    let fails = self.fail_rpc.lock().unwrap();
    for key in [(id, NetSend), (target, NetRecv)] {
      if let Some(err_type) = fails.get(&key) {
        return Err(err_type.make_error(key.0, key.1));
      }
    }
    Ok(())
  }
}

impl RaftNetworkFactory<MemConfig> for TypedRaftRouter {
  type Network = RaftRouterNetwork;

  async fn new_client(&mut self, target: MemNodeId, _node: &()) -> Self::Network {
    RaftRouterNetwork {
      target,
      owner: self.clone(),
    }
  }
}

pub struct RaftRouterNetwork {
  target: MemNodeId,
  owner: TypedRaftRouter,
}

impl RaftNetwork<MemConfig> for RaftRouterNetwork {
  type SnapshotData = MemSnapshotData;

  async fn append_entries(
    &mut self,
    mut rpc: AppendEntriesRequest<MemConfig>,
    _option: RPCOption,
  ) -> Result<AppendEntriesResponse<MemConfig>, RPCError<MemConfig>> {
    let from_id = *rpc.vote.leader_id().node_id();

    self.owner.count_rpc(RPCTypes::AppendEntries);
    self
      .owner
      .call_rpc_pre_hook(rpc.clone(), from_id, self.target)
      .await?;
    self.owner.emit_rpc_error(from_id, self.target)?;
    self.owner.rand_send_delay().await;

    let truncated = {
      let n = rpc.entries.len() as u64;
      let mut x = self.owner.append_entries_quota.lock().unwrap();
      let q = *x;

      if let Some(quota) = q {
        if quota < n {
          rpc.entries.truncate(quota as usize);
          *x = Some(0);
          if let Some(last) = rpc.entries.last() {
            Some(Some(last.log_id()))
          } else {
            Some(rpc.prev_log_id)
          }
        } else {
          *x = Some(quota - n);
          None
        }
      } else {
        None
      }
    };

    let node = self.owner.get_raft_handle(&self.target)?;
    let resp = node.append_entries(rpc.clone()).await;
    let resp = resp.map_err(|e| {
      RPCError::Unreachable(Unreachable::<MemConfig>::from_string(format!(
        "error: {e} target={}",
        self.target
      )))
    })?;

    self
      .owner
      .call_rpc_post_hook(rpc, resp.clone(), from_id, self.target)
      .await?;

    if let Some(truncated) = truncated {
      match resp {
        AppendEntriesResponse::Success => Ok(AppendEntriesResponse::PartialSuccess(truncated)),
        _ => Ok(resp),
      }
    } else {
      Ok(resp)
    }
  }

  async fn full_snapshot(
    &mut self,
    vote: Vote<<MemConfig as RaftTypeConfig>::LeaderId>,
    snapshot: SnapshotOf<MemConfig, Self::SnapshotData>,
    _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
    _option: RPCOption,
  ) -> Result<SnapshotResponse<MemConfig>, StreamingError<MemConfig>> {
    let from_id = *vote.leader_id().node_id();

    self.owner.count_rpc(RPCTypes::InstallSnapshot);
    self
      .owner
      .call_rpc_pre_hook(snapshot.clone(), from_id, self.target)
      .await?;
    self.owner.emit_rpc_error(from_id, self.target)?;
    self.owner.rand_send_delay().await;

    let node = self.owner.get_raft_handle(&self.target)?;
    let resp = node.install_full_snapshot(vote, snapshot.clone()).await;
    let resp = resp.map_err(|e| {
      RPCError::Unreachable(Unreachable::<MemConfig>::from_string(format!(
        "error: {e} target={}",
        self.target
      )))
    })?;

    self
      .owner
      .call_rpc_post_hook(snapshot, resp.clone(), from_id, self.target)
      .await?;

    Ok(resp)
  }

  async fn vote(
    &mut self,
    rpc: VoteRequest<MemConfig>,
    _option: RPCOption,
  ) -> Result<VoteResponse<MemConfig>, RPCError<MemConfig>> {
    let from_id = *rpc.vote.leader_id().node_id();

    self.owner.count_rpc(RPCTypes::Vote);
    self
      .owner
      .call_rpc_pre_hook(rpc.clone(), from_id, self.target)
      .await?;
    self.owner.emit_rpc_error(from_id, self.target)?;
    self.owner.rand_send_delay().await;

    let node = self.owner.get_raft_handle(&self.target)?;
    let resp = node.vote(rpc.clone()).await;
    let resp = resp.map_err(|e| {
      RPCError::Unreachable(Unreachable::<MemConfig>::from_string(format!(
        "error: {e} target={}",
        self.target
      )))
    })?;

    self
      .owner
      .call_rpc_post_hook(rpc, resp.clone(), from_id, self.target)
      .await?;

    Ok(resp)
  }

  async fn pre_vote(
    &mut self,
    rpc: VoteRequest<MemConfig>,
    _option: RPCOption,
  ) -> Result<VoteResponse<MemConfig>, RPCError<MemConfig>> {
    let from_id = *rpc.vote.leader_id().node_id();

    self.owner.count_rpc(RPCTypes::Vote);
    self
      .owner
      .call_rpc_pre_hook(rpc.clone(), from_id, self.target)
      .await?;
    self.owner.emit_rpc_error(from_id, self.target)?;
    self.owner.rand_send_delay().await;

    let node = self.owner.get_raft_handle(&self.target)?;
    let resp = node.pre_vote(rpc.clone()).await;
    let resp = resp.map_err(|e| {
      RPCError::Unreachable(Unreachable::<MemConfig>::from_string(format!(
        "error: {e} target={}",
        self.target
      )))
    })?;

    self
      .owner
      .call_rpc_post_hook(rpc, resp.clone(), from_id, self.target)
      .await?;

    Ok(resp)
  }

  async fn transfer_leader(
    &mut self,
    rpc: TransferLeaderRequest<MemConfig>,
    _option: RPCOption,
  ) -> Result<TransferLeaderResponse<MemConfig>, RPCError<MemConfig>> {
    let from_id = *rpc.from_leader().leader_id().node_id();

    self.owner.count_rpc(RPCTypes::TransferLeader);
    self
      .owner
      .call_rpc_pre_hook(rpc.clone(), from_id, self.target)
      .await?;
    self.owner.emit_rpc_error(from_id, self.target)?;
    self.owner.rand_send_delay().await;

    let node = self.owner.get_raft_handle(&self.target)?;
    let resp = node.handle_transfer_leader(rpc.clone()).await;
    let resp = resp.map_err(|err| {
      RPCError::Unreachable(Unreachable::<MemConfig>::from_string(format!(
        "error: {err} target={}",
        self.target
      )))
    })?;

    self
      .owner
      .call_rpc_post_hook(rpc, resp.clone(), from_id, self.target)
      .await?;

    Ok(resp)
  }
}
