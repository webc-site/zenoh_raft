use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures_util::Stream;
use futures_util::TryStreamExt;
use futures_util::lock::Mutex as AsyncMutex;

use crate::EntryPayload;
use crate::OptionalSend;
use crate::alias::EntryOf;
use crate::alias::LogIdOf;
use crate::alias::SnapshotMetaOf;
use crate::alias::SnapshotOf;
use crate::alias::StoredMembershipOf;
use crate::alias::VoteOf;
use crate::entry::RaftEntry;
use crate::impls::leader_id_adv::LeaderId;
use crate::storage::EntryResponder;
use crate::storage::IOFlushed;
use crate::storage::LogState;
use crate::storage::RaftLogReader;
use crate::storage::RaftLogStorage;
use crate::storage::RaftSnapshotBuilder;
use crate::storage::RaftStateMachine;
use crate::type_config::TypeConfigExt;

/// 测试应用请求类型
#[derive(Debug, Clone, PartialEq, Eq, bitcode::Encode, bitcode::Decode, derive_more::Display)]
#[display("ClientRequest{{client:{client}, serial:{serial}, status:{status}}}")]
pub struct ClientRequest {
    /// 客户端标识
    pub client: String,
    /// 请求序列号
    pub serial: u64,
    /// 状态描述
    pub status: String,
}

/// 泛型测试辅助构建 ClientRequest
pub trait IntoMemClientRequest<T> {
    fn make_request(client_id: impl ToString, serial: u64) -> T;
}

impl IntoMemClientRequest<ClientRequest> for ClientRequest {
    fn make_request(client_id: impl ToString, serial: u64) -> Self {
        Self {
            client: client_id.to_string(),
            serial,
            status: format!("request-{serial}"),
        }
    }
}

/// 测试应用响应类型
#[derive(Debug, Clone, PartialEq, Eq, bitcode::Encode, bitcode::Decode)]
pub struct ClientResponse(pub Option<String>);

pub type MemNodeId = u64;

crate::declare_raft_types!(
    pub TypeConfig:
        D = ClientRequest,
        R = ClientResponse,
        Node = (),
        NodeId = MemNodeId,
        Term = u64,
        LeaderId = LeaderId<Self::Term, Self::NodeId>,
);

pub type MemConfig = TypeConfig;

/// 快照数据结构
#[derive(Debug, Clone)]
pub struct MemStoreSnapshot {
    pub meta: SnapshotMetaOf<TypeConfig>,
    pub data: Vec<u8>,
}

/// 状态机内部数据
#[derive(Debug, Default, Clone, PartialEq, Eq, bitcode::Encode, bitcode::Decode)]
pub struct MemStoreStateMachine {
    pub last_applied_log: Option<LogIdOf<TypeConfig>>,
    pub last_membership: StoredMembershipOf<TypeConfig>,
    pub client_status: BTreeMap<String, String>,
}

/// 阻塞操作类型
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum BlockOperation {
    DelayBuildingSnapshot,
    BuildSnapshot,
    PurgeLog,
}

/// 模拟延迟与阻塞配置
#[derive(Clone, Debug, Default)]
pub struct BlockConfig {
    inner: Arc<Mutex<BTreeMap<BlockOperation, Duration>>>,
}

impl BlockConfig {
    pub fn set_blocking(&self, block: BlockOperation, d: Duration) {
        self.inner.lock().unwrap().insert(block, d);
    }

    pub fn get_blocking(&self, block: &BlockOperation) -> Option<Duration> {
        self.inner.lock().unwrap().get(block).cloned()
    }

    pub fn clear_blocking(&mut self, block: BlockOperation) {
        self.inner.lock().unwrap().remove(&block);
    }
}

/// 内存日志存储
pub struct MemLogStore {
    last_purged_log_id: AsyncMutex<Option<LogIdOf<TypeConfig>>>,
    pub enable_saving_committed: AtomicBool,
    committed: AsyncMutex<Option<LogIdOf<TypeConfig>>>,
    log: AsyncMutex<BTreeMap<u64, EntryOf<TypeConfig>>>,
    block: BlockConfig,
    vote: AsyncMutex<Option<VoteOf<TypeConfig>>>,
    pub return_empty_limited_get: AtomicBool,
    pub fail_next_limited_get: AtomicBool,
}

impl MemLogStore {
    pub fn new(block: BlockConfig) -> Self {
        Self {
            last_purged_log_id: AsyncMutex::new(None),
            enable_saving_committed: AtomicBool::new(true),
            committed: AsyncMutex::new(None),
            log: AsyncMutex::new(BTreeMap::new()),
            block,
            vote: AsyncMutex::new(None),
            return_empty_limited_get: AtomicBool::new(false),
            fail_next_limited_get: AtomicBool::new(false),
        }
    }

    pub fn set_return_empty_limited_get(&self, value: bool) {
        self.return_empty_limited_get
            .store(value, Ordering::Relaxed);
    }

    pub fn set_fail_next_limited_get(&self, value: bool) {
        self.fail_next_limited_get.store(value, Ordering::Relaxed);
    }
}

/// 内存状态机
pub struct MemStateMachine {
    sm: AsyncMutex<MemStoreStateMachine>,
    allow_build_snapshot: Arc<AtomicBool>,
    current_snapshot: AsyncMutex<Option<MemStoreSnapshot>>,
    pub block: BlockConfig,
    pub try_create_snapshot_builder_count: Arc<AtomicU64>,
    panic_on_apply: AtomicBool,
}

impl MemStateMachine {
    pub fn new(block: BlockConfig) -> Self {
        Self {
            sm: AsyncMutex::new(MemStoreStateMachine::default()),
            allow_build_snapshot: Arc::new(AtomicBool::new(true)),
            current_snapshot: AsyncMutex::new(None),
            block,
            try_create_snapshot_builder_count: Arc::new(AtomicU64::new(0)),
            panic_on_apply: AtomicBool::new(false),
        }
    }

    pub fn allow_build_snapshot(&self, allowed: bool) {
        self.allow_build_snapshot.store(allowed, Ordering::Relaxed);
    }

    pub fn panic_on_apply(&self, panic: bool) {
        self.panic_on_apply.store(panic, Ordering::Relaxed);
    }

    pub fn take_try_create_snapshot_builder_count(&self) -> u64 {
        self.try_create_snapshot_builder_count
            .swap(0, Ordering::Relaxed)
    }

    pub async fn drop_snapshot(&self) {
        let mut current = self.current_snapshot.lock().await;
        *current = None;
    }

    pub async fn get_state_machine(&self) -> MemStoreStateMachine {
        self.sm.lock().await.clone()
    }

    pub async fn clear_state_machine(&self) {
        let mut sm = self.sm.lock().await;
        *sm = MemStoreStateMachine::default();
    }
}

/// 创建新的内存存储和状态机
pub fn new_mem_store() -> (Arc<MemLogStore>, Arc<MemStateMachine>) {
    let block = BlockConfig::default();
    (
        Arc::new(MemLogStore::new(block.clone())),
        Arc::new(MemStateMachine::new(block)),
    )
}

impl RaftLogReader<TypeConfig> for Arc<MemLogStore> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf<TypeConfig>>, io::Error> {
        let log = self.log.lock().await;
        let entries = log.range(range).map(|(_, ent)| ent.clone()).collect();
        Ok(entries)
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<TypeConfig>>, io::Error> {
        Ok(*self.vote.lock().await)
    }

    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> Result<Vec<EntryOf<TypeConfig>>, io::Error> {
        if self.fail_next_limited_get.swap(false, Ordering::Relaxed) {
            log::info!("limited_get_log_entries({start}, {end}): returning io::Error for testing");
            return Err(io::Error::other("injected limited_get_log_entries error"));
        }

        if self.return_empty_limited_get.load(Ordering::Relaxed) {
            log::info!("limited_get_log_entries({start}, {end}): returning empty for testing");
            return Ok(vec![]);
        }
        self.try_get_log_entries(start..end).await
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<MemStateMachine> {
    type SnapshotData = Cursor<Vec<u8>>;

    async fn build_snapshot(
        &mut self,
    ) -> Result<SnapshotOf<TypeConfig, Cursor<Vec<u8>>>, io::Error> {
        if let Some(d) = self
            .block
            .get_blocking(&BlockOperation::DelayBuildingSnapshot)
        {
            log::info!("delay snapshot build for {d:?}");
            TypeConfig::sleep(d).await;
        }

        let (data, last_applied_log, last_membership) = {
            let sm = self.sm.lock().await;
            let data = bitcode::encode(&*sm);
            let last_applied_log = sm.last_applied_log;
            let last_membership = sm.last_membership.clone();

            if let Some(d) = self.block.get_blocking(&BlockOperation::BuildSnapshot) {
                log::info!("blocking snapshot build for {d:?}");
                TypeConfig::sleep(d).await;
            }
            (data, last_applied_log, last_membership)
        };

        let meta = SnapshotMetaOf::<TypeConfig> {
            last_log_id: last_applied_log,
            last_membership,
        };

        let snapshot = MemStoreSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        {
            let mut current_snapshot = self.current_snapshot.lock().await;
            *current_snapshot = Some(snapshot);
        }

        Ok(SnapshotOf::<TypeConfig, Cursor<Vec<u8>>> {
            meta,
            snapshot: Cursor::new(data),
        })
    }
}

impl RaftLogStorage<TypeConfig> for Arc<MemLogStore> {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        let log = self.log.lock().await;
        let last = log.values().next_back().map(|ent| ent.log_id());
        let last_purged = *self.last_purged_log_id.lock().await;

        let last = match last {
            None => last_purged,
            Some(x) => Some(x),
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &VoteOf<TypeConfig>) -> Result<(), io::Error> {
        log::debug!("save_vote: {vote:?}");
        let mut h = self.vote.lock().await;
        *h = Some(*vote);
        Ok(())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<TypeConfig>>,
    ) -> Result<(), io::Error> {
        let enabled = self.enable_saving_committed.load(Ordering::Relaxed);
        log::debug!("save_committed: {committed:?}, enabled: {enabled}");
        if !enabled {
            return Ok(());
        }
        let mut c = self.committed.lock().await;
        *c = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<TypeConfig>>, io::Error> {
        let enabled = self.enable_saving_committed.load(Ordering::Relaxed);
        log::debug!("read_committed, enabled: {enabled}");
        if !enabled {
            return Ok(None);
        }
        Ok(*self.committed.lock().await)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<TypeConfig>> + OptionalSend,
    {
        let mut log = self.log.lock().await;
        for entry in entries {
            log.insert(entry.index(), entry);
        }
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(
        &mut self,
        last_log_id: Option<LogIdOf<TypeConfig>>,
    ) -> Result<(), io::Error> {
        log::debug!("truncate_after: ({last_log_id:?}, +oo)");
        let start_index = match last_log_id {
            Some(log_id) => log_id.index() + 1,
            None => 0,
        };

        let mut log = self.log.lock().await;
        let keys: Vec<u64> = log.range(start_index..).map(|(k, _)| *k).collect();
        for key in keys {
            log.remove(&key);
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogIdOf<TypeConfig>) -> Result<(), io::Error> {
        log::debug!("purge_log_upto: {log_id:?}");

        if let Some(d) = self.block.get_blocking(&BlockOperation::PurgeLog) {
            log::info!("block purging log for {d:?}");
            TypeConfig::sleep(d).await;
        }

        {
            let mut ld = self.last_purged_log_id.lock().await;
            assert!(*ld <= Some(log_id));
            *ld = Some(log_id);
        }

        {
            let mut log = self.log.lock().await;
            let keys: Vec<u64> = log.range(..=log_id.index()).map(|(k, _)| *k).collect();
            for key in keys {
                log.remove(&key);
            }
        }
        Ok(())
    }
}

impl RaftStateMachine<TypeConfig> for Arc<MemStateMachine> {
    type SnapshotData = Cursor<Vec<u8>>;
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogIdOf<TypeConfig>>, StoredMembershipOf<TypeConfig>), io::Error> {
        let sm = self.sm.lock().await;
        Ok((sm.last_applied_log, sm.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin + OptionalSend,
    {
        let mut sm = self.sm.lock().await;

        while let Some((entry, responder)) = entries.try_next().await? {
            if self.panic_on_apply.load(Ordering::Relaxed) {
                panic!("injected state-machine worker panic during apply");
            }

            sm.last_applied_log = Some(entry.log_id);

            let response = match entry.payload {
                EntryPayload::Blank => ClientResponse(None),
                EntryPayload::Normal(ref data) => {
                    let previous = sm
                        .client_status
                        .insert(data.client.clone(), data.status.clone());
                    ClientResponse(previous)
                }
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership =
                        StoredMembershipOf::<TypeConfig>::new(Some(entry.log_id), mem.clone());
                    ClientResponse(None)
                }
            };

            if let Some(responder) = responder {
                responder.send(response);
            }
        }
        Ok(())
    }

    async fn try_create_snapshot_builder(&mut self, force: bool) -> Option<Self::SnapshotBuilder> {
        self.try_create_snapshot_builder_count
            .fetch_add(1, Ordering::Relaxed);
        if force || self.allow_build_snapshot.load(Ordering::Relaxed) {
            Some(self.get_snapshot_builder().await)
        } else {
            None
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<TypeConfig>,
        snapshot: Self::SnapshotData,
    ) -> Result<(), io::Error> {
        let data = snapshot.into_inner();
        let new_sm: MemStoreStateMachine = bitcode::decode(&data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        let new_snapshot = MemStoreSnapshot {
            meta: meta.clone(),
            data,
        };

        {
            let mut sm = self.sm.lock().await;
            *sm = new_sm;
        }

        {
            let mut current_snapshot = self.current_snapshot.lock().await;
            *current_snapshot = Some(new_snapshot);
        }
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<SnapshotOf<TypeConfig, Self::SnapshotData>>, io::Error> {
        let snapshot = self.current_snapshot.lock().await;
        match &*snapshot {
            Some(s) => Ok(Some(SnapshotOf::<TypeConfig, Self::SnapshotData> {
                meta: s.meta.clone(),
                snapshot: Cursor::new(s.data.clone()),
            })),
            None => Ok(None),
        }
    }
}
