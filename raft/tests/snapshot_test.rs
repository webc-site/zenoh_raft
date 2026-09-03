//! 快照构建、流式传输与安装测试套件

mod fixtures;

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use fixtures::{RaftRouter, log_id, timeout};
use maplit::btreeset;
use zenoh_raft::{
  Config, Entry, EntryPayload, Membership, RaftLogReader, RaftSnapshotBuilder, ServerState,
  SnapshotPolicy, StorageHelper, Vote,
  alias::{SnapshotMetaOf, StoredMembershipOf},
  raft::{AppendEntriesRequest, AppendEntriesResponse},
  storage::{RaftLogStorage, RaftLogStorageExt, RaftStateMachine},
  testing::{
    blank_ent, membership_ent,
    memstore::{BlockOperation, ClientRequest, IntoMemClientRequest, TypeConfig},
  },
  type_config::TypeConfigExt,
};

/// 快照构建与快照触发测试
#[compio::test]
async fn test_building_snapshot() -> Result<()> {
  let snapshot_threshold: u64 = 10;

  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      enable_elect: false,
      snapshot_policy: SnapshotPolicy::LogsSinceLast(snapshot_threshold),
      max_in_snapshot_log_to_keep: 2,
      purge_batch_size: 1,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  router
    .client_request_many(0, "0", (snapshot_threshold - 1 - log_index) as usize)
    .await?;
  log_index = snapshot_threshold - 1;

  for id in [0, 1, 2] {
    router
      .wait(&id, timeout())
      .applied_index(Some(log_index), "all entries applied")
      .await?;
  }

  router
    .wait(&0, timeout())
    .snapshot(log_id(1, 0, log_index), "snapshot generated")
    .await?;

  Ok(())
}

/// 手动触发快照 (trigger().snapshot()) 测试
#[compio::test]
async fn test_trigger_snapshot() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      enable_elect: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  let n0 = router.get_raft_handle(&0)?;
  n0.client_write(ClientRequest::make_request("manual", 1))
    .await?;
  log_index += 1;

  n0.trigger().snapshot().await?;

  router
    .wait(&0, timeout())
    .snapshot(
      log_id(1, 0, log_index),
      "manually triggered snapshot exists",
    )
    .await?;

  Ok(())
}

/// 快照安装与追赶 (install_snapshot / catch up) 测试
#[compio::test]
async fn test_install_snapshot_and_catchup() -> Result<()> {
  let snapshot_threshold: u64 = 10;
  let log_cnt = snapshot_threshold + 5;

  let config = Arc::new(
    Config {
      snapshot_policy: SnapshotPolicy::LogsSinceLast(snapshot_threshold),
      max_in_snapshot_log_to_keep: 0,
      purge_batch_size: 1,
      enable_heartbeat: false,
      enable_elect: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router.new_cluster(btreeset! {0}, btreeset! {}).await?;

  router
    .client_request_many(0, "0", (snapshot_threshold - 1 - log_index) as usize)
    .await?;
  log_index = snapshot_threshold - 1;

  router
    .wait(&0, timeout())
    .applied_index(Some(log_index), "send log to trigger snapshot")
    .await?;
  router
    .wait(&0, timeout())
    .snapshot(log_id(1, 0, log_index), "snapshot")
    .await?;

  router
    .client_request_many(0, "0", (log_cnt - log_index) as usize)
    .await?;
  log_index = log_cnt;

  router.new_raft_node(1).await;
  router.add_learner(0, 1).await?;
  log_index += 1;

  for id in [0, 1] {
    router
      .wait(&id, timeout())
      .applied_index(Some(log_index), "all nodes catch up")
      .await?;
  }
  router
    .wait(&1, timeout())
    .snapshot(
      log_id(1, 0, snapshot_threshold - 1),
      "learner received snapshot",
    )
    .await?;

  let (_sto1, mut sm1) = router.get_storage_handle(&1)?;
  let snap1 = sm1.get_current_snapshot().await?.unwrap();
  assert_eq!(
    snap1.meta.last_log_id,
    Some(log_id(1, 0, snapshot_threshold - 1))
  );

  Ok(())
}

/// 状态机拒绝创建快照时的行为验证
#[compio::test]
async fn test_sm_can_refuse_snapshot_building() -> Result<()> {
  let config = Arc::new(
    Config {
      snapshot_policy: SnapshotPolicy::Never,
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router.new_cluster(btreeset! {0}, btreeset! {}).await?;

  log_index += router.client_request_many(0, "0", 10).await?;
  router
    .wait(&0, timeout())
    .applied_index(Some(log_index), "write logs")
    .await?;

  {
    let (_, sm) = router.get_storage_handle(&0)?;
    sm.allow_build_snapshot(false);
  }

  {
    let n0 = router.get_raft_handle(&0)?;
    let snapshot_progress = n0.watch_snapshot_progress();
    n0.trigger().snapshot().await?;

    TypeConfig::sleep(Duration::from_millis(200)).await;

    let (_, mut sm) = router.get_storage_handle(&0)?;
    let snapshot = sm.get_current_snapshot().await?;
    assert!(snapshot.is_none());

    let state = n0.with_raft_state(|st| st.clone()).await?;
    assert_eq!(SnapshotMetaOf::<TypeConfig>::default(), state.snapshot_meta);
    assert_eq!(None, snapshot_progress.get());
  }

  {
    let (_, sm) = router.get_storage_handle(&0)?;
    sm.allow_build_snapshot(true);
  }

  {
    let n0 = router.get_raft_handle(&0)?;
    n0.trigger().snapshot().await?;

    router
      .wait(&0, timeout())
      .snapshot(log_id(1, 0, log_index), "snapshot created")
      .await?;
  }

  Ok(())
}

/// 构建快照不会阻塞追加日志请求
#[compio::test]
async fn test_building_snapshot_does_not_block_append() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_tick: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router.new_cluster(btreeset! {0,1}, btreeset! {}).await?;

  let follower = router.get_raft_handle(&1)?;

  {
    let (mut _sto1, sm1) = router.get_storage_handle(&1)?;
    sm1
      .block
      .set_blocking(BlockOperation::BuildSnapshot, Duration::from_millis(5_000));
  }

  {
    log_index += router.client_request_many(0, "0", 10).await?;
    router
      .wait(&1, timeout())
      .applied_index(Some(log_index), "written 10 logs")
      .await?;

    follower.trigger().snapshot().await?;
    TypeConfig::sleep(Duration::from_millis(500)).await;

    let res = router
      .wait(&1, Some(Duration::from_millis(500)))
      .snapshot(log_id(1, 0, log_index), "building snapshot is blocked")
      .await;
    assert!(res.is_err());
  }

  {
    let rpc = AppendEntriesRequest::<TypeConfig> {
      vote: Vote::new_committed(1, 0),
      prev_log_id: Some(log_id(1, 0, log_index)),
      entries: vec![blank_ent::<TypeConfig>(1, 0, 15)],
      leader_commit: None,
    };

    let node = router.get_raft_handle(&1)?;
    let fu = node.append_entries(rpc);
    let fu = TypeConfig::timeout(Duration::from_millis(500), fu);
    let resp = fu.await??;
    assert!(resp.is_success());
  }

  Ok(())
}

/// 快照内的日志自动裁剪测试
#[compio::test]
async fn test_purge_in_snapshot_logs() -> Result<()> {
  let max_keep = 2;

  let config = Arc::new(
    Config {
      max_in_snapshot_log_to_keep: max_keep,
      purge_batch_size: 1,
      enable_tick: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router.new_cluster(btreeset! {0}, btreeset! {1}).await?;

  let leader = router.get_raft_handle(&0)?;
  let learner = router.get_raft_handle(&1)?;

  let (_sto0, mut _sm0) = router.get_storage_handle(&0)?;

  {
    log_index += router.client_request_many(0, "0", 10).await?;
    leader.trigger().snapshot().await?;
    leader
      .wait(timeout())
      .snapshot(log_id(1, 0, log_index), "building 1st snapshot")
      .await?;
    let (mut sto0, mut _sm0) = router.get_storage_handle(&0)?;

    TypeConfig::sleep(Duration::from_millis(500)).await;

    let logs = sto0.try_get_log_entries(..).await?;
    assert_eq!(max_keep as usize, logs.len());
  }

  {
    router.set_network_error(1, true);

    log_index += router.client_request_many(0, "0", 5).await?;
    router
      .wait(&0, timeout())
      .applied_index(Some(log_index), "write another 5 logs")
      .await?;

    leader.trigger().snapshot().await?;
    leader
      .wait(timeout())
      .snapshot(log_id(1, 0, log_index), "building 2nd snapshot")
      .await?;
  }

  TypeConfig::sleep(Duration::from_millis(2_000)).await;

  {
    router.set_network_error(1, false);

    learner
      .wait(timeout())
      .snapshot(log_id(1, 0, log_index), "learner install snapshot")
      .await?;

    let (mut sto1, mut _sm) = router.get_storage_handle(&1)?;
    let logs = sto1.try_get_log_entries(..).await?;
    assert_eq!(0, logs.len());
  }

  let (mut sto0, _) = router.get_storage_handle(&0)?;
  let logs = sto0.try_get_log_entries(..).await?;
  assert_eq!(
    log_index + 1 - max_keep,
    logs[0].log_id.index(),
    "leader's local logs are purged"
  );

  Ok(())
}

/// 快照安装覆盖旧成员关系
#[compio::test]
async fn test_snapshot_overrides_membership() -> Result<()> {
  let snapshot_threshold: u64 = 10;

  let config = Arc::new(
    Config {
      snapshot_policy: SnapshotPolicy::LogsSinceLast(snapshot_threshold),
      max_in_snapshot_log_to_keep: 0,
      purge_batch_size: 1,
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config.clone());

  let mut log_index = router.new_cluster(btreeset! {0}, btreeset! {}).await?;

  router
    .client_request_many(0, "0", (snapshot_threshold - 1 - log_index) as usize)
    .await?;
  log_index = snapshot_threshold - 1;

  router
    .wait(&0, timeout())
    .applied_index(Some(log_index), "send log to trigger snapshot")
    .await?;
  router
    .wait(&0, timeout())
    .snapshot(log_id(1, 0, log_index), "snapshot")
    .await?;

  router.new_raft_node(1).await;
  let (mut sto, mut sm) = router.get_storage_handle(&1)?;

  let req = AppendEntriesRequest {
    vote: Vote::new_committed(1, 0),
    prev_log_id: None,
    entries: vec![
      blank_ent::<TypeConfig>(0, 0, 0),
      Entry {
        log_id: log_id(1, 0, 1),
        payload: EntryPayload::Membership(Membership::new_with_defaults(vec![btreeset! {2,3}], [])),
      },
    ],
    leader_commit: Some(log_id(0, 0, 0)),
  };

  let node = router.get_raft_handle(&1)?;
  node.append_entries(req).await?;

  let m = StorageHelper::new(&mut sto, &mut sm)
    .get_membership()
    .await?;
  assert_eq!(
    &StoredMembershipOf::<TypeConfig>::default(),
    m.committed().as_ref()
  );

  let snapshot_index = log_index;
  router.add_learner(0, 1).await?;
  log_index += 1;

  for id in [0, 1] {
    router
      .wait(&id, timeout())
      .applied_index(Some(log_index), "add learner")
      .await?;
  }
  router
    .wait(&1, timeout())
    .snapshot(log_id(1, 0, snapshot_index), "")
    .await?;

  let m = StorageHelper::new(&mut sto, &mut sm)
    .get_membership()
    .await?;
  assert_eq!(
    &Membership::new_with_defaults(vec![btreeset! {0}], btreeset! {1}),
    m.committed().membership(),
    "membership should be overridden by the snapshot"
  );

  Ok(())
}

/// 快照安装删除冲突日志
#[compio::test]
async fn test_snapshot_delete_conflicting_logs() -> Result<()> {
  let snapshot_threshold: u64 = 10;

  let config = Arc::new(
    Config {
      snapshot_policy: SnapshotPolicy::LogsSinceLast(snapshot_threshold),
      max_in_snapshot_log_to_keep: 0,
      purge_batch_size: 1,
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let mut log_index;

  {
    let (mut sto0, sm0) = router.new_store();
    sto0.save_vote(&Vote::new(4, 0)).await?;
    sto0
      .blocking_append([membership_ent::<TypeConfig>(0, 0, 0, vec![btreeset! {0}])])
      .await?;
    log_index = 1;

    router.new_raft_node_with_sto(0, sto0, sm0).await;
    router
      .wait(&0, timeout())
      .state(ServerState::Leader, "init node-0 server-state")
      .await?;
    router
      .wait(&0, timeout())
      .applied_index(Some(log_index), "init node-0 log")
      .await?;
  }

  router
    .client_request_many(0, "0", (snapshot_threshold - 1 - log_index) as usize)
    .await?;
  log_index = snapshot_threshold - 1;

  router
    .wait(&0, timeout())
    .applied_index(Some(log_index), "trigger snapshot")
    .await?;
  router
    .wait(&0, timeout())
    .snapshot(log_id(5, 0, log_index), "build snapshot")
    .await?;

  router.new_raft_node(1).await;

  let req = AppendEntriesRequest {
    vote: Vote::new_committed(1, 0),
    prev_log_id: None,
    entries: vec![
      blank_ent::<TypeConfig>(0, 0, 0),
      blank_ent::<TypeConfig>(1, 0, 1),
      Entry {
        log_id: log_id(1, 0, 2),
        payload: EntryPayload::Membership(Membership::new_with_defaults(vec![btreeset! {2,3}], [])),
      },
      blank_ent::<TypeConfig>(1, 0, 3),
      blank_ent::<TypeConfig>(1, 0, 4),
    ],
    leader_commit: Some(log_id(1, 0, 2)),
  };

  let node = router.get_raft_handle(&1)?;
  node.append_entries(req).await?;

  {
    let (mut sto0, mut sm0) = router.get_storage_handle(&0)?;
    let snap = {
      let mut b = sm0.get_snapshot_builder().await;
      b.build_snapshot().await?
    };

    let vote = sto0.read_vote().await?.unwrap();
    let node = router.get_raft_handle(&1)?;
    node.install_full_snapshot(vote, snap).await?;

    router
      .wait(&1, timeout())
      .snapshot(log_id(5, 0, log_index), "node-1 snapshot")
      .await?;
  }

  {
    let (mut sto1, mut sm1) = router.get_storage_handle(&1)?;
    let m = StorageHelper::new(&mut sto1, &mut sm1)
      .get_membership()
      .await?;

    assert_eq!(
      &Membership::new_with_defaults(vec![btreeset! {0}], []),
      m.committed().membership()
    );

    let log_st = sto1.get_log_state().await?;
    assert_eq!(
      Some(log_id(5, 0, snapshot_threshold - 1)),
      log_st.last_purged_log_id
    );
    assert_eq!(
      Some(log_id(5, 0, snapshot_threshold - 1)),
      log_st.last_log_id
    );
  }

  Ok(())
}

/// 构建快照不会阻塞日志应用到状态机
#[compio::test]
async fn test_building_snapshot_does_not_block_apply() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_tick: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router.new_cluster(btreeset! {0, 1}, btreeset! {}).await?;

  let follower = router.get_raft_handle(&1)?;

  {
    let (mut _sto1, sm1) = router.get_storage_handle(&1)?;
    sm1.block.set_blocking(
      BlockOperation::DelayBuildingSnapshot,
      Duration::from_millis(5_000),
    );
  }

  {
    log_index += router.client_request_many(0, "0", 10).await?;
    router
      .wait(&1, timeout())
      .applied_index(Some(log_index), "written 10 logs")
      .await?;

    follower.trigger().snapshot().await?;
    TypeConfig::sleep(Duration::from_millis(500)).await;

    let res = router
      .wait(&1, Some(Duration::from_millis(500)))
      .snapshot(log_id(1, 0, log_index), "building snapshot is blocked")
      .await;
    assert!(res.is_err(), "snapshot should be blocked and cannot finish");
  }

  {
    let next = log_index + 1;

    let rpc = AppendEntriesRequest::<TypeConfig> {
      vote: Vote::new_committed(1, 0),
      prev_log_id: Some(log_id(1, 0, log_index)),
      entries: vec![blank_ent::<TypeConfig>(1, 0, next)],
      leader_commit: Some(log_id(1, 0, next)),
    };

    let node = router.get_raft_handle(&1)?;
    let resp = node.append_entries(rpc).await?;
    assert_eq!(resp, AppendEntriesResponse::Success);

    router
      .wait(&1, timeout())
      .applied_index(
        Some(next),
        format!(
          "log at index {} can be applied, while snapshot is building",
          next
        ),
      )
      .await?;
  }

  Ok(())
}

/// SnapshotPolicy::Never 策略下不自动触发快照
#[compio::test]
async fn test_snapshot_policy_never() -> Result<()> {
  let config = Arc::new(
    Config {
      snapshot_policy: SnapshotPolicy::Never,
      enable_heartbeat: false,
      enable_elect: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router.new_cluster(btreeset! {0}, btreeset! {}).await?;

  log_index += router.client_request_many(0, "0", 20).await?;
  router
    .wait(&0, timeout())
    .applied_index(Some(log_index), "write 20 logs")
    .await?;

  let n0 = router.get_raft_handle(&0)?;
  let snap = n0.get_snapshot().await?;
  assert!(
    snap.is_none(),
    "never policy does not create snapshot automatically"
  );

  Ok(())
}

/// 第二次压缩快照不应丢失集群成员关系
#[compio::test]
async fn test_snapshot_uses_prev_snap_membership() -> Result<()> {
  let snapshot_threshold: u64 = 10;

  let config = Arc::new(
    Config {
      snapshot_policy: SnapshotPolicy::LogsSinceLast(snapshot_threshold),
      max_in_snapshot_log_to_keep: 3,
      purge_batch_size: 1,
      enable_tick: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config.clone());

  let mut log_index = router.new_cluster(btreeset! {0,1}, btreeset! {}).await?;

  let (mut sto0, mut sm0) = router.get_storage_handle(&0)?;

  {
    router
      .client_request_many(0, "0", (snapshot_threshold - 1 - log_index) as usize)
      .await?;
    log_index = snapshot_threshold - 1;

    for id in [0, 1] {
      router
        .wait(&id, timeout())
        .applied_index(Some(log_index), "send log to trigger snapshot")
        .await?;
    }
    router
      .wait(&0, timeout())
      .snapshot(log_id(1, 0, log_index), "1st snapshot")
      .await?;

    {
      let logs = sto0.try_get_log_entries(..).await?;
      assert_eq!(3, logs.len(), "only one applied log is kept");
    }
    let m = StorageHelper::new(&mut sto0, &mut sm0)
      .get_membership()
      .await?;

    assert_eq!(
      &Membership::new_with_defaults(vec![btreeset! {0,1}], []),
      m.committed().membership(),
    );
    assert_eq!(
      &Membership::new_with_defaults(vec![btreeset! {0,1}], []),
      m.effective().membership(),
    );
  }

  {
    router
      .client_request_many(0, "0", (snapshot_threshold * 2 - 1 - log_index) as usize)
      .await?;
    log_index = snapshot_threshold * 2 - 1;

    for id in [0, 1] {
      router
        .wait(&id, None)
        .applied_index(Some(log_index), "send log to trigger snapshot")
        .await?;
    }
    router
      .wait(&0, None)
      .snapshot(log_id(1, 0, log_index), "2nd snapshot")
      .await?;
  }

  {
    {
      let logs = sto0.try_get_log_entries(..).await?;
      assert_eq!(3, logs.len(), "only one applied log");
    }
    let m = StorageHelper::new(&mut sto0, &mut sm0)
      .get_membership()
      .await?;

    assert_eq!(
      &Membership::new_with_defaults(vec![btreeset! {0,1}], []),
      m.committed().membership(),
    );
    assert_eq!(
      &Membership::new_with_defaults(vec![btreeset! {0,1}], []),
      m.effective().membership(),
    );
  }

  Ok(())
}

/// 复制任务不应永远阻塞日志裁剪
#[compio::test]
async fn test_replication_does_not_block_purge() -> Result<()> {
  let max_keep = 2;

  let config = Arc::new(
    Config {
      max_in_snapshot_log_to_keep: max_keep,
      purge_batch_size: 1,
      enable_tick: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router.new_cluster(btreeset! {0}, btreeset! {1,2}).await?;

  let leader = router.get_raft_handle(&0)?;

  router.set_network_error(1, true);
  router.set_network_error(2, true);

  {
    log_index += router.client_request_many(0, "0", 10).await?;

    leader.trigger().snapshot().await?;
    leader
      .wait(timeout())
      .snapshot(log_id(1, 0, log_index), "built snapshot")
      .await?;

    TypeConfig::sleep(Duration::from_millis(2_000)).await;

    let (mut sto0, mut _sm0) = router.get_storage_handle(&0)?;
    let logs = sto0.try_get_log_entries(..).await?;
    assert_eq!(
      max_keep as usize,
      logs.len(),
      "leader's local logs are purged"
    );
  }

  Ok(())
}

/// 快照构建中途到达的策略触发不应丢失 (Issue 1829)
#[compio::test]
async fn test_lost_snapshot_trigger() -> Result<()> {
  use zenoh_raft::LogIdOptionExt;

  const FIRST: u64 = 9;
  const FINAL: u64 = 23;

  let config = Arc::new(
    Config {
      snapshot_policy: SnapshotPolicy::LogsSinceLast(10),
      enable_heartbeat: false,
      enable_tick: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let log_index = router.new_cluster(btreeset! {0}, btreeset! {}).await?;
  let n0 = router.get_raft_handle(&0)?;

  {
    let (_, sm) = router.get_storage_handle(&0)?;
    sm.block
      .set_blocking(BlockOperation::BuildSnapshot, Duration::from_millis(2_000));
  }

  {
    let count = (FIRST - log_index) as usize;
    router.client_request_many(0, "cli", count).await?;
    router
      .wait(&0, timeout())
      .applied_index(Some(FIRST), "applied up to the first threshold")
      .await?;
  }

  {
    for i in 0..(FINAL - FIRST) {
      n0.client_write_ff(ClientRequest::make_request("ff", i), None)
        .await?;
    }
  }

  let m = router
    .wait(&0, timeout())
    .committed_index(Some(FINAL), "fire-and-forget writes committed")
    .await?;
  assert!(
    m.last_applied.index() < Some(FINAL),
    "apply must be pinned behind committed while build holds the sm lock: last_applied={:?}, committed={:?}",
    m.last_applied.index(),
    m.local_committed.index(),
  );

  router
    .wait(&0, Some(Duration::from_millis(6_000)))
    .applied_index(Some(FINAL), "build released; applies catch up")
    .await?;

  router
    .wait(&0, Some(Duration::from_millis(6_000)))
    .snapshot(
      log_id(1, 0, FINAL),
      "snapshot converges after the in-flight build completes",
    )
    .await?;

  Ok(())
}

/// 当 Follower/Learner 所需的日志已被清除时，Leader 自动切换为快照复制
#[compio::test]
async fn test_switch_to_snapshot_replication_when_lacking_log() -> Result<()> {
  let snapshot_threshold: u64 = 20;
  let log_cnt = snapshot_threshold + 11;

  let config = Arc::new(
    Config {
      snapshot_policy: SnapshotPolicy::LogsSinceLast(snapshot_threshold),
      max_in_snapshot_log_to_keep: 0,
      purge_batch_size: 1,
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config);

  let mut log_index = router.new_cluster(btreeset! {0}, btreeset! {}).await?;

  router
    .client_request_many(0, "0", (snapshot_threshold - 1 - log_index) as usize)
    .await?;
  log_index = snapshot_threshold - 1;

  router
    .wait(&0, None)
    .applied_index(Some(log_index), "send log to trigger snapshot")
    .await?;
  router
    .wait(&0, None)
    .snapshot(log_id(1, 0, log_index), "snapshot")
    .await?;

  {
    let (mut sto, mut sm) = router.get_storage_handle(&0)?;
    assert_eq!(
      sto.get_log_state().await?.last_log_id,
      Some(log_id(1, 0, log_index))
    );
    assert_eq!(sto.read_vote().await?, Some(Vote::new_committed(1, 0)));

    let (last_applied, _) = sm.applied_state().await?;
    assert_eq!(last_applied, Some(log_id(1, 0, log_index)));

    let snap = sm.get_current_snapshot().await?.unwrap();
    assert_eq!(snap.meta.last_log_id, Some(log_id(1, 0, log_index)));
  }

  router
    .client_request_many(0, "0", (log_cnt - log_index) as usize)
    .await?;
  log_index = log_cnt;

  router.new_raft_node(1).await;
  router.add_learner(0, 1).await?;
  log_index += 1;

  for id in [0, 1] {
    router
      .wait(&id, None)
      .applied_index(Some(log_index), "add learner")
      .await?;
  }
  router
    .wait(&1, None)
    .snapshot(log_id(1, 0, snapshot_threshold - 1), "")
    .await?;

  for id in [0, 1] {
    let (mut sto, mut sm) = router.get_storage_handle(&id)?;
    assert_eq!(
      sto.get_log_state().await?.last_log_id,
      Some(log_id(1, 0, log_index))
    );
    assert_eq!(sto.read_vote().await?, Some(Vote::new_committed(1, 0)));

    let (last_applied, _) = sm.applied_state().await?;
    assert_eq!(last_applied, Some(log_id(1, 0, log_index)));

    let snap = sm.get_current_snapshot().await?.unwrap();
    assert_eq!(
      snap.meta.last_log_id,
      Some(log_id(1, 0, snapshot_threshold - 1))
    );
  }

  Ok(())
}

/// 向不可达节点传输快照时不应永久阻塞
#[compio::test]
async fn test_snapshot_to_unreachable_node_should_not_block() -> Result<()> {
  let config = Arc::new(
    Config {
      purge_batch_size: 1,
      max_in_snapshot_log_to_keep: 0,
      enable_heartbeat: false,
      enable_elect: false,
      backoff: "10ms, 10ms".to_string(),
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config);
  let mut log_index = router.new_cluster(btreeset! {0, 1}, btreeset! {2}).await?;

  router.set_network_error(2, true);

  let n = 10;
  log_index += router.client_request_many(0, "0", n).await?;
  router
    .wait(&0, timeout())
    .applied_index(Some(log_index), "writes")
    .await?;

  let n0 = router.get_raft_handle(&0)?;
  n0.trigger().snapshot().await?;
  n0.wait(timeout())
    .snapshot(log_id(1, 0, log_index), "snapshot")
    .await?;
  n0.wait(timeout())
    .purged(Some(log_id(1, 0, log_index)), "purged")
    .await?;

  n0.change_membership([0], true).await?;
  n0.wait(timeout())
    .voter_ids([0], "change membership to {0}")
    .await?;

  Ok(())
}

/// install_full_snapshot 使用更低的 Vote 应被拒绝
#[compio::test]
async fn test_install_snapshot_lower_vote() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      enable_tick: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config);
  let _log_index = router.new_cluster(btreeset! {0}, btreeset! {}).await?;

  let n0 = router.get_raft_handle(&0)?;
  n0.trigger().snapshot().await?;
  n0.wait(timeout())
    .snapshot(log_id(1, 0, _log_index), "snapshot")
    .await?;

  let snap = n0.get_snapshot().await?.unwrap();

  let _res = n0
    .append_entries(AppendEntriesRequest {
      vote: Vote::new_committed(2, 1),
      prev_log_id: None,
      entries: vec![],
      leader_commit: None,
    })
    .await;
  let vote = n0.with_raft_state(|st| *st.vote_ref()).await?;
  assert_eq!(Vote::new_committed(2, 1), vote);

  let got = n0
    .install_full_snapshot(Vote::new_committed(1, 1), snap)
    .await?;
  assert_eq!(Vote::new_committed(2, 1), got.vote);

  Ok(())
}
