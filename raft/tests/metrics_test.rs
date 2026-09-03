//! Raft 指标与监控测试套件

mod fixtures;

use std::sync::Arc;

use anyhow::Result;
use fixtures::{RaftRouter, log_id, timeout};
use maplit::btreeset;
use zenoh_raft::{Config, ServerState, type_config::TypeConfigExt};

/// 当前 Leader 指标测试
#[compio::test]
async fn test_current_leader() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      enable_elect: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  let leader = router.leader();
  assert_eq!(leader, Some(0));

  let m0 = router.get_metrics(&0)?;
  assert_eq!(m0.current_leader, Some(0));
  assert_eq!(m0.state, ServerState::Leader);

  let m1 = router.get_metrics(&1)?;
  assert_eq!(m1.current_leader, Some(0));
  assert_eq!(m1.state, ServerState::Follower);

  Ok(())
}

/// 状态机应用指标一致性测试
#[compio::test]
async fn test_metrics_state_machine_consistency() -> Result<()> {
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

  router.client_request_many(0, "metrics", 5).await?;
  log_index += 5;

  for id in [0, 1, 2] {
    router
      .wait(&id, timeout())
      .applied_index(Some(log_index), "all metrics updated")
      .await?;
  }

  Ok(())
}

/// Leader 指标 (last_quorum_acked, replication metrics, purged) 监控测试
#[compio::test]
async fn test_leader_metrics() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: true,
      heartbeat_interval: 100,
      election_timeout_min: 500,
      election_timeout_max: 600,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router.new_cluster(btreeset! {0, 1}, btreeset! {2}).await?;

  router.client_request_many(0, "metrics", 5).await?;
  log_index += 5;

  for id in [0, 1, 2] {
    router
      .wait(&id, timeout())
      .applied_index(Some(log_index), "applied 5 writes")
      .await?;
  }

  let n0 = router.get_raft_handle(&0)?;
  let metrics = n0.metrics().borrow_watched().clone();
  assert_eq!(metrics.state, ServerState::Leader);
  assert_eq!(metrics.current_term, 1);
  assert_eq!(metrics.last_log_index, Some(log_index));
  assert!(
    metrics
      .membership_config
      .membership()
      .voter_ids()
      .any(|x| x == 0)
  );
  assert!(
    metrics
      .membership_config
      .membership()
      .voter_ids()
      .any(|x| x == 1)
  );
  assert!(
    metrics
      .membership_config
      .membership()
      .learner_ids()
      .any(|x| x == 2)
  );

  Ok(())
}

/// Server metrics 与 Data metrics 行为测试
#[compio::test]
async fn test_server_metrics_and_data_metrics() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      enable_elect: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config.clone());

  let mut log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

  let node = router.get_raft_handle(&0)?;
  let server_metrics = node.server_metrics();
  let data_metrics = node.data_metrics();

  let current_leader = router.current_leader(0).await;
  let server_metrics_1 = {
    let sm = server_metrics.borrow_watched();
    sm.clone()
  };
  let leader = server_metrics_1.current_leader;
  assert_eq!(leader, current_leader);

  let n = 10;
  log_index += router.client_request_many(0, "foo", n).await?;
  router
    .wait(&0, timeout())
    .applied_index(Some(log_index), "applied log index")
    .await?;

  let last_log_index = data_metrics
    .borrow_watched()
    .last_log
    .map(|x| x.index())
    .unwrap_or_default();
  assert_eq!(last_log_index, log_index);

  let server_metrics_2 = server_metrics.borrow_watched().clone();
  assert_eq!(
    server_metrics_1, server_metrics_2,
    "server metrics should not update on pure data write"
  );

  Ok(())
}

/// 心跳指标 (heartbeat metrics) 监控测试
#[compio::test]
async fn test_heartbeat_metrics() -> Result<()> {
  use zenoh_raft::{testing::memstore::TypeConfig, type_config::TypeConfigExt};

  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      heartbeat_interval: 50,
      enable_elect: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config.clone());

  let _log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;
  let leader = router.get_raft_handle(&0)?;

  let now = TypeConfig::now();
  leader.trigger().heartbeat().await?;

  leader
    .wait(timeout())
    .metrics(
      |metrics| {
        let heartbeat = match metrics.heartbeat.as_ref() {
          Some(h) => h,
          None => return false,
        };
        let node1 = heartbeat.get(&1);
        let node2 = heartbeat.get(&2);

        match (node1, node2) {
          (Some(Some(n1)), Some(Some(n2))) => (**n1 >= now) && (**n2 >= now),
          _ => false,
        }
      },
      "quorum acked heartbeat refreshed",
    )
    .await?;

  Ok(())
}

/// 已提交成员配置 (committed_membership_config) 指标测试
#[compio::test]
async fn test_committed_membership_config() -> Result<()> {
  use std::time::Duration;

  use zenoh_raft::{Membership, testing::memstore::TypeConfig};

  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      enable_elect: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config.clone());

  let _log_index = router.new_cluster(btreeset! {0,1}, btreeset! {}).await?;
  let node = router.get_raft_handle(&0)?;

  let old_membership = Membership::new_with_defaults(vec![btreeset! {0,1}], []);
  let new_membership = Membership::new_with_defaults(vec![btreeset! {0,1}], [2]);

  {
    let metrics = node.metrics().borrow_watched().clone();
    assert_eq!(
      metrics.committed_membership_config,
      metrics.membership_config
    );
    assert_eq!(
      &old_membership,
      metrics.committed_membership_config.membership()
    );
  }

  router.new_raft_node(2).await;
  router.set_network_error(1, true);

  let n0 = node.clone();
  let add_learner_handle = TypeConfig::spawn(async move { n0.add_learner(2, (), false).await });

  {
    let metrics = router
      .wait(&0, Some(Duration::from_millis(3_000)))
      .metrics(
        |x| x.membership_config.membership() == &new_membership,
        "the effective membership config contains the new learner",
      )
      .await?;

    assert_eq!(
      &old_membership,
      metrics.committed_membership_config.membership()
    );
    assert!(metrics.committed_membership_config.log_id() < metrics.membership_config.log_id());
  }

  {
    router.set_network_error(1, false);
    add_learner_handle.await??;

    let metrics = router
      .wait(&0, Some(Duration::from_millis(3_000)))
      .metrics(
        |x| x.committed_membership_config == x.membership_config,
        "the committed membership config catches up with the effective one",
      )
      .await?;
    assert_eq!(
      &new_membership,
      metrics.committed_membership_config.membership()
    );

    let server_metrics = node.server_metrics().borrow_watched().clone();
    assert_eq!(
      metrics.committed_membership_config,
      server_metrics.committed_membership_config
    );
  }

  Ok(())
}

/// cluster_committed 指标测试
#[compio::test]
async fn test_cluster_committed_metric() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      enable_elect: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config.clone());

  let mut log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;
  log_index += router.client_request_many(0, "foo", 10).await?;

  for id in [0, 1, 2] {
    router
      .wait(&id, timeout())
      .applied_index(Some(log_index), "applied log index")
      .await?;

    let node = router.get_raft_handle(&id)?;
    let metrics = node.metrics().borrow_watched().clone();
    let data_metrics = node.data_metrics().borrow_watched().clone();

    assert_eq!(
      metrics.cluster_committed.as_ref().map(|x| x.index()),
      Some(log_index),
    );
    assert_eq!(metrics.cluster_committed, metrics.local_committed);

    assert_eq!(data_metrics.local_committed, metrics.local_committed);
    assert_eq!(data_metrics.cluster_committed, metrics.cluster_committed);
  }

  Ok(())
}

/// wait() 超时条件测试
#[compio::test]
async fn test_metrics_wait_timeout() -> Result<()> {
  use std::time::Duration;

  use zenoh_raft::metrics::WaitError;

  let config = Arc::new(
    Config {
      enable_tick: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config.clone());

  let log_index = router.new_cluster(btreeset! {0}, btreeset! {}).await?;
  let never_written = log_index + 1;
  let rst = router
    .wait(&0, Some(Duration::from_millis(500)))
    .applied_index(Some(never_written), "timeout waiting for log")
    .await;

  assert!(matches!(rst, Err(WaitError::Timeout(..))));

  Ok(())
}

/// on_cluster_leader_change API 测试
#[compio::test]
async fn test_on_cluster_leader_change_api() -> Result<()> {
  use std::{sync::Mutex, time::Duration};

  use zenoh_raft::{testing::memstore::TypeConfig, type_config::TypeConfigExt, vote::RaftLeaderId};

  let config = Arc::new(
    Config {
      enable_elect: false,
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config.clone());

  let _log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  let n1 = router.get_raft_handle(&1)?;
  type LeaderChangeEvent = (Option<(u64, u64, bool)>, (u64, u64, bool));
  let changes: Arc<Mutex<Vec<LeaderChangeEvent>>> = Arc::new(Mutex::new(Vec::new()));
  let changes_clone = changes.clone();

  let mut handle = n1.on_cluster_leader_change(move |old, new| {
    let old_val =
      old.map(|(leader_id, committed)| (leader_id.term(), *leader_id.node_id(), committed));
    let new_val = (new.0.term(), *new.0.node_id(), new.1);
    changes_clone.lock().unwrap().push((old_val, new_val));
    async {}
  });

  TypeConfig::sleep(Duration::from_millis(100)).await;

  {
    let got = changes.lock().unwrap().clone();
    let want = vec![(None, (1, 0, true))];
    assert_eq!(got, want);
  }

  router.remove_node(0);
  TypeConfig::sleep(Duration::from_millis(700)).await;

  let n2 = router.get_raft_handle(&2)?;
  n2.trigger().elect(false).await?;

  n2.wait(Some(Duration::from_millis(2000)))
    .state(ServerState::Leader, "wait for node 2 to become leader")
    .await?;

  n1.wait(Some(Duration::from_millis(2000)))
    .current_leader(2, "wait for node 1 to see node 2 as leader")
    .await?;

  TypeConfig::sleep(Duration::from_millis(100)).await;

  {
    let got = changes.lock().unwrap().clone();
    let want = vec![(None, (1, 0, true)), (Some((1, 0, true)), (2, 2, false))];
    assert_eq!(got, want);
  }

  handle.close().await;

  Ok(())
}

/// on_leader_change API 测试
#[compio::test]
async fn test_on_leader_change_api() -> Result<()> {
  use std::{sync::Mutex, time::Duration};

  use zenoh_raft::{testing::memstore::TypeConfig, type_config::TypeConfigExt, vote::RaftLeaderId};

  let config = Arc::new(
    Config {
      enable_elect: false,
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config.clone());

  let _log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  let n0 = router.get_raft_handle(&0)?;

  let started: Arc<Mutex<Vec<(u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));
  let stopped: Arc<Mutex<Vec<(u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));

  let started_clone = started.clone();
  let stopped_clone = stopped.clone();

  let mut handle = n0.on_leader_change(
    move |leader_id| {
      started_clone
        .lock()
        .unwrap()
        .push((leader_id.term(), *leader_id.node_id()));
      async {}
    },
    move |old_leader_id| {
      stopped_clone
        .lock()
        .unwrap()
        .push((old_leader_id.term(), *old_leader_id.node_id()));
      async {}
    },
  );

  TypeConfig::sleep(Duration::from_millis(100)).await;

  {
    let got = started.lock().unwrap().clone();
    assert_eq!(got, vec![(1, 0)]);

    let got = stopped.lock().unwrap().clone();
    assert_eq!(got, vec![]);
  }

  TypeConfig::sleep(Duration::from_millis(700)).await;

  let n2 = router.get_raft_handle(&2)?;
  n2.trigger().elect(false).await?;

  n2.wait(Some(Duration::from_millis(2000)))
    .state(ServerState::Leader, "wait for node 2 to become leader")
    .await?;

  n0.wait(Some(Duration::from_millis(2000)))
    .current_leader(2, "wait for node 0 to see node 2 as leader")
    .await?;

  TypeConfig::sleep(Duration::from_millis(100)).await;

  {
    let got = started.lock().unwrap().clone();
    assert_eq!(got, vec![(1, 0)]);

    let got = stopped.lock().unwrap().clone();
    assert_eq!(got, vec![(1, 0)]);
  }

  handle.close().await;

  Ok(())
}

/// 确保 on_leader_change 的异步 Future 被正确 await
#[compio::test]
async fn test_on_leader_change_future_is_awaited() -> Result<()> {
  use std::{
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
  };

  use zenoh_raft::{testing::memstore::TypeConfig, type_config::TypeConfigExt};

  let config = Arc::new(
    Config {
      enable_elect: false,
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config.clone());

  let _log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;
  let n0 = router.get_raft_handle(&0)?;

  let start_counter = Arc::new(AtomicU32::new(0));
  let stop_counter = Arc::new(AtomicU32::new(0));

  let start_counter_clone = start_counter.clone();
  let stop_counter_clone = stop_counter.clone();

  let mut handle = n0.on_leader_change(
    move |_leader_id| {
      let counter = start_counter_clone.clone();
      async move {
        TypeConfig::yield_now().await;
        counter.fetch_add(1, Ordering::SeqCst);
      }
    },
    move |_old_leader_id| {
      let counter = stop_counter_clone.clone();
      async move {
        TypeConfig::yield_now().await;
        counter.fetch_add(1, Ordering::SeqCst);
      }
    },
  );

  TypeConfig::sleep(Duration::from_millis(100)).await;

  assert_eq!(start_counter.load(Ordering::SeqCst), 1);
  assert_eq!(stop_counter.load(Ordering::SeqCst), 0);

  TypeConfig::sleep(Duration::from_millis(700)).await;

  let n2 = router.get_raft_handle(&2)?;
  n2.trigger().elect(false).await?;

  n2.wait(Some(Duration::from_millis(2000)))
    .state(ServerState::Leader, "wait for node 2 to become leader")
    .await?;

  n0.wait(Some(Duration::from_millis(2000)))
    .current_leader(2, "wait for node 0 to see node 2 as leader")
    .await?;

  TypeConfig::sleep(Duration::from_millis(100)).await;

  assert_eq!(start_counter.load(Ordering::SeqCst), 1);
  assert_eq!(stop_counter.load(Ordering::SeqCst), 1);

  handle.close().await;

  Ok(())
}

/// 确保 on_cluster_leader_change 的异步 Future 被正确 await
#[compio::test]
async fn test_on_cluster_leader_change_future_is_awaited() -> Result<()> {
  use std::{
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
  };

  use zenoh_raft::{testing::memstore::TypeConfig, type_config::TypeConfigExt};

  let config = Arc::new(
    Config {
      enable_elect: false,
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config.clone());

  let _log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;
  let n1 = router.get_raft_handle(&1)?;

  let callback_counter = Arc::new(AtomicU32::new(0));
  let callback_counter_clone = callback_counter.clone();

  let mut handle = n1.on_cluster_leader_change(move |_old, _new| {
    let counter = callback_counter_clone.clone();
    async move {
      TypeConfig::yield_now().await;
      counter.fetch_add(1, Ordering::SeqCst);
    }
  });

  TypeConfig::sleep(Duration::from_millis(100)).await;

  assert_eq!(callback_counter.load(Ordering::SeqCst), 1);

  TypeConfig::sleep(Duration::from_millis(700)).await;

  let n2 = router.get_raft_handle(&2)?;
  n2.trigger().elect(false).await?;

  n2.wait(Some(Duration::from_millis(2000)))
    .state(ServerState::Leader, "wait for node 2 to become leader")
    .await?;

  n1.wait(Some(Duration::from_millis(2000)))
    .current_leader(2, "wait for node 1 to see node 2 as leader")
    .await?;

  TypeConfig::sleep(Duration::from_millis(100)).await;

  assert_eq!(callback_counter.load(Ordering::SeqCst), 2);

  handle.close().await;

  Ok(())
}

/// 应用进度监听 API (watch_apply_progress: get & wait_until_ge) 测试
#[compio::test]
async fn test_apply_progress_api() -> Result<()> {
  use zenoh_raft::{testing::memstore::TypeConfig, type_config::TypeConfigExt};

  let config = Arc::new(
    Config {
      enable_tick: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router.new_cluster(btreeset! {0}, btreeset! {}).await?;

  let n0 = router.get_raft_handle(&0)?;
  let progress = n0.watch_apply_progress();

  let got = progress.get();
  let want = Some(log_id(1, 0, log_index));
  assert_eq!(got, want);

  let target_index = log_index + 5;
  let target = Some(log_id(1, 0, target_index));

  let n0_clone = router.get_raft_handle(&0)?;
  let handle = TypeConfig::spawn(async move {
    let mut progress = n0_clone.watch_apply_progress();
    progress.wait_until_ge(&target).await
  });

  log_index += router.client_request_many(0, "foo", 5).await?;

  let got_wait = handle.await??;
  let got_get = progress.get();

  let want = Some(log_id(1, 0, log_index));
  assert_eq!(got_wait, want);
  assert_eq!(got_get, want);

  Ok(())
}

/// 提交进度监听 API (watch_commit_progress: get & wait_until_ge) 测试
#[compio::test]
async fn test_commit_progress_api() -> Result<()> {
  use zenoh_raft::{testing::memstore::TypeConfig, type_config::TypeConfigExt};

  let config = Arc::new(
    Config {
      enable_tick: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router.new_cluster(btreeset! {0}, btreeset! {}).await?;

  let n0 = router.get_raft_handle(&0)?;
  let progress = n0.watch_commit_progress();

  let got = progress.get();
  let want = Some(log_id(1, 0, log_index));
  assert_eq!(got, want);

  let target_index = log_index + 5;
  let target = Some(log_id(1, 0, target_index));

  let n0_clone = router.get_raft_handle(&0)?;
  let handle = TypeConfig::spawn(async move {
    let mut progress = n0_clone.watch_commit_progress();
    progress.wait_until_ge(&target).await
  });

  log_index += router.client_request_many(0, "foo", 5).await?;

  let got_wait = handle.await??;
  let got_get = progress.get();

  let want = Some(log_id(1, 0, log_index));
  assert_eq!(got_wait, want);
  assert_eq!(got_get, want);

  Ok(())
}

/// 日志进度监听 API (watch_log_progress: get & wait_until_ge) 测试
#[compio::test]
async fn test_log_progress_api() -> Result<()> {
  use zenoh_raft::{FlushPoint, Vote, testing::memstore::TypeConfig, type_config::TypeConfigExt};

  let config = Arc::new(
    Config {
      enable_tick: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router.new_cluster(btreeset! {0}, btreeset! {}).await?;

  let n0 = router.get_raft_handle(&0)?;
  let log_progress = n0.watch_log_progress();

  let got = log_progress.get();
  let want = Some(FlushPoint::new(
    Vote::new_committed(1, 0),
    Some(log_id(1, 0, log_index)),
  ));
  assert_eq!(got, want);

  let target_index = log_index + 5;
  let target = Some(FlushPoint::new(
    Vote::new_committed(1, 0),
    Some(log_id(1, 0, target_index)),
  ));

  let n0_clone = router.get_raft_handle(&0)?;
  let handle = TypeConfig::spawn(async move {
    let mut progress = n0_clone.watch_log_progress();
    progress.wait_until_ge(&target).await
  });

  log_index += router.client_request_many(0, "foo", 5).await?;

  let got_wait = handle.await??;
  let got_get = log_progress.get();

  let want = Some(FlushPoint::new(
    Vote::new_committed(1, 0),
    Some(log_id(1, 0, log_index)),
  ));
  assert_eq!(got_wait, want);
  assert_eq!(got_get, want);

  Ok(())
}

/// 伴随 Leader 切换的日志进度监听测试
#[compio::test]
async fn test_log_progress_with_leader_change() -> Result<()> {
  use std::time::Duration;

  use zenoh_raft::{FlushPoint, Vote, testing::memstore::TypeConfig, type_config::TypeConfigExt};

  let config = Arc::new(
    Config {
      enable_tick: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  let n1 = router.get_raft_handle(&1)?;
  let log_progress = n1.watch_log_progress();

  let got = log_progress.get();
  let want = Some(FlushPoint::new(
    Vote::new_committed(1, 0),
    Some(log_id(1, 0, log_index)),
  ));
  assert_eq!(got, want);

  let target_index = log_index + 4;
  let target = Some(FlushPoint::new(
    Vote::new_committed(2, 1),
    Some(log_id(2, 1, target_index)),
  ));

  let n0 = router.get_raft_handle(&0)?;
  n0.shutdown().await?;

  TypeConfig::sleep(Duration::from_millis(500)).await;

  let n1_clone = router.get_raft_handle(&1)?;
  let handle = TypeConfig::spawn(async move {
    let mut progress = n1_clone.watch_log_progress();
    progress.wait_until_ge(&target).await
  });

  router.remove_node(0);

  let n1 = router.get_raft_handle(&1)?;
  n1.trigger().elect(false).await?;

  router
    .wait(&1, Some(Duration::from_millis(2000)))
    .leader_with_quorum_acked(None, "wait for node 1 leader")
    .await?;
  log_index += 1;
  log_index += router.client_request_many(1, "foo", 3).await?;

  let got_wait = handle.await??;
  let got_get = log_progress.get();

  let want = Some(FlushPoint::new(
    Vote::new_committed(2, 1),
    Some(log_id(2, 1, log_index)),
  ));
  assert_eq!(got_wait, want);
  assert_eq!(got_get, want);

  Ok(())
}

/// 投票进度监听 API (watch_vote_progress: get & wait_until_ge) 测试
#[compio::test]
async fn test_vote_progress_api() -> Result<()> {
  use std::time::Duration;

  use zenoh_raft::{Vote, testing::memstore::TypeConfig, type_config::TypeConfigExt};

  let config = Arc::new(
    Config {
      enable_tick: false,
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config.clone());
  let _log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  let n1 = router.get_raft_handle(&1)?;
  let vote_progress = n1.watch_vote_progress();

  let got = vote_progress.get();
  let want = Some(Vote::new_committed(1, 0));
  assert_eq!(got, want);

  let n0 = router.get_raft_handle(&0)?;
  n0.shutdown().await?;

  TypeConfig::sleep(Duration::from_millis(500)).await;

  let n1 = router.get_raft_handle(&1)?;
  let handle = TypeConfig::spawn(async move {
    let mut progress = n1.watch_vote_progress();
    let target = Some(Vote::new(2, 1));
    progress.wait_until_ge(&target).await
  });

  let n1 = router.get_raft_handle(&1)?;
  n1.trigger().elect(false).await?;

  let got_wait = handle.await??;
  let got_get = vote_progress.get();

  let want = Some(Vote::new(2, 1));
  assert_eq!(got_wait, want);
  assert!(got_get == want || got_get == Some(Vote::new_committed(2, 1)));

  Ok(())
}

/// 快照进度监听 API (watch_snapshot_progress: get & wait_until_ge) 测试
#[compio::test]
async fn test_snapshot_progress_api() -> Result<()> {
  use zenoh_raft::{testing::memstore::TypeConfig, type_config::TypeConfigExt};

  let config = Arc::new(
    Config {
      enable_tick: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router.new_cluster(btreeset! {0}, btreeset! {}).await?;

  let n0 = router.get_raft_handle(&0)?;
  let progress = n0.watch_snapshot_progress();

  let got = progress.get();
  assert_eq!(got, None);

  log_index += router.client_request_many(0, "foo", 5).await?;

  let target = Some(log_id(1, 0, log_index));

  let n0_clone = router.get_raft_handle(&0)?;
  let handle = TypeConfig::spawn(async move {
    let mut progress = n0_clone.watch_snapshot_progress();
    progress.wait_until_ge(&target).await
  });

  n0.trigger().snapshot().await?;

  let got_wait = handle.await??;
  let got_get = progress.get();

  let want = Some(log_id(1, 0, log_index));
  assert_eq!(got_wait, want);
  assert_eq!(got_get, want);

  Ok(())
}

use std::{
  sync::atomic::{AtomicU64, Ordering},
  time::Duration,
};
use zenoh_raft::{
  SnapshotPolicy,
  metrics::MetricsRecorder,
  storage::RaftLogStorage,
  testing::memstore::TypeConfig,
};

#[derive(Debug, Default)]
struct TestRecorder {
  pub apply_batch_total: AtomicU64,
  pub append_batch_total: AtomicU64,
  pub write_batch_total: AtomicU64,

  pub current_term: AtomicU64,
  pub last_log_index: AtomicU64,
  pub committed_index: AtomicU64,
  pub applied_index: AtomicU64,
  pub snapshot_index: AtomicU64,
  pub purged_index: AtomicU64,
  pub server_state: AtomicU64,

  pub vote_count: AtomicU64,
  pub heartbeat_count: AtomicU64,
  pub append_count: AtomicU64,
}

impl MetricsRecorder for TestRecorder {
  fn record_apply_batch(&self, n: u64) {
    self.apply_batch_total.fetch_add(n, Ordering::Relaxed);
  }

  fn record_append_batch(&self, n: u64) {
    self.append_batch_total.fetch_add(n, Ordering::Relaxed);
  }

  fn record_write_batch(&self, n: u64) {
    self.write_batch_total.fetch_add(n, Ordering::Relaxed);
  }

  fn set_current_term(&self, v: u64) {
    self.current_term.store(v, Ordering::Relaxed);
  }

  fn set_last_log_index(&self, v: u64) {
    self.last_log_index.store(v, Ordering::Relaxed);
  }

  fn set_committed_index(&self, v: u64) {
    self.committed_index.store(v, Ordering::Relaxed);
  }

  fn set_applied_index(&self, v: u64) {
    self.applied_index.store(v, Ordering::Relaxed);
  }

  fn set_snapshot_index(&self, v: u64) {
    self.snapshot_index.store(v, Ordering::Relaxed);
  }

  fn set_purged_index(&self, v: u64) {
    self.purged_index.store(v, Ordering::Relaxed);
  }

  fn set_server_state(&self, v: u8) {
    self.server_state.store(v as u64, Ordering::Relaxed);
  }

  fn increment_vote(&self) {
    self.vote_count.fetch_add(1, Ordering::Relaxed);
  }

  fn increment_heartbeat(&self) {
    self.heartbeat_count.fetch_add(1, Ordering::Relaxed);
  }

  fn increment_append(&self) {
    self.append_count.fetch_add(1, Ordering::Relaxed);
  }
}

/// 测试 Leader 上的 MetricsRecorder 收集各项指标
#[compio::test]
async fn test_metrics_recorder_all_fields() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      enable_elect: false,
      snapshot_policy: SnapshotPolicy::LogsSinceLast(5),
      max_in_snapshot_log_to_keep: 0,
      purge_batch_size: 1,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config);
  let mut log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  let recorder = Arc::new(TestRecorder::default());
  let leader_id = router.leader().expect("leader found");
  let leader = router.get_raft_handle(&leader_id)?;
  leader.set_metrics_recorder(Some(recorder.clone())).await?;

  log_index += router.client_request_many(leader_id, "test", 10).await?;

  for node_id in [0, 1, 2] {
    router
      .wait(&node_id, timeout())
      .applied_index(Some(log_index), "applied")
      .await?;
  }

  leader.trigger().heartbeat().await?;
  leader.trigger().snapshot().await?;
  router
    .wait(&leader_id, timeout())
    .snapshot(log_id(1, leader_id, log_index), "snapshot")
    .await?;

  leader.trigger().purge_log(log_index).await?;
  router
    .wait(&leader_id, timeout())
    .purged(Some(log_id(1, leader_id, log_index)), "purged")
    .await?;

  TypeConfig::sleep(Duration::from_millis(100)).await;

  assert!(recorder.write_batch_total.load(Ordering::Relaxed) > 0);
  assert!(recorder.append_batch_total.load(Ordering::Relaxed) > 0);
  assert!(recorder.apply_batch_total.load(Ordering::Relaxed) > 0);

  assert!(recorder.current_term.load(Ordering::Relaxed) >= 1);
  assert!(recorder.last_log_index.load(Ordering::Relaxed) > 0);
  assert!(recorder.committed_index.load(Ordering::Relaxed) > 0);
  assert!(recorder.applied_index.load(Ordering::Relaxed) > 0);
  assert!(recorder.snapshot_index.load(Ordering::Relaxed) > 0);
  assert!(recorder.purged_index.load(Ordering::Relaxed) > 0);
  assert_eq!(recorder.server_state.load(Ordering::Relaxed), 3);
  assert!(recorder.heartbeat_count.load(Ordering::Relaxed) > 0);

  Ok(())
}

/// 测试 Follower 上的 MetricsRecorder 收集接收 RPC 指标
#[compio::test]
async fn test_metrics_recorder_on_follower() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      enable_elect: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config);
  let mut log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  let recorder = Arc::new(TestRecorder::default());
  let follower = router.get_raft_handle(&1)?;
  follower.set_metrics_recorder(Some(recorder.clone())).await?;

  let leader_id = router.leader().expect("leader found");
  log_index += router.client_request_many(leader_id, "test", 5).await?;

  router
    .wait(&1, timeout())
    .applied_index(Some(log_index), "follower applied")
    .await?;

  let node2 = router.get_raft_handle(&2)?;
  node2.trigger().elect(false).await?;

  TypeConfig::sleep(Duration::from_millis(200)).await;

  assert!(recorder.append_count.load(Ordering::Relaxed) > 0);
  assert!(recorder.append_batch_total.load(Ordering::Relaxed) > 0);
  assert!(recorder.apply_batch_total.load(Ordering::Relaxed) > 0);
  assert!(recorder.vote_count.load(Ordering::Relaxed) > 0);
  assert!(recorder.current_term.load(Ordering::Relaxed) >= 1);
  assert!(recorder.last_log_index.load(Ordering::Relaxed) > 0);
  assert!(recorder.applied_index.load(Ordering::Relaxed) > 0);

  let state = recorder.server_state.load(Ordering::Relaxed);
  assert!(state == 1 || state == 2);

  Ok(())
}

/// 测试 metrics 报告已清除的 purged 日志 ID
#[compio::test]
async fn test_purged_metrics() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      max_in_snapshot_log_to_keep: 0,
      purge_batch_size: 1,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config);
  let mut log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  log_index += router.client_request_many(0, "foo", 10).await?;

  let n0 = router.get_raft_handle(&0)?;
  n0.trigger().snapshot().await?;
  n0.wait(timeout())
    .snapshot(log_id(1, 0, log_index), "build snapshot")
    .await?;

  n0.wait(timeout())
    .metrics(
      |m| m.purged == Some(log_id(1, 0, log_index)),
      "purged is reported to metrics",
    )
    .await?;

  let (mut sto0, _sm0) = router.get_storage_handle(&0)?;
  let state = sto0.get_log_state().await?;
  assert_eq!(state.last_purged_log_id, Some(log_id(1, 0, log_index)));

  Ok(())
}

/// 测试从 metrics 获取 leader_last_ack / last_quorum_acked
#[compio::test]
async fn test_leader_last_ack() -> Result<()> {
  let heartbeat_interval = 50;
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      heartbeat_interval,
      enable_elect: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config);
  let _ = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  let n0 = router.get_raft_handle(&0)?;
  let acked_before = n0.metrics().borrow_watched().last_quorum_acked;
  assert!(acked_before.is_some());

  TypeConfig::sleep(Duration::from_millis(50)).await;
  let started = TypeConfig::now();
  n0.trigger().heartbeat().await?;
  n0.wait(timeout())
    .leader_with_quorum_acked(Some(started), "last_quorum_acked refreshed")
    .await?;

  let acked_after = n0.metrics().borrow_watched().last_quorum_acked;
  assert!(acked_after.unwrap().into_inner() >= started);

  Ok(())
}


