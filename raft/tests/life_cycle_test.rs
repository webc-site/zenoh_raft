//! 节点生命周期（初始化、重启、恢复、停机）测试套件

mod fixtures;

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use fixtures::{MemLogStore, MemRaft, MemStateMachine, RaftRouter, log_id, timeout};
use maplit::{btreemap, btreeset};
use zenoh_raft::{
  Config, RaftLogReader, ServerState, Vote,
  metrics::WaitError,
  storage::{RaftLogStorage, RaftStateMachine},
  testing::memstore::{ClientRequest, IntoMemClientRequest},
};

/// 节点初始化测试
#[compio::test]
async fn test_initialization() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  router.new_raft_node(0).await;

  router
    .wait(&0, timeout())
    .state(ServerState::Learner, "starts as learner")
    .await?;

  router.initialize(0).await?;

  router
    .wait(&0, timeout())
    .state(ServerState::Leader, "becomes leader after init")
    .await?;
  router
    .wait(&0, timeout())
    .applied_index(Some(1), "init log applied")
    .await?;

  Ok(())
}

/// 节点安全停机测试
#[compio::test]
async fn test_shutdown() -> Result<()> {
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

  let n0 = router.get_raft_handle(&0)?;
  n0.shutdown().await?;

  let res = n0.client_write(ClientRequest::make_request("foo", 1)).await;
  assert!(res.is_err(), "writes to shutdown node must fail");

  Ok(())
}

/// 单节点重启与状态恢复测试
#[compio::test]
async fn test_single_node_restart() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      enable_elect: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router.new_cluster(btreeset! {0}, btreeset! {}).await?;

  router.client_request(0, "client", 1).await?;
  log_index += 1;

  let (_r0, sto0, sm0) = router.remove_node(0).unwrap();

  router
    .new_raft_node_with_sto(0, sto0.clone(), sm0.clone())
    .await;

  let (_sto, mut sm) = (sto0, sm0);
  let (last_applied, _) = sm.applied_state().await?;
  assert_eq!(Some(log_id(1, 0, log_index)), last_applied);

  router
    .wait(&0, timeout())
    .applied_index(Some(log_index), "state restored")
    .await?;

  Ok(())
}

/// Follower 重启后不会因过快选举而打断稳定的集群
#[compio::test]
async fn test_follower_restart_does_not_interrupt() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      election_timeout_min: 3_000,
      election_timeout_max: 4_000,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let _log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

  let m = router.get_metrics(&0)?;
  let term = m.current_term;

  let (n2, sto2, sm2) = router.remove_node(2).unwrap();
  n2.shutdown().await?;

  let (n1, sto1, sm1) = router.remove_node(1).unwrap();
  n1.shutdown().await?;

  let (n0, _sto0, _sm0) = router.remove_node(0).unwrap();
  n0.shutdown().await?;

  router.new_raft_node_with_sto(1, sto1, sm1).await;
  router.new_raft_node_with_sto(2, sto2, sm2).await;
  let res = router
    .wait(&1, Some(Duration::from_millis(1_000)))
    .metrics(|x| x.current_term > term, "node increase term")
    .await;

  assert!(res.is_err(), "term should not increase immediately");

  router
    .wait(&1, Some(Duration::from_millis(9_000)))
    .metrics(
      |x| x.current_term > term,
      "node increases term after full election timeout",
    )
    .await?;

  Ok(())
}

/// 丢失全部状态的 Leader 重启后不会重新当选 Leader
#[compio::test]
async fn test_leader_restart_clears_state() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      enable_elect: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  router.enable_saving_committed = false;

  router.new_raft_node(0).await;
  router.new_raft_node(1).await;
  router.new_raft_node(2).await;

  let n0 = router.get_raft_handle(&0)?;
  n0.initialize(btreemap! {0 => (), 1=>(), 2=>()}).await?;
  let mut log_index = 1;

  n0.wait(timeout())
    .state(ServerState::Leader, "node-0 should become leader")
    .await?;
  n0.wait(timeout())
    .applied_index(Some(log_index), "node-0 applied log")
    .await?;

  log_index += router.client_request_many(0, "foo", 1).await?;
  router
    .wait(&1, timeout())
    .applied_index(Some(log_index), "node-1 applied log")
    .await?;
  router
    .wait(&2, timeout())
    .applied_index(Some(log_index), "node-2 applied log")
    .await?;

  let (node, _log, _sm): (MemRaft, MemLogStore, MemStateMachine) = router.remove_node(0).unwrap();
  node.shutdown().await?;

  router.new_raft_node(0).await;
  let n0 = router.get_raft_handle(&0)?;

  n0.initialize(btreemap! {0 => (), 1=>(), 2=>()}).await?;
  n0.trigger().elect(false).await?;

  let res = n0
    .wait(timeout())
    .state(ServerState::Leader, "should not become leader upon restart")
    .await;
  assert!(res.is_err());

  Ok(())
}

/// 单 Follower 重启后由于自身即为 Quorum 能迅速恢复为 Leader
#[compio::test]
async fn test_single_follower_restart() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      election_timeout_min: 3_000,
      election_timeout_max: 4_000,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router.new_cluster(btreeset! {0}, btreeset! {}).await?;

  router.client_request_many(0, "foo", 1).await?;
  log_index += 1;

  let (node, mut sto, sm) = router.remove_node(0).unwrap();
  node.shutdown().await?;
  let v = sto.read_vote().await?;

  if let Some(v) = v {
    sto
      .save_vote(&Vote::new(v.leader_id().term + 1, v.leader_id().node_id))
      .await?;
  }

  router.new_raft_node_with_sto(0, sto, sm).await;
  router
    .wait(&0, Some(Duration::from_millis(1_000)))
    .state(
      ServerState::Leader,
      "single node restarted and became leader quickly",
    )
    .await?;

  log_index += 1;

  router.client_request_many(0, "foo", 1).await?;
  log_index += 1;

  router
    .wait(&0, timeout())
    .applied_index(Some(log_index), "node-0 works")
    .await?;

  Ok(())
}

/// 单 Leader 重启后会重新应用全部日志（自身构成 Quorum）
#[compio::test]
async fn test_single_leader_restart_re_apply_logs() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  router.enable_saving_committed = false;

  let mut log_index = router.new_cluster(btreeset! {0}, btreeset! {}).await?;

  log_index += router.client_request_many(0, "foo", 1).await?;

  let (node, ls, sm): (MemRaft, MemLogStore, MemStateMachine) = router.remove_node(0).unwrap();
  node.shutdown().await?;

  sm.clear_state_machine().await;

  router.new_raft_node_with_sto(0, ls, sm).await;
  router
    .wait(&0, timeout())
    .state(ServerState::Leader, "become leader upon restart")
    .await?;

  router
    .wait(&0, timeout())
    .applied_index(Some(log_index), "node-0 works")
    .await?;

  Ok(())
}

/// `wait_for_recovery` 在集群 Commit 重新确立且状态机追赶后返回
#[compio::test]
async fn test_wait_for_recovery_recovers_state_machine() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: true,
      enable_elect: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  router.enable_saving_committed = false;

  let log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

  let (node, ls, sm): (MemRaft, MemLogStore, MemStateMachine) = router.remove_node(1).unwrap();
  node.shutdown().await?;

  sm.clear_state_machine().await;

  router.new_raft_node_with_sto(1, ls, sm).await;

  let m = router
    .get_raft_handle(&1)?
    .wait_for_recovery(timeout())
    .await?;

  assert!(m.cluster_committed.is_some(), "cluster commit is perceived");
  assert!(
    m.last_applied.as_ref().map(|x| x.index()) >= Some(log_index),
    "state machine recovered: last_applied {:?} >= {}",
    m.last_applied,
    log_index
  );

  Ok(())
}

/// 当无法重新确立集群 Commit 时 `wait_for_recovery` 超时
#[compio::test]
async fn test_wait_for_recovery_times_out_without_quorum() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      enable_elect: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());

  let _log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

  for id in [1, 2] {
    let (node, _ls, _sm): (MemRaft, MemLogStore, MemStateMachine) = router.remove_node(id).unwrap();
    node.shutdown().await?;
  }

  let (node, ls, sm): (MemRaft, MemLogStore, MemStateMachine) = router.remove_node(0).unwrap();
  node.shutdown().await?;

  router.new_raft_node_with_sto(0, ls, sm).await;
  router
    .wait(&0, timeout())
    .state(ServerState::Leader, "restore leadership without election")
    .await?;

  let res = router
    .get_raft_handle(&0)?
    .wait_for_recovery(Some(Duration::from_millis(500)))
    .await;
  assert!(
    matches!(res, Err(WaitError::Timeout(..))),
    "expected timeout without quorum, got {:?}",
    res
  );

  Ok(())
}

/// 重启且未重新选举的 Leader 不恢复 cluster_committed
#[compio::test]
async fn test_leader_restart_cluster_committed_not_restored() -> Result<()> {
  use std::sync::atomic::{AtomicU64, Ordering};

  use fixtures::rpc_request::RpcRequest;
  use zenoh_raft::{RPCTypes, testing::memstore::TypeConfig, type_config::TypeConfigExt};

  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      enable_elect: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

  {
    let m = router.get_metrics(&0)?;
    assert_eq!(
      Some(log_index),
      m.cluster_committed.as_ref().map(|x| x.index()),
      "cluster_committed established by quorum replication"
    );
    assert_eq!(
      Some(log_index),
      m.local_committed.as_ref().map(|x| x.index())
    );
  }

  for id in [1, 2] {
    let (node, _ls, _sm): (MemRaft, MemLogStore, MemStateMachine) = router.remove_node(id).unwrap();
    node.shutdown().await?;
  }

  {
    let (node, ls, sm): (MemRaft, MemLogStore, MemStateMachine) = router.remove_node(0).unwrap();
    node.shutdown().await?;

    router.new_raft_node_with_sto(0, ls, sm).await;
    router
      .wait(&0, timeout())
      .state(ServerState::Leader, "restore leadership without election")
      .await?;
    router
      .wait(&0, timeout())
      .applied_index(Some(log_index), "applied restored from storage")
      .await?;
  }

  {
    let m = router.get_metrics(&0)?;
    assert_eq!(
      Some(log_index),
      m.local_committed.as_ref().map(|x| x.index()),
      "local_committed restored from storage"
    );
    assert_eq!(
      None, m.cluster_committed,
      "cluster_committed must not be restored from storage"
    );
  }

  {
    let total = Arc::new(AtomicU64::new(0));
    let non_null = Arc::new(AtomicU64::new(0));
    {
      let total = total.clone();
      let non_null = non_null.clone();
      use futures_util::future::ready;
      router
        .set_rpc_pre_hook(
          RPCTypes::AppendEntries,
          move |_router, req, from_id, _target| {
            if from_id == 0
              && let RpcRequest::AppendEntries(ae) = &req
            {
              total.fetch_add(1, Ordering::Relaxed);
              if ae.leader_commit.is_some() {
                non_null.fetch_add(1, Ordering::Relaxed);
              }
            }
            Box::pin(ready(Ok(())))
          },
        )
        .await;
    }

    router.get_raft_handle(&0)?.trigger().heartbeat().await?;

    let mut sent = 0;
    for _ in 0..100 {
      sent = total.load(Ordering::Relaxed);
      if sent > 0 {
        break;
      }
      TypeConfig::sleep(Duration::from_millis(10)).await;
    }
    assert!(
      sent > 0,
      "the restored leader must broadcast at least one heartbeat"
    );
    assert_eq!(
      0,
      non_null.load(Ordering::Relaxed),
      "a restored leader's heartbeat must broadcast a null leader_commit"
    );
  }

  Ok(())
}

/// 当 enable_leader_restore=false 时重启 Leader 重新进行选举
#[compio::test]
async fn test_leader_restart_leader_restore_disabled() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      enable_leader_restore: Some(false),
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router.new_cluster(btreeset! {0}, btreeset! {}).await?;

  log_index += router.client_request_many(0, "foo", 1).await?;

  {
    let (node, ls, sm): (MemRaft, MemLogStore, MemStateMachine) = router.remove_node(0).unwrap();
    node.shutdown().await?;

    router.new_raft_node_with_sto(0, ls, sm).await;
  }

  {
    router
      .wait(&0, timeout())
      .state(ServerState::Leader, "leader again via election")
      .await?;
    router
      .wait(&0, timeout())
      .vote(Vote::new_committed(2, 0), "vote moved to term 2")
      .await?;

    log_index += 1;
    router
      .wait(&0, timeout())
      .applied_index(Some(log_index), "logs re-applied, plus the noop")
      .await?;
  }

  Ok(())
}

/// 临时（内存）状态机重启结合持久快照恢复测试 (Issue 881)
#[compio::test]
async fn test_issue_881_transient_state_machine() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  router.enable_saving_committed = true;

  let mut log_index = router.new_cluster(btreeset! {0}, btreeset! {}).await?;

  log_index += router.client_request_many(0, "foo", 10).await?;
  router
    .wait(&0, timeout())
    .applied_index(Some(log_index), "node-0 applied")
    .await?;

  let snapshot_log_index = log_index;

  {
    let n = router.get_raft_handle(&0)?;
    n.trigger().snapshot().await?;
    router
      .wait(&0, timeout())
      .snapshot(log_id(1, 0, snapshot_log_index), "snapshot created")
      .await?;
  }

  log_index += router.client_request_many(0, "bar", 5).await?;
  router
    .wait(&0, timeout())
    .applied_index(Some(log_index), "node-0 applied new logs")
    .await?;

  {
    let (node, ls, sm): (MemRaft, MemLogStore, MemStateMachine) = router.remove_node(0).unwrap();
    node.shutdown().await?;

    sm.clear_state_machine().await;
    router.new_raft_node_with_sto(0, ls, sm).await;
  }

  router
    .wait(&0, timeout())
    .applied_index(Some(log_index), "node-0 recovered by snapshot + log replay")
    .await?;

  Ok(())
}

/// 非成员 Leader 重启时以 Learner 状态启动 (Issue 920)
#[compio::test]
async fn test_issue_920_non_member_leader_restart() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());

  let (mut log_store, sm) = router.new_store();
  log_store.save_vote(&Vote::new_committed(1, 0)).await?;
  router.new_raft_node_with_sto(0, log_store, sm).await;

  router
    .wait(&0, timeout())
    .state(ServerState::Learner, "node 0 becomes learner when startup")
    .await?;

  Ok(())
}

use zenoh_raft::{
  Membership,
  errors::{Fatal, InitializeError, NotAllowed, NotInMembers},
};

/// 尝试使用不包含目标节点的成员配置初始化应返回 NotInMembers 错误
#[compio::test]
async fn test_initialize_err_target_not_include_target() -> Result<()> {
  let config = Arc::new(Config::default().validate()?);
  let mut router = RaftRouter::new(config);
  router.new_raft_node(0).await;
  router.new_raft_node(1).await;

  for node_id in 0..2 {
    let n = router.get_raft_handle(&node_id)?;
    let res = n.initialize(btreeset! {9}).await;
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert_eq!(
      InitializeError::NotInMembers(NotInMembers {
        node_id,
        membership: Membership::new_with_defaults(vec![btreeset! {9}], [])
      }),
      err.into_api_error().unwrap()
    );
  }

  Ok(())
}

/// 重复初始化已初始化的节点应返回 NotAllowed 错误
#[compio::test]
async fn test_initialize_err_not_allowed() -> Result<()> {
  let config = Arc::new(Config::default().validate()?);
  let mut router = RaftRouter::new(config);
  router.new_raft_node(0).await;

  let n0 = router.get_raft_handle(&0)?;
  n0.initialize(btreeset! {0}).await?;
  n0.wait(timeout()).log_index(Some(1), "init").await?;

  let res = n0.initialize(btreeset! {0}).await;
  assert!(res.is_err());
  let err = res.unwrap_err();
  assert_eq!(
    InitializeError::NotAllowed(NotAllowed {
      last_log_id: Some(log_id(1, 0, 1)),
      vote: Vote::new_committed(1, 0)
    }),
    err.into_api_error().unwrap()
  );

  Ok(())
}

/// 当 RaftCore 发生 panic 时，后续请求应返回 Fatal::Panicked
#[compio::test]
async fn test_return_error_after_panic() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config);
  router.new_cluster(btreeset! {0}, btreeset! {}).await?;

  router
    .external_request(0, |_s| {
      panic!("foo");
    })
    .await?;

  let res = router.client_request(0, "foo", 2).await;
  let err = res.unwrap_err();
  assert_eq!(Fatal::Panicked, err.into_fatal().unwrap());

  Ok(())
}

/// 状态机 Worker panic 退出后 get_snapshot 应安全返回 Fatal::Stopped 而不挂起
#[compio::test]
async fn test_get_snapshot_returns_stopped_when_sm_worker_dies() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config);
  router.new_cluster(btreeset! {0}, btreeset! {}).await?;

  {
    let (_log_store, sm) = router.get_storage_handle(&0)?;
    sm.panic_on_apply(true);
  }

  let res = router.client_request(0, "c", 1).await;
  assert!(res.is_err());

  let raft = router.get_raft_handle(&0)?;
  let err = raft.get_snapshot().await.unwrap_err();
  assert_eq!(Fatal::Stopped, err.into_fatal().unwrap());

  Ok(())
}

/// with_state_machine 闭包发生 panic 时应返回 Fatal::Stopped
#[compio::test]
async fn test_with_state_machine_returns_stopped_when_func_panics() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config);
  router.new_cluster(btreeset! {0}, btreeset! {}).await?;

  let raft = router.get_raft_handle(&0)?;
  let res = raft
    .with_state_machine::<_, ()>(|_sm| Box::pin(async move { panic!("injected worker panic") }))
    .await;
  assert_eq!(Fatal::Stopped, res.unwrap_err());

  Ok(())
}

/// 节点停机后访问应返回 Fatal::Stopped
#[compio::test]
async fn test_return_error_after_shutdown() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config);
  router.new_cluster(btreeset! {0}, btreeset! {}).await?;

  let n = router.get_raft_handle(&0)?;
  n.shutdown().await?;

  let res = router.client_request(0, "foo", 2).await;
  let err = res.unwrap_err();
  assert_eq!(Fatal::Stopped, err.into_fatal().unwrap());

  Ok(())
}

