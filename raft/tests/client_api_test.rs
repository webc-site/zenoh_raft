//! 客户端 API 与一致性读测试套件

mod fixtures;

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use fixtures::{RaftRouter, log_id, timeout};
use futures_util::{StreamExt, TryStreamExt, stream::FuturesUnordered};
use maplit::btreeset;
use zenoh_raft::{
  Config, RaftLogReader, ReadPolicy, ServerState, SnapshotPolicy, Vote,
  errors::{ClientWriteError, Fatal, ForwardToLeader, RaftError},
  impls::ProgressResponder,
  raft::{AppendEntriesRequest, ClientWriteResponse, TransferLeaderRequest, WriteResponse},
  storage::{RaftLogStorage, RaftStateMachine},
  testing::{
    blank_ent,
    memstore::{ClientRequest, IntoMemClientRequest, MemStateMachine, TypeConfig},
  },
  type_config::TypeConfigExt,
};

/// 并发大量客户端写入，断言集群稳定且数据一致
#[compio::test]
async fn test_client_writes() -> Result<()> {
  let config = Arc::new(
    Config {
      snapshot_policy: SnapshotPolicy::LogsSinceLast(500),
      election_timeout_min: 500,
      election_timeout_max: 1000,
      enable_tick: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

  {
    let leader = router.leader().expect("leader not found");
    let mut clients = FuturesUnordered::new();
    clients.push(router.client_request_many(leader, "0", 20));
    clients.push(router.client_request_many(leader, "1", 20));
    clients.push(router.client_request_many(leader, "2", 20));
    clients.push(router.client_request_many(leader, "3", 20));
    clients.push(router.client_request_many(leader, "4", 20));
    clients.push(router.client_request_many(leader, "5", 20));
    while clients.next().await.is_some() {}

    log_index += 20 * 6;
    for id in [0, 1, 2] {
      router
        .wait(&id, None)
        .applied_index(Some(log_index), "sync logs")
        .await?;
    }
  }

  for id in [0, 1, 2] {
    let (mut sto, mut sm) = router.get_storage_handle(&id)?;
    let last_log_id = sto.get_log_state().await?.last_log_id;
    assert_eq!(Some(log_id(1, 0, log_index)), last_log_id);

    let vote = sto.read_vote().await?.unwrap();
    assert_eq!(Vote::new_committed(1, 0), vote);

    let (last_applied, _) = sm.applied_state().await?;
    assert_eq!(Some(log_id(1, 0, log_index)), last_applied);
  }

  Ok(())
}

/// 客户端批量写入 `client_write_many`
#[compio::test]
async fn test_client_write_many() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_tick: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

  let n0 = router.get_raft_handle(&0)?;

  let requests: Vec<ClientRequest> = (0..5)
    .map(|i| ClientRequest::make_request("batch", i))
    .collect();

  let mut stream = n0.client_write_many(requests).await?;

  let mut results: Vec<WriteResponse<TypeConfig>> = Vec::new();
  while let Some(result) = stream.try_next().await? {
    results.push(result?);
  }

  assert_eq!(5, results.len());

  for (i, resp) in results.iter().enumerate() {
    assert_eq!(log_id(1, 0, log_index + 1 + i as u64), resp.log_id);
  }

  log_index += 5;

  assert_eq!(None, results[0].response.0.as_deref());
  assert_eq!(Some("request-0"), results[1].response.0.as_deref());
  assert_eq!(Some("request-1"), results[2].response.0.as_deref());
  assert_eq!(Some("request-2"), results[3].response.0.as_deref());
  assert_eq!(Some("request-3"), results[4].response.0.as_deref());

  for id in [0, 1, 2] {
    router
      .wait(&id, None)
      .applied_index(Some(log_index), "batch writes applied")
      .await?;
  }

  Ok(())
}

/// 客户端写入 WriteRequest 构建器 API 测试
#[compio::test]
async fn test_write_builder() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_tick: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let _ = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

  let n0 = router.get_raft_handle(&0)?;

  let (responder, complete_rx) = ProgressResponder::complete_only();
  n0.write(ClientRequest::make_request("foo", 2))
    .responder(responder)
    .await?;
  let got: ClientWriteResponse<TypeConfig> = complete_rx.await??;
  assert_eq!(None, got.response().0.as_deref());

  n0.write(ClientRequest::make_request("foo", 3)).await?;

  let (responder, complete_rx) = ProgressResponder::complete_only();
  n0.write(ClientRequest::make_request("foo", 4))
    .responder(responder)
    .await?;
  let got: ClientWriteResponse<TypeConfig> = complete_rx.await??;
  assert_eq!(Some("request-3"), got.response().0.as_deref());

  Ok(())
}

/// 写入空日志 (write_blank) 作为屏障和 Term 推进
#[compio::test]
async fn test_write_blank() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_tick: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

  let n0 = router.get_raft_handle(&0)?;

  let resp = n0.write_blank().await?;
  log_index += 1;
  assert_eq!(&log_id(1, 0, log_index), resp.log_id());

  for id in [0, 1, 2] {
    router
      .wait(&id, None)
      .applied_index(Some(log_index), "blank entry applied")
      .await?;
  }

  let (mut sto, _sm) = router.get_storage_handle(&0)?;
  let entries = sto.try_get_log_entries(log_index..=log_index).await?;
  assert_eq!(1, entries.len());

  n0.client_write(ClientRequest::make_request("after_blank", 1))
    .await?;
  log_index += 1;

  for id in [0, 1, 2] {
    router
      .wait(&id, None)
      .applied_index(Some(log_index), "normal entry after blank")
      .await?;
  }

  Ok(())
}

/// 触发日志裁剪 API `trigger_purge_log`
#[compio::test]
async fn test_trigger_purge_log() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      snapshot_policy: SnapshotPolicy::Never,
      max_in_snapshot_log_to_keep: u64::MAX,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

  log_index += router.client_request_many(0, "0", 10).await?;
  for id in [0, 1, 2] {
    router
      .wait(&id, timeout())
      .applied_index(Some(log_index), "write logs")
      .await?;
  }

  let n0 = router.get_raft_handle(&0)?;
  n0.trigger().snapshot().await?;
  router
    .wait(&0, timeout())
    .snapshot(log_id(1, 0, log_index), "node-0 snapshot")
    .await?;

  let snapshot_index = log_index;

  log_index += router.client_request_many(0, "0", 10).await?;
  for id in [0, 1, 2] {
    router
      .wait(&id, timeout())
      .applied_index(Some(log_index), "write logs 2")
      .await?;
  }

  n0.trigger().purge_log(snapshot_index).await?;
  router
    .wait(&0, timeout())
    .purged(
      Some(log_id(1, 0, snapshot_index)),
      format!("node-0 purged up to {snapshot_index}"),
    )
    .await?;

  n0.trigger().purge_log(log_index).await?;
  let res = router
    .wait(&0, timeout())
    .purged(
      Some(log_id(1, 0, log_index)),
      format!("node-0 cannot purge up to {log_index}"),
    )
    .await;
  assert!(res.is_err(), "cannot purge logs not in snapshot");

  Ok(())
}

/// 通过 `get_snapshot` 获取当前快照
#[compio::test]
async fn test_get_snapshot() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let log_index = router.new_cluster(btreeset! {0,1}, btreeset! {}).await?;

  let n1 = router.get_raft_handle(&1)?;
  let curr_snap = n1.get_snapshot().await?;
  assert!(curr_snap.is_none());

  n1.trigger().snapshot().await?;
  router
    .wait(&1, timeout())
    .snapshot(log_id(1, 0, log_index), "node-1 snapshot")
    .await?;

  let curr_snap = n1.get_snapshot().await?;
  let snap = curr_snap.unwrap();
  assert_eq!(snap.meta.last_log_id, Some(log_id(1, 0, log_index)));

  Ok(())
}

/// 连续调用两次 trigger().snapshot() 不会引起崩溃
#[compio::test]
async fn test_trigger_snapshot_twice_at_same_last_applied() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      enable_tick: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let log_index = router.new_cluster(btreeset! {0}, btreeset! {}).await?;

  let n0 = router.get_raft_handle(&0)?;
  let snap_at = log_id(1, 0, log_index);

  n0.trigger().snapshot().await?;
  router
    .wait(&0, timeout())
    .snapshot(snap_at, "first snapshot built")
    .await?;

  n0.trigger().snapshot().await?;
  TypeConfig::sleep(Duration::from_millis(500)).await;

  router.client_request_many(0, "after_dup_snap", 1).await?;
  let log_index = log_index + 1;
  router
    .wait(&0, timeout())
    .applied_index(Some(log_index), "raft core survived duplicate snapshot")
    .await?;

  Ok(())
}

/// `with_raft_state` 访问内部 RaftState
#[compio::test]
async fn test_with_raft_state() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

  let n0 = router.get_raft_handle(&0)?;

  let committed = n0
    .with_raft_state(|st| st.local_committed().cloned())
    .await?;
  assert_eq!(Some(log_id(1, 0, log_index)), committed);

  let cluster_committed = n0
    .with_raft_state(|st| st.cluster_committed().cloned())
    .await?;
  assert_eq!(Some(log_id(1, 0, log_index)), cluster_committed);

  n0.shutdown().await?;
  let res = n0.with_raft_state(|st| st.local_committed().cloned()).await;
  assert_eq!(Err(Fatal::Stopped), res);

  Ok(())
}

/// `with_state_machine` 访问内部状态机
#[compio::test]
async fn test_with_state_machine() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

  let n0 = router.get_raft_handle(&0)?;

  let applied = n0
    .with_state_machine(|sm: &mut Arc<MemStateMachine>| {
      let sm = sm.clone();
      Box::pin(async move {
        let d = sm.get_state_machine().await;
        d.last_applied_log
      })
    })
    .await?;
  assert_eq!(applied, Some(log_id(1, 0, log_index)));

  n0.shutdown().await?;
  let res = n0
    .with_state_machine(|_sm: &mut Arc<MemStateMachine>| Box::pin(async move {}))
    .await;
  assert_eq!(Err(Fatal::Stopped), res);

  Ok(())
}

/// Leader 退出且日志被覆盖时写入返回 ForwardToLeader
#[compio::test]
async fn test_write_when_leader_quit_and_log_revert() -> Result<()> {
  let config = Arc::new(
    Config {
      heartbeat_interval: 100,
      election_timeout_min: 200,
      election_timeout_max: 300,
      enable_tick: false,
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let log_index = router.new_cluster(btreeset! {0,1}, btreeset! {}).await?;

  router.set_unreachable(1, true);

  let (tx, rx) = TypeConfig::oneshot();
  {
    let n0 = router.get_raft_handle(&0)?;
    TypeConfig::spawn(async move {
      let res = n0.client_write(ClientRequest::make_request("cli", 1)).await;
      tx.send(res);
    });
  }

  TypeConfig::sleep(Duration::from_millis(500)).await;

  {
    let n0 = router.get_raft_handle(&0)?;
    let _append_res = n0
      .append_entries(AppendEntriesRequest {
        vote: Vote::new_committed(10, 1),
        prev_log_id: Some(log_id(10, 1, log_index + 1)),
        entries: vec![],
        leader_commit: None,
      })
      .await?;
  }

  let write_res = rx.await?;
  let raft_err = write_res.unwrap_err();
  assert_eq!(
    raft_err,
    RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader {
      leader_id: Some(1),
      leader_node: Some(()),
    }))
  );

  Ok(())
}

/// Leader 切换但日志已被提交时，写入仍能成功返回
#[compio::test]
async fn test_write_when_leader_switched() -> Result<()> {
  let config = Arc::new(
    Config {
      heartbeat_interval: 100,
      election_timeout_min: 200,
      election_timeout_max: 300,
      enable_tick: false,
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let log_index = router.new_cluster(btreeset! {0,1}, btreeset! {}).await?;

  router.set_unreachable(1, true);

  let (tx, rx) = TypeConfig::oneshot();
  {
    let n0 = router.get_raft_handle(&0)?;
    TypeConfig::spawn(async move {
      let res = n0.client_write(ClientRequest::make_request("cli", 1)).await;
      tx.send(res);
    });
  }

  TypeConfig::sleep(Duration::from_millis(500)).await;

  {
    let n0 = router.get_raft_handle(&0)?;
    let _append_res = n0
      .append_entries(AppendEntriesRequest {
        vote: Vote::new_committed(10, 1),
        prev_log_id: Some(log_id(1, 0, log_index + 1)),
        entries: vec![],
        leader_commit: Some(log_id(1, 0, log_index + 1)),
      })
      .await?;
  }

  let write_res = rx.await?;
  let ok_resp = write_res?;
  assert_eq!(
    ok_resp.log_id,
    log_id(1, 0, log_index + 1),
    "client write committed"
  );

  Ok(())
}

/// 线性一致性读测试 (ReadIndex & 隔离检测)
#[compio::test]
async fn test_client_reads() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  router.network_send_delay(0);

  let _log_index = router
    .new_cluster(btreeset! {0,1,2,3}, btreeset! {})
    .await?;

  let leader = router.leader().expect("leader not found");
  assert_eq!(leader, 0);

  router
    .ensure_linearizable(leader, ReadPolicy::ReadIndex)
    .await?;

  router
    .ensure_linearizable(1, ReadPolicy::ReadIndex)
    .await
    .expect_err("follower 1 should fail");
  router
    .ensure_linearizable(2, ReadPolicy::ReadIndex)
    .await
    .expect_err("follower 2 should fail");

  router.set_network_error(1, true);
  router
    .ensure_linearizable(leader, ReadPolicy::ReadIndex)
    .await?;

  Ok(())
}

/// 租约读 (LeaseRead) 策略测试
#[compio::test]
async fn test_ensure_linearizable_with_lease_read() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      enable_elect: false,
      heartbeat_interval: 1000,
      election_timeout_min: 1001,
      election_timeout_max: 1002,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  router.network_send_delay(0);

  let _log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;
  let leader = router.leader().expect("leader not found");

  TypeConfig::sleep(Duration::from_millis(300)).await;

  let rpc_count_before = router.get_rpc_count();
  let before = *rpc_count_before
    .get(&zenoh_raft::RPCTypes::AppendEntries)
    .unwrap_or(&0);

  router
    .ensure_linearizable(leader, ReadPolicy::LeaseRead)
    .await?;

  let rpc_count_after = router.get_rpc_count();
  let after = *rpc_count_after
    .get(&zenoh_raft::RPCTypes::AppendEntries)
    .unwrap_or(&0);
  assert_eq!(before, after, "LeaseRead does not emit extra heartbeats");

  Ok(())
}

/// Leader 主动转移领导权 (TransferLeader) 测试
#[compio::test]
async fn test_transfer_leader() -> Result<()> {
  let config = Arc::new(
    Config {
      election_timeout_min: 150,
      election_timeout_max: 300,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let _log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

  let n0 = router.get_raft_handle(&0)?;
  let n1 = router.get_raft_handle(&1)?;
  let n2 = router.get_raft_handle(&2)?;

  let metrics = n0.metrics().borrow_watched().clone();
  let leader_vote = metrics.vote;
  let last_log_id = metrics.last_applied;

  let req = TransferLeaderRequest::new(leader_vote, 2, last_log_id);

  n1.handle_transfer_leader(req.clone()).await??;
  n2.handle_transfer_leader(req.clone()).await??;

  n2.wait(timeout())
    .state(ServerState::Leader, "node-2 become leader")
    .await?;
  n0.wait(timeout())
    .state(ServerState::Follower, "node-0 become follower")
    .await?;

  Ok(())
}

/// Trigger transfer leader API 测试
#[compio::test]
async fn test_trigger_transfer_leader() -> Result<()> {
  let config = Arc::new(
    Config {
      election_timeout_min: 150,
      election_timeout_max: 300,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let _log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

  let n0 = router.get_raft_handle(&0)?;
  let n1 = router.get_raft_handle(&1)?;
  let n2 = router.get_raft_handle(&2)?;

  n0.trigger().transfer_leader(2).await?;

  n2.wait(timeout())
    .state(ServerState::Leader, "node-2 become leader")
    .await?;
  n0.wait(timeout())
    .state(ServerState::Follower, "node-0 become follower")
    .await?;
  n1.wait(timeout())
    .state(ServerState::Follower, "node-1 become follower")
    .await?;

  Ok(())
}

/// 测试通过 install_full_snapshot API 覆盖/安装快照
#[compio::test]
async fn test_install_full_snapshot() -> Result<()> {
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

  router.set_unreachable(2, true);

  log_index += router.client_request_many(0, "foo", 3).await?;
  router
    .wait(&0, timeout())
    .applied_index(Some(log_index), "write more log")
    .await?;
  router
    .wait(&1, timeout())
    .applied_index(Some(log_index), "write more log")
    .await?;

  let snap;
  {
    let n0 = router.get_raft_handle(&0)?;
    n0.trigger().snapshot().await?;
    router
      .wait(&0, timeout())
      .snapshot(log_id(1, 0, log_index), "node-0 snapshot")
      .await?;
    snap = n0.get_snapshot().await?.unwrap();
  }

  {
    let n1 = router.get_raft_handle(&1)?;
    let resp = n1
      .install_full_snapshot(Vote::new(0, 0), snap.clone())
      .await?;
    assert_eq!(Vote::new_committed(1, 0), resp.vote);
    n1.with_raft_state(|state| {
      assert_eq!(None, state.snapshot_meta.last_log_id);
    })
    .await?;
  }

  {
    let n2 = router.get_raft_handle(&2)?;
    let resp = n2
      .install_full_snapshot(Vote::new_committed(1, 0), snap.clone())
      .await?;
    assert_eq!(Vote::new_committed(1, 0), resp.vote);
    n2.with_raft_state(move |state| {
      assert_eq!(
        Some(log_id(1, 0, log_index)),
        state.snapshot_meta.last_log_id
      );
    })
    .await?;
  }

  Ok(())
}

/// Leader 租约过期时拒绝新写入，但保留已挂起写入并在租约恢复后成功提交
#[compio::test]
async fn test_client_write_requires_valid_quorum_lease() -> Result<()> {
  let config = Arc::new(
    Config {
      heartbeat_interval: 20,
      election_timeout_min: 100,
      election_timeout_max: 200,
      enable_tick: false,
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

  router.set_unreachable(1, true);
  router.set_unreachable(2, true);

  let (responder, pending_rx) = ProgressResponder::complete_only();
  n0.client_write_ff(ClientRequest::make_request("pending", 1), Some(responder))
    .await?;
  log_index += 1;
  n0.wait(timeout())
    .log_index(Some(log_index), "pending write appended")
    .await?;

  TypeConfig::sleep(Duration::from_millis(config.election_timeout_max)).await;

  let rejected = TypeConfig::timeout(
    Duration::from_millis(100),
    n0.client_write(ClientRequest::make_request("rejected", 2)),
  )
  .await
  .expect("an expired leader lease rejects a new write")
  .unwrap_err();

  assert_eq!(
    RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader::empty())),
    rejected
  );

  let metrics = n0.metrics().borrow_watched().clone();
  assert_eq!(ServerState::Leader, metrics.state);
  assert_eq!(Some(0), metrics.current_leader);
  assert_eq!(Some(log_index), metrics.last_log_index);

  router.set_unreachable(1, false);
  router.set_unreachable(2, false);

  let refresh_started = TypeConfig::now();
  n0.trigger().heartbeat().await?;
  n0.wait(timeout())
    .leader_with_quorum_acked(Some(refresh_started), "leader lease recovered")
    .await?;

  let recovered = n0
    .client_write(ClientRequest::make_request("recovered", 3))
    .await?;
  log_index += 1;
  assert_eq!(log_id(1, 0, log_index), recovered.log_id);

  let pending = pending_rx.await??;
  assert_eq!(log_id(1, 0, log_index - 1), pending.log_id);

  Ok(())
}

/// Raft 节点角色与成员查询 API (is_leader, node_id, voter_ids, learner_ids, as_leader) 测试
#[compio::test]
async fn test_api_node_roles() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_tick: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config.clone());
  router.new_cluster(btreeset! {0, 1}, btreeset! {2}).await?;

  let leader = router.get_raft_handle(&0)?;
  let follower = router.get_raft_handle(&1)?;
  let learner = router.get_raft_handle(&2)?;

  assert!(leader.is_leader());
  assert!(!follower.is_leader());
  assert!(!learner.is_leader());

  assert_eq!(leader.node_id(), &0);
  assert_eq!(follower.node_id(), &1);
  assert_eq!(learner.node_id(), &2);

  let voters: Vec<u64> = leader.voter_ids().collect();
  assert_eq!(voters.len(), 2);
  assert!(voters.contains(&0));
  assert!(voters.contains(&1));

  let learners: Vec<u64> = leader.learner_ids().collect();
  assert_eq!(learners, vec![2]);

  let leader_info = leader.as_leader().expect("leader info");
  assert_eq!(leader_info.leader_id().node_id, 0);

  let forward = follower.as_leader().expect_err("follower forward");
  assert_eq!(forward.leader_id, Some(0));

  Ok(())
}

/// 延迟网络模拟下的集群初始化、配置变更与日志写入
#[compio::test]
async fn test_lagging_network_write() -> Result<()> {
  let config = Arc::new(
    Config {
      heartbeat_interval: 100,
      election_timeout_min: 300,
      election_timeout_max: 600,
      enable_tick: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::builder(config).send_delay(30).build();

  let mut log_index = router.new_cluster(btreeset! {0}, btreeset! {1, 2}).await?;

  router.client_request_many(0, "client", 1).await?;
  log_index += 1;
  for id in [0, 1, 2] {
    router
      .wait(&id, timeout())
      .applied_index(Some(log_index), "write one log")
      .await?;
  }

  let node = router.get_raft_handle(&0)?;
  node.change_membership([0, 1, 2], false).await?;
  log_index += 2;
  router
    .wait(&0, None)
    .state(ServerState::Leader, "changed")
    .await?;
  for node in [1, 2] {
    router
      .wait(&node, None)
      .state(ServerState::Follower, "changed")
      .await?;
  }
  for id in [0, 1, 2] {
    router
      .wait(&id, timeout())
      .applied_index(Some(log_index), "3 candidates")
      .await?;
  }

  router.client_request_many(0, "client", 1).await?;
  log_index += 1;
  for id in [0, 1, 2] {
    router
      .wait(&id, timeout())
      .applied_index(Some(log_index), "write 2nd log")
      .await?;
  }

  Ok(())
}

/// 使用 ProgressResponder 进行 client_write_ff 测试
#[compio::test]
async fn test_client_write_ff_with_progress_responder() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_tick: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

  let n0 = router.get_raft_handle(&0)?;

  let (responder, commit_rx, complete_rx) = ProgressResponder::new();
  n0.client_write_ff(ClientRequest::make_request("foo", 10), Some(responder))
    .await?;
  log_index += 1;

  let commit_log_id = commit_rx.await?;
  assert_eq!(log_id(1, 0, log_index), commit_log_id);

  let result = complete_rx.await?;
  let response = result?;
  assert_eq!(log_id(1, 0, log_index), response.log_id);
  assert_eq!(None, response.response().0.as_deref());

  n0.client_write_ff(ClientRequest::make_request("foo", 11), None)
    .await?;
  log_index += 1;

  let (responder, commit_rx, complete_rx) = ProgressResponder::new();
  n0.client_write_ff(ClientRequest::make_request("foo", 12), Some(responder))
    .await?;
  log_index += 1;

  let commit_log_id_2 = commit_rx.await?;
  assert_eq!(log_id(1, 0, log_index), commit_log_id_2);

  let result = complete_rx.await?;
  let response = result?;
  assert_eq!(log_id(1, 0, log_index), response.log_id);
  assert_eq!(Some("request-11"), response.response().0.as_deref());

  Ok(())
}

/// 快照安装覆盖旧未提交写入时清理残留 responder (Issue 1761)
#[compio::test]
async fn test_write_then_superseded_by_snapshot_install() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_tick: false,
      enable_heartbeat: false,
      enable_elect: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

  router.set_unreachable(0, true);

  let (tx, rx) = TypeConfig::oneshot();
  {
    let n0 = router.get_raft_handle(&0)?;
    TypeConfig::spawn(async move {
      let res = n0.client_write(ClientRequest::make_request("cli", 1)).await;
      tx.send(res);
    });
  }

  TypeConfig::sleep(Duration::from_millis(500)).await;

  {
    let n1 = router.get_raft_handle(&1)?;
    n1.trigger().elect(false).await?;
    router
      .wait(&1, timeout())
      .applied_index(Some(log_index + 1), "node 1 leader blank")
      .await?;
  }

  let snap_index = log_index + 1 + router.client_request_many(1, "foo", 10).await?;
  router
    .wait(&1, timeout())
    .applied_index(Some(snap_index), "node 1 advanced")
    .await?;
  router
    .wait(&2, timeout())
    .applied_index(Some(snap_index), "node 2 advanced")
    .await?;

  let snap = {
    let n1 = router.get_raft_handle(&1)?;
    n1.trigger().snapshot().await?;
    router
      .wait(&1, timeout())
      .snapshot(log_id(2, 1, snap_index), "node 1 snapshot")
      .await?;
    n1.get_snapshot().await?.unwrap()
  };

  {
    let n0 = router.get_raft_handle(&0)?;
    n0.install_full_snapshot(Vote::new_committed(2, 1), snap)
      .await?;
  }

  {
    let n0 = router.get_raft_handle(&0)?;
    n0.append_entries(AppendEntriesRequest {
      vote: Vote::new_committed(2, 1),
      prev_log_id: Some(log_id(2, 1, snap_index)),
      entries: vec![blank_ent::<TypeConfig>(2, 1, snap_index + 1)],
      leader_commit: Some(log_id(2, 1, snap_index + 1)),
    })
    .await?;
  }

  let write_res = rx.await?;
  let raft_err = write_res.unwrap_err();
  assert_eq!(
    raft_err,
    RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader {
      leader_id: Some(1),
      leader_node: Some(()),
    }))
  );

  Ok(())
}

use std::future::ready;

use fixtures::{expect_quorum_not_enough, rpc_request::RpcRequest};
use zenoh_raft::{
  Instant, LinearizerOption, LogIdOptionExt,
  async_runtime::BoxFuture,
  errors::{LinearizableReadError, NetworkError, RPCError},
  network::RPCTypes,
  raft::TransferLeaderError,
  vote::RaftLeaderId,
};

/// 转移目标节点离线时，不会阻碍后续新领导选举
#[compio::test]
async fn test_transfer_leader_with_dead_target_does_not_block_election() -> Result<()> {
  let election_timeout_max = 300;
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      election_timeout_min: 150,
      election_timeout_max,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config);
  router.network_send_delay(0);
  let _log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  let n0 = router.get_raft_handle(&0)?;
  let n2 = router.get_raft_handle(&2)?;

  router.set_network_error(1, true);

  let r = router.clone();
  TypeConfig::spawn(async move {
    loop {
      let _ = r.ensure_linearizable(0, ReadPolicy::ReadIndex).await;
      TypeConfig::sleep(Duration::from_millis(20)).await;
    }
  });

  n0.trigger().transfer_leader(1).await?;

  let mut got = false;
  for _ in 0..20 {
    let res = n0.ensure_linearizable(ReadPolicy::ReadIndex).await;
    if let Err(e) = &res
      && let Some(LinearizableReadError::ForwardToLeader(fwd)) = e.api_error()
      && fwd.leader_id == Some(1)
    {
      got = true;
      break;
    }
    TypeConfig::sleep(Duration::from_millis(10)).await;
  }
  assert!(
    got,
    "expected ForwardToLeader(1) within 200ms after transfer"
  );

  n2.wait(Some(Duration::from_millis(5 * election_timeout_max)))
    .state(
      ServerState::Leader,
      "n2 elects after n0 stops emitting read-index AE",
    )
    .await?;

  Ok(())
}

/// 转移中的 Leader 必须拦截 LeaseRead 并重定向
#[compio::test]
async fn test_transfer_leader_blocks_lease_read() -> Result<()> {
  let config = Arc::new(
    Config {
      heartbeat_interval: 1000,
      election_timeout_min: 1001,
      election_timeout_max: 1002,
      enable_heartbeat: false,
      enable_elect: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config);
  router.network_send_delay(0);
  router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  let n0 = router.get_raft_handle(&0)?;

  n0.wait(Some(Duration::from_millis(500)))
    .leader_with_quorum_acked(None, "leader has last_quorum_acked")
    .await?;

  n0.ensure_linearizable(ReadPolicy::LeaseRead)
    .await
    .expect("LeaseRead succeeds on healthy leader");

  router.set_network_error(1, true);
  n0.trigger().transfer_leader(1).await?;

  let mut got = false;
  for _ in 0..20 {
    let res = n0.ensure_linearizable(ReadPolicy::LeaseRead).await;
    if let Err(e) = &res
      && let Some(LinearizableReadError::ForwardToLeader(fwd)) = e.api_error()
      && fwd.leader_id == Some(1)
    {
      got = true;
      break;
    }
    TypeConfig::sleep(Duration::from_millis(10)).await;
  }
  assert!(
    got,
    "expected ForwardToLeader(1) for LeaseRead after transfer"
  );

  Ok(())
}

/// 领导转移选举请求可以覆盖其他投票者的租约
#[compio::test]
async fn test_transfer_leader_overrides_lease() -> Result<()> {
  let config = Arc::new(
    Config {
      election_timeout_min: 10_000,
      election_timeout_max: 10_001,
      enable_heartbeat: false,
      enable_elect: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config);
  router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  let n0 = router.get_raft_handle(&0)?;
  let n2 = router.get_raft_handle(&2)?;

  let metrics = n0.metrics().borrow_watched().clone();
  let leader_vote = metrics.vote;
  let last_log_id = metrics.last_applied;

  let req = TransferLeaderRequest::new(leader_vote, 2, last_log_id);
  n2.handle_transfer_leader(req).await??;

  n2.wait(timeout())
    .state(ServerState::Leader, "node-2 becomes leader within lease")
    .await?;
  n0.wait(timeout())
    .state(ServerState::Follower, "node-0 steps down")
    .await?;

  Ok(())
}

/// 向尚未同步提升日志的提升 Learner 转移领导权不应崩溃
#[compio::test]
async fn test_transfer_leader_to_promoted_learner_without_promotion_log_does_not_panic()
-> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      enable_tick: false,
      election_timeout_min: 100,
      election_timeout_max: 200,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config);
  router.network_send_delay(0);

  let mut log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {3})
    .await?;
  let n0 = router.get_raft_handle(&0)?;
  let n3 = router.get_raft_handle(&3)?;

  router
    .wait(&3, timeout())
    .metrics(
      |m| {
        m.state == ServerState::Learner
          && !m.membership_config.voter_ids().any(|id| id == 3)
          && m.last_log_index == Some(log_index)
      },
      "node 3 starts as caught-up learner",
    )
    .await?;

  router.set_network_error(3, true);
  n0.change_membership([0, 1, 2, 3], false).await?;
  log_index += 2;

  router
    .wait(&0, timeout())
    .metrics(
      |m| {
        m.membership_config.voter_ids().any(|id| id == 3)
          && m.last_applied.as_ref().map(|log_id| log_id.index()) == Some(log_index)
      },
      "leader sees node 3 as voter",
    )
    .await?;

  let leader_metrics = n0.metrics().borrow_watched().clone();
  let expected_last_log_id = leader_metrics.last_applied;
  let actual_last_log_id = n3.data_metrics().borrow_watched().last_log;
  let req = TransferLeaderRequest::new(leader_metrics.vote, 3, expected_last_log_id);

  let resp = n3.handle_transfer_leader(req).await;
  assert_eq!(
    Ok(Err(TransferLeaderError::LogNotFlushed {
      expected: expected_last_log_id,
      actual: actual_last_log_id,
    })),
    resp
  );

  TypeConfig::sleep(Duration::from_millis(50)).await;
  assert!(n3.is_initialized().await.is_ok());

  Ok(())
}

/// 测试读取日志 ID API (get_read_log_id)
#[compio::test]
async fn test_get_read_log_id() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      enable_elect: false,
      heartbeat_interval: 100,
      election_timeout_min: 101,
      election_timeout_max: 102,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config);
  let mut log_index = router.new_cluster(btreeset! {0, 1}, btreeset! {}).await?;

  let block_to_n0 = |_router: &_, req, _id, target| {
    let err = || {
      Err(RPCError::Network(NetworkError::<TypeConfig>::from_string(
        "block append-entries to node 0",
      )))
    };

    let res = if target == 0 {
      match req {
        RpcRequest::AppendEntries(a) => {
          if a.entries.is_empty() {
            Ok(())
          } else {
            err()
          }
        }
        _ => unreachable!(),
      }
    } else {
      Ok(())
    };

    Box::pin(ready(res)) as BoxFuture<_>
  };

  router
    .set_rpc_pre_hook(RPCTypes::AppendEntries, block_to_n0)
    .await;
  TypeConfig::sleep(Duration::from_millis(200)).await;

  let leader = router.leader().expect("leader found");
  let read_log_id = router
    .get_read_log_id(leader, ReadPolicy::ReadIndex)
    .await?;
  assert_eq!(read_log_id.0.index(), log_index);

  router.rpc_pre_hook(RPCTypes::AppendEntries, None).await;
  log_index += router.client_request_many(leader, "foo", 2).await?;

  let read_log_id = router
    .get_read_log_id(leader, ReadPolicy::ReadIndex)
    .await?;
  assert_eq!(read_log_id.0.index(), log_index);

  Ok(())
}

/// 使用 ReadIndex 策略进行线性化读
#[compio::test]
async fn test_ensure_linearizable_with_read_index() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      enable_elect: false,
      heartbeat_interval: 100,
      election_timeout_min: 101,
      election_timeout_max: 102,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config);
  router.network_send_delay(0);
  let _log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  let leader = router.leader().expect("leader found");
  assert_eq!(leader, 0);

  let rpc_count_before = router.get_rpc_count();
  let append_before = *rpc_count_before.get(&RPCTypes::AppendEntries).unwrap_or(&0);

  router
    .ensure_linearizable(leader, ReadPolicy::ReadIndex)
    .await?;

  let rpc_count_after = router.get_rpc_count();
  let append_after = *rpc_count_after.get(&RPCTypes::AppendEntries).unwrap_or(&0);
  assert!(append_after > append_before);

  router.set_network_error(1, true);
  router
    .ensure_linearizable(leader, ReadPolicy::ReadIndex)
    .await?;

  router.set_network_error(2, true);
  let res = router
    .ensure_linearizable(leader, ReadPolicy::ReadIndex)
    .await;
  assert!(res.is_err());

  Ok(())
}

/// 线性化读等待超时配置测试
#[compio::test]
async fn test_ensure_linearizable_with_wait_timeout() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      enable_elect: false,
      heartbeat_interval: 100,
      election_timeout_min: 999,
      election_timeout_max: 1000,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config.clone());
  router.network_send_delay(0);
  router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  let leader = router.get_raft_handle(&0)?;
  router.set_network_error(1, true);
  router.set_network_error(2, true);

  let leader_lease = Duration::from_millis(config.election_timeout_max);
  let lease_read_margin = leader_lease / 2;

  {
    let option =
      LinearizerOption::new(Some(Duration::ZERO), true).with_wait_timeout(Duration::ZERO);
    let start = Instant::now();
    let res = leader.get_read_linearizer(option).await;
    let elapsed = start.elapsed();

    let got = expect_quorum_not_enough(res.unwrap_err());
    assert_eq!(btreeset! {0}, got);
    assert!(elapsed < lease_read_margin);
  }

  {
    let wait_timeout = Duration::from_millis(100);
    let option = LinearizerOption::new(Some(Duration::ZERO), true).with_wait_timeout(wait_timeout);
    let start = Instant::now();
    let res = leader.get_read_linearizer(option).await;
    let elapsed = start.elapsed();

    let got = expect_quorum_not_enough(res.unwrap_err());
    assert_eq!(btreeset! {0}, got);
    assert!(elapsed >= wait_timeout);
    assert!(elapsed < lease_read_margin);
  }

  Ok(())
}

/// 当禁用即时心跳时，读排队等待周期性心跳
#[compio::test]
async fn test_linearizer_waits_for_periodic_heartbeat_when_immediate_heartbeat_disabled()
-> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      enable_elect: false,
      heartbeat_interval: 100,
      election_timeout_min: 1001,
      election_timeout_max: 1002,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config.clone());
  router.network_send_delay(0);
  let log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  let leader = router.leader().expect("leader found");
  TypeConfig::sleep(Duration::from_millis(300)).await;

  let leader_handle = router.get_raft_handle(&leader).unwrap();
  let metrics = leader_handle.metrics().borrow_watched().clone();
  let expected_leader_id = metrics.vote.leader_id().to_committed();
  let expected_applied = metrics.last_applied;

  TypeConfig::sleep(Duration::from_millis(config.election_timeout_max)).await;

  let rpc_count_before = router.get_rpc_count();
  let before = *rpc_count_before.get(&RPCTypes::AppendEntries).unwrap_or(&0);

  let option = LinearizerOption::new(None, false);
  let mut read_future = Box::pin(leader_handle.get_read_linearizer(option));
  let submitted_status = futures_util::poll!(read_future.as_mut());
  assert!(submitted_status.is_pending());

  leader_handle.with_raft_state(|_| ()).await?;
  let queued_status = futures_util::poll!(read_future.as_mut());
  assert!(queued_status.is_pending());

  let rpc_count_after = router.get_rpc_count();
  let after = *rpc_count_after.get(&RPCTypes::AppendEntries).unwrap_or(&0);
  assert_eq!(before, after);

  leader_handle.runtime_config().heartbeat(true);
  let heartbeat_wait = Duration::from_millis(config.heartbeat_interval * 5);
  let heartbeat_result = TypeConfig::timeout(heartbeat_wait, read_future).await;
  leader_handle.runtime_config().heartbeat(false);

  let linearizer = heartbeat_result.expect("heartbeat completes queued read")?;
  assert_eq!(
    (
      &leader,
      &expected_leader_id,
      log_index,
      expected_applied.as_ref()
    ),
    (
      linearizer.node_id(),
      linearizer.read_log_id().committed_leader_id(),
      linearizer.read_log_id().index(),
      linearizer.applied()
    )
  );

  Ok(())
}

/// 从 Follower 发起线性化读应当失败
#[compio::test]
async fn test_ensure_linearizable_not_process_from_followers() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      enable_elect: false,
      heartbeat_interval: 100,
      election_timeout_min: 101,
      election_timeout_max: 102,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config);
  router.network_send_delay(0);
  router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  let leader = router.leader().expect("leader found");
  assert_eq!(leader, 0);

  router
    .ensure_linearizable(1, ReadPolicy::ReadIndex)
    .await
    .expect_err("follower 1 should fail ReadIndex");

  router
    .ensure_linearizable(1, ReadPolicy::LeaseRead)
    .await
    .expect_err("follower 1 should fail LeaseRead");

  Ok(())
}

/// Follower 追赶 Leader applied 状态后可服务本地线性化读
#[compio::test]
async fn test_ensure_linearizable_process_from_followers() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      enable_elect: false,
      heartbeat_interval: 100,
      election_timeout_min: 101,
      election_timeout_max: 102,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config);
  router.network_send_delay(0);
  let mut log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  let leader_node_id = router.leader().expect("leader found");
  let leader = router.get_raft_handle(&leader_node_id).unwrap();

  let block_to_follower_n1 = |_router: &_, req, _id, target| {
    let err = || {
      Err(RPCError::Network(NetworkError::<TypeConfig>::from_string(
        "block append-entries to follower 1",
      )))
    };

    let res = if target == 1 {
      match req {
        RpcRequest::AppendEntries(a) => {
          if a.entries.is_empty() {
            Ok(())
          } else {
            err()
          }
        }
        _ => unreachable!(),
      }
    } else {
      Ok(())
    };

    Box::pin(ready(res)) as BoxFuture<_>
  };

  router
    .set_rpc_pre_hook(RPCTypes::AppendEntries, block_to_follower_n1)
    .await;
  log_index += router.client_request_many(leader_node_id, "foo", 1).await?;
  leader
    .wait(timeout())
    .applied_index(Some(log_index), "applied")
    .await?;

  let linearizer = leader.get_read_linearizer(ReadPolicy::ReadIndex).await?;
  assert_eq!(linearizer.read_log_id().index(), log_index);
  assert_eq!(linearizer.applied().index(), Some(log_index));

  let follower_n1 = router.get_raft_handle(&1).unwrap();
  let res = linearizer
    .clone()
    .try_await_ready(&follower_n1, Some(Duration::from_millis(500)))
    .await?;
  assert!(res.is_err(), "follower n1 blocked from last log");

  let follower_n2 = router.get_raft_handle(&2).unwrap();
  let state = linearizer.await_ready(&follower_n2).await?;
  assert_eq!(state.applied().index(), Some(log_index));

  router.rpc_pre_hook(RPCTypes::AppendEntries, None).await;
  let linearizer2 = leader.get_read_linearizer(ReadPolicy::ReadIndex).await?;
  let state = linearizer2.await_ready(&follower_n1).await?;
  assert_eq!(state.applied().index(), Some(log_index));

  Ok(())
}
