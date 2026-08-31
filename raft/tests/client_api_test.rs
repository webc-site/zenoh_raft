//! 客户端 API 与一致性读测试套件

mod fixtures;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use futures_util::stream::FuturesUnordered;
use maplit::btreeset;
use zenoh_raft::Config;
use zenoh_raft::RaftLogReader;
use zenoh_raft::ReadPolicy;
use zenoh_raft::ServerState;
use zenoh_raft::SnapshotPolicy;
use zenoh_raft::Vote;
use zenoh_raft::errors::ClientWriteError;
use zenoh_raft::errors::Fatal;
use zenoh_raft::errors::ForwardToLeader;
use zenoh_raft::errors::RaftError;
use zenoh_raft::impls::ProgressResponder;
use zenoh_raft::raft::AppendEntriesRequest;
use zenoh_raft::raft::ClientWriteResponse;
use zenoh_raft::raft::TransferLeaderRequest;
use zenoh_raft::raft::WriteResponse;
use zenoh_raft::storage::RaftLogStorage;
use zenoh_raft::storage::RaftStateMachine;
use zenoh_raft::testing::memstore::ClientRequest;
use zenoh_raft::testing::memstore::IntoMemClientRequest;
use zenoh_raft::testing::memstore::MemStateMachine;
use zenoh_raft::testing::memstore::TypeConfig;
use zenoh_raft::type_config::TypeConfigExt;

use fixtures::RaftRouter;
use fixtures::log_id;
use fixtures::timeout;

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
