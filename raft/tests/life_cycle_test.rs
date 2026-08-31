//! 节点生命周期（初始化、重启、恢复、停机）测试套件

mod fixtures;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use maplit::btreemap;
use maplit::btreeset;
use zenoh_raft::Config;
use zenoh_raft::RaftLogReader;
use zenoh_raft::ServerState;
use zenoh_raft::Vote;
use zenoh_raft::metrics::WaitError;
use zenoh_raft::storage::RaftLogStorage;
use zenoh_raft::storage::RaftStateMachine;
use zenoh_raft::testing::memstore::ClientRequest;
use zenoh_raft::testing::memstore::IntoMemClientRequest;

use fixtures::MemLogStore;
use fixtures::MemRaft;
use fixtures::MemStateMachine;
use fixtures::RaftRouter;
use fixtures::log_id;
use fixtures::timeout;

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
        sto.save_vote(&Vote::new(v.leader_id().term + 1, v.leader_id().node_id))
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
        let (node, _ls, _sm): (MemRaft, MemLogStore, MemStateMachine) =
            router.remove_node(id).unwrap();
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
