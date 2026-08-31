//! Raft 指标与监控测试套件

mod fixtures;

use std::sync::Arc;

use anyhow::Result;
use maplit::btreeset;
use zenoh_raft::Config;
use zenoh_raft::ServerState;

use fixtures::RaftRouter;
use fixtures::timeout;

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
