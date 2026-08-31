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
