//! 日志存储与持久化 Commit 测试套件

mod fixtures;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use maplit::btreeset;
use zenoh_raft::Config;
use zenoh_raft::SnapshotPolicy;
use zenoh_raft::storage::RaftLogStorage;

use fixtures::RaftRouter;
use fixtures::log_id;
use fixtures::timeout;

/// 日志清理 (trigger().purge_log()) 测试
#[compio::test]
async fn test_log_store_purge() -> Result<()> {
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
    let mut log_index = router
        .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
        .await?;

    log_index += router.client_request_many(0, "0", 10).await?;

    for id in [0, 1, 2] {
        router
            .wait(&id, timeout())
            .applied_index(Some(log_index), "nodes write logs")
            .await?;
    }

    let n0 = router.get_raft_handle(&0)?;
    n0.trigger().snapshot().await?;
    router
        .wait(&0, timeout())
        .snapshot(log_id(1, 0, log_index), "node 0 snapshot")
        .await?;

    let snapshot_index = log_index;

    log_index += router.client_request_many(0, "0", 10).await?;

    for id in [0, 1, 2] {
        router
            .wait(&id, timeout())
            .applied_index(Some(log_index), "nodes write more logs")
            .await?;
    }

    n0.trigger().purge_log(snapshot_index).await?;
    router
        .wait(&0, timeout())
        .purged(
            Some(log_id(1, 0, snapshot_index)),
            "node 0 purged up to snapshot",
        )
        .await?;

    Ok(())
}

/// 应用日志前将 committed log id 写入存储
#[compio::test]
async fn test_write_committed_log_id_to_log_store() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_tick: false,
            ..Default::default()
        }
        .validate()?,
    );

    let mut router = RaftRouter::new(config.clone());
    let mut log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

    log_index += router.client_request_many(0, "0", 10).await?;

    for i in [0, 1, 2] {
        router
            .wait(&i, Some(Duration::from_millis(1000)))
            .applied_index(Some(log_index), "write logs")
            .await?;
    }

    for id in [0, 1, 2] {
        let (_, mut ls, _) = router.remove_node(id).unwrap();
        let committed = ls.read_committed().await?;
        assert_eq!(
            Some(log_id(1, 0, log_index)),
            committed,
            "node-{} committed",
            id
        );
    }

    Ok(())
}
