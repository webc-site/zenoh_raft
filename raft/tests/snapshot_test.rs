//! 快照构建、流式传输与安装测试套件

mod fixtures;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use maplit::btreeset;
use zenoh_raft::Config;
use zenoh_raft::Entry;
use zenoh_raft::EntryPayload;
use zenoh_raft::Membership;
use zenoh_raft::RaftLogReader;
use zenoh_raft::RaftSnapshotBuilder;
use zenoh_raft::ServerState;
use zenoh_raft::SnapshotPolicy;
use zenoh_raft::StorageHelper;
use zenoh_raft::Vote;
use zenoh_raft::alias::SnapshotMetaOf;
use zenoh_raft::alias::StoredMembershipOf;
use zenoh_raft::raft::AppendEntriesRequest;
use zenoh_raft::storage::RaftLogStorage;
use zenoh_raft::storage::RaftLogStorageExt;
use zenoh_raft::storage::RaftStateMachine;
use zenoh_raft::testing::blank_ent;
use zenoh_raft::testing::membership_ent;
use zenoh_raft::testing::memstore::BlockOperation;
use zenoh_raft::testing::memstore::ClientRequest;
use zenoh_raft::testing::memstore::IntoMemClientRequest;
use zenoh_raft::testing::memstore::TypeConfig;
use zenoh_raft::type_config::TypeConfigExt;

use fixtures::RaftRouter;
use fixtures::log_id;
use fixtures::timeout;

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
        sm1.block
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
                payload: EntryPayload::Membership(Membership::new_with_defaults(
                    vec![btreeset! {2,3}],
                    [],
                )),
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
        sto0.blocking_append([membership_ent::<TypeConfig>(0, 0, 0, vec![btreeset! {0}])])
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
                payload: EntryPayload::Membership(Membership::new_with_defaults(
                    vec![btreeset! {2,3}],
                    [],
                )),
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
