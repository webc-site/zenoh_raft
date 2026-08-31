//! 日志追加与心跳测试套件

mod fixtures;

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Result;
use maplit::btreeset;
use zenoh_raft::Config;
use zenoh_raft::Entry;
use zenoh_raft::EntryPayload;
use zenoh_raft::RaftLogReader;
use zenoh_raft::ServerState;
use zenoh_raft::Vote;
use zenoh_raft::alias::EntryOf;
use zenoh_raft::raft::AppendEntriesRequest;
use zenoh_raft::raft::VoteRequest;
use zenoh_raft::storage::RaftLogStorage;
use zenoh_raft::storage::RaftLogStorageExt;
use zenoh_raft::storage::RaftStateMachine;
use zenoh_raft::testing::blank_ent;
use zenoh_raft::testing::membership_ent;
use zenoh_raft::testing::memstore::ClientRequest;
use zenoh_raft::testing::memstore::MemLogStore;
use zenoh_raft::testing::memstore::TypeConfig;
use zenoh_raft::type_config::TypeConfigExt;

use fixtures::RaftRouter;
use fixtures::log_id;
use fixtures::timeout;

/// Leader 在追加日志时发现更高 Vote 会退回 Follower 状态
#[compio::test]
async fn test_append_sees_higher_vote() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_heartbeat: false,
            enable_elect: false,
            election_timeout_min: 500,
            election_timeout_max: 501,
            ..Default::default()
        }
        .validate()?,
    );

    let mut router = RaftRouter::new(config.clone());
    let _log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

    let n0 = router.get_raft_handle(&0)?;
    router.set_unreachable(1, true);

    {
        TypeConfig::sleep(Duration::from_millis(800)).await;

        let refresh_started = TypeConfig::now();
        n0.trigger().heartbeat().await?;
        n0.wait(timeout())
            .leader_with_quorum_acked(Some(refresh_started), "node-0 quorum lease recovered")
            .await?;

        router.set_unreachable(1, false);

        let node = router.get_raft_handle(&1)?;
        let resp = node
            .vote(VoteRequest {
                vote: Vote::new(10, 1),
                last_log_id: Some(log_id(10, 1, 5)),
                leadership_transfer: false,
                is_pre_vote: false,
            })
            .await?;

        assert!(resp.is_granted_to(&Vote::new(10, 1)));
    }

    {
        router
            .wait(&0, timeout())
            .state(ServerState::Leader, "node-0 is leader")
            .await?;

        TypeConfig::spawn(async move {
            let _ = n0
                .client_write(ClientRequest {
                    client: "0".to_string(),
                    serial: 1,
                    status: "2".to_string(),
                })
                .await;
        });

        TypeConfig::sleep(Duration::from_millis(500)).await;

        router
            .wait(&0, timeout())
            .state(ServerState::Follower, "node-0 becomes follower")
            .await?;

        router
            .external_request(0, |st| {
                assert_eq!(&Vote::new(10, 1), st.vote_ref(), "higher vote is stored");
            })
            .await?;
    }

    Ok(())
}

/// 测试日志追加冲突的各种处理分支与截断
#[compio::test]
async fn test_append_conflicts() -> Result<()> {
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
        .applied_index(None, "empty")
        .await?;
    router
        .wait(&0, timeout())
        .state(ServerState::Learner, "empty")
        .await?;

    let (r0, sto0, _sm0) = router.remove_node(0).unwrap();
    check_logs(sto0.clone(), vec![]).await?;

    // case 0: prev_log_id == None, 0 logs
    let req = AppendEntriesRequest {
        vote: Vote::new_committed(1, 2),
        prev_log_id: None,
        entries: vec![],
        leader_commit: Some(log_id(1, 0, 2)),
    };
    let resp = r0.append_entries(req).await?;
    assert!(resp.is_success());
    assert!(!resp.is_conflict());

    // case 0: prev_log_id == None, 1 logs
    let req = AppendEntriesRequest {
        vote: Vote::new_committed(1, 2),
        prev_log_id: None,
        entries: vec![blank_ent::<TypeConfig>(0, 0, 0)],
        leader_commit: Some(log_id(1, 0, 2)),
    };
    let resp = r0.append_entries(req).await?;
    assert!(resp.is_success());
    assert!(!resp.is_conflict());

    // case 0: append multiple logs
    let req = AppendEntriesRequest {
        vote: Vote::new_committed(1, 2),
        prev_log_id: Some(log_id(0, 0, 0)),
        entries: vec![
            blank_ent::<TypeConfig>(1, 0, 1),
            blank_ent::<TypeConfig>(1, 0, 2),
            blank_ent::<TypeConfig>(1, 0, 3),
            blank_ent::<TypeConfig>(1, 0, 4),
        ],
        leader_commit: Some(log_id(1, 0, 2)),
    };
    let resp = r0.append_entries(req).await?;
    assert!(resp.is_success());
    assert!(!resp.is_conflict());

    check_logs(sto0.clone(), vec![0, 1, 1, 1, 1]).await?;

    // case 2: prev_log_id == 1-2, 覆盖日志
    let req = AppendEntriesRequest {
        vote: Vote::new_committed(1, 2),
        prev_log_id: Some(log_id(1, 0, 2)),
        entries: vec![blank_ent::<TypeConfig>(2, 0, 3)],
        leader_commit: Some(log_id(1, 0, 2)),
    };
    let resp = r0.append_entries(req).await?;
    assert!(resp.is_success());
    assert!(!resp.is_conflict());

    check_logs(sto0.clone(), vec![0, 1, 1, 2]).await?;

    Ok(())
}

/// 空条目下的日志追加冲突检测
#[compio::test]
async fn test_conflict_with_empty_entries() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_heartbeat: false,
            ..Default::default()
        }
        .validate()?,
    );

    let mut router = RaftRouter::new(config.clone());
    router.new_raft_node(0).await;

    // prev_log_id 不存在时即使 entries 为空也应当返回冲突
    let rpc = AppendEntriesRequest::<TypeConfig> {
        vote: Vote::new_committed(1, 1),
        prev_log_id: Some(log_id(1, 0, 5)),
        entries: vec![],
        leader_commit: Some(log_id(1, 0, 5)),
    };

    let node = router.get_raft_handle(&0)?;
    let resp = node.append_entries(rpc).await?;
    assert!(!resp.is_success());
    assert!(resp.is_conflict());

    // 填充日志
    let rpc = AppendEntriesRequest::<TypeConfig> {
        vote: Vote::new_committed(1, 1),
        prev_log_id: None,
        entries: vec![
            blank_ent::<TypeConfig>(0, 0, 0),
            blank_ent::<TypeConfig>(1, 0, 1),
            Entry {
                log_id: log_id(1, 0, 2),
                payload: EntryPayload::Normal(ClientRequest {
                    client: "foo".to_string(),
                    serial: 1,
                    status: "bar".to_string(),
                }),
            },
        ],
        leader_commit: Some(log_id(1, 0, 5)),
    };

    let node = router.get_raft_handle(&0)?;
    let resp = node.append_entries(rpc).await?;
    assert!(resp.is_success());
    assert!(!resp.is_conflict());

    // 验证以不存在的 prev_log_id=3 追加空条目产生冲突
    let rpc = AppendEntriesRequest::<TypeConfig> {
        vote: Vote::new_committed(1, 1),
        prev_log_id: Some(log_id(1, 0, 3)),
        entries: vec![],
        leader_commit: Some(log_id(1, 0, 5)),
    };

    let node = router.get_raft_handle(&0)?;
    let resp = node.append_entries(rpc).await?;
    assert!(!resp.is_success());
    assert!(resp.is_conflict());

    Ok(())
}

/// 追加带有更高 Term 的日志时更新本地持久化 Vote
#[compio::test]
async fn test_append_entries_with_bigger_term() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_heartbeat: false,
            enable_elect: false,
            ..Default::default()
        }
        .validate()?,
    );
    let mut router = RaftRouter::new(config.clone());
    let log_index = router.new_cluster(btreeset! {0}, btreeset! {1}).await?;

    for id in [0, 1] {
        let (mut sto, mut sm) = router.get_storage_handle(&id)?;
        assert_eq!(
            sto.get_log_state().await?.last_log_id,
            Some(log_id(1, 0, log_index))
        );
        assert_eq!(sto.read_vote().await?, Some(Vote::new_committed(1, 0)));

        let (last_applied, _) = sm.applied_state().await?;
        assert_eq!(last_applied, Some(log_id(1, 0, log_index)));
    }

    let req = AppendEntriesRequest::<TypeConfig> {
        vote: Vote::new_committed(2, 1),
        prev_log_id: Some(log_id(1, 0, log_index)),
        entries: vec![],
        leader_commit: Some(log_id(1, 0, log_index)),
    };

    let node = router.get_raft_handle(&0)?;
    let resp = node.append_entries(req).await?;
    assert!(resp.is_success());

    let (mut sto, mut sm) = router.get_storage_handle(&0)?;
    assert_eq!(
        sto.get_log_state().await?.last_log_id,
        Some(log_id(1, 0, log_index))
    );
    assert_eq!(sto.read_vote().await?, Some(Vote::new_committed(2, 1)));

    let (last_applied, _) = sm.applied_state().await?;
    assert_eq!(last_applied, Some(log_id(1, 0, log_index)));

    Ok(())
}

/// 存在多条不一致日志时复制不会发生永久阻塞
#[compio::test]
async fn test_append_inconsistent_log() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_heartbeat: false,
            ..Default::default()
        }
        .validate()?,
    );
    let mut router = RaftRouter::new(config.clone());
    router.new_raft_node(0).await;

    let mut log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

    let (r0, mut sto0, sm0) = router.remove_node(0).unwrap();
    let (r1, sto1, sm1) = router.remove_node(1).unwrap();
    let (r2, mut sto2, sm2) = router.remove_node(2).unwrap();

    r0.shutdown().await?;
    r1.shutdown().await?;
    r2.shutdown().await?;

    for i in log_index + 1..=100 {
        sto0.blocking_append([blank_ent::<TypeConfig>(2, 1, i)])
            .await?;
        sto2.blocking_append([blank_ent::<TypeConfig>(3, 3, i)])
            .await?;
    }

    sto0.save_vote(&Vote::new(4, 1)).await?;
    sto2.save_vote(&Vote::new(3, 3)).await?;

    log_index = 100;

    router
        .new_raft_node_with_sto(1, sto1.clone(), sm1.clone())
        .await;
    router.set_network_error(1, true);

    router
        .new_raft_node_with_sto(0, sto0.clone(), sm0.clone())
        .await;
    router
        .new_raft_node_with_sto(2, sto2.clone(), sm2.clone())
        .await;

    log_index += 1;

    router
        .wait(&0, Some(Duration::from_millis(2000)))
        .state(ServerState::Follower, "node 0 become follower")
        .await?;

    router
        .wait(&2, Some(Duration::from_millis(5000)))
        .state(ServerState::Leader, "node 2 become leader")
        .await?;

    router
        .wait(&0, Some(Duration::from_millis(2000)))
        .applied_index_at_least(Some(log_index), "sync log to node 0")
        .await?;

    let logs = sto0.try_get_log_entries(60..=60).await?;
    assert_eq!(
        3,
        logs.first().unwrap().log_id.committed_leader_id().term,
        "log is overridden by leader logs"
    );

    Ok(())
}

/// Follower 收到心跳后维持租约并拒绝投票请求
#[compio::test]
async fn test_heartbeat_reject_vote() -> Result<()> {
    let config = Arc::new(
        Config {
            heartbeat_interval: 200,
            election_timeout_min: 1000,
            election_timeout_max: 1001,
            ..Default::default()
        }
        .validate()?,
    );
    let mut router = RaftRouter::new(config.clone());

    let now = TypeConfig::now();
    TypeConfig::sleep(Duration::from_millis(1)).await;

    let log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {3}).await?;

    let vote_modified_time = Arc::new(Mutex::new(Some(TypeConfig::now())));
    {
        let m = vote_modified_time.clone();

        router
            .external_request(1, move |state| {
                let mut l = m.lock().unwrap();
                *l = state.vote_last_modified();
                assert!(state.vote_last_modified() > Some(now));
            })
            .await?;

        let now = TypeConfig::now();
        TypeConfig::sleep(Duration::from_millis(700)).await;

        let m = vote_modified_time.clone();

        router
            .external_request(1, move |state| {
                let l = m.lock().unwrap();
                assert!(state.vote_last_modified() > Some(now));
                assert!(state.vote_last_modified() > *l);
            })
            .await?;
    }

    let node0 = router.get_raft_handle(&0)?;
    let node1 = router.get_raft_handle(&1)?;

    {
        let res = node1
            .vote(VoteRequest::new(Vote::new(10, 2), Some(log_id(10, 1, 10))))
            .await?;
        assert!(!res.is_granted_to(&Vote::new(10, 2)), "vote is rejected");
    }

    {
        TypeConfig::sleep(Duration::from_millis(1500)).await;
        router
            .wait(&1, timeout())
            .applied_index(Some(log_index), "no log is written")
            .await?;
    }

    {
        node0.runtime_config().heartbeat(false);
        TypeConfig::sleep(Duration::from_millis(1500)).await;

        router
            .wait(&1, timeout())
            .applied_index(Some(log_index), "no log is written")
            .await?;

        let res = node1
            .vote(VoteRequest::new(Vote::new(10, 2), Some(log_id(10, 1, 10))))
            .await?;
        assert!(
            res.is_granted_to(&Vote::new(10, 2)),
            "vote is granted after leader lease expired"
        );
    }

    Ok(())
}

/// 测试追加日志时更新成员关系配置
#[compio::test]
async fn test_append_updates_membership() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_heartbeat: false,
            ..Default::default()
        }
        .validate()?,
    );

    let mut router = RaftRouter::new(config.clone());
    router.new_raft_node(0).await;

    let (r0, _sto0, _sm0) = router.remove_node(0).unwrap();

    let req = AppendEntriesRequest {
        vote: Vote::new_committed(1, 1),
        prev_log_id: None,
        entries: vec![
            blank_ent::<TypeConfig>(0, 0, 0),
            blank_ent::<TypeConfig>(1, 0, 1),
            membership_ent::<TypeConfig>(1, 0, 2, vec![btreeset! {1, 2}]),
            blank_ent::<TypeConfig>(1, 0, 3),
            membership_ent::<TypeConfig>(1, 0, 4, vec![btreeset! {1, 2, 3, 4}]),
            blank_ent::<TypeConfig>(1, 0, 5),
        ],
        leader_commit: Some(log_id(0, 0, 0)),
    };

    let resp = r0.append_entries(req).await?;
    assert!(resp.is_success());
    assert!(!resp.is_conflict());

    r0.wait(timeout())
        .voter_ids([1, 2, 3, 4], "append-entries update membership")
        .await?;

    let req = AppendEntriesRequest {
        vote: Vote::new_committed(2, 2),
        prev_log_id: Some(log_id(1, 0, 2)),
        entries: vec![blank_ent::<TypeConfig>(2, 0, 3)],
        leader_commit: Some(log_id(0, 0, 0)),
    };

    let resp = r0.append_entries(req).await?;
    assert!(resp.is_success());
    assert!(!resp.is_conflict());

    r0.wait(timeout())
        .voter_ids([1, 2], "deleting inconsistent logs updates membership")
        .await?;

    Ok(())
}

/// 单节点对孤立 Learner 的复制测试
#[compio::test]
async fn test_replication_1_voter_to_isolated_learner() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_heartbeat: false,
            enable_elect: false,
            ..Default::default()
        }
        .validate()?,
    );

    let mut router = RaftRouter::new(config.clone());
    let _log_index = router.new_cluster(btreeset! {0}, btreeset! {1}).await?;

    router.client_request(0, "client", 1).await?;
    router
        .wait(&1, timeout())
        .applied_index(Some(3), "learner applies logs")
        .await?;

    Ok(())
}

/// 心跳发送能够维持 Leader 租约
#[compio::test]
async fn test_enable_heartbeat() -> Result<()> {
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
    router
        .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
        .await?;

    TypeConfig::sleep(Duration::from_millis(800)).await;

    let n0 = router.get_raft_handle(&0)?;
    n0.wait(timeout())
        .state(ServerState::Leader, "node-0 remains leader with heartbeats")
        .await?;

    Ok(())
}

async fn check_logs(mut log_store: Arc<MemLogStore>, terms: Vec<u64>) -> Result<()> {
    let logs = log_store.try_get_log_entries(..).await?;
    let want: Vec<EntryOf<TypeConfig>> = terms
        .iter()
        .enumerate()
        .map(|(i, term)| blank_ent::<TypeConfig>(*term, 0, i as u64))
        .collect();

    let w = format!("{want:?}");
    let g = format!("{logs:?}");
    assert_eq!(w, g);
    Ok(())
}
