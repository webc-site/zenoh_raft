//! 集群成员变更测试套件

mod fixtures;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use maplit::btreeset;
use zenoh_raft::ChangeMembers;
use zenoh_raft::Config;
use zenoh_raft::EntryPayload;
use zenoh_raft::LogIdOptionExt;
use zenoh_raft::Precondition;
use zenoh_raft::Raft;
use zenoh_raft::ServerState;
use zenoh_raft::alias::LeaderIdOf;
use zenoh_raft::alias::LogIdOf;
use zenoh_raft::errors::ClientWriteError;
use zenoh_raft::errors::ForwardToLeader;
use zenoh_raft::errors::PreconditionFailed;
use zenoh_raft::errors::RaftError;
use zenoh_raft::raft::ChangeMembershipRequest;
use zenoh_raft::testing::memstore::MemNodeId;
use zenoh_raft::testing::memstore::TypeConfig;
use zenoh_raft::type_config::TypeConfigExt;
use zenoh_raft::vote::RaftLeaderId;

use fixtures::RaftRouter;
use fixtures::timeout;

/// 添加 Learner 测试
#[compio::test]
async fn test_add_learner() -> Result<()> {
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

    router.new_raft_node(1).await;
    router.add_learner(0, 1).await?;
    log_index += 1;

    router
        .wait(&1, timeout())
        .applied_index(Some(log_index), "learner caught up")
        .await?;

    Ok(())
}

/// 成员变更 (change_membership) 测试
#[compio::test]
async fn test_change_membership() -> Result<()> {
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

    router.new_raft_node(3).await;
    router.add_learner(0, 3).await?;
    log_index += 1;

    let n0 = router.get_raft_handle(&0)?;
    n0.change_membership(btreeset! {0, 1, 2, 3}, false).await?;
    log_index += 2;

    for id in [0, 1, 2, 3] {
        router
            .wait(&id, timeout())
            .applied_index(Some(log_index), "all 4 nodes in cluster applied membership")
            .await?;
    }

    Ok(())
}

/// Leader 从 Voter 列表中被移除时降级 (step down) 测试
#[compio::test]
async fn test_step_down() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_heartbeat: false,
            enable_elect: false,
            ..Default::default()
        }
        .validate()?,
    );

    let mut router = RaftRouter::new(config.clone());
    let _ = router
        .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
        .await?;

    let n0 = router.get_raft_handle(&0)?;
    n0.change_membership(btreeset! {1, 2}, false).await?;

    router
        .wait(&0, timeout())
        .state(ServerState::Learner, "node 0 stepped down to learner")
        .await?;

    Ok(())
}

/// Learner 重启后维持 Learner 状态
#[compio::test]
async fn test_learner_restart() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_heartbeat: false,
            ..Default::default()
        }
        .validate()?,
    );

    let mut router = RaftRouter::new(config.clone());
    let mut log_index = router.new_cluster(btreeset! {0}, btreeset! {1}).await?;

    router.client_request(0, "foo", 1).await?;
    log_index += 1;

    for id in [0, 1] {
        router
            .wait(&id, None)
            .applied_index(Some(log_index), "write one log")
            .await?;
    }

    let (node0, _sto0, _sm0) = router.remove_node(0).unwrap();
    node0.shutdown().await?;

    let (node1, sto1, sm1) = router.remove_node(1).unwrap();
    node1.shutdown().await?;

    let restarted = Raft::new(1, config.clone(), router.clone(), sto1, sm1).await?;
    restarted
        .wait(timeout())
        .applied_index(Some(log_index), "log after restart")
        .await?;
    restarted
        .wait(timeout())
        .state(ServerState::Learner, "server state after restart")
        .await?;

    Ok(())
}

/// 单节点集群创建与写入
#[compio::test]
async fn test_single_node() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_heartbeat: false,
            ..Default::default()
        }
        .validate()?,
    );

    let mut router = RaftRouter::new(config.clone());
    let mut log_index = router.new_cluster(btreeset! {0}, btreeset! {}).await?;

    router.client_request(0, "foo", 1).await?;
    log_index += 1;

    router
        .wait(&0, None)
        .applied_index(Some(log_index), "write one log")
        .await?;

    Ok(())
}

/// 前置条件 LastMembershipLogId 校验完成联合共识成员变更
#[compio::test]
async fn test_matching_membership_log_id_completes_joint_change() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_heartbeat: false,
            ..Default::default()
        }
        .validate()?,
    );
    let mut router = RaftRouter::new(config.clone());

    let mut log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {3}).await?;
    let leader = router.get_raft_handle(&0)?;

    let membership_log_id = {
        let metrics = leader.metrics().borrow_watched().clone();
        *metrics.membership_config.log_id()
    };

    let precondition = Precondition::LastMembershipLogId {
        last_membership_log_id: membership_log_id,
    };
    let request = ChangeMembershipRequest::<TypeConfig>::new([0, 1, 2, 3], false)
        .with_payload(EntryPayload::Blank, EntryPayload::Blank)
        .with_preconditions([precondition]);
    let change = leader.change_membership_with_payload(request);
    let outcome = change.await?;
    assert!(outcome.joint.is_some());
    let resp = &outcome.uniform;

    log_index += 2;

    let voters = resp
        .membership
        .as_ref()
        .unwrap()
        .voter_ids()
        .collect::<BTreeSet<_>>();
    assert_eq!(btreeset! {0,1,2,3}, voters);

    for node_id in [0, 1, 2, 3] {
        router
            .wait(&node_id, timeout())
            .applied_index(Some(log_index), "uniform config applied")
            .await?;
    }

    Ok(())
}

/// 过期的 LastMembershipLogId 前置条件拒绝变更
#[compio::test]
async fn test_stale_membership_log_id_rejects_change() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_heartbeat: false,
            ..Default::default()
        }
        .validate()?,
    );
    let mut router = RaftRouter::new(config.clone());

    let mut log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {3}).await?;
    let leader = router.get_raft_handle(&0)?;

    let stale_log_id = {
        let metrics = leader.metrics().borrow_watched().clone();
        *metrics.membership_config.log_id()
    };

    leader.change_membership([0, 1, 2, 3], false).await?;
    log_index += 2;

    let current_log_id = {
        let metrics = leader.metrics().borrow_watched().clone();
        *metrics.membership_config.log_id()
    };

    let precondition = Precondition::LastMembershipLogId {
        last_membership_log_id: stale_log_id,
    };
    let err = leader
        .change_membership_if([0, 1, 2], false, [precondition])
        .await
        .unwrap_err();

    let want = PreconditionFailed::LastMembershipLogIdMismatch {
        expected: stale_log_id,
        actual: current_log_id,
    };
    assert_eq!(
        RaftError::APIError(ClientWriteError::PreconditionFailed(want)),
        err
    );

    let metrics = leader.metrics().borrow_watched().clone();
    assert_eq!(current_log_id, *metrics.membership_config.log_id());
    assert_eq!(Some(log_index), metrics.last_log_index);

    Ok(())
}

/// 前置条件 CommittedLeaderId 保护成员变更
#[compio::test]
async fn test_committed_leader_id_guards_the_change() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_heartbeat: false,
            ..Default::default()
        }
        .validate()?,
    );
    let mut router = RaftRouter::new(config.clone());

    let mut log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {3}).await?;
    let leader = router.get_raft_handle(&0)?;

    let established = {
        let metrics = leader.metrics().borrow_watched().clone();
        LeaderIdOf::<TypeConfig>::new_committed(
            metrics.current_term,
            metrics.current_leader.unwrap(),
        )
    };
    let other = LeaderIdOf::<TypeConfig>::new_committed(100, 2);

    let precondition = Precondition::CommittedLeaderId {
        committed_leader_id: other,
    };
    let err = leader
        .change_membership_if([0, 1, 2, 3], false, [precondition])
        .await
        .unwrap_err();

    let want = PreconditionFailed::CommittedLeaderIdMismatch {
        expected: other,
        actual: Some(established),
    };
    assert_eq!(
        RaftError::APIError(ClientWriteError::PreconditionFailed(want)),
        err
    );

    let precondition = Precondition::CommittedLeaderId {
        committed_leader_id: established,
    };
    leader
        .change_membership_if([0, 1, 2, 3], false, [precondition])
        .await?;
    log_index += 2;

    router
        .wait(&0, timeout())
        .applied_index(Some(log_index), "uniform config applied")
        .await?;

    Ok(())
}

/// 前置条件 LastLogId 保护成员变更
#[compio::test]
async fn test_last_log_id_guards_the_change() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_heartbeat: false,
            ..Default::default()
        }
        .validate()?,
    );
    let mut router = RaftRouter::new(config.clone());

    let mut log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {3}).await?;
    let leader = router.get_raft_handle(&0)?;

    let metrics = leader.metrics().borrow_watched().clone();
    let leader_id = LeaderIdOf::<TypeConfig>::new_committed(
        metrics.current_term,
        metrics.current_leader.unwrap(),
    );
    let last_index = metrics.last_log_index.unwrap();
    let last_log_id = Some(LogIdOf::<TypeConfig>::new(leader_id, last_index));
    let earlier_log_id = Some(LogIdOf::<TypeConfig>::new(leader_id, last_index - 1));

    let precondition = Precondition::LastLogId {
        last_log_id: earlier_log_id,
    };
    let err = leader
        .change_membership_if([0, 1, 2, 3], false, [precondition])
        .await
        .unwrap_err();

    let want = PreconditionFailed::LastLogIdMismatch {
        expected: earlier_log_id,
        actual: last_log_id,
    };
    assert_eq!(
        RaftError::APIError(ClientWriteError::PreconditionFailed(want)),
        err
    );

    let precondition = Precondition::LastLogId { last_log_id };
    leader
        .change_membership_if([0, 1, 2, 3], false, [precondition])
        .await?;
    log_index += 2;

    router
        .wait(&0, timeout())
        .applied_index(Some(log_index), "uniform config applied")
        .await?;

    Ok(())
}

/// Follower 响应成员变更时返回 ForwardToLeader 错误
#[compio::test]
async fn test_follower_answers_forward_to_leader() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_heartbeat: false,
            ..Default::default()
        }
        .validate()?,
    );
    let mut router = RaftRouter::new(config.clone());

    let _log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {3}).await?;
    let follower = router.get_raft_handle(&1)?;

    let precondition = Precondition::LastMembershipLogId {
        last_membership_log_id: None,
    };
    let err = follower
        .change_membership_if([0, 1, 2, 3], false, [precondition])
        .await
        .unwrap_err();

    let want = ClientWriteError::ForwardToLeader(ForwardToLeader::new(0, ()));
    assert_eq!(RaftError::APIError(want), err);

    Ok(())
}

/// 联合共识变更在未达到新配置 Quorum 前不会被提交
#[compio::test]
async fn test_commit_joint_config_during_0_to_012() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_heartbeat: false,
            ..Default::default()
        }
        .validate()?,
    );

    let mut router = RaftRouter::new(config.clone());
    let log_index = router.new_cluster(btreeset! {0}, btreeset! {1,2}).await?;

    router.set_network_error(1, true);
    router.set_network_error(2, true);

    TypeConfig::spawn({
        let router = router.clone();
        async move {
            let node = router.get_raft_handle(&0).unwrap();
            let _x = node.change_membership([0, 1, 2], false).await;
        }
    });

    let res = router
        .wait(&0, Some(Duration::from_millis(1000)))
        .metrics(
            |x| x.last_applied.index() > Some(log_index),
            "the next joint log should not commit",
        )
        .await;
    assert!(res.is_err(), "joint log should not commit");

    Ok(())
}

/// 成员变更案例集测试：包含 add, remove 及直接 change
#[compio::test]
async fn test_change_membership_cases() -> Result<()> {
    async fn change_from_to(
        old: BTreeSet<MemNodeId>,
        change_members: BTreeSet<MemNodeId>,
    ) -> Result<()> {
        let new = change_members;
        let only_in_new = new.difference(&old);
        let only_in_old = old.difference(&new);

        let config = Arc::new(
            Config {
                enable_heartbeat: false,
                enable_elect: false,
                ..Default::default()
            }
            .validate()?,
        );
        let mut router = RaftRouter::new(config.clone());
        let mut log_index = router.new_cluster(old.clone(), btreeset! {}).await?;

        for id in only_in_new {
            router.new_raft_node(*id).await;
            router.add_learner(0, *id).await?;
            log_index += 1;
        }

        let node = router.get_raft_handle(&0)?;
        node.change_membership(new.clone(), false).await?;
        log_index += 1;
        if new != old {
            log_index += 1;
        }

        for id in new.iter() {
            router
                .wait(id, timeout())
                .applied_index_at_least(Some(log_index), "new cluster applied")
                .await?;
        }

        for id in only_in_old {
            router
                .wait(id, timeout())
                .metrics(
                    |x| x.state != ServerState::Leader,
                    "removed node is not leader",
                )
                .await?;
        }

        Ok(())
    }

    async fn change_by_add(old: BTreeSet<MemNodeId>, add: &[MemNodeId]) -> Result<()> {
        let change = ChangeMembers::AddVoterIds(add.iter().copied().collect());
        let new = old
            .clone()
            .union(&add.iter().copied().collect())
            .copied()
            .collect::<BTreeSet<_>>();
        let only_in_new = new.difference(&old);

        let config = Arc::new(
            Config {
                enable_heartbeat: false,
                enable_elect: false,
                ..Default::default()
            }
            .validate()?,
        );
        let mut router = RaftRouter::new(config.clone());
        let mut log_index = router.new_cluster(old.clone(), btreeset! {}).await?;

        for id in only_in_new {
            router.new_raft_node(*id).await;
            router.add_learner(0, *id).await?;
            log_index += 1;
        }

        let node = router.get_raft_handle(&0)?;
        node.change_membership(change, false).await?;
        log_index += 1;
        if new != old {
            log_index += 1;
        }

        for id in new.iter() {
            router
                .wait(id, timeout())
                .applied_index_at_least(Some(log_index), "new cluster applied")
                .await?;
        }

        Ok(())
    }

    async fn change_by_remove(old: BTreeSet<MemNodeId>, remove: &[MemNodeId]) -> Result<()> {
        let change = ChangeMembers::RemoveVoters(remove.iter().copied().collect());
        let new = old
            .clone()
            .difference(&remove.iter().copied().collect())
            .copied()
            .collect::<BTreeSet<_>>();

        let config = Arc::new(
            Config {
                enable_heartbeat: false,
                enable_elect: false,
                ..Default::default()
            }
            .validate()?,
        );
        let mut router = RaftRouter::new(config.clone());
        let mut log_index = router.new_cluster(old.clone(), btreeset! {}).await?;

        let node = router.get_raft_handle(&0)?;
        node.change_membership(change, false).await?;
        log_index += 1;
        if new != old {
            log_index += 1;
        }

        for id in new.iter() {
            router
                .wait(id, timeout())
                .applied_index_at_least(Some(log_index), "new cluster applied")
                .await?;
        }

        Ok(())
    }

    change_from_to(btreeset! {0}, btreeset! {0, 1}).await?;
    change_from_to(btreeset! {0, 1}, btreeset! {0, 1, 2}).await?;
    change_by_add(btreeset! {0}, &[1, 2]).await?;
    change_by_remove(btreeset! {0, 1, 2}, &[1]).await?;

    Ok(())
}

/// 并发写入与添加 Learner 测试
#[compio::test]
async fn test_concurrent_write_and_add_learner() -> Result<()> {
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

    router.new_raft_node(1).await;

    let router_clone = router.clone();
    let handle = TypeConfig::spawn(async move {
        router_clone.client_request_many(0, "conc", 5).await?;
        Ok::<(), anyhow::Error>(())
    });

    router.add_learner(0, 1).await?;
    log_index += 1;

    handle.await??;
    log_index += 5;

    for id in [0, 1] {
        router
            .wait(&id, timeout())
            .applied_index(Some(log_index), "concurrent write & add learner done")
            .await?;
    }

    Ok(())
}
