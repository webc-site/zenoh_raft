//! 集群成员变更测试套件

mod fixtures;

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use anyhow::Result;
use fixtures::{RaftRouter, timeout};
use maplit::btreeset;
use zenoh_raft::{
  ChangeMembers, Config, EntryPayload, LogIdOptionExt, Precondition, Raft, ServerState,
  alias::{LeaderIdOf, LogIdOf},
  errors::{ClientWriteError, ForwardToLeader, PreconditionFailed, RaftError},
  raft::ChangeMembershipRequest,
  testing::memstore::{MemNodeId, TypeConfig},
  type_config::TypeConfigExt,
  vote::RaftLeaderId,
};

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
    LeaderIdOf::<TypeConfig>::new_committed(metrics.current_term, metrics.current_leader.unwrap())
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
  let leader_id =
    LeaderIdOf::<TypeConfig>::new_committed(metrics.current_term, metrics.current_leader.unwrap());
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

/// 0 变更为 01234 后的 Leader 选举测试
#[compio::test]
async fn test_leader_election_after_changing_0_to_01234() -> Result<()> {
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
    .new_cluster(btreeset! {0,1,2,3,4}, btreeset! {})
    .await?;

  router.set_network_error(0, true);
  TypeConfig::sleep(Duration::from_millis(700)).await;

  let node_1 = router.get_raft_handle(&1)?;
  node_1.trigger().elect(false).await?;
  log_index += 1;

  router
    .wait(&1, timeout())
    .metrics(|x| x.current_leader == Some(1), "wait for new leader")
    .await?;

  for node_id in [1, 2, 3, 4] {
    router
      .wait(&node_id, timeout())
      .applied_index(Some(log_index), "replicate and apply log to every node")
      .await?;
  }

  let leader_id = 1;

  router.set_network_error(0, false);
  router
    .wait(&0, timeout())
    .metrics(
      |x| x.current_leader == Some(leader_id) && x.last_applied.index() == Some(log_index),
      "wait for restored node-0 to sync",
    )
    .await?;

  let current_leader = router.leader().expect("expected to find current leader");
  assert_eq!(
    leader_id, current_leader,
    "expected cluster leadership to stay the same"
  );

  Ok(())
}

const HANG_RPC: Duration = Duration::from_secs(30);

async fn setup_hung_follower() -> Result<fixtures::TypedRaftRouter> {
  use zenoh_raft::RPCTypes;
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

  router
    .set_rpc_pre_hook(
      RPCTypes::AppendEntries,
      move |_router, _req, _from, target| {
        Box::pin(async move {
          if target == 2 {
            TypeConfig::sleep(HANG_RPC).await;
          }
          Ok(())
        })
      },
    )
    .await;

  router.client_request(0, "before-remove", 1).await?;
  log_index += 1;
  router
    .wait(&0, timeout())
    .applied_index(Some(log_index), "write commits via the {0,1} quorum")
    .await?;

  Ok(router)
}

/// 移除挂起的 Follower 不会阻塞 RaftCore 循环 (测试 1)
#[compio::test]
async fn test_remove_hung_follower_must_not_block_raft_core_loop_1() -> Result<()> {
  let router = setup_hung_follower().await?;

  let leader = router.get_raft_handle(&0)?;
  let membership_change = leader.change_membership([0, 1], false);
  TypeConfig::timeout(Duration::from_secs(10), membership_change)
    .await
    .expect("membership change completed within the window")?;

  Ok(())
}

/// 移除挂起的 Follower 不会阻塞 RaftCore 循环 (测试 2: 保持处理写入)
#[compio::test]
async fn test_remove_hung_follower_must_not_block_raft_core_loop_2() -> Result<()> {
  let router = setup_hung_follower().await?;

  let leader = router.get_raft_handle(&0)?;
  let _membership = TypeConfig::spawn(async move {
    let _ = leader.change_membership([0, 1], false).await;
  });

  TypeConfig::sleep(Duration::from_millis(500)).await;

  let refresh_started = TypeConfig::now();
  router.get_raft_handle(&0)?.trigger().heartbeat().await?;
  router
    .wait(&0, timeout())
    .leader_with_quorum_acked(
      Some(refresh_started),
      "leader lease recovered through the surviving quorum",
    )
    .await?;

  let write = router.client_request(0, "after-remove", 2);
  TypeConfig::timeout(Duration::from_secs(10), write)
    .await
    .expect("write completed within the window")?;

  Ok(())
}

/// 节点从集群中移除后停止对其日志复制
#[compio::test]
async fn test_add_remove_voter() -> Result<()> {
  let c01234 = btreeset![0, 1, 2, 3, 4];
  let c0123 = btreeset![0, 1, 2, 3];

  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router.new_cluster(c01234.clone(), btreeset! {}).await?;

  router.client_request_many(0, "client", 100).await?;
  log_index += 100;

  for id in c01234.iter() {
    router
      .wait(id, timeout())
      .applied_index(Some(log_index), "write 100 logs")
      .await?;
  }

  let node = router.get_raft_handle(&0)?;
  node.change_membership(c0123.clone(), false).await?;
  log_index += 2;

  for id in c0123.iter() {
    router
      .wait(id, timeout())
      .applied_index(Some(log_index), "removed node-4 from membership")
      .await?;
  }

  router.client_request_many(0, "client", 100).await?;
  log_index += 100;

  for id in c0123.iter() {
    router
      .wait(id, timeout())
      .applied_index(Some(log_index), "4 nodes recv logs 100~200")
      .await?;
  }

  let x = router.latest_metrics();
  assert!(x[4].last_log_index < Some(log_index - 50));

  router
    .wait(&4, timeout())
    .metrics(
      |x| x.state == ServerState::Learner || x.state == ServerState::Candidate,
      "node-4 is left a learner or candidate",
    )
    .await?;

  Ok(())
}

/// 移除不可达 Follower 后停止对其复制
#[compio::test]
async fn test_stop_replication_to_removed_unreachable_follower() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  router.new_raft_node(0).await;

  let mut log_index = router
    .new_cluster(btreeset! {0, 1, 2, 3, 4}, btreeset! {})
    .await?;

  router.set_network_error(4, true);
  let node4_log_index = log_index;

  let node = router.get_raft_handle(&0)?;
  node.change_membership([0, 1, 2], false).await?;
  log_index += 2;

  for i in &[0, 1, 2] {
    router
      .wait(i, timeout())
      .metrics(
        |x| x.last_log_index >= Some(log_index),
        "0,1,2 recv 2 change-membership logs",
      )
      .await?;
  }

  router
    .wait(&0, timeout())
    .metrics(
      |x| x.replication.as_ref().map(|y| y.contains_key(&4)) == Some(false),
      "stopped replication to node 4",
    )
    .await?;

  router.set_network_error(4, false);

  router
    .wait(&4, timeout())
    .metrics(
      |x| {
        x.last_log_index == Some(node4_log_index)
          && (x.state == ServerState::Candidate || x.state == ServerState::Follower)
      },
      "node 4 stopped recv log and start to elect",
    )
    .await?;

  Ok(())
}

/// 未初始化节点上的成员变更请求直接被拒绝
#[compio::test]
async fn test_change_membership_on_uninitialized_node() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  router.new_raft_node(0).await;

  let n0 = router.get_raft_handle(&0)?;
  let err = n0.add_learner(0, (), false).await.unwrap_err();
  assert_eq!(
    RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader::empty())),
    err
  );

  Ok(())
}

/// 将已同步大量日志的 Learner 提升为 Voter 时不会破坏单调递增保证 (Issue 584)
#[compio::test]
async fn test_replication_state_not_reverted_when_adding_learner_as_voter() -> Result<()> {
  let config = Arc::new(
    Config {
      max_in_snapshot_log_to_keep: 2000,
      enable_tick: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router.new_cluster(btreeset! {0}, btreeset! {1}).await?;

  let n = 50u64;
  router
    .client_request_many(0, "foo", (n - log_index) as usize)
    .await?;
  log_index = n;

  router
    .wait(&1, timeout())
    .applied_index(Some(log_index), "replicate all logs to learner")
    .await?;

  let leader = router.get_raft_handle(&0)?;
  leader.change_membership([0, 1], false).await?;

  Ok(())
}

use std::future::ready;

use fixtures::rpc_request::RpcRequest;
use zenoh_raft::{
  Membership, StepDownPolicy, Vote,
  async_runtime::BoxFuture,
  errors::{
    ChangeMembershipError, NetworkError, RPCError, UncommittedLeaderLog,
    UnsupportedMembershipTransition,
  },
  network::RPCTypes,
  testing::memstore::{ClientRequest, IntoMemClientRequest},
};

/// 移除 Leader 并将其转为 Learner 时，旧 Leader 提交 2 条成员变更日志后不卸任，保持为 Leader (非投票人 Leader)
#[compio::test]
async fn test_remove_leader_and_convert_to_learner() -> Result<()> {
  let config = Arc::new(
    Config {
      election_timeout_min: 800,
      election_timeout_max: 1000,
      enable_elect: false,
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config);
  let mut log_index = router
    .new_cluster(btreeset! {0, 1}, btreeset! {2, 3})
    .await?;

  let old_leader = 0;
  let node = router.get_raft_handle(&old_leader)?;
  node.change_membership([1, 2, 3], true).await?;
  log_index += 2;

  router
    .wait(&old_leader, timeout())
    .applied_index(Some(log_index), "old leader commits 2 membership logs")
    .await?;

  TypeConfig::sleep(Duration::from_millis(500)).await;

  router
    .wait(&0, timeout())
    .state(ServerState::Leader, "old leader stays as non-voter leader")
    .await?;

  Ok(())
}

/// 移除 Leader 时自动通过 leadership-transfer 移交领导权
#[compio::test]
async fn test_remove_leader_auto_transfer() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      enable_elect: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config);
  let _log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  let node = router.get_raft_handle(&0)?;
  node.change_membership([1, 2], false).await?;

  router
    .wait(&1, timeout())
    .metrics(
      |x| matches!(x.current_leader, Some(1) | Some(2)) && x.current_term >= 2,
      "a new leader is established by transfer-leader",
    )
    .await?;

  let metrics = router
    .wait(&0, timeout())
    .state(ServerState::Learner, "removed leader steps down")
    .await?;
  assert_eq!(metrics.current_term, 1);

  Ok(())
}

/// 禁用自动 step down 时，移除的 Leader 保持领导地位，直到手动 refresh_server_state
#[compio::test]
async fn test_remove_leader_manual_step_down() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      enable_elect: false,
      removed_leader_step_down: StepDownPolicy::Never,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config);
  let _log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  let node = router.get_raft_handle(&0)?;
  node.change_membership([1, 2], false).await?;

  let res = router
    .wait(&0, Some(Duration::from_millis(500)))
    .metrics(
      |x| x.state != ServerState::Leader,
      "the removed leader leaves Leader state",
    )
    .await;
  assert!(res.is_err());

  node.trigger().refresh_server_state(None, None).await?;
  router
    .wait(&0, timeout())
    .state(ServerState::Learner, "removed leader reverts to learner")
    .await?;

  Ok(())
}

/// 使用带防护条件的 refresh_server_state 卸任移除的 Leader
#[compio::test]
async fn test_remove_leader_fenced_step_down() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      enable_elect: false,
      removed_leader_step_down: StepDownPolicy::Never,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config);
  let mut log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  let node = router.get_raft_handle(&0)?;
  node.change_membership([1, 2], false).await?;
  log_index += 2;

  let metrics = router
    .wait(&0, timeout())
    .applied_index(Some(log_index), "applied membership logs")
    .await?;
  let vote = metrics.vote;
  let membership_log_id = (*metrics.membership_config.log_id()).unwrap();

  let mismatching_vote = Vote::new(100, 100);
  node
    .trigger()
    .refresh_server_state(Some(mismatching_vote), None)
    .await?;
  let res = router
    .wait(&0, Some(Duration::from_millis(500)))
    .metrics(|x| x.state != ServerState::Leader, "leaves Leader")
    .await;
  assert!(res.is_err());

  let mismatching_log_id = fixtures::log_id(1, 0, 1);
  node
    .trigger()
    .refresh_server_state(None, Some(mismatching_log_id))
    .await?;
  let res = router
    .wait(&0, Some(Duration::from_millis(500)))
    .metrics(|x| x.state != ServerState::Leader, "leaves Leader")
    .await;
  assert!(res.is_err());

  node
    .trigger()
    .refresh_server_state(Some(vote), Some(membership_log_id))
    .await?;
  router
    .wait(&0, timeout())
    .state(ServerState::Learner, "reverts to learner")
    .await?;

  Ok(())
}

/// 移除 Leader 并切换到仅节点 2，访问新集群不应返回指向已移除 Leader 的重定向
#[compio::test]
async fn test_remove_leader_access_new_cluster() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      enable_elect: false,
      removed_leader_step_down: StepDownPolicy::Never,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config);
  let mut log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  let orig_leader = 0;
  let node = router.get_raft_handle(&orig_leader)?;
  node.change_membership([2], false).await?;
  log_index += 2;

  router
    .wait(&2, timeout())
    .metrics(|x| x.last_log_index == Some(log_index), "node 2 got logs")
    .await?;

  router
    .wait(&orig_leader, timeout())
    .applied_index(Some(log_index), "old leader committed")
    .await?;

  let res = router
    .send_client_request(2, ClientRequest::make_request("foo", 1))
    .await;
  match res {
    Ok(_) => panic!("expected error"),
    Err(cli_err) => match cli_err.api_error().unwrap() {
      ClientWriteError::ForwardToLeader(fwd) => {
        assert!(fwd.leader_id.is_none());
        assert!(fwd.leader_node.is_none());
      }
      _ => panic!("expected ForwardToLeader"),
    },
  }

  let n2 = router.get_raft_handle(&2)?;
  n2.runtime_config().elect(true);
  n2.wait(timeout())
    .state(ServerState::Leader, "n2 elects")
    .await?;
  log_index += 1;

  router
    .send_client_request(2, ClientRequest::make_request("foo", 1))
    .await?;
  log_index += 1;

  n2.wait(timeout())
    .applied_index(Some(log_index), "n2 handles write")
    .await?;

  Ok(())
}

/// 单个投票者的增删每次各写入一条日志
#[compio::test]
async fn test_add_and_remove_one_voter_write_one_entry_each() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_tick: false,
      enable_heartbeat: false,
      enable_elect: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config);
  let mut log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {3})
    .await?;
  let leader = router.get_raft_handle(&0)?;

  let proposed = Membership::new_with_defaults(vec![btreeset! {0, 1, 2, 3}], []);
  let resp = leader
    .append_membership(proposed.clone(), EntryPayload::Blank, [])
    .await?;
  log_index += 1;

  assert_eq!(log_index, resp.log_id.index);
  assert_eq!(Some(proposed), resp.membership);

  for node_id in [0, 1, 2, 3] {
    router
      .wait(&node_id, timeout())
      .applied_index(Some(log_index), "voter 3 added")
      .await?;
  }

  let proposed = Membership::new_with_defaults(vec![btreeset! {0, 1, 2}], []);
  let resp = leader
    .append_membership(proposed.clone(), EntryPayload::Blank, [])
    .await?;
  log_index += 1;

  assert_eq!(log_index, resp.log_id.index);
  assert_eq!(Some(proposed), resp.membership);

  for node_id in [0, 1, 2] {
    router
      .wait(&node_id, timeout())
      .applied_index(Some(log_index), "voter 3 removed")
      .await?;
  }

  Ok(())
}

/// 不支持的直接成员转换不写入日志并返回错误
#[compio::test]
async fn test_unsupported_transition_writes_no_log() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_tick: false,
      enable_heartbeat: false,
      enable_elect: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config);
  let log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {3})
    .await?;
  let leader = router.get_raft_handle(&0)?;

  let membership_before = leader.metrics().borrow_watched().membership_config.clone();

  let proposed = Membership::new_with_defaults(vec![btreeset! {1, 2, 3}], []);
  let err = leader
    .append_membership(proposed, EntryPayload::Blank, [])
    .await
    .unwrap_err();

  let transition = UnsupportedMembershipTransition {
    previous: vec![btreeset! {0, 1, 2}],
    proposed: vec![btreeset! {1, 2, 3}],
  };
  let want = ClientWriteError::ChangeMembershipError(
    ChangeMembershipError::UnsupportedMembershipTransition(transition),
  );
  assert_eq!(RaftError::APIError(want), err);

  let metrics = leader.metrics().borrow_watched().clone();
  assert_eq!(Some(log_index), metrics.last_log_index);
  assert_eq!(membership_before, metrics.membership_config);

  Ok(())
}

/// 用户构造的 Joint 配置每次各用一条日志进入与退出
#[compio::test]
async fn test_joint_membership_is_entered_and_left_in_one_entry_each() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_tick: false,
      enable_heartbeat: false,
      enable_elect: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config);
  let mut log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {3, 4})
    .await?;
  let leader = router.get_raft_handle(&0)?;

  let configs = vec![
    btreeset! {0, 1, 2},
    btreeset! {2, 3, 4},
    btreeset! {0, 3, 4},
  ];
  let proposed = Membership::new_with_defaults(configs, []);
  let resp = leader
    .append_membership(proposed.clone(), EntryPayload::Blank, [])
    .await?;
  log_index += 1;

  assert_eq!(log_index, resp.log_id.index);
  assert_eq!(Some(proposed), resp.membership);

  for node_id in [0, 1, 2, 3, 4] {
    router
      .wait(&node_id, timeout())
      .applied_index(Some(log_index), "joint config applied")
      .await?;
  }

  let proposed = Membership::new_with_defaults(vec![btreeset! {0, 1, 2}], []);
  let resp = leader
    .append_membership(proposed.clone(), EntryPayload::Blank, [])
    .await?;
  log_index += 1;

  assert_eq!(log_index, resp.log_id.index);
  assert_eq!(Some(proposed), resp.membership);

  for node_id in [0, 1, 2] {
    router
      .wait(&node_id, timeout())
      .applied_index(Some(log_index), "uniform config applied")
      .await?;
  }

  Ok(())
}

/// LastMembershipLogId 前置条件保护 append_membership
#[compio::test]
async fn test_membership_log_id_precondition_guards_the_append() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_tick: false,
      enable_heartbeat: false,
      enable_elect: false,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config);
  let mut log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {3})
    .await?;
  let leader = router.get_raft_handle(&0)?;

  let observed_log_id = *leader.metrics().borrow_watched().membership_config.log_id();

  let precondition = Precondition::LastMembershipLogId {
    last_membership_log_id: observed_log_id,
  };
  let proposed = Membership::new_with_defaults(vec![btreeset! {0, 1, 2, 3}], []);
  let resp = leader
    .append_membership(proposed, EntryPayload::Blank, [precondition])
    .await?;
  log_index += 1;
  assert_eq!(log_index, resp.log_id.index);

  let current_log_id = *leader.metrics().borrow_watched().membership_config.log_id();

  let precondition = Precondition::LastMembershipLogId {
    last_membership_log_id: observed_log_id,
  };
  let proposed = Membership::new_with_defaults(vec![btreeset! {0, 1, 2}], []);
  let err = leader
    .append_membership(proposed, EntryPayload::Blank, [precondition])
    .await
    .unwrap_err();

  let want = PreconditionFailed::LastMembershipLogIdMismatch {
    expected: observed_log_id,
    actual: current_log_id,
  };
  assert_eq!(
    RaftError::APIError(ClientWriteError::PreconditionFailed(want)),
    err
  );

  let metrics = leader.metrics().borrow_watched().clone();
  assert_eq!(Some(log_index), metrics.last_log_index);
  assert_eq!(current_log_id, *metrics.membership_config.log_id());

  Ok(())
}

/// 未提交的 Leader 空日志阻止 append_membership
#[compio::test]
async fn test_uncommitted_leader_log_blocks_the_append() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_elect: false,
      heartbeat_interval: 50,
      election_timeout_min: 500,
      election_timeout_max: 501,
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config);
  let _log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {3})
    .await?;
  let old_leader = router.get_raft_handle(&0)?;
  let new_leader = router.get_raft_handle(&1)?;

  router
    .set_rpc_pre_hook(RPCTypes::AppendEntries, |_router, req, from, to| {
      let mut carries_entries = false;
      if let RpcRequest::AppendEntries(append) = &req {
        carries_entries = !append.entries.is_empty();
      }

      let res = if carries_entries {
        let msg = format!("blocked: {from}->{to} append-entries with entries");
        Err(RPCError::Network(NetworkError::<TypeConfig>::from_string(
          msg,
        )))
      } else {
        Ok(())
      };
      Box::pin(ready(res)) as BoxFuture<_>
    })
    .await;

  old_leader.trigger().transfer_leader(1).await?;
  new_leader
    .wait(timeout())
    .metrics(
      |m| m.state == ServerState::Leader && m.last_quorum_acked.is_some(),
      "node 1 leads and a quorum keeps acking it",
    )
    .await?;

  let blocked_metrics = new_leader.metrics().borrow_watched().clone();
  let noop_log_id = {
    let leader_id = LeaderIdOf::<TypeConfig>::new_committed(blocked_metrics.current_term, 1);
    LogIdOf::<TypeConfig>::new(leader_id, blocked_metrics.last_log_index.unwrap())
  };
  let proposed = Membership::new_with_defaults(vec![btreeset! {0, 1, 2, 3}], []);

  let err = new_leader
    .append_membership(proposed.clone(), EntryPayload::Blank, [])
    .await
    .unwrap_err();

  let uncommitted = UncommittedLeaderLog {
    committed: None,
    leader_log_id: noop_log_id,
  };
  let want = ClientWriteError::ChangeMembershipError(ChangeMembershipError::UncommittedLeaderLog(
    uncommitted,
  ));
  assert_eq!(RaftError::APIError(want), err);

  let stale = Precondition::LastMembershipLogId {
    last_membership_log_id: None,
  };
  let err = new_leader
    .append_membership(proposed.clone(), EntryPayload::Blank, [stale])
    .await
    .unwrap_err();

  let want = PreconditionFailed::LastMembershipLogIdMismatch {
    expected: None,
    actual: *blocked_metrics.membership_config.log_id(),
  };
  assert_eq!(
    RaftError::APIError(ClientWriteError::PreconditionFailed(want)),
    err
  );

  router.rpc_pre_hook(RPCTypes::AppendEntries, None).await;
  new_leader
    .wait(timeout())
    .applied_index(Some(noop_log_id.index()), "blank log committed")
    .await?;

  let resp = new_leader
    .append_membership(proposed.clone(), EntryPayload::Blank, [])
    .await?;

  assert_eq!(noop_log_id.index() + 1, resp.log_id.index);
  assert_eq!(Some(proposed), resp.membership);

  Ok(())
}
