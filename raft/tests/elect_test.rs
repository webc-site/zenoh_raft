//! 选举与投票测试套件

mod fixtures;

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use fixtures::{Direction::NetSend, RaftRouter, log_id, timeout};
use futures_util::future::ready;
use maplit::btreeset;
use zenoh_raft::{
  Config, RPCTypes, ServerState, Vote,
  errors::{NetworkError, RPCError},
  raft::VoteRequest,
  storage::{RaftLogStorage, RaftLogStorageExt},
  testing::{blank_ent, membership_ent, memstore::TypeConfig},
  type_config::TypeConfigExt,
};

/// 投票请求中的 last_log 必须大于等于本地节点最后一条日志
#[compio::test]
async fn test_elect_compare_last_log() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());

  let (mut sto0, sm0) = router.new_store();
  let (mut sto1, sm1) = router.new_store();

  {
    sto0.save_vote(&Vote::new(10, 0)).await?;
    sto0
      .blocking_append([
        blank_ent::<TypeConfig>(0, 0, 0),
        membership_ent::<TypeConfig>(2, 0, 1, vec![btreeset! {0,1}]),
      ])
      .await?;
  }

  {
    sto1.save_vote(&Vote::new(10, 0)).await?;
    sto1
      .blocking_append([
        blank_ent::<TypeConfig>(0, 0, 0),
        membership_ent::<TypeConfig>(1, 0, 1, vec![btreeset! {0,1}]),
        blank_ent::<TypeConfig>(1, 0, 2),
      ])
      .await?;
  }

  router
    .new_raft_node_with_sto(0, sto0.clone(), sm0.clone())
    .await;
  router
    .new_raft_node_with_sto(1, sto1.clone(), sm1.clone())
    .await;

  router
    .wait(&0, timeout())
    .state(ServerState::Leader, "only node 0 becomes leader")
    .await?;

  Ok(())
}

/// 具有更高 Term 的节点能够从当前 Leader 夺取领导权
#[compio::test]
async fn test_elect_seize_leadership() -> Result<()> {
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

  let n0 = router.get_raft_handle(&0)?;
  n0.wait(timeout())
    .state(ServerState::Leader, "node 0 becomes leader")
    .await?;

  TypeConfig::sleep(Duration::from_millis(500)).await;

  {
    let n1 = router.get_raft_handle(&1)?;
    n1.trigger().elect(false).await?;
    n1.wait(timeout())
      .state(ServerState::Leader, "node 1 becomes leader")
      .await?;
  }

  Ok(())
}

/// 当前已经是 Leader 的节点触发选举将被忽略（防止自相残杀导致可用性中断）
#[compio::test]
async fn test_elect_while_leader() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: true,
      enable_elect: true,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

  let n0 = router.get_raft_handle(&0)?;
  n0.wait(timeout())
    .state(ServerState::Leader, "node 0 is the initial leader")
    .await?;

  let term_before = n0.metrics().borrow_watched().current_term;

  n0.trigger().elect(false).await?;
  n0.trigger().elect(true).await?;

  TypeConfig::sleep(Duration::from_millis(500)).await;

  {
    let m = n0.metrics().borrow_watched().clone();
    assert_eq!(ServerState::Leader, m.state, "node 0 remains a Leader");
    assert_eq!(term_before, m.current_term, "the term is not inflated");
    assert_eq!(Some(0), m.current_leader, "node 0 remains the leader");
  }

  for id in [1, 2] {
    router
      .get_raft_handle(&id)?
      .wait(timeout())
      .metrics(
        |m| m.current_leader == Some(0),
        "follower still follows node 0",
      )
      .await?;
  }

  router.client_request_many(0, "foo", 1).await?;

  Ok(())
}

/// 启用 Pre-Vote 后，孤立的 Follower 不会无限膨胀其 Term
#[compio::test]
async fn test_pre_vote_prevents_term_inflation() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_pre_vote: Some(true),
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

  let n0 = router.get_raft_handle(&0)?;
  let n1 = router.get_raft_handle(&1)?;
  n0.wait(timeout())
    .state(ServerState::Leader, "node 0 is leader")
    .await?;
  n1.wait(timeout())
    .state(ServerState::Follower, "node 1 is follower")
    .await?;

  let follower_term_before = n1.metrics().borrow_watched().current_term;
  let leader_term_before = n0.metrics().borrow_watched().current_term;

  router.set_network_error(1, true);
  TypeConfig::sleep(Duration::from_millis(500)).await;

  let follower_term_after = n1.metrics().borrow_watched().current_term;
  assert_eq!(
    follower_term_before, follower_term_after,
    "Pre-Vote must keep the isolated follower from bumping its term"
  );

  n0.wait(timeout())
    .state(ServerState::Leader, "node 0 still leader")
    .await?;
  assert_eq!(
    leader_term_before,
    n0.metrics().borrow_watched().current_term
  );

  router.set_network_error(1, false);
  n1.wait(timeout())
    .state(ServerState::Follower, "node 1 rejoins as follower")
    .await?;

  Ok(())
}

/// 在 Leader 失联后，集群能通过 Pre-Vote 选出新的 Leader
#[compio::test]
async fn test_pre_vote_elects_new_leader_after_leader_isolated() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_pre_vote: Some(true),
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

  let n0 = router.get_raft_handle(&0)?;
  n0.wait(timeout())
    .state(ServerState::Leader, "node 0 is leader")
    .await?;

  router.set_network_error(0, true);

  for id in [1, 2] {
    router
      .wait(&id, Some(Duration::from_secs(5)))
      .metrics(
        |m| m.current_leader == Some(1) || m.current_leader == Some(2),
        "a new leader is elected via Pre-Vote after the old leader is isolated",
      )
      .await?;
  }

  Ok(())
}

/// 包含更高 Vote 的 Pre-Vote 拒绝响应能让发起方同步至该更高 Vote
#[compio::test]
async fn test_pre_vote_rejection_with_higher_vote_catches_up_requester() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      enable_elect: false,
      enable_pre_vote: Some(true),
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

  let n1 = router.get_raft_handle(&1)?;
  let n2 = router.get_raft_handle(&2)?;

  router.set_network_error(0, true);
  TypeConfig::sleep(Duration::from_millis(config.election_timeout_max * 2)).await;

  let resp = n2
    .vote(VoteRequest {
      vote: Vote::new(10, 2),
      last_log_id: Some(log_id(1, 0, log_index)),
      leadership_transfer: true,
      is_pre_vote: false,
    })
    .await?;
  assert!(resp.vote_granted);
  router
    .wait(&2, timeout())
    .vote(Vote::new(10, 2), "node 2 has a higher vote")
    .await?;

  n1.trigger().elect(true).await?;
  router
    .wait(&1, Some(Duration::from_millis(1_000)))
    .vote(
      Vote::new(10, 2),
      "node 1 catches up from a higher Pre-Vote rejection",
    )
    .await?;

  n1.trigger().elect(true).await?;
  n1.wait(timeout())
    .state(ServerState::Leader, "node 1 becomes leader")
    .await?;

  Ok(())
}

/// 租约过期 Follower 发起的 Pre-Vote 会被具有活跃 Quorum 租约的 Leader 拒绝
#[compio::test]
async fn test_manual_pre_vote_from_lease_expired_follower() -> Result<()> {
  const ELECTION_TIMEOUT_MAX: u64 = 151;
  const LEADER_LEASE: Duration = Duration::from_millis(ELECTION_TIMEOUT_MAX);
  const ELECTION_OBSERVATION: Duration = Duration::from_millis(ELECTION_TIMEOUT_MAX * 2);

  let config = Arc::new(
    Config {
      election_timeout_min: 150,
      election_timeout_max: ELECTION_TIMEOUT_MAX,
      enable_elect: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config);
  let _log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  let n0 = router.get_raft_handle(&0)?;
  let n1 = router.get_raft_handle(&1)?;

  let before = n1
    .wait(timeout())
    .metrics(
      |m| m.state == ServerState::Follower && m.current_leader == Some(0),
      "node 1 follows node 0",
    )
    .await?;

  router
    .set_rpc_pre_hook(RPCTypes::AppendEntries, move |_router, _req, f, t| {
      let res = if f == 0 && t == 1 {
        let err = NetworkError::<TypeConfig>::from_string(format!("blocked: {f}->{t}"));
        Err(RPCError::Network(err))
      } else {
        Ok(())
      };
      Box::pin(ready(res))
    })
    .await;

  for _ in 0..200 {
    let last_modified = router
      .with_raft_state(1, |state| state.vote_last_modified())
      .await?;
    let now = TypeConfig::now();
    let expired = match last_modified {
      Some(last_modified) => now > last_modified + LEADER_LEASE,
      None => true,
    };
    if expired {
      break;
    }
    TypeConfig::sleep(Duration::from_millis(20)).await;
  }

  let heartbeat_at = TypeConfig::now();
  n0.trigger().heartbeat().await?;
  n0.wait(timeout())
    .metrics(
      |m| {
        m.last_quorum_acked
          .is_some_and(|acked| acked.into_inner() >= heartbeat_at)
      },
      "node 0's quorum-ack lease is fresh after heartbeat",
    )
    .await?;

  let vote_rpcs_before = router
    .get_rpc_count()
    .get(&RPCTypes::Vote)
    .copied()
    .unwrap_or(0);
  n1.trigger().elect(true).await?;

  TypeConfig::sleep(ELECTION_OBSERVATION).await;

  let vote_rpcs_after = router
    .get_rpc_count()
    .get(&RPCTypes::Vote)
    .copied()
    .unwrap_or(0);
  assert!(
    vote_rpcs_after > vote_rpcs_before,
    "node 1's lease is expired, so pre_elect sent round out"
  );

  let n1_after = n1.metrics().borrow_watched().clone();
  assert_eq!(before.current_term, n1_after.current_term);
  assert_eq!(ServerState::Follower, n1_after.state);

  let n0_after = n0.metrics().borrow_watched().clone();
  assert_eq!(before.current_term, n0_after.current_term);
  assert_eq!(ServerState::Leader, n0_after.state);

  Ok(())
}

/// 健康 Follower 租约未过期时不会发起 Pre-Vote 选举
#[compio::test]
async fn test_manual_pre_vote_from_healthy_follower_with_stale_peer() -> Result<()> {
  const ELECTION_TIMEOUT_MAX: u64 = 151;
  const LEADER_LEASE: Duration = Duration::from_millis(ELECTION_TIMEOUT_MAX);
  const ELECTION_OBSERVATION: Duration = Duration::from_millis(ELECTION_TIMEOUT_MAX * 2);

  let config = Arc::new(
    Config {
      election_timeout_min: 150,
      election_timeout_max: ELECTION_TIMEOUT_MAX,
      enable_elect: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config);
  let _log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  let n0 = router.get_raft_handle(&0)?;
  let n1 = router.get_raft_handle(&1)?;

  let before = n1
    .wait(timeout())
    .metrics(
      |m| m.state == ServerState::Follower && m.current_leader == Some(0),
      "node 1 follows node 0",
    )
    .await?;

  router
    .set_rpc_pre_hook(RPCTypes::AppendEntries, move |_router, _req, f, t| {
      let res = if f == 0 && t == 2 {
        let err = NetworkError::<TypeConfig>::from_string(format!("blocked: {f}->{t}"));
        Err(RPCError::Network(err))
      } else {
        Ok(())
      };
      Box::pin(ready(res))
    })
    .await;

  for _ in 0..200 {
    let last_modified = router
      .with_raft_state(2, |state| state.vote_last_modified())
      .await?;
    let now = TypeConfig::now();
    let expired = match last_modified {
      Some(last_modified) => now > last_modified + LEADER_LEASE,
      None => true,
    };
    if expired {
      break;
    }
    TypeConfig::sleep(Duration::from_millis(20)).await;
  }

  let heartbeat_at = TypeConfig::now();
  n0.trigger().heartbeat().await?;
  n0.wait(timeout())
    .metrics(
      |m| {
        m.last_quorum_acked
          .is_some_and(|acked| acked.into_inner() >= heartbeat_at)
      },
      "node 0's quorum-ack lease is fresh after heartbeat",
    )
    .await?;

  let vote_rpcs_before = router
    .get_rpc_count()
    .get(&RPCTypes::Vote)
    .copied()
    .unwrap_or(0);
  n1.trigger().elect(true).await?;

  TypeConfig::sleep(ELECTION_OBSERVATION).await;

  let vote_rpcs_after = router
    .get_rpc_count()
    .get(&RPCTypes::Vote)
    .copied()
    .unwrap_or(0);
  assert_eq!(
    vote_rpcs_before, vote_rpcs_after,
    "node 1 lease is valid, so pre_elect refused to start"
  );

  let n1_after = n1.metrics().borrow_watched().clone();
  assert_eq!(before.current_term, n1_after.current_term);
  assert_eq!(ServerState::Follower, n1_after.state);

  let n0_after = n0.metrics().borrow_watched().clone();
  assert_eq!(before.current_term, n0_after.current_term);
  assert_eq!(ServerState::Leader, n0_after.state);

  Ok(())
}

use fixtures::{Direction::NetRecv, rpc_error_type::RpcErrorType};

/// 即使所有出站 RPC 失败，只要仍能接收心跳，Follower 就不能主动发起选举
#[compio::test]
async fn test_inbound_heartbeats_prevent_election_when_outbound_rpcs_fail() -> Result<()> {
  let config = Config {
    enable_pre_vote: Some(false),
    election_timeout_min: 150,
    election_timeout_max: 151,
    ..Default::default()
  }
  .validate()?;
  let mut router = RaftRouter::new(Arc::new(config));

  let _log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  let n0 = router.get_raft_handle(&0)?;
  let n1 = router.get_raft_handle(&1)?;

  let before = n1
    .wait(Some(Duration::from_millis(2_000)))
    .metrics(
      |metrics| metrics.state == ServerState::Follower && metrics.current_leader == Some(0),
      "node 1 follows node 0",
    )
    .await?;

  router.set_rpc_failure(1, NetSend, Some(RpcErrorType::NetworkError));

  let heartbeat_at = TypeConfig::now();
  n0.trigger().heartbeat().await?;

  let mut heartbeat_received = false;
  for _ in 0..20 {
    let last_modified = router
      .with_raft_state(1, |state| state.vote_last_modified())
      .await?;
    if last_modified > Some(heartbeat_at) {
      heartbeat_received = true;
      break;
    }
    TypeConfig::sleep(Duration::from_millis(50)).await;
  }
  assert!(
    heartbeat_received,
    "node 1 must receive the forced heartbeat"
  );

  TypeConfig::sleep(Duration::from_millis(300)).await;

  let after = n1.metrics().borrow_watched().clone();
  assert_eq!(before.current_term, after.current_term);
  assert_eq!(before.vote, after.vote);
  assert_eq!(Some(0), after.current_leader);
  assert_eq!(ServerState::Follower, after.state);

  Ok(())
}

/// 当 Follower 无法接收心跳但可以出站 RPC 时，必须发起选举
#[compio::test]
async fn test_missing_inbound_heartbeats_start_election() -> Result<()> {
  let config = Config {
    enable_pre_vote: Some(false),
    election_timeout_min: 150,
    election_timeout_max: 151,
    ..Default::default()
  }
  .validate()?;
  let mut router = RaftRouter::new(Arc::new(config));

  let _log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  let n1 = router.get_raft_handle(&1)?;
  let before = n1
    .wait(Some(Duration::from_millis(2_000)))
    .metrics(
      |metrics| metrics.state == ServerState::Follower && metrics.current_leader == Some(0),
      "node 1 follows node 0",
    )
    .await?;

  router.set_rpc_failure(1, NetRecv, Some(RpcErrorType::NetworkError));

  let campaigned = router
    .wait(&1, Some(Duration::from_millis(2_000)))
    .metrics(
      |metrics| metrics.current_term > before.current_term,
      "node 1 advances its term after leader lease expires",
    )
    .await?;

  assert_eq!(Vote::new(campaigned.current_term, 1), campaigned.vote);
  assert_eq!(ServerState::Candidate, campaigned.state);

  Ok(())
}

/// 禁用 Pre-Vote 时，被隔离的 Follower 会随着超时不断抬高 Term
#[compio::test]
async fn test_without_pre_vote_term_inflates() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_pre_vote: Some(false),
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config);
  router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  let n0 = router.get_raft_handle(&0)?;
  let n1 = router.get_raft_handle(&1)?;
  n0.wait(timeout())
    .state(ServerState::Leader, "node 0 is leader")
    .await?;
  n1.wait(timeout())
    .state(ServerState::Follower, "node 1 is follower")
    .await?;

  let follower_term_before = n1.metrics().borrow_watched().current_term;

  router.set_network_error(1, true);
  TypeConfig::sleep(Duration::from_millis(1500)).await;

  let follower_term_after = n1.metrics().borrow_watched().current_term;
  assert!(follower_term_after > follower_term_before);

  Ok(())
}

/// 完全隔离的节点在返回 Unreachable 时不能算作 Pre-Vote Grant
#[compio::test]
async fn test_pre_vote_unreachable_peer_is_not_a_grant() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_pre_vote: Some(true),
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config);
  router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  let n1 = router.get_raft_handle(&1)?;
  n1.wait(timeout())
    .state(ServerState::Follower, "node 1 is follower")
    .await?;

  let follower_term_before = n1.metrics().borrow_watched().current_term;

  router.set_unreachable(1, true);
  TypeConfig::sleep(Duration::from_millis(1000)).await;

  assert_eq!(
    follower_term_before,
    n1.metrics().borrow_watched().current_term
  );

  Ok(())
}
