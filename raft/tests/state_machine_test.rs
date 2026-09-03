//! 状态机操作与异常恢复测试套件

mod fixtures;

use std::sync::Arc;

use anyhow::Result;
use fixtures::{RaftRouter, log_id, timeout};
use maplit::btreeset;
use zenoh_raft::{
  Config, LogIdOptionExt, Membership,
  alias::StoredMembershipOf,
  storage::RaftStateMachine,
  testing::memstore::{ClientRequest, IntoMemClientRequest, TypeConfig},
};

/// 状态机日志应用完整性测试
#[compio::test]
async fn test_state_machine_apply() -> Result<()> {
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
  for i in 1..=5 {
    n0.client_write(ClientRequest::make_request("sm_key", i))
      .await?;
    log_index += 1;
  }

  for id in [0, 1, 2] {
    router
      .wait(&id, timeout())
      .applied_index(Some(log_index), "all applied")
      .await?;

    let (_sto, sm) = router.get_storage_handle(&id)?;
    let sm_data = sm.get_state_machine().await;
    assert_eq!(
      sm_data.client_status.get("sm_key").map(|s| s.as_str()),
      Some("request-5")
    );
  }

  Ok(())
}

/// 状态机应用成员变更配置测试
#[compio::test]
async fn test_state_machine_apply_membership() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router.new_cluster(btreeset! {0}, btreeset! {}).await?;

  for i in 0..=0 {
    let (_sto, mut sm) = router.get_storage_handle(&i)?;
    assert_eq!(
      StoredMembershipOf::<TypeConfig>::new(
        Some(log_id(0, 0, 0)),
        Membership::new_with_defaults(vec![btreeset! {0}], [])
      ),
      sm.applied_state().await?.1
    );
  }

  router.new_raft_node(1).await;
  router.new_raft_node(2).await;
  router.new_raft_node(3).await;
  router.new_raft_node(4).await;

  router.add_learner(0, 1).await?;
  router.add_learner(0, 2).await?;
  router.add_learner(0, 3).await?;
  router.add_learner(0, 4).await?;
  log_index += 4;
  router
    .wait(&0, None)
    .applied_index(Some(log_index), "add learner")
    .await?;

  let node = router.get_raft_handle(&0)?;
  node.change_membership([0, 1, 2], false).await?;
  log_index += 2;

  for i in 0..5 {
    router
      .wait(&i, None)
      .metrics(
        |x| x.last_applied.index() >= Some(log_index - 1),
        "joint log applied",
      )
      .await?;
  }

  for i in 0..3 {
    router
      .wait(&i, None)
      .metrics(
        |x| x.last_applied.index() == Some(log_index),
        "uniform log applied",
      )
      .await?;

    let (_sto, mut sm) = router.get_storage_handle(&i)?;
    let (_, last_membership) = sm.applied_state().await?;
    assert_eq!(
      StoredMembershipOf::<TypeConfig>::new(
        Some(log_id(1, 0, log_index)),
        Membership::new_with_defaults(vec![btreeset! {0, 1, 2}], btreeset! {3, 4})
      ),
      last_membership
    );
  }

  Ok(())
}
