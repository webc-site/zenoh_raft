//! 日志复制与网络流测试套件

mod fixtures;

use std::{
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  time::Duration,
};

use anyhow::Result;
use fixtures::{RaftRouter, timeout};
use futures_util::future::ready;
use maplit::btreeset;
use zenoh_raft::{
  Config, RPCTypes,
  errors::{RPCError, Unreachable},
  testing::memstore::{ClientRequest, IntoMemClientRequest, TypeConfig},
  type_config::TypeConfigExt,
};

/// 日志复制流与多节点同步测试
#[compio::test]
async fn test_replication_stream() -> Result<()> {
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
  for i in 1..=10 {
    n0.client_write(ClientRequest::make_request("rep", i))
      .await?;
    log_index += 1;
  }

  for id in [0, 1, 2] {
    router
      .wait(&id, timeout())
      .applied_index(Some(log_index), "all nodes replicate 10 entries")
      .await?;
  }

  Ok(())
}

/// 复制到不可达节点不会阻塞集群推进
#[compio::test]
async fn test_replicate_to_unreachable_node() -> Result<()> {
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

  router.set_unreachable(2, true);

  let n0 = router.get_raft_handle(&0)?;
  n0.client_write(ClientRequest::make_request("rep", 100))
    .await?;
  log_index += 1;

  router
    .wait(&0, timeout())
    .applied_index(Some(log_index), "leader applied")
    .await?;
  router
    .wait(&1, timeout())
    .applied_index(Some(log_index), "follower 1 applied")
    .await?;

  Ok(())
}

/// 复制支持 PartialSuccess 部分成功响应
#[compio::test]
async fn test_append_entries_partial_success() -> Result<()> {
  let config = Arc::new(
    Config {
      ..Default::default()
    }
    .validate()?,
  );
  let mut router = RaftRouter::new(config.clone());

  let mut log_index = router.new_cluster(btreeset! {0,1}, btreeset! {}).await?;

  let quota = 2;
  let n = 5;

  router.set_append_entries_quota(Some(quota));

  let r = router.clone();
  TypeConfig::spawn(async move {
    let _ = r.client_request_many(0, "0", n as usize).await;
  });
  log_index += quota;

  router
    .wait(&0, timeout())
    .applied_index(Some(log_index), format!("{quota} writes"))
    .await?;

  log_index += 1;
  let res = router
    .wait(&0, timeout())
    .applied_index(
      Some(log_index),
      format!("log index {log_index} limited by quota"),
    )
    .await;
  assert!(res.is_err(), "log index {log_index} is limited by quota");

  router.set_append_entries_quota(Some(1));
  router
    .wait(&0, timeout())
    .applied_index(Some(log_index), format!("log index {log_index} replicated"))
    .await?;

  Ok(())
}

/// 当 limited_get_log_entries 返回空时优雅降级并继续复制
#[compio::test]
async fn test_empty_log_entries() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  log_index += router.client_request_many(0, "foo", 5).await?;
  router
    .wait(&1, timeout())
    .applied_index(Some(log_index), "node 1 replicated")
    .await?;
  router
    .wait(&2, timeout())
    .applied_index(Some(log_index), "node 2 replicated")
    .await?;

  router.set_return_empty_limited_get(&0, true)?;

  {
    let raft = router.get_raft_handle(&0)?;
    raft
      .client_write_ff(ClientRequest::make_request("bar", 1), None)
      .await?;

    TypeConfig::sleep(Duration::from_millis(800)).await;

    raft.with_raft_state(|_| ()).await?;
  }

  router.set_return_empty_limited_get(&0, false)?;
  log_index += 1;

  router
    .wait(&1, timeout())
    .applied_index(Some(log_index), "node 1 replicated")
    .await?;
  router
    .wait(&2, timeout())
    .applied_index(Some(log_index), "node 2 replicated")
    .await?;

  Ok(())
}

/// 遇到不可达错误时复制退避
#[compio::test]
async fn test_append_entries_backoff() -> Result<()> {
  let config = Arc::new(
    Config {
      heartbeat_interval: 5_000,
      election_timeout_min: 10_000,
      election_timeout_max: 10_001,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

  let counts0 = router.get_rpc_count();
  let n = 10u64;

  router
    .set_rpc_pre_hook(RPCTypes::AppendEntries, |_router, _req, _id, target| {
      let res = if target == 2 {
        Err(RPCError::Unreachable(
          Unreachable::<TypeConfig>::from_string("unreachable"),
        ))
      } else {
        Ok(())
      };
      Box::pin(ready(res))
    })
    .await;

  router.client_request_many(0, "0", n as usize).await?;
  log_index += n;

  router
    .wait(&0, timeout())
    .applied_index(Some(log_index), format!("{n} writes"))
    .await?;

  let counts1 = router.get_rpc_count();

  let c0 = *counts0.get(&RPCTypes::AppendEntries).unwrap_or(&0);
  let c1 = *counts1.get(&RPCTypes::AppendEntries).unwrap_or(&0);

  assert!(
    n < c1 - c0 && c1 - c0 < n * 4,
    "append-entries should backoff when a Unreachable error is found"
  );

  Ok(())
}

/// 复制流成功后清除退避状态
#[compio::test]
async fn test_backoff_cleared_after_success() -> Result<()> {
  let config = Arc::new(
    Config {
      heartbeat_interval: 5_000,
      election_timeout_min: 10_000,
      election_timeout_max: 10_001,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  router.new_cluster(btreeset! {0, 1}, btreeset! {}).await?;

  let failures_remaining = Arc::new(AtomicU64::new(2));
  {
    let failures_remaining = failures_remaining.clone();
    router
      .set_rpc_pre_hook(
        RPCTypes::AppendEntries,
        move |_router, _req, _id, target| {
          let should_fail = target == 1 && failures_remaining.load(Ordering::SeqCst) > 0;
          let res = if should_fail {
            failures_remaining.fetch_sub(1, Ordering::SeqCst);
            Err(RPCError::Unreachable(
              Unreachable::<TypeConfig>::from_string("transient"),
            ))
          } else {
            Ok(())
          };
          Box::pin(ready(res))
        },
      )
      .await;
  }

  router.client_request_many(0, "trigger", 1).await?;
  TypeConfig::sleep(Duration::from_millis(300)).await;

  assert_eq!(
    failures_remaining.load(Ordering::SeqCst),
    0,
    "precondition: both injected errors must have been consumed"
  );

  let n: u64 = 10;
  let start = TypeConfig::now();
  router.client_request_many(0, "after", n as usize).await?;
  let elapsed = TypeConfig::now() - start;

  assert!(
    elapsed < Duration::from_millis(1_000),
    "{n} post-recovery writes took {elapsed:?}; backoff must be cleared"
  );

  Ok(())
}

/// 重启不可达节点后重新加入集群并追赶日志
#[compio::test]
async fn test_append_entries_backoff_rejoin() -> Result<()> {
  let config = Arc::new(
    Config {
      election_timeout_min: 100,
      election_timeout_max: 200,
      enable_elect: false,
      enable_heartbeat: true,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

  let n = 10;
  router.set_unreachable(0, true);

  let (_, ls0, sm0) = router.remove_node(0).unwrap();
  let n1 = router.get_raft_handle(&1)?;

  {
    TypeConfig::sleep(Duration::from_millis(1_000)).await;
    n1.trigger().elect(false).await?;
    n1.wait(timeout())
      .leader_with_quorum_acked(None, "node-1 elects and establishes its leader lease")
      .await?;
  }

  {
    log_index += router.client_request_many(1, "1", n as usize).await?;
    n1.wait(timeout())
      .applied_index_at_least(Some(log_index), format!("node-1 commit {n} writes"))
      .await?;
  }

  {
    router.new_raft_node_with_sto(0, ls0, sm0).await;
    router.set_unreachable(0, false);

    router
      .wait(&0, timeout())
      .applied_index_at_least(Some(log_index), format!("node-0 commit {n} writes"))
      .await?;
  }

  Ok(())
}

/// 心跳不会在 Follower 落后时引起日志回退 Panic (Issue 1500)
#[compio::test]
async fn test_issue_1500_heartbeat_cause_reversion_panic() -> Result<()> {
  use fixtures::rpc_request::RpcRequest;

  let config = Arc::new(
    Config {
      enable_heartbeat: true,
      allow_log_reversion: Some(false),
      election_timeout_min: 800,
      election_timeout_max: 801,
      heartbeat_interval: 100,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

  router
    .set_rpc_post_hook(
      RPCTypes::AppendEntries,
      |_router, req, _resp, _id, target| {
        let sleep_ms = if target == 2 {
          match req {
            RpcRequest::AppendEntries(append) if append.entries.is_empty() => 10,
            _ => 0,
          }
        } else {
          0
        };

        let fu = async move {
          if sleep_ms > 0 {
            TypeConfig::sleep(Duration::from_millis(sleep_ms)).await;
          }
          Ok(())
        };

        Box::pin(fu)
      },
    )
    .await;

  {
    log_index += router.client_request_many(0, "foo", 50).await?;
    router
      .wait(&0, timeout())
      .applied_index(Some(log_index), "commit all written entries")
      .await?;
  }

  {
    let n0 = router.get_raft_handle(&0)?;
    n0.with_raft_state(|_st| ()).await?;
  }

  Ok(())
}

/// 避免冗余的纯提交 AppendEntries (Issue 2004)
#[compio::test]
async fn test_no_redundant_commit_only_append_entries() -> Result<()> {
  use fixtures::rpc_request::RpcRequest;

  const WRITES: usize = 50;

  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config.clone());
  let mut log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  let entry_less = Arc::new(AtomicU64::new(0));
  let with_entries = Arc::new(AtomicU64::new(0));

  {
    let entry_less = entry_less.clone();
    let with_entries = with_entries.clone();

    router
      .set_rpc_pre_hook(RPCTypes::AppendEntries, move |_router, req, _from, _to| {
        if let RpcRequest::AppendEntries(append) = &req {
          if append.entries.is_empty() {
            entry_less.fetch_add(1, Ordering::Relaxed);
          } else {
            with_entries.fetch_add(1, Ordering::Relaxed);
          }
        }
        Box::pin(async move { Ok(()) })
      })
      .await;
  }

  {
    log_index += router.client_request_many(0, "foo", WRITES).await?;

    router
      .wait(&1, timeout())
      .applied_index(Some(log_index), "node 1 applied")
      .await?;
    router
      .wait(&2, timeout())
      .applied_index(Some(log_index), "node 2 applied")
      .await?;
  }

  let entry_less = entry_less.load(Ordering::Relaxed);
  let with_entries = with_entries.load(Ordering::Relaxed);

  assert!(
    entry_less <= with_entries,
    "expect at most one entry-less AppendEntries per log-carrying one, but got {entry_less} entry-less and {with_entries} with entries"
  );

  Ok(())
}

/// 存储错误停止复制并不发送畸形 AppendEntries (Issue 1795)
#[compio::test]
async fn test_storage_error_stops_replication() -> Result<()> {
  use std::sync::atomic::AtomicBool;

  use fixtures::{log_id, rpc_request::RpcRequest};
  use zenoh_raft::SnapshotPolicy;

  let snapshot_threshold = 20;
  let live_log_index = snapshot_threshold + 11;

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
    .client_request_many(0, "snapshot", (snapshot_threshold - 1 - log_index) as usize)
    .await?;
  log_index = snapshot_threshold - 1;

  router
    .wait(&0, timeout())
    .applied_index(Some(log_index), "snapshot trigger entries")
    .await?;
  router
    .wait(&0, timeout())
    .snapshot(log_id(1, 0, log_index), "leader has built snapshot")
    .await?;
  router
    .wait(&0, timeout())
    .purged(
      Some(log_id(1, 0, log_index)),
      "leader has purged snapshot logs",
    )
    .await?;

  router
    .client_request_many(0, "suffix", (live_log_index - log_index) as usize)
    .await?;
  log_index = live_log_index;
  router
    .wait(&0, timeout())
    .applied_index(Some(log_index), "leader has live suffix")
    .await?;

  let sent_malformed = Arc::new(AtomicBool::new(false));
  {
    let sent_malformed = sent_malformed.clone();
    router
      .set_rpc_pre_hook(RPCTypes::AppendEntries, move |_router, req, _id, target| {
        let malformed = match req {
          RpcRequest::AppendEntries(append) if target == 1 => {
            append.prev_log_id.is_none()
              && append
                .entries
                .first()
                .is_some_and(|entry| entry.log_id.index() > 0)
          }
          _ => false,
        };

        let res = if malformed {
          sent_malformed.store(true, Ordering::SeqCst);
          Err(RPCError::Unreachable(
            Unreachable::<TypeConfig>::from_string("malformed append-entries"),
          ))
        } else {
          Ok(())
        };

        use futures_util::future::ready;
        Box::pin(ready(res))
      })
      .await;
  }

  router.new_raft_node(1).await;
  router.set_fail_next_limited_get(&0, true)?;

  let leader = router.get_raft_handle(&0)?;
  let _ = leader.add_learner(1, (), false).await;

  TypeConfig::sleep(Duration::from_millis(800)).await;

  assert!(
    !sent_malformed.load(Ordering::SeqCst),
    "replication retried after a storage error and sent prev_log_id=None with entries after a purged prefix"
  );

  Ok(())
}

use zenoh_raft::{
  errors::{AllowNextRevertError, ForwardToLeader, NodeNotFound, Operation},
  testing::memstore::new_mem_store,
};

/// 开启 allow_log_reversion 后，Leader 允许 Follower 将日志回退到更早的状态
#[compio::test]
async fn test_feature_loosen_follower_log_revert() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_tick: false,
      enable_heartbeat: false,
      max_payload_entries: 1,
      allow_log_reversion: Some(true),
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config);
  let mut log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {3})
    .await?;

  log_index += router.client_request_many(0, "0", 10).await?;
  for i in [0, 1, 2, 3] {
    router
      .wait(&i, timeout())
      .applied_index(Some(log_index), "10 writes")
      .await?;
  }

  let (_raft, _ls, _sm) = router.remove_node(3).unwrap();
  let (log, sm) = new_mem_store();

  router.new_raft_node_with_sto(3, log, sm).await;
  router.add_learner(0, 3).await?;
  log_index += 1;

  log_index += router.client_request_many(0, "0", 10).await?;
  for i in [0, 1, 2, 3] {
    router
      .wait(&i, timeout())
      .applied_index(Some(log_index), "10 writes")
      .await?;
  }

  Ok(())
}

/// 通过 Trigger::allow_next_revert 单次允许 Follower 日志回滚
#[compio::test]
async fn test_allow_follower_log_revert() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_tick: false,
      enable_heartbeat: false,
      max_payload_entries: 1,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config);
  let mut log_index = router.new_cluster(btreeset! {0}, btreeset! {1}).await?;

  log_index += router.client_request_many(0, "0", 10).await?;
  for i in [0, 1] {
    router
      .wait(&i, timeout())
      .applied_index(Some(log_index), "10 writes")
      .await?;
  }

  let n0 = router.get_raft_handle(&0)?;
  n0.trigger().allow_next_revert(&1, true).await??;

  let (_raft, _ls, _sm) = router.remove_node(1).unwrap();
  let (log, sm) = new_mem_store();

  router.new_raft_node_with_sto(1, log, sm).await;
  router.add_learner(0, 1).await?;
  log_index += 1;

  log_index += router.client_request_many(0, "0", 10).await?;
  for i in [0, 1] {
    router
      .wait(&i, timeout())
      .applied_index(Some(log_index), "10 writes")
      .await?;
  }

  Ok(())
}

/// Trigger::allow_next_revert 调用时的错误处理
#[compio::test]
async fn test_allow_follower_log_revert_errors() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_tick: false,
      enable_heartbeat: false,
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config);
  let _log_index = router.new_cluster(btreeset! {0}, btreeset! {1}).await?;

  let n0 = router.get_raft_handle(&0)?;
  let res = n0.trigger().allow_next_revert(&2, true).await?;
  assert_eq!(
    Err(AllowNextRevertError::NodeNotFound(NodeNotFound::new(
      2,
      Operation::AllowNextRevert
    ))),
    res
  );

  let n1 = router.get_raft_handle(&1)?;
  let res = n1.trigger().allow_next_revert(&0, true).await?;
  assert_eq!(
    Err(AllowNextRevertError::ForwardToLeader(ForwardToLeader::new(
      0,
      ()
    ))),
    res
  );

  Ok(())
}

/// Follower 状态清空后重启，Leader 通过心跳发现并自动恢复
#[compio::test]
async fn test_follower_clear_restart_recover() -> Result<()> {
  let config = Arc::new(
    Config {
      enable_heartbeat: false,
      enable_elect: false,
      allow_log_reversion: Some(true),
      ..Default::default()
    }
    .validate()?,
  );

  let mut router = RaftRouter::new(config);
  router.enable_saving_committed = false;

  let log_index = router
    .new_cluster(btreeset! {0, 1, 2}, btreeset! {})
    .await?;

  let (n1, _log, _sm) = router.remove_node(1).unwrap();
  n1.shutdown().await?;

  router.new_raft_node(1).await;

  let n0 = router.get_raft_handle(&0)?;
  n0.trigger().heartbeat().await?;

  let n1 = router.get_raft_handle(&1)?;
  n1.wait(timeout())
    .applied_index(Some(log_index), "should recovered")
    .await?;

  Ok(())
}
