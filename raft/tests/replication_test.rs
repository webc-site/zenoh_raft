//! 日志复制与网络流测试套件

mod fixtures;

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::Result;
use futures_util::future::ready;
use maplit::btreeset;
use zenoh_raft::Config;
use zenoh_raft::RPCTypes;
use zenoh_raft::errors::RPCError;
use zenoh_raft::errors::Unreachable;
use zenoh_raft::testing::memstore::ClientRequest;
use zenoh_raft::testing::memstore::IntoMemClientRequest;
use zenoh_raft::testing::memstore::TypeConfig;
use zenoh_raft::type_config::TypeConfigExt;

use fixtures::RaftRouter;
use fixtures::timeout;

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
        raft.client_write_ff(ClientRequest::make_request("bar", 1), None)
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
