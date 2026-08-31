//! 管理与运维 API 测试套件

mod fixtures;

use std::sync::Arc;

use anyhow::Result;
use maplit::btreeset;
use zenoh_raft::Config;
use zenoh_raft::testing::memstore::ClientRequest;
use zenoh_raft::testing::memstore::IntoMemClientRequest;

use fixtures::RaftRouter;

/// 节点状态查询与外部请求接口测试
#[compio::test]
async fn test_management_api() -> Result<()> {
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
    n0.client_write(ClientRequest::make_request("mgmt", 1))
        .await?;

    let last_log = router
        .with_raft_state(0, |st| st.log_ids.last().cloned())
        .await?;
    assert!(last_log.is_some());

    let is_init = n0.is_initialized().await?;
    assert!(is_init);

    Ok(())
}

/// 通过 `Raft::config()` 读取配置
#[compio::test]
async fn test_raft_config() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_tick: false,
            election_timeout_min: 123,
            election_timeout_max: 124,
            ..Default::default()
        }
        .validate()?,
    );

    let mut router = RaftRouter::new(config.clone());
    let _log_index = router.new_cluster(btreeset! {0}, btreeset! {}).await?;

    let n0 = router.get_raft_handle(&0)?;
    let c = n0.config();

    assert!(!c.enable_tick);
    assert_eq!(c.election_timeout_min, 123);
    assert_eq!(c.election_timeout_max, 124);

    Ok(())
}
