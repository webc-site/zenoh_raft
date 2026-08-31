#![recursion_limit = "256"]

//! 基于真实 Zenoh QUIC Plain 与 QUIC TLS 传输层的分布式 Raft 集群集成测试

mod fixtures;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use fixtures::get_available_port;
use maplit::btreeset;
use zenoh::query::QueryTarget;
use zenoh_raft::Config;
use zenoh_raft::Raft;
use zenoh_raft::ReadPolicy;
use zenoh_raft::ServerState;
use zenoh_raft::ZenohNetworkConfig;
use zenoh_raft::ZenohNetworkFactory;
use zenoh_raft::ZenohRaftServer;
use zenoh_raft::ZenohSessionBuilder;
use zenoh_raft::ZenohTlsConfig;
use zenoh_raft::testing::memstore::ClientRequest;
use zenoh_raft::testing::memstore::IntoMemClientRequest;
use zenoh_raft::testing::memstore::TypeConfig;
use zenoh_raft::testing::memstore::new_mem_store;

async fn run_3node_cluster_test(
    session: Arc<zenoh::Session>,
    key_prefix: &str,
    request_key: &str,
) -> Result<()> {
    let raft_config = Arc::new(
        Config {
            enable_heartbeat: true,
            heartbeat_interval: 100,
            election_timeout_min: 500,
            election_timeout_max: 600,
            ..Default::default()
        }
        .validate()?,
    );

    let net_config = ZenohNetworkConfig {
        key_prefix: key_prefix.to_string(),
        default_timeout: Duration::from_secs(3),
        query_target: QueryTarget::BestMatching,
    };

    let (sto0, sm0) = new_mem_store();
    let net0 = ZenohNetworkFactory::<TypeConfig>::new(session.clone(), net_config.clone());
    let raft0 = Raft::new(0, raft_config.clone(), net0, sto0, sm0.clone()).await?;

    let (sto1, sm1) = new_mem_store();
    let net1 = ZenohNetworkFactory::<TypeConfig>::new(session.clone(), net_config.clone());
    let raft1 = Raft::new(1, raft_config.clone(), net1, sto1, sm1.clone()).await?;

    let (sto2, sm2) = new_mem_store();
    let net2 = ZenohNetworkFactory::<TypeConfig>::new(session.clone(), net_config.clone());
    let raft2 = Raft::new(2, raft_config.clone(), net2, sto2, sm2.clone()).await?;

    let _srv0 = ZenohRaftServer::start(&session, raft0.clone(), key_prefix, 0).await?;
    let _srv1 = ZenohRaftServer::start(&session, raft1.clone(), key_prefix, 1).await?;
    let _srv2 = ZenohRaftServer::start(&session, raft2.clone(), key_prefix, 2).await?;

    raft0.initialize(btreeset! {0}).await?;
    raft0
        .wait(Some(Duration::from_secs(5)))
        .state(ServerState::Leader, "node 0 becomes leader")
        .await?;

    raft0.add_learner(1, (), true).await?;
    raft0.add_learner(2, (), true).await?;
    raft0.change_membership(btreeset! {0, 1, 2}, false).await?;

    for (node, id) in [(&raft0, 0), (&raft1, 1), (&raft2, 2)] {
        node.wait(Some(Duration::from_secs(5)))
            .applied_index(Some(5), &format!("node {id} applied membership"))
            .await?;
    }

    for i in 1..=5 {
        raft0
            .client_write(ClientRequest::make_request(request_key, i))
            .await?;
    }

    for (node, id) in [(&raft0, 0), (&raft1, 1), (&raft2, 2)] {
        node.wait(Some(Duration::from_secs(5)))
            .applied_index(Some(10), &format!("node {id} applied 5 writes"))
            .await?;
    }

    for sm in [&sm0, &sm1, &sm2] {
        let sm_data = sm.get_state_machine().await;
        assert_eq!(
            sm_data.client_status.get(request_key).map(|s| s.as_str()),
            Some("request-5")
        );
    }

    raft0.ensure_linearizable(ReadPolicy::ReadIndex).await?;
    raft0.ensure_linearizable(ReadPolicy::LeaseRead).await?;

    Ok(())
}

/// 3 节点基于 QUIC Plain 传输层的 Raft 集群测试
#[compio::test]
async fn test_zenoh_raft_cluster_over_quic_plain() -> Result<()> {
    let key_prefix = "test_zenoh_quic_plain";
    let port = get_available_port();
    let ep = format!("127.0.0.1:{port}");

    let session = Arc::new(
        ZenohSessionBuilder::new()
            .quic_plain(&ep, true)
            .open()
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?,
    );

    run_3node_cluster_test(session, key_prefix, "zenoh_plain_k").await
}

/// 3 节点基于 QUIC TLS 传输层的 Raft 集群测试
#[compio::test]
async fn test_zenoh_raft_cluster_over_quic_tls() -> Result<()> {
    let tls_config = ZenohTlsConfig::self_signed().map_err(|e| anyhow::anyhow!("{e}"))?;

    let key_prefix = "test_zenoh_quic_tls";
    let port = get_available_port();
    let ep = format!("127.0.0.1:{port}");

    let session = Arc::new(
        ZenohSessionBuilder::new()
            .quic_tls(&ep, true, tls_config)
            .open()
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?,
    );

    run_3node_cluster_test(session, key_prefix, "zenoh_tls_k").await
}
