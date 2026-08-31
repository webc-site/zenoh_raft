#![recursion_limit = "256"]

//! Zenoh QUIC Plain 与 QUIC TLS 会话通信基础测试

mod fixtures;

use std::error::Error;
use std::time::Duration;

use compio::time::sleep;
use fixtures::get_available_port;
use zenoh::Wait;
use zenoh_raft::ZenohSessionBuilder;
use zenoh_raft::ZenohTlsConfig;

async fn run_zenoh_session_ping_test(
    session1: &zenoh::Session,
    session2: &zenoh::Session,
    prefix: &str,
    msg: &'static [u8],
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let ping_key = format!("{prefix}/ping");
    let selector = format!("{prefix}/**");

    let _queryable = session1
        .declare_queryable(&selector)
        .callback(move |query| {
            let payload = query.payload().map(|p| p.to_bytes()).unwrap_or_default();
            let _ = query.reply(query.key_expr(), payload).wait();
        })
        .await
        .map_err(|e| e.to_string())?;

    for _ in 0..20 {
        sleep(Duration::from_millis(100)).await;
        if let Ok(replies) = session2
            .get(&ping_key)
            .payload(msg.to_vec())
            .timeout(Duration::from_secs(2))
            .await
        {
            while let Ok(reply) = replies.recv_async().await {
                if let Ok(sample) = reply.result() {
                    assert_eq!(sample.payload().to_bytes().as_ref(), msg);
                    return Ok(());
                }
            }
        }
    }
    panic!("Must receive reply over Zenoh queryable for {prefix}");
}

/// 测试基于 QUIC Plain 传输的 Zenoh 会话通信
#[compio::test]
async fn test_zenoh_session_quic_plain() -> Result<(), Box<dyn Error + Send + Sync>> {
    let port = get_available_port();
    let ep = format!("127.0.0.1:{port}");
    let tls = ZenohTlsConfig::self_signed()?;

    // 节点 1：监听 QUIC Plain 端点
    let session1 = ZenohSessionBuilder::new()
        .quic_tls(&ep, true, tls.clone())
        .open()
        .await?;

    // 节点 2：连接至节点 1 的 QUIC Plain 端点
    let session2 = ZenohSessionBuilder::new()
        .quic_tls(&ep, false, tls)
        .open()
        .await?;

    run_zenoh_session_ping_test(
        &session1,
        &session2,
        "test/quic_plain",
        b"hello from quic plain",
    )
    .await
}

/// 测试基于 QUIC TLS 传输的 Zenoh 会话通信
#[compio::test]
async fn test_zenoh_session_quic_tls() -> Result<(), Box<dyn Error + Send + Sync>> {
    let cert = rcgen::generate_simple_self_signed(["localhost".to_string()])?;
    let cert_pem = cert.cert.pem();
    let key_pem = cert.signing_key.serialize_pem();

    let tls_config = ZenohTlsConfig::from_pem(&cert_pem, &key_pem, Some(&cert_pem), false);

    let port = get_available_port();
    let ep = format!("127.0.0.1:{port}");

    // 节点 1：监听 QUIC TLS 端点
    let session1 = ZenohSessionBuilder::new()
        .quic_tls(&ep, true, tls_config.clone())
        .open()
        .await?;

    // 节点 2：连接至节点 1 的 QUIC TLS 端点
    let session2 = ZenohSessionBuilder::new()
        .quic_tls(&ep, false, tls_config)
        .open()
        .await?;

    run_zenoh_session_ping_test(
        &session1,
        &session2,
        "test/quic_tls",
        b"hello from quic tls",
    )
    .await
}
