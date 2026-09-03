#![recursion_limit = "256"]

//! Zenoh transport layer (QUIC Plain, QUIC TLS, and TCP) session communication basic tests
//! Zenoh 传输层（QUIC Plain、QUIC TLS 与 TCP）会话通信基础测试

mod fixtures;

use std::time::Duration;

use anyhow::Result;
use compio::time::sleep;
use fixtures::get_available_port;
use zenoh::Wait;
use zenoh_raft::{ZenohSessionBuilder, ZenohTlsConfig};

async fn run_zenoh_session_ping_test(
  session1: &zenoh::Session,
  session2: &zenoh::Session,
  prefix: &str,
  msg: &[u8],
) -> Result<()> {
  let ping_key = format!("{prefix}/ping");
  let selector = format!("{prefix}/**");

  let _queryable = session1
    .declare_queryable(&selector)
    .callback(move |query| {
      let payload = query.payload().cloned().unwrap_or_default();
      let _ = query.reply(query.key_expr(), payload).wait();
    })
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;

  for _ in 0..20 {
    sleep(Duration::from_millis(100)).await;
    if let Ok(replies) = session2
      .get(&ping_key)
      .payload(msg)
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
  anyhow::bail!("Must receive reply over Zenoh queryable for {prefix}")
}

/// Test Zenoh session communication over QUIC Plain (UDP + rel=1 + multistream=1, certificate-free)
/// 测试基于 QUIC Plain (UDP + rel=1 + multistream=1, 免证书) 传输的 Zenoh 会话通信
#[compio::test]
async fn test_zenoh_session_quic_plain() -> Result<()> {
  let port = get_available_port();
  let ep = format!("127.0.0.1:{port}");

  let session1 = ZenohSessionBuilder::new()
    .quic_plain(&ep, true)
    .open()
    .await?;

  let session2 = ZenohSessionBuilder::new()
    .quic_plain(&ep, false)
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

/// Test Zenoh session communication over native plain TCP transport
/// 测试基于原生纯明文 TCP 传输的 Zenoh 会话通信
#[compio::test]
async fn test_zenoh_session_tcp() -> Result<()> {
  let port = get_available_port();
  let ep = format!("127.0.0.1:{port}");

  let session1 = ZenohSessionBuilder::new().tcp(&ep, true).open().await?;

  let session2 = ZenohSessionBuilder::new().tcp(&ep, false).open().await?;

  run_zenoh_session_ping_test(&session1, &session2, "test/tcp", b"hello from tcp").await
}

/// Test Zenoh session communication over QUIC TLS transport
/// 测试基于 QUIC TLS 传输的 Zenoh 会话通信
#[compio::test]
async fn test_zenoh_session_quic_tls() -> Result<()> {
  let cert = rcgen::generate_simple_self_signed(["localhost".to_string()])?;
  let cert_pem = cert.cert.pem();
  let key_pem = cert.signing_key.serialize_pem();

  let tls_config = ZenohTlsConfig::from_pem(&cert_pem, &key_pem, Some(&cert_pem), false);

  let port = get_available_port();
  let ep = format!("127.0.0.1:{port}");

  // Node 1: listen on QUIC TLS endpoint
  // 节点 1：监听 QUIC TLS 端点
  let session1 = ZenohSessionBuilder::new()
    .quic_tls(&ep, true, tls_config.clone())
    .open()
    .await?;

  // Node 2: connect to Node 1's QUIC TLS endpoint
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
