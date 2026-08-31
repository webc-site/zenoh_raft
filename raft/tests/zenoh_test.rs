#![recursion_limit = "256"]

//! Zenoh QUIC Plain 与 QUIC TLS 会话通信基础测试

use std::error::Error;
use std::sync::atomic::AtomicU16;
use std::sync::atomic::Ordering;
use std::time::Duration;

use zenoh::Wait;
use zenoh_raft::ZenohSessionBuilder;
use zenoh_raft::ZenohTlsConfig;

fn get_available_port() -> u16 {
    static PORT_COUNTER: AtomicU16 = AtomicU16::new(0);
    let offset = PORT_COUNTER.fetch_add(1, Ordering::Relaxed);
    fastrand::u16(32000..58000).wrapping_add(offset)
}

/// 测试基于 QUIC Plain 传输的 Zenoh 会话通信
#[compio::test]
async fn test_zenoh_session_quic_plain() -> Result<(), Box<dyn Error + Send + Sync>> {
    let port = get_available_port();
    let ep = format!("127.0.0.1:{port}");
    let tls = ZenohTlsConfig::self_signed()?;

    // 节点 1：监听 QUIC 端点
    let session1 = ZenohSessionBuilder::new()
        .quic_tls(&ep, true, tls.clone())
        .open()
        .await?;

    let _queryable1 = session1
        .declare_queryable("test/quic_plain/**")
        .callback(move |query| {
            let key = query.key_expr().clone();
            let payload = query.payload().map(|p| p.to_bytes()).unwrap_or_default();
            let _ = query.reply(key, payload).wait();
        })
        .await
        .map_err(|e| e.to_string())?;

    // 节点 2：连接至节点 1 的 QUIC 端点
    let session2 = ZenohSessionBuilder::new()
        .quic_tls(&ep, false, tls)
        .open()
        .await?;

    let replies = session2
        .get("test/quic_plain/ping")
        .payload(b"hello from quic plain".to_vec())
        .timeout(Duration::from_secs(5))
        .await
        .map_err(|e| e.to_string())?;

    let mut received = false;
    while let Ok(reply) = replies.recv_async().await {
        if let Ok(sample) = reply.result() {
            assert_eq!(
                sample.payload().to_bytes().as_ref(),
                b"hello from quic plain"
            );
            received = true;
            break;
        }
    }
    assert!(received, "Must receive reply over QUIC Plain queryable");
    Ok(())
}

/// 测试基于 QUIC TLS 传输的 Zenoh 会话通信
#[compio::test]
async fn test_zenoh_session_quic_tls() -> Result<(), Box<dyn Error + Send + Sync>> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
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

    let _queryable1 = session1
        .declare_queryable("test/quic_tls/**")
        .callback(move |query| {
            let key = query.key_expr().clone();
            let payload = query.payload().map(|p| p.to_bytes()).unwrap_or_default();
            let _ = query.reply(key, payload).wait();
        })
        .await
        .map_err(|e| e.to_string())?;

    // 节点 2：连接至节点 1 的 QUIC TLS 端点
    let session2 = ZenohSessionBuilder::new()
        .quic_tls(&ep, false, tls_config)
        .open()
        .await?;

    let replies = session2
        .get("test/quic_tls/ping")
        .payload(b"hello from quic tls".to_vec())
        .timeout(Duration::from_secs(5))
        .await
        .map_err(|e| e.to_string())?;

    let mut received = false;
    while let Ok(reply) = replies.recv_async().await {
        if let Ok(sample) = reply.result() {
            assert_eq!(sample.payload().to_bytes().as_ref(), b"hello from quic tls");
            received = true;
            break;
        }
    }
    assert!(received, "Must receive reply over QUIC TLS queryable");
    Ok(())
}
