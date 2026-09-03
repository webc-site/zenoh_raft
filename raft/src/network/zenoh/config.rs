//! Zenoh Raft network and session configuration
//! Zenoh Raft 网络与会话配置

use std::time::Duration;

use anyerror::AnyError;
use zenoh::{key_expr::KeyExpr, query::QueryTarget};

/// Default key expression prefix
/// 默认键表达式前缀
pub const DEFAULT_KEY_PREFIX: &str = "zenoh_raft";
/// Default request timeout duration
/// 默认请求超时时间
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// Zenoh network configuration
/// Zenoh 网络配置
#[derive(Debug, Clone)]
pub struct ZenohNetworkConfig {
  /// Key expression prefix, e.g. "zenoh_raft"
  /// 键表达式前缀，例如 "zenoh_raft"
  pub key_prefix: String,
  /// Default request timeout duration
  /// 默认请求超时时间
  pub default_timeout: Duration,
  /// Query target routing strategy
  /// 查询目标策略
  pub query_target: QueryTarget,
}

impl Default for ZenohNetworkConfig {
  fn default() -> Self {
    Self {
      key_prefix: DEFAULT_KEY_PREFIX.to_string(),
      default_timeout: DEFAULT_TIMEOUT,
      query_target: QueryTarget::BestMatching,
    }
  }
}

impl ZenohNetworkConfig {
  /// Validate the legality of network configuration
  /// 校验网络配置的合法性
  pub fn validate(&self) -> Result<(), AnyError> {
    if self.key_prefix.is_empty() {
      return Err(AnyError::error("key_prefix cannot be empty"));
    }
    KeyExpr::try_from(self.key_prefix.as_str())
      .map_err(|e| AnyError::error(format!("invalid key_prefix: {e}")))?;
    Ok(())
  }
}

/// Zenoh TLS / QUIC TLS configuration
/// Zenoh TLS / QUIC TLS 配置
#[derive(Debug, Clone)]
pub struct ZenohTlsConfig {
  /// Base64 encoded certificate
  /// Base64 编码的证书
  pub cert_base64: String,
  /// Base64 encoded private key
  /// Base64 编码的私钥
  pub key_base64: String,
  /// Base64 encoded root CA certificate (defaults to certificate itself if None)
  /// Base64 编码的根 CA 证书（若为 None 则默认使用证书自身）
  pub root_ca_base64: Option<String>,
  /// Whether to verify server domain name upon connection
  /// 连接时是否验证证书域名
  pub verify_name_on_connect: bool,
}

impl ZenohTlsConfig {
  /// Create custom TLS configuration
  /// 创建自定义 TLS 配置
  pub fn new(
    cert_base64: impl Into<String>,
    key_base64: impl Into<String>,
    root_ca_base64: Option<String>,
    verify_name_on_connect: bool,
  ) -> Self {
    Self {
      cert_base64: cert_base64.into(),
      key_base64: key_base64.into(),
      root_ca_base64,
      verify_name_on_connect,
    }
  }

  /// Create TLS configuration from PEM formatted strings
  /// 从 PEM 字符串创建 TLS 配置
  pub fn from_pem(
    cert_pem: &str,
    key_pem: &str,
    root_ca_pem: Option<&str>,
    verify_name_on_connect: bool,
  ) -> Self {
    let b64_cert = data_encoding::BASE64.encode(cert_pem.as_bytes());
    let b64_key = data_encoding::BASE64.encode(key_pem.as_bytes());
    let root_ca_base64 = root_ca_pem.map(|ca| data_encoding::BASE64.encode(ca.as_bytes()));
    Self::new(b64_cert, b64_key, root_ca_base64, verify_name_on_connect)
  }
}

/// Zenoh session builder supporting UDP reliable, plain TCP, and QUIC TLS transports
/// Zenoh 会话构造器（支持 UDP 可靠传输、TCP 明文以及 QUIC TLS 传输）
#[derive(Debug, Clone, Default)]
pub struct ZenohSessionBuilder {
  pub listen_endpoints: Vec<String>,
  pub connect_endpoints: Vec<String>,
  pub tls: Option<ZenohTlsConfig>,
  pub enable_multicast: bool,
  pub enable_gossip: bool,
}

const KEY_SCOUTING_MULTICAST: &str = "scouting/multicast/enabled";
const KEY_SCOUTING_GOSSIP: &str = "scouting/gossip/enabled";
const KEY_LISTEN_ENDPOINTS: &str = "listen/endpoints";
const KEY_CONNECT_ENDPOINTS: &str = "connect/endpoints";
const KEY_TLS_LISTEN_CERT: &str = "transport/link/tls/listen_certificate_base64";
const KEY_TLS_LISTEN_KEY: &str = "transport/link/tls/listen_private_key_base64";
const KEY_TLS_CONNECT_CERT: &str = "transport/link/tls/connect_certificate_base64";
const KEY_TLS_CONNECT_KEY: &str = "transport/link/tls/connect_private_key_base64";
const KEY_TLS_ROOT_CA: &str = "transport/link/tls/root_ca_certificate_base64";
const KEY_TLS_VERIFY_NAME: &str = "transport/link/tls/verify_name_on_connect";

#[inline]
fn strip_endpoint_scheme(ep: &str) -> &str {
  ep.strip_prefix("udp/")
    .or_else(|| ep.strip_prefix("tcp/"))
    .or_else(|| ep.strip_prefix("quic/"))
    .unwrap_or(ep)
}

/// Format as a QUIC TLS endpoint
/// 格式化为 QUIC TLS 端点
#[inline]
pub fn format_quic_endpoint(endpoint: &str) -> String {
  let ep = endpoint.trim();
  if ep.starts_with("quic/") {
    ep.to_string()
  } else {
    format!("quic/{}", strip_endpoint_scheme(ep))
  }
}

/// Format as a plain TCP endpoint
/// 格式化为 TCP 明文端点
#[inline]
pub fn format_tcp_endpoint(endpoint: &str) -> String {
  let ep = endpoint.trim();
  if ep.starts_with("tcp/") {
    ep.to_string()
  } else {
    format!("tcp/{}", strip_endpoint_scheme(ep))
  }
}

/// Format as a plain best-effort UDP endpoint
/// 格式化为普通尽力而为 UDP 端点
#[inline]
pub fn format_udp_endpoint(endpoint: &str) -> String {
  let ep = endpoint.trim();
  if ep.starts_with("udp/") {
    ep.to_string()
  } else {
    format!("udp/{}", strip_endpoint_scheme(ep))
  }
}

/// Format endpoint as a Zenoh native certificate-free QUIC Plain endpoint (UDP + rel=1 + multistream=1)
/// 将端点格式化为 Zenoh 原生免证书 QUIC Plain 端点 (UDP + rel=1 + multistream=1)
#[inline]
pub fn format_quic_plain_endpoint(endpoint: &str) -> String {
  let ep = endpoint.trim();
  let core = strip_endpoint_scheme(ep);

  let has_rel = core.contains("rel=");
  let has_multistream = core.contains("multistream=");

  // Return directly if already containing both reliability and multistream config.
  // 若已同时包含可靠流与多路复用配置则直接返回。
  if has_rel && has_multistream {
    if ep.starts_with("udp/") {
      return ep.to_string();
    }
    return format!("udp/{core}");
  }

  // Pre-allocate capacity once to avoid multiple reallocations during push_str.
  // 单次预分配容量，避免多次追加导致动态扩容 realloc。
  let mut s = String::with_capacity(core.len() + 32);
  s.push_str("udp/");
  s.push_str(core);

  if !core.contains('?') {
    s.push_str("?rel=1;multistream=1");
  } else {
    if !has_rel {
      if !s.ends_with('?') && !s.ends_with(';') {
        s.push(';');
      }
      s.push_str("rel=1");
    }
    if !has_multistream {
      if !s.ends_with('?') && !s.ends_with(';') {
        s.push(';');
      }
      s.push_str("multistream=1");
    }
  }
  s
}

/// Compatibility alias: format endpoint as a UDP reliable endpoint (equivalent to [`format_quic_plain_endpoint`])
/// 兼容别名：将端点格式化为基于 UDP 的可靠端点（等价于 [`format_quic_plain_endpoint`]）
#[inline]
pub fn format_udp_reliable_endpoint(endpoint: &str) -> String {
  format_quic_plain_endpoint(endpoint)
}

impl ZenohSessionBuilder {
  /// Create a new session builder
  /// 创建新的会话构造器
  pub fn new() -> Self {
    Self::default()
  }

  /// Add a QUIC Plain endpoint (Zenoh native certificate-free QUIC reliable transport: UDP + rel=1 + multistream=1)
  /// 添加 QUIC Plain 端点（基于 Zenoh 原生免证书 QUIC 可靠传输：UDP + rel=1 + multistream=1）
  pub fn quic_plain(mut self, endpoint: impl AsRef<str>, is_listener: bool) -> Self {
    let formatted = format_quic_plain_endpoint(endpoint.as_ref());
    if is_listener {
      self.listen_endpoints.push(formatted);
    } else {
      self.connect_endpoints.push(formatted);
    }
    self
  }

  /// Compatibility alias: add a UDP reliable endpoint (equivalent to [`ZenohSessionBuilder::quic_plain`])
  /// 兼容别名：添加基于 UDP 的可靠端点（等价于 [`ZenohSessionBuilder::quic_plain`]）
  #[inline]
  pub fn udp_reliable(self, endpoint: impl AsRef<str>, is_listener: bool) -> Self {
    self.quic_plain(endpoint, is_listener)
  }

  /// Add a native plain TCP endpoint (no certificates required, zero TLS handshake overhead)
  /// 添加原生纯明文 TCP 端点（无任何证书要求，零 TLS 握手开销）
  pub fn tcp(mut self, endpoint: impl AsRef<str>, is_listener: bool) -> Self {
    let formatted = format_tcp_endpoint(endpoint.as_ref());
    if is_listener {
      self.listen_endpoints.push(formatted);
    } else {
      self.connect_endpoints.push(formatted);
    }
    self
  }

  /// Add a plain best-effort (unreliable) UDP endpoint
  /// 添加普通尽力而为（Best-effort，非可靠）UDP 端点
  pub fn udp(mut self, endpoint: impl AsRef<str>, is_listener: bool) -> Self {
    let formatted = format_udp_endpoint(endpoint.as_ref());
    if is_listener {
      self.listen_endpoints.push(formatted);
    } else {
      self.connect_endpoints.push(formatted);
    }
    self
  }

  /// Add a general locator endpoint (supports any Zenoh endpoint e.g. `tcp/...`, `udp/...?rel=1;multistream=1`, `quic/...`)
  /// 添加通用 Locator 端点（支持任意 Zenoh 端点如 `tcp/...`, `udp/...?rel=1;multistream=1`, `quic/...` 等）
  pub fn endpoint(mut self, endpoint: impl AsRef<str>, is_listener: bool) -> Self {
    let ep = endpoint.as_ref().trim().to_string();
    if is_listener {
      self.listen_endpoints.push(ep);
    } else {
      self.connect_endpoints.push(ep);
    }
    self
  }

  /// Add a QUIC TLS endpoint with user-provided TLS credentials
  /// 添加 QUIC TLS 端点（配置用户指定的 TLS 凭据）
  pub fn quic_tls(
    mut self,
    endpoint: impl AsRef<str>,
    is_listener: bool,
    tls: ZenohTlsConfig,
  ) -> Self {
    let formatted = format_quic_endpoint(endpoint.as_ref());
    if is_listener {
      self.listen_endpoints.push(formatted);
    } else {
      self.connect_endpoints.push(formatted);
    }
    self.tls = Some(tls);
    self
  }

  /// Set TLS configuration
  /// 设置 TLS 配置
  pub fn tls(mut self, tls: ZenohTlsConfig) -> Self {
    self.tls = Some(tls);
    self
  }

  /// Build `zenoh::Config`
  /// 构建 `zenoh::Config`
  pub fn build_config(&self) -> Result<zenoh::Config, AnyError> {
    let mut config = zenoh::Config::default();
    insert_bool(&mut config, KEY_SCOUTING_MULTICAST, self.enable_multicast)?;
    insert_bool(&mut config, KEY_SCOUTING_GOSSIP, self.enable_gossip)?;

    if !self.listen_endpoints.is_empty() {
      insert_json(&mut config, KEY_LISTEN_ENDPOINTS, &self.listen_endpoints)?;
    }

    if !self.connect_endpoints.is_empty() {
      insert_json(&mut config, KEY_CONNECT_ENDPOINTS, &self.connect_endpoints)?;
    }

    if let Some(tls) = &self.tls {
      insert_json(&mut config, KEY_TLS_LISTEN_CERT, &tls.cert_base64)?;
      insert_json(&mut config, KEY_TLS_LISTEN_KEY, &tls.key_base64)?;
      insert_json(&mut config, KEY_TLS_CONNECT_CERT, &tls.cert_base64)?;
      insert_json(&mut config, KEY_TLS_CONNECT_KEY, &tls.key_base64)?;

      let root_ca = tls.root_ca_base64.as_ref().unwrap_or(&tls.cert_base64);
      insert_json(&mut config, KEY_TLS_ROOT_CA, root_ca)?;
      insert_bool(&mut config, KEY_TLS_VERIFY_NAME, tls.verify_name_on_connect)?;
    }

    Ok(config)
  }

  /// Open Zenoh session
  /// 打开 Zenoh 会话
  pub async fn open(&self) -> Result<zenoh::Session, AnyError> {
    let config = self.build_config()?;
    zenoh::open(config)
      .await
      .map_err(|e| AnyError::error(e.to_string()))
  }
}

#[inline]
fn insert_json(
  config: &mut zenoh::Config,
  key: &str,
  val: &impl sonic_rs::Serialize,
) -> Result<(), AnyError> {
  let json = sonic_rs::to_string(val).map_err(|e| AnyError::error(e.to_string()))?;
  config
    .insert_json5(key, &json)
    .map_err(|e| AnyError::error(e.to_string()))
}

#[inline]
fn insert_bool(config: &mut zenoh::Config, key: &str, val: bool) -> Result<(), AnyError> {
  config
    .insert_json5(key, if val { "true" } else { "false" })
    .map_err(|e| AnyError::error(e.to_string()))
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_network_config_default_and_validate() {
    let cfg = ZenohNetworkConfig::default();
    assert_eq!(cfg.key_prefix, DEFAULT_KEY_PREFIX);
    assert_eq!(cfg.default_timeout, DEFAULT_TIMEOUT);
    assert!(cfg.validate().is_ok());

    let empty_cfg = ZenohNetworkConfig {
      key_prefix: "".to_string(),
      ..Default::default()
    };
    assert!(empty_cfg.validate().is_err());
  }

  #[test]
  fn test_format_endpoints() {
    assert_eq!(
      format_quic_endpoint("127.0.0.1:1234"),
      "quic/127.0.0.1:1234"
    );
    assert_eq!(
      format_quic_plain_endpoint("127.0.0.1:1234"),
      "udp/127.0.0.1:1234?rel=1;multistream=1"
    );
    assert_eq!(
      format_quic_plain_endpoint("udp/127.0.0.1:1234"),
      "udp/127.0.0.1:1234?rel=1;multistream=1"
    );
    assert_eq!(
      format_quic_plain_endpoint("quic/127.0.0.1:1234"),
      "udp/127.0.0.1:1234?rel=1;multistream=1"
    );
    assert_eq!(format_tcp_endpoint("127.0.0.1:1234"), "tcp/127.0.0.1:1234");
  }

  #[test]
  fn test_builder_build_config() {
    let plain_builder = ZenohSessionBuilder::new()
      .quic_plain("127.0.0.1:7449", true)
      .tcp("127.0.0.1:7450", false);
    assert!(plain_builder.tls.is_none());
    assert!(plain_builder.build_config().is_ok());

    let tls = ZenohTlsConfig::new("cert_b64", "key_b64", None, false);
    let tls_builder = ZenohSessionBuilder::new().quic_tls("127.0.0.1:7447", true, tls);
    assert!(tls_builder.tls.is_some());
    assert!(tls_builder.build_config().is_ok());
  }
}
