//! Zenoh Raft 网络与会话配置

use std::time::Duration;

use anyerror::AnyError;
use zenoh::query::QueryTarget;

/// Zenoh 网络配置
#[derive(Debug, Clone)]
pub struct ZenohNetworkConfig {
    /// 键表达式前缀，例如 "zenoh_raft"
    pub key_prefix: String,
    /// 默认请求超时时间
    pub default_timeout: Duration,
    /// 查询目标策略
    pub query_target: QueryTarget,
}

impl Default for ZenohNetworkConfig {
    fn default() -> Self {
        Self {
            key_prefix: "zenoh_raft".to_string(),
            default_timeout: Duration::from_secs(5),
            query_target: QueryTarget::BestMatching,
        }
    }
}

/// Zenoh TLS / QUIC TLS 配置
#[derive(Debug, Clone)]
pub struct ZenohTlsConfig {
    /// Base64 编码的证书
    pub cert_base64: String,
    /// Base64 编码的私钥
    pub key_base64: String,
    /// Base64 编码的根 CA 证书（若为 None 则默认使用证书自身）
    pub root_ca_base64: Option<String>,
    /// 连接时是否验证证书域名
    pub verify_name_on_connect: bool,
}

impl ZenohTlsConfig {
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

    /// 生成自签名证书配置（用于 QUIC Plain 或测试环境）
    pub fn self_signed() -> Result<Self, AnyError> {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])
            .map_err(|e| AnyError::error(e.to_string()))?;
        let cert_pem = cert.cert.pem();
        let key_pem = cert.signing_key.serialize_pem();
        Ok(Self::from_pem(&cert_pem, &key_pem, None, false))
    }
}

/// Zenoh 会话构造器（专门针对 QUIC Plain 与 QUIC TLS 优化）
#[derive(Debug, Clone, Default)]
pub struct ZenohSessionBuilder {
    pub listen_endpoints: Vec<String>,
    pub connect_endpoints: Vec<String>,
    pub tls: Option<ZenohTlsConfig>,
    pub enable_multicast: bool,
    pub enable_gossip: bool,
}

impl ZenohSessionBuilder {
    /// 创建新的会话构造器
    pub fn new() -> Self {
        Self::default()
    }

    /// 添加 QUIC 端点
    pub fn quic_endpoint(mut self, endpoint: impl AsRef<str>, is_listener: bool) -> Self {
        let ep = endpoint.as_ref();
        let formatted = if ep.starts_with("quic/") {
            ep.to_string()
        } else {
            format!("quic/{ep}")
        };
        if is_listener {
            self.listen_endpoints.push(formatted);
        } else {
            self.connect_endpoints.push(formatted);
        }
        self
    }

    /// 添加 QUIC Plain 端点（自动配置自签名 TLS 凭据满足 QUIC 协议强制加密要求）
    pub fn quic_plain(mut self, endpoint: impl AsRef<str>, is_listener: bool) -> Self {
        self = self.quic_endpoint(endpoint, is_listener);
        if self.tls.is_none()
            && let Ok(tls) = ZenohTlsConfig::self_signed()
        {
            self.tls = Some(tls);
        }
        self
    }

    /// 添加 QUIC TLS 端点（配置指定的 TLS 凭据）
    pub fn quic_tls(
        mut self,
        endpoint: impl AsRef<str>,
        is_listener: bool,
        tls: ZenohTlsConfig,
    ) -> Self {
        self = self.quic_endpoint(endpoint, is_listener);
        self.tls = Some(tls);
        self
    }

    /// 设置 TLS 配置
    pub fn tls(mut self, tls: ZenohTlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    /// 构建 `zenoh::Config`
    pub fn build_config(&self) -> Result<zenoh::Config, AnyError> {
        let mut config = zenoh::Config::default();
        config
            .insert_json5(
                "scouting/multicast/enabled",
                if self.enable_multicast {
                    "true"
                } else {
                    "false"
                },
            )
            .map_err(|e| AnyError::error(e.to_string()))?;
        config
            .insert_json5(
                "scouting/gossip/enabled",
                if self.enable_gossip { "true" } else { "false" },
            )
            .map_err(|e| AnyError::error(e.to_string()))?;

        if !self.listen_endpoints.is_empty() {
            let json = sonic_rs::to_string(&self.listen_endpoints)
                .map_err(|e| AnyError::error(e.to_string()))?;
            config
                .insert_json5("listen/endpoints", &json)
                .map_err(|e| AnyError::error(e.to_string()))?;
        }

        if !self.connect_endpoints.is_empty() {
            let json = sonic_rs::to_string(&self.connect_endpoints)
                .map_err(|e| AnyError::error(e.to_string()))?;
            config
                .insert_json5("connect/endpoints", &json)
                .map_err(|e| AnyError::error(e.to_string()))?;
        }

        // 如果存在监听端点但未设置 TLS，自动为 QUIC 提供自签名凭据
        let tls_option = self.tls.clone().or_else(|| {
            if !self.listen_endpoints.is_empty() {
                ZenohTlsConfig::self_signed().ok()
            } else {
                None
            }
        });

        if let Some(tls) = tls_option {
            let cert_json = sonic_rs::to_string(&tls.cert_base64)
                .map_err(|e| AnyError::error(e.to_string()))?;
            let key_json =
                sonic_rs::to_string(&tls.key_base64).map_err(|e| AnyError::error(e.to_string()))?;
            config
                .insert_json5("transport/link/tls/listen_certificate_base64", &cert_json)
                .map_err(|e| AnyError::error(e.to_string()))?;
            config
                .insert_json5("transport/link/tls/listen_private_key_base64", &key_json)
                .map_err(|e| AnyError::error(e.to_string()))?;
            config
                .insert_json5("transport/link/tls/connect_certificate_base64", &cert_json)
                .map_err(|e| AnyError::error(e.to_string()))?;
            config
                .insert_json5("transport/link/tls/connect_private_key_base64", &key_json)
                .map_err(|e| AnyError::error(e.to_string()))?;

            let root_ca = tls.root_ca_base64.as_ref().unwrap_or(&tls.cert_base64);
            let root_ca_json =
                sonic_rs::to_string(root_ca).map_err(|e| AnyError::error(e.to_string()))?;
            config
                .insert_json5(
                    "transport/link/tls/root_ca_certificate_base64",
                    &root_ca_json,
                )
                .map_err(|e| AnyError::error(e.to_string()))?;
            config
                .insert_json5(
                    "transport/link/tls/verify_name_on_connect",
                    if tls.verify_name_on_connect {
                        "true"
                    } else {
                        "false"
                    },
                )
                .map_err(|e| AnyError::error(e.to_string()))?;
        }

        Ok(config)
    }

    /// 打开 Zenoh 会话
    pub async fn open(&self) -> Result<zenoh::Session, AnyError> {
        let config = self.build_config()?;
        zenoh::open(config)
            .await
            .map_err(|e| AnyError::error(e.to_string()))
    }
}
