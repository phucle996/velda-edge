//! External Third-Party (`velda-etp`) declarative configuration, host/port resolution,
//! bincode JSON adapter, and deterministic 64-bit FNV-1a fingerprinting.
//!
//! ```text
//!   EtpEntryConfig::validate() / resolve_endpoint() / fingerprint()
//!         │
//!         ├─► [1] Validate id != "", pool_size > 0, connect_timeout_ms > 0
//!         ├─► [2] validate_security() ──► explicit plain | tls | mtls | start_tls invariants
//!         ├─► [3] resolve_endpoint()  ──► host (+ optional port) | brokers[] | seed_nodes[]
//!         └─► [4] fingerprint()       ──► 64-bit FNV-1a (64-bit block hash + canonical JSON)
//!
//!   FNV-1a Hash Input Stream & Config Layout:
//!   ┌──────────────────────────────────────────────────────────────────────────────┐
//!   │ [len+id] [len+kind] [sec:1B] [pool_size:8B] [conn_to:8B] [idle_to:8B]        │
//!   │ [tls_flag:1B] [ca_pem] [cli_cert] [cli_key] [sni] [insecure:1B] [ver:1B]     │
//!   │ [pin_sha256] [sorted canonical JSON]                                         │
//!   └──────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::connect::{EtpTlsConfig, EtpTlsVersion, EtpTransportSecurity};
use crate::error::EtpError;

// ============================================================================
// ETP Kind & Pool Settings
// ============================================================================

/// Supported External Third-Party data source categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EtpKind {
    #[serde(rename = "redis")]
    Redis,
    #[serde(rename = "clickhouse")]
    ClickHouse,
    #[serde(rename = "s3")]
    S3,
    #[serde(rename = "loki")]
    Loki,
    #[serde(rename = "elasticsearch")]
    Elasticsearch,
    #[serde(rename = "otlp")]
    Otlp,
    #[serde(rename = "kafka")]
    Kafka,
    #[serde(rename = "statsd")]
    Statsd,
    #[serde(rename = "vault")]
    Vault,
    #[serde(rename = "aws_secrets_manager")]
    AwsSecretsManager,
    #[serde(rename = "keycloak")]
    Keycloak,
    #[serde(rename = "hydra")]
    Hydra,
    #[serde(rename = "authelia")]
    Authelia,
    #[serde(rename = "okta")]
    Okta,
    #[serde(rename = "auth0")]
    Auth0,
    #[serde(rename = "entra_id")]
    EntraId,
    #[serde(rename = "cognito")]
    Cognito,
    #[serde(rename = "oidc")]
    Oidc,
    #[serde(rename = "dynamodb")]
    DynamoDb,
    #[serde(rename = "memcached")]
    Memcached,
    #[serde(rename = "postgres")]
    Postgres,
    #[serde(rename = "mysql")]
    MySql,
    #[serde(rename = "nats")]
    Nats,
    #[serde(rename = "valkey")]
    Valkey,
    #[serde(rename = "dragonfly")]
    Dragonfly,
    #[serde(rename = "aws_elasticache")]
    AwsElastiCache,
    #[serde(rename = "gcp_memorystore")]
    GcpMemorystore,
    #[serde(rename = "upstash")]
    Upstash,
}

impl EtpKind {
    /// Returns the canonical lowercase string identifier for this ETP kind.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Redis => "redis",
            Self::ClickHouse => "clickhouse",
            Self::S3 => "s3",
            Self::Loki => "loki",
            Self::Elasticsearch => "elasticsearch",
            Self::Otlp => "otlp",
            Self::Kafka => "kafka",
            Self::Statsd => "statsd",
            Self::Vault => "vault",
            Self::AwsSecretsManager => "aws_secrets_manager",
            Self::Keycloak => "keycloak",
            Self::Hydra => "hydra",
            Self::Authelia => "authelia",
            Self::Okta => "okta",
            Self::Auth0 => "auth0",
            Self::EntraId => "entra_id",
            Self::Cognito => "cognito",
            Self::Oidc => "oidc",
            Self::DynamoDb => "dynamodb",
            Self::Memcached => "memcached",
            Self::Postgres => "postgres",
            Self::MySql => "mysql",
            Self::Nats => "nats",
            Self::Valkey => "valkey",
            Self::Dragonfly => "dragonfly",
            Self::AwsElastiCache => "aws_elasticache",
            Self::GcpMemorystore => "gcp_memorystore",
            Self::Upstash => "upstash",
        }
    }

    /// Parses an ETP kind identifier (case-insensitive).
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        let normalized = s.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "redis" => Some(Self::Redis),
            "clickhouse" | "click_house" => Some(Self::ClickHouse),
            "s3" => Some(Self::S3),
            "loki" => Some(Self::Loki),
            "elasticsearch" => Some(Self::Elasticsearch),
            "otlp" => Some(Self::Otlp),
            "kafka" => Some(Self::Kafka),
            "statsd" => Some(Self::Statsd),
            "vault" | "openbao" => Some(Self::Vault),
            "aws_secrets_manager" | "aws_secrets" => Some(Self::AwsSecretsManager),
            "keycloak" => Some(Self::Keycloak),
            "hydra" | "ory_hydra" => Some(Self::Hydra),
            "authelia" => Some(Self::Authelia),
            "okta" => Some(Self::Okta),
            "auth0" => Some(Self::Auth0),
            "entra_id" | "azure_ad" => Some(Self::EntraId),
            "cognito" | "aws_cognito" => Some(Self::Cognito),
            "oidc" => Some(Self::Oidc),
            "dynamodb" | "dynamo_db" => Some(Self::DynamoDb),
            "memcached" => Some(Self::Memcached),
            "postgres" | "postgresql" => Some(Self::Postgres),
            "mysql" | "mariadb" => Some(Self::MySql),
            "nats" => Some(Self::Nats),
            "valkey" => Some(Self::Valkey),
            "dragonfly" | "dragonflydb" => Some(Self::Dragonfly),
            "aws_elasticache" | "elasticache" => Some(Self::AwsElastiCache),
            "gcp_memorystore" | "memorystore" => Some(Self::GcpMemorystore),
            "upstash" => Some(Self::Upstash),
            _ => None,
        }
    }

    /// Default TCP/UDP port used when a port is omitted from endpoint settings.
    pub const fn default_port(&self, tls_enabled: bool) -> u16 {
        match self {
            Self::Redis
            | Self::Valkey
            | Self::Dragonfly
            | Self::AwsElastiCache
            | Self::GcpMemorystore => {
                if tls_enabled {
                    6380
                } else {
                    6379
                }
            }
            Self::ClickHouse => {
                if tls_enabled {
                    8443
                } else {
                    8123
                }
            }
            Self::S3 | Self::Oidc | Self::Upstash => {
                if tls_enabled {
                    443
                } else {
                    80
                }
            }
            Self::Loki => {
                if tls_enabled {
                    443
                } else {
                    3100
                }
            }
            Self::Elasticsearch => 9200,
            Self::Otlp => 4317,
            Self::Kafka => 9092,
            Self::Statsd => 8125,
            Self::Vault => 8200,
            Self::AwsSecretsManager
            | Self::Okta
            | Self::Auth0
            | Self::EntraId
            | Self::Cognito
            | Self::DynamoDb => 443,
            Self::Keycloak => {
                if tls_enabled {
                    8443
                } else {
                    8080
                }
            }
            Self::Hydra => 4444,
            Self::Authelia => 9091,
            Self::Memcached => 11211,
            Self::Postgres => 5432,
            Self::MySql => 3306,
            Self::Nats => 4222,
        }
    }
}

impl fmt::Display for EtpKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for EtpKind {
    type Err = EtpError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str(s).ok_or_else(|| EtpError::Config(format!("Unknown ETP kind '{s}'")))
    }
}

const fn default_pool_size() -> usize {
    4
}

const fn default_connect_timeout_ms() -> u64 {
    3000
}

const fn default_idle_timeout_ms() -> u64 {
    60_000
}

fn default_settings() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}

/// Connection pool tuning parameters for an ETP entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EtpPoolSettings {
    #[serde(default = "default_pool_size")]
    pub pool_size: usize,
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    #[serde(default = "default_idle_timeout_ms")]
    pub idle_timeout_ms: u64,
}

impl Default for EtpPoolSettings {
    fn default() -> Self {
        Self {
            pool_size: default_pool_size(),
            connect_timeout_ms: default_connect_timeout_ms(),
            idle_timeout_ms: default_idle_timeout_ms(),
        }
    }
}

/// Custom serde adapter allowing `serde_json::Value` to roundtrip cleanly through
/// non-self-describing binary encoders such as `bincode` while staying native JSON in
/// human-readable formats.
pub mod json_value_bincode {
    use super::*;

    pub fn serialize<S>(value: &serde_json::Value, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            value.serialize(serializer)
        } else {
            serializer.serialize_str(&value.to_string())
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<serde_json::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            serde_json::Value::deserialize(deserializer)
        } else {
            let s = String::deserialize(deserializer)?;
            serde_json::from_str(&s).map_err(serde::de::Error::custom)
        }
    }
}

// ============================================================================
// Declarative Entry Config & Validation
// ============================================================================

/// Legacy URL-style keys rejected cleanly in favor of explicit `host` + `port` (+ `base_path`).
const FORBIDDEN_URL_SETTING_KEYS: &[&str] = &[
    "endpoint",
    "url",
    "addr",
    "address",
    "endpoints",
    "directory_url",
    "jwks_uri",
    "introspection_url",
];

/// Declarative configuration for a single External Third-Party dependency.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EtpEntryConfig {
    pub id: String,
    pub kind: EtpKind,
    pub security: EtpTransportSecurity,
    #[serde(default)]
    pub pool: EtpPoolSettings,
    #[serde(default)]
    pub tls: Option<EtpTlsConfig>,
    #[serde(default = "default_settings", with = "json_value_bincode")]
    pub settings: serde_json::Value,
}

impl EtpEntryConfig {
    /// Returns the declared transport security mode for this ETP entry.
    #[inline]
    pub const fn transport_security(&self) -> EtpTransportSecurity {
        self.security
    }

    /// Returns `true` when traffic is protected by TLS (`tls`, `mtls`, or `start_tls`).
    #[inline]
    pub const fn is_tls_transport(&self) -> bool {
        !matches!(self.security, EtpTransportSecurity::Plain)
    }

    /// Returns the compiled TLS settings implied by `self.security` and `self.tls`.
    ///
    /// - `Plain` yields `None`.
    /// - `Tls` and `StartTls` fall back to `EtpTlsConfig::default()` (WebPKI root CAs, TLS 1.3/1.2)
    ///   when `self.tls` is `None`.
    /// - `Mtls` returns `self.tls.clone()` (validated to contain `client_cert_pem` + `client_key_pem`).
    pub fn effective_tls(&self) -> Option<EtpTlsConfig> {
        match self.security {
            EtpTransportSecurity::Plain => None,
            EtpTransportSecurity::Tls | EtpTransportSecurity::StartTls => {
                Some(self.tls.clone().unwrap_or_default())
            }
            EtpTransportSecurity::Mtls => self.tls.clone(),
        }
    }

    /// Enforces that `self.security` is among the modes supported by a specific ETP provider.
    pub fn require_security(&self, allowed: &[EtpTransportSecurity]) -> Result<(), EtpError> {
        if allowed.contains(&self.security) {
            return Ok(());
        }
        let supported: Vec<&str> = allowed.iter().map(EtpTransportSecurity::as_str).collect();
        Err(EtpError::Config(format!(
            "ETP '{}' (kind '{}') does not support security '{}'; supported: [{}]",
            self.id,
            self.kind.as_str(),
            self.security.as_str(),
            supported.join(", ")
        )))
    }

    /// Validates consistency between declared `security` mode and `tls` configuration.
    pub fn validate_security(&self) -> Result<(), EtpError> {
        match self.security {
            EtpTransportSecurity::Plain => {
                if self.tls.is_some() {
                    return Err(EtpError::Config(format!(
                        "ETP '{}' has security='plain' and must not configure 'tls'",
                        self.id
                    )));
                }
            }
            EtpTransportSecurity::Tls => {
                if let Some(tls) = &self.tls {
                    tls.validate()?;
                    if tls.client_cert_pem.is_some() || tls.client_key_pem.is_some() {
                        return Err(EtpError::Config(format!(
                            "ETP '{}' has security='tls' and must not specify client_cert_pem/client_key_pem (use security='mtls')",
                            self.id
                        )));
                    }
                }
            }
            EtpTransportSecurity::Mtls => {
                let Some(tls) = &self.tls else {
                    return Err(EtpError::Config(format!(
                        "ETP '{}' has security='mtls' and requires 'tls' with client_cert_pem and client_key_pem",
                        self.id
                    )));
                };
                tls.validate()?;
                if tls.client_cert_pem.is_none() || tls.client_key_pem.is_none() {
                    return Err(EtpError::Config(format!(
                        "ETP '{}' has security='mtls' and requires both client_cert_pem and client_key_pem",
                        self.id
                    )));
                }
            }
            EtpTransportSecurity::StartTls => {
                if let Some(tls) = &self.tls {
                    tls.validate()?;
                }
            }
        }
        Ok(())
    }

    /// Validates configuration invariants, security policy, TLS syntax, and host/port settings.
    pub fn validate(&self) -> Result<(), EtpError> {
        if self.id.trim().is_empty() {
            return Err(EtpError::Config("ETP entry 'id' cannot be empty".into()));
        }
        if self.pool.pool_size == 0 || self.pool.pool_size > crate::arena::MAX_BASELINE_SLOTS {
            return Err(EtpError::Config(format!(
                "ETP '{}' pool_size ({}) must be in 1..={}",
                self.id,
                self.pool.pool_size,
                crate::arena::MAX_BASELINE_SLOTS
            )));
        }
        if self.pool.connect_timeout_ms == 0 {
            return Err(EtpError::Config(format!(
                "ETP '{}' connect_timeout_ms must be greater than 0",
                self.id
            )));
        }
        self.validate_security()?;

        let obj = self.settings.as_object().ok_or_else(|| {
            EtpError::Config(format!("ETP '{}' settings must be a JSON object", self.id))
        })?;

        for &forbidden in FORBIDDEN_URL_SETTING_KEYS {
            if let Some(val) = obj.get(forbidden)
                && !val.is_null()
            {
                return Err(EtpError::Config(format!(
                    "ETP '{}' setting '{}' is not allowed; configure explicit 'host' + optional 'port' (plus 'base_path' for HTTP) and 'security' instead of URLs",
                    self.id, forbidden
                )));
            }
        }

        if let Some(bp_val) = obj.get("base_path")
            && !bp_val.is_null()
        {
            let bp = bp_val.as_str().ok_or_else(|| {
                EtpError::Config(format!(
                    "ETP '{}' setting 'base_path' must be a string",
                    self.id
                ))
            })?;
            normalize_base_path(bp, &self.id)?;
        }

        let (host, port) = self.resolve_endpoint()?;
        if host.is_empty() || port == 0 {
            return Err(EtpError::Config(format!(
                "ETP '{}' resolved invalid endpoint '{host}:{port}'",
                self.id
            )));
        }

        if let Some(bucket) = obj.get("bucket").and_then(|v| v.as_str())
            && bucket.trim().is_empty()
        {
            return Err(EtpError::Config(format!(
                "ETP '{}' setting 'bucket' cannot be empty",
                self.id
            )));
        }

        Ok(())
    }

    /// Extracts `(host, port)` from `settings` strictly without URL schemes.
    pub fn resolve_endpoint(&self) -> Result<(String, u16), EtpError> {
        let obj = self.settings.as_object().ok_or_else(|| {
            EtpError::Config(format!("ETP '{}' settings must be a JSON object", self.id))
        })?;

        let is_tls = self.is_tls_transport();

        // 1. Explicit `host` (+ optional `port`)
        if let Some(host_val) = obj.get("host") {
            let raw_host = host_val
                .as_str()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    EtpError::Config(format!(
                        "ETP '{}' setting 'host' must be a non-empty string",
                        self.id
                    ))
                })?;
            let host = parse_host_only(raw_host, &self.id)?;
            let port = if let Some(port_val) = obj.get("port") {
                let p = port_val.as_u64().ok_or_else(|| {
                    EtpError::Config(format!(
                        "ETP '{}' setting 'port' must be an integer",
                        self.id
                    ))
                })?;
                u16::try_from(p).map_err(|_| {
                    EtpError::Config(format!(
                        "ETP '{}' port {p} is outside valid u16 range",
                        self.id
                    ))
                })?
            } else {
                self.kind.default_port(is_tls)
            };
            if port == 0 {
                return Err(EtpError::Config(format!(
                    "ETP '{}' port must be greater than 0",
                    self.id
                )));
            }
            return Ok((host, port));
        }

        // 2. Multi-node array keys (`brokers` for Kafka, `seed_nodes` for cluster KV pools)
        for key in ["brokers", "seed_nodes"] {
            if let Some(val) = obj.get(key) {
                let arr = val.as_array().ok_or_else(|| {
                    EtpError::Config(format!(
                        "ETP '{}' setting '{key}' must be a JSON array",
                        self.id
                    ))
                })?;
                let first = arr
                    .first()
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        EtpError::Config(format!(
                            "ETP '{}' setting '{key}' must contain at least one non-empty host[:port] string",
                            self.id
                        ))
                    })?;
                return parse_host_port_target(first, self.kind, is_tls, &self.id);
            }
        }

        // 3. Provider-specific SaaS/Cloud host fields (domain / tenant_id / region)
        match self.kind {
            EtpKind::Okta | EtpKind::Auth0 => {
                if let Some(domain) = obj
                    .get("domain")
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                {
                    let host = parse_host_only(domain, &self.id)?;
                    return Ok((host, self.kind.default_port(is_tls)));
                }
            }
            EtpKind::EntraId => {
                if let Some(tenant) = obj
                    .get("tenant_id")
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                {
                    let _ = tenant;
                    return Ok((
                        "login.microsoftonline.com".to_string(),
                        self.kind.default_port(is_tls),
                    ));
                }
            }
            EtpKind::Cognito => {
                if let Some(region) = obj
                    .get("region")
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                {
                    return Ok((
                        format!("cognito-idp.{region}.amazonaws.com"),
                        self.kind.default_port(is_tls),
                    ));
                }
            }
            EtpKind::AwsSecretsManager => {
                if let Some(region) = obj
                    .get("region")
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                {
                    return Ok((
                        format!("secretsmanager.{region}.amazonaws.com"),
                        self.kind.default_port(is_tls),
                    ));
                }
            }
            EtpKind::DynamoDb => {
                if let Some(region) = obj
                    .get("region")
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                {
                    return Ok((
                        format!("dynamodb.{region}.amazonaws.com"),
                        self.kind.default_port(is_tls),
                    ));
                }
            }
            _ => {}
        }

        Err(EtpError::Config(format!(
            "ETP '{}' settings missing host/port target specification",
            self.id
        )))
    }

    /// Computes a deterministic 64-bit FNV-1a fingerprint across all fields of this entry.
    ///
    /// Used by `crate::registry::reconcile` for O(1) equality checks on hot reload.
    pub fn fingerprint(&self) -> u64 {
        let mut hash: u64 = FNV1A_OFFSET_BASIS;
        fnv1a_write_str(&mut hash, &self.id);
        fnv1a_write_str(&mut hash, self.kind.as_str());
        let sec_byte = match self.security {
            EtpTransportSecurity::Plain => 0u8,
            EtpTransportSecurity::Tls => 1u8,
            EtpTransportSecurity::Mtls => 2u8,
            EtpTransportSecurity::StartTls => 3u8,
        };
        fnv1a_write_u8(&mut hash, sec_byte);
        fnv1a_write_u64(&mut hash, self.pool.pool_size as u64);
        fnv1a_write_u64(&mut hash, self.pool.connect_timeout_ms);
        fnv1a_write_u64(&mut hash, self.pool.idle_timeout_ms);

        match &self.tls {
            None => fnv1a_write_u8(&mut hash, 0),
            Some(tls) => {
                fnv1a_write_u8(&mut hash, 1);
                fnv1a_write_opt_str(&mut hash, tls.ca_cert_pem.as_deref());
                fnv1a_write_opt_str(&mut hash, tls.client_cert_pem.as_deref());
                fnv1a_write_opt_str(&mut hash, tls.client_key_pem.as_deref());
                fnv1a_write_opt_str(&mut hash, tls.server_name.as_deref());
                fnv1a_write_u8(&mut hash, u8::from(tls.insecure_skip_verify));
                let ver_byte = match tls.tls_version {
                    EtpTlsVersion::Auto => 0u8,
                    EtpTlsVersion::Tls12 => 1u8,
                    EtpTlsVersion::Tls13 => 2u8,
                };
                fnv1a_write_u8(&mut hash, ver_byte);
                fnv1a_write_opt_str(&mut hash, tls.pinned_cert_sha256.as_deref());
            }
        }

        fnv1a_write_canonical_json(&mut hash, &self.settings);
        hash
    }
}

// ============================================================================
// Host / Port & Base Path Parsing Helpers (Zero URL Schemes)
// ============================================================================

/// Validates a host-only value (hostname, IPv4, or IPv6) with zero scheme, path, or `:port`.
pub(crate) fn parse_host_only(raw: &str, id: &str) -> Result<String, EtpError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(EtpError::Config(format!("ETP '{id}' host cannot be empty")));
    }
    if trimmed.contains("://")
        || trimmed.contains('/')
        || trimmed.contains('?')
        || trimmed.contains('#')
        || trimmed.contains('@')
        || trimmed.chars().any(char::is_whitespace)
    {
        return Err(EtpError::Config(format!(
            "ETP '{id}' host '{trimmed}' must be a bare hostname or IP without scheme, path, or credentials"
        )));
    }
    if let Some(after_open) = trimmed.strip_prefix('[') {
        let Some(inner) = after_open.strip_suffix(']') else {
            return Err(EtpError::Config(format!(
                "ETP '{id}' host '{trimmed}' must not embed ':port' (use 'port' field instead)"
            )));
        };
        if inner.parse::<std::net::Ipv6Addr>().is_err() {
            return Err(EtpError::Config(format!(
                "ETP '{id}' host '{trimmed}' has invalid bracketed IPv6 address"
            )));
        }
        return Ok(inner.to_string());
    }
    if trimmed.contains(':') && trimmed.parse::<std::net::Ipv6Addr>().is_err() {
        return Err(EtpError::Config(format!(
            "ETP '{id}' host '{trimmed}' must not embed ':port' (use 'port' field instead)"
        )));
    }
    Ok(trimmed.to_string())
}

/// Parses a `host` or `host:port` node target string (e.g. Kafka broker or cluster seed_node),
/// rejecting URL schemes, paths, and credentials.
pub(crate) fn parse_host_port_target(
    raw: &str,
    kind: EtpKind,
    is_tls: bool,
    id: &str,
) -> Result<(String, u16), EtpError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(EtpError::Config(format!(
            "ETP '{id}' node target cannot be empty"
        )));
    }
    if trimmed.contains("://")
        || trimmed.contains('/')
        || trimmed.contains('?')
        || trimmed.contains('#')
        || trimmed.contains('@')
        || trimmed.chars().any(char::is_whitespace)
    {
        return Err(EtpError::Config(format!(
            "ETP '{id}' target '{trimmed}' must be 'host' or 'host:port' without URL schemes or paths"
        )));
    }

    // Bracketed IPv6 literal: `[::1]:6379` or `[::1]`
    if let Some(after_open) = trimmed.strip_prefix('[') {
        let (addr, rest) = after_open.split_once(']').ok_or_else(|| {
            EtpError::Config(format!("ETP '{id}' target '{trimmed}' has unclosed '['"))
        })?;
        if addr.is_empty() || addr.parse::<std::net::Ipv6Addr>().is_err() {
            return Err(EtpError::Config(format!(
                "ETP '{id}' target '{trimmed}' has invalid IPv6 host"
            )));
        }
        let port = if rest.is_empty() {
            kind.default_port(is_tls)
        } else if let Some(port_str) = rest.strip_prefix(':') {
            parse_nonzero_port(port_str, id, trimmed)?
        } else {
            return Err(EtpError::Config(format!(
                "ETP '{id}' target '{trimmed}' has invalid authority suffix '{rest}'"
            )));
        };
        return Ok((addr.to_string(), port));
    }

    // Standard `host:port` vs bare IPv6 / bare hostname
    if let Some((host, port_str)) = trimmed.rsplit_once(':')
        && !host.contains(':')
    {
        if host.is_empty() {
            return Err(EtpError::Config(format!(
                "ETP '{id}' target '{trimmed}' has empty host"
            )));
        }
        let port = parse_nonzero_port(port_str, id, trimmed)?;
        return Ok((host.to_string(), port));
    }

    let port = kind.default_port(is_tls);
    Ok((trimmed.to_string(), port))
}

/// Validates and canonicalizes an optional HTTP `base_path` (e.g. `"/custom"` or `""`).
pub(crate) fn normalize_base_path(raw: &str, id: &str) -> Result<String, EtpError> {
    let trimmed = raw.trim();
    if trimmed.contains("://") || trimmed.contains('?') || trimmed.contains('#') {
        return Err(EtpError::Config(format!(
            "ETP '{id}' base_path '{trimmed}' must be a path prefix, not a URL or query"
        )));
    }
    let trimmed = trimmed.trim_end_matches('/');
    if trimmed.is_empty() {
        Ok(String::new())
    } else if trimmed.starts_with('/') {
        Ok(trimmed.to_string())
    } else {
        Ok(format!("/{trimmed}"))
    }
}

/// Extracts optional `settings.base_path` from an ETP JSON settings object.
pub(crate) fn read_base_path(
    obj: &serde_json::Map<String, serde_json::Value>,
    id: &str,
) -> Result<String, EtpError> {
    match obj.get("base_path").and_then(|v| v.as_str()) {
        Some(raw) => normalize_base_path(raw, id),
        None => Ok(String::new()),
    }
}

/// Formats `host` + `port` for URL construction (`[ipv6]:port` vs `host:port`).
pub(crate) fn format_host_port(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// Formats a canonical HTTP/gRPC endpoint URI derived strictly from `(scheme, host, port, base_path)`.
pub(crate) fn synthesize_endpoint_uri(
    scheme: &str,
    host: &str,
    port: u16,
    base_path: &str,
) -> String {
    format!("{scheme}://{}{base_path}", format_host_port(host, port))
}

pub(crate) fn format_host_header(host: &str, port: u16, is_tls: bool) -> String {
    let is_default = (is_tls && port == 443) || (!is_tls && port == 80);
    if is_default {
        host.to_string()
    } else if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// Shared connection-field bundle extracted by HTTP-based ETP configs (`config.rs`).
pub(crate) struct HttpEndpointFields {
    pub endpoint: String,
    pub host: String,
    pub port: u16,
    pub host_header: String,
    pub bearer_token: Option<String>,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub custom_headers: Vec<(String, String)>,
    pub tls: Option<EtpTlsConfig>,
}

/// Extracts connection endpoint, TLS-aware `Host` header, and optional HTTP client auth fields.
pub(crate) fn extract_http_endpoint_fields(
    entry: &EtpEntryConfig,
    expected_kind: EtpKind,
) -> Result<HttpEndpointFields, EtpError> {
    if entry.kind != expected_kind {
        return Err(EtpError::KindMismatch {
            id: entry.id.clone(),
            expected: expected_kind.as_str(),
            actual: entry.kind.as_str(),
        });
    }
    entry.validate()?;
    entry.require_security(&[
        EtpTransportSecurity::Plain,
        EtpTransportSecurity::Tls,
        EtpTransportSecurity::Mtls,
    ])?;

    let (host, port) = entry.resolve_endpoint()?;
    let obj = entry.settings.as_object().ok_or_else(|| {
        EtpError::Config(format!(
            "{} ETP '{}' settings must be an object",
            expected_kind.as_str(),
            entry.id
        ))
    })?;

    let is_tls = entry.is_tls_transport();
    let base_path = read_base_path(obj, &entry.id)?;
    let scheme = if is_tls { "https" } else { "http" };
    let endpoint = synthesize_endpoint_uri(scheme, &host, port, &base_path);
    let host_header = format_host_header(&host, port, is_tls);

    let opt_str = |key: &str| -> Option<String> {
        obj.get(key)
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string)
    };

    let mut custom_headers = Vec::new();
    if let Some(hdrs) = obj.get("headers").and_then(|v| v.as_object()) {
        for (k, v) in hdrs {
            if let Some(vs) = v.as_str() {
                custom_headers.push((k.clone(), vs.to_string()));
            }
        }
    }

    Ok(HttpEndpointFields {
        endpoint,
        host,
        port,
        host_header,
        bearer_token: opt_str("bearer_token"),
        client_id: opt_str("client_id"),
        client_secret: opt_str("client_secret"),
        custom_headers,
        tls: entry.effective_tls(),
    })
}

fn parse_nonzero_port(port_str: &str, id: &str, raw: &str) -> Result<u16, EtpError> {
    let port: u16 = port_str.parse().map_err(|_| {
        EtpError::Config(format!(
            "ETP '{id}' target '{raw}' has invalid port '{port_str}'"
        ))
    })?;
    if port == 0 {
        return Err(EtpError::Config(format!(
            "ETP '{id}' target '{raw}' port must be greater than 0"
        )));
    }
    Ok(port)
}

// ============================================================================
// Deterministic 64-bit FNV-1a Helpers
// ============================================================================

const FNV1A_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1A_PRIME: u64 = 0x0100_0000_01b3;

#[inline]
fn fnv1a_update(mut h: u64, bytes: &[u8]) -> u64 {
    let (chunks, remainder) = bytes.as_chunks::<8>();
    for chunk in chunks {
        let word = u64::from_le_bytes(*chunk);
        h = (h ^ word).wrapping_mul(FNV1A_PRIME);
    }
    for &b in remainder {
        h = (h ^ u64::from(b)).wrapping_mul(FNV1A_PRIME);
    }
    h
}

#[inline]
fn fnv1a_write_u8(hash: &mut u64, byte: u8) {
    *hash = (*hash ^ u64::from(byte)).wrapping_mul(FNV1A_PRIME);
}

#[inline]
fn fnv1a_write_bytes(hash: &mut u64, bytes: &[u8]) {
    *hash = fnv1a_update(*hash, bytes);
}

#[inline]
fn fnv1a_write_u64(hash: &mut u64, val: u64) {
    *hash = (*hash ^ val).wrapping_mul(FNV1A_PRIME);
}

#[inline]
fn fnv1a_write_str(hash: &mut u64, s: &str) {
    fnv1a_write_u64(hash, s.len() as u64);
    fnv1a_write_bytes(hash, s.as_bytes());
}

#[inline]
fn fnv1a_write_opt_str(hash: &mut u64, s: Option<&str>) {
    match s {
        None => fnv1a_write_u8(hash, 0),
        Some(inner) => {
            fnv1a_write_u8(hash, 1);
            fnv1a_write_str(hash, inner);
        }
    }
}

fn fnv1a_write_canonical_json(hash: &mut u64, val: &serde_json::Value) {
    match val {
        serde_json::Value::Null => fnv1a_write_u8(hash, 0x10),
        serde_json::Value::Bool(b) => {
            fnv1a_write_u8(hash, 0x11);
            fnv1a_write_u8(hash, u8::from(*b));
        }
        serde_json::Value::Number(n) => {
            fnv1a_write_u8(hash, 0x12);
            fnv1a_write_str(hash, &n.to_string());
        }
        serde_json::Value::String(s) => {
            fnv1a_write_u8(hash, 0x13);
            fnv1a_write_str(hash, s);
        }
        serde_json::Value::Array(items) => {
            fnv1a_write_u8(hash, 0x14);
            fnv1a_write_u64(hash, items.len() as u64);
            for item in items {
                fnv1a_write_canonical_json(hash, item);
            }
        }
        serde_json::Value::Object(map) => {
            fnv1a_write_u8(hash, 0x15);
            fnv1a_write_u64(hash, map.len() as u64);
            let mut entries: Vec<(&String, &serde_json::Value)> = map.iter().collect();
            entries.sort_by_key(|(a, _)| *a);
            for (k, v) in entries {
                fnv1a_write_str(hash, k);
                fnv1a_write_canonical_json(hash, v);
            }
        }
    }
}
