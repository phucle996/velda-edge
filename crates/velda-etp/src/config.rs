//! External Third-Party (`velda-etp`) declarative configuration, endpoint resolution,
//! bincode JSON adapter, and deterministic 64-bit FNV-1a fingerprinting.
//!
//! ```text
//!   EtpEntryConfig::validate() / resolve_endpoint() / fingerprint()
//!         │
//!         ├─► [1] Validate id != "", pool_size > 0, connect_timeout_ms > 0
//!         ├─► [2] tls.validate() ──► delegated to connect::EtpTlsConfig
//!         ├─► [3] resolve_endpoint() ──► host/port | url/endpoint | brokers[] ──► (host, port)
//!         └─► [4] fingerprint() ──► 64-bit FNV-1a (64-bit block hash + canonical JSON)
//!
//!   FNV-1a Hash Input Stream & Config Layout:
//!   ┌──────────────────────────────────────────────────────────────────────────────┐
//!   │ [len+id] [len+kind] [pool_size:8B] [conn_to:8B] [idle_to:8B] [tls_flag:1B]   │
//!   │ [ca_pem] [cli_cert] [cli_key] [sni] [insecure:1B] [ver:1B] [pin_sha256]      │
//!   │ [sorted canonical JSON]                                                      │
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
            _ => None,
        }
    }

    /// Default TCP/UDP port used when a port is omitted from endpoint settings.
    pub const fn default_port(&self, tls_enabled: bool) -> u16 {
        match self {
            Self::Redis => {
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
            Self::S3 => {
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

/// Declarative configuration for a single External Third-Party dependency.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EtpEntryConfig {
    pub id: String,
    pub kind: EtpKind,
    #[serde(default)]
    pub pool: EtpPoolSettings,
    #[serde(default)]
    pub tls: Option<EtpTlsConfig>,
    #[serde(default = "default_settings", with = "json_value_bincode")]
    pub settings: serde_json::Value,
}

impl EtpEntryConfig {
    /// Returns the effective transport security mode for this ETP entry.
    pub fn transport_security(&self) -> EtpTransportSecurity {
        match &self.tls {
            Some(tls) => tls.transport_security(),
            None => EtpTransportSecurity::Plain,
        }
    }

    /// Validates configuration invariants, TLS syntax, and endpoint target settings.
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
        if let Some(tls) = &self.tls {
            tls.validate()?;
        }
        if !self.settings.is_object() {
            return Err(EtpError::Config(format!(
                "ETP '{}' settings must be a JSON object",
                self.id
            )));
        }

        let (host, port) = self.resolve_endpoint()?;
        if host.is_empty() || port == 0 {
            return Err(EtpError::Config(format!(
                "ETP '{}' resolved invalid endpoint '{host}:{port}'",
                self.id
            )));
        }

        if let Some(bucket) = self.settings.get("bucket").and_then(|v| v.as_str())
            && bucket.trim().is_empty()
        {
            return Err(EtpError::Config(format!(
                "ETP '{}' setting 'bucket' cannot be empty",
                self.id
            )));
        }

        Ok(())
    }

    /// Extracts `(host, port)` from `settings` according to kind-specific conventions.
    pub fn resolve_endpoint(&self) -> Result<(String, u16), EtpError> {
        let obj = self.settings.as_object().ok_or_else(|| {
            EtpError::Config(format!("ETP '{}' settings must be a JSON object", self.id))
        })?;

        let tls_hint = self.tls.is_some();

        // 1. Explicit `host` (+ optional `port`)
        if let Some(host_val) = obj.get("host") {
            let host = host_val
                .as_str()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    EtpError::Config(format!(
                        "ETP '{}' setting 'host' must be a non-empty string",
                        self.id
                    ))
                })?;
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
                self.kind.default_port(tls_hint)
            };
            if port == 0 {
                return Err(EtpError::Config(format!(
                    "ETP '{}' port must be greater than 0",
                    self.id
                )));
            }
            return Ok((host.to_string(), port));
        }

        // 2. Single URL / endpoint / address string keys
        for key in [
            "endpoint",
            "url",
            "addr",
            "address",
            "directory_url",
            "jwks_uri",
            "introspection_url",
        ] {
            if let Some(val) = obj.get(key) {
                let raw = val
                    .as_str()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        EtpError::Config(format!(
                            "ETP '{}' setting '{key}' must be a non-empty string",
                            self.id
                        ))
                    })?;
                return parse_endpoint_target(raw, self.kind, tls_hint, &self.id);
            }
        }

        // 3. Array keys (`brokers` for Kafka, `endpoints` for general multi-node targets)
        for key in ["brokers", "endpoints"] {
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
                            "ETP '{}' setting '{key}' must contain at least one non-empty endpoint string",
                            self.id
                        ))
                    })?;
                return parse_endpoint_target(first, self.kind, tls_hint, &self.id);
            }
        }

        Err(EtpError::Config(format!(
            "ETP '{}' settings missing endpoint specification (host/port, endpoint/url, or brokers)",
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
// Endpoint Target & Path Parsing Helpers
// ============================================================================

pub(crate) fn parse_endpoint_target(
    raw: &str,
    kind: EtpKind,
    tls_hint: bool,
    id: &str,
) -> Result<(String, u16), EtpError> {
    let (scheme_tls, without_scheme) = if let Some((scheme, rest)) = raw.split_once("://") {
        let scheme_lower = scheme.to_ascii_lowercase();
        let is_tls_scheme = matches!(
            scheme_lower.as_str(),
            "https" | "rediss" | "wss" | "grpcs" | "tls"
        );
        (Some(is_tls_scheme), rest)
    } else {
        (None, raw)
    };

    let authority = without_scheme.split('/').next().unwrap_or(without_scheme);
    let authority = authority
        .rsplit_once('@')
        .map(|(_, host_part)| host_part)
        .unwrap_or(authority);

    if authority.is_empty() {
        return Err(EtpError::Config(format!(
            "ETP '{id}' endpoint '{raw}' has an empty host"
        )));
    }

    let is_tls = scheme_tls.unwrap_or(tls_hint);

    // Bracketed IPv6 literal: `[::1]:6379` or `[::1]`
    if let Some(after_open) = authority.strip_prefix('[') {
        let (addr, rest) = after_open.split_once(']').ok_or_else(|| {
            EtpError::Config(format!("ETP '{id}' endpoint '{raw}' has unclosed '['"))
        })?;
        if addr.is_empty() {
            return Err(EtpError::Config(format!(
                "ETP '{id}' endpoint '{raw}' has empty IPv6 host"
            )));
        }
        let port = if rest.is_empty() {
            default_port_for_scheme_or_kind(scheme_tls, kind, is_tls)
        } else if let Some(port_str) = rest.strip_prefix(':') {
            parse_nonzero_port(port_str, id, raw)?
        } else {
            return Err(EtpError::Config(format!(
                "ETP '{id}' endpoint '{raw}' has invalid authority suffix '{rest}'"
            )));
        };
        return Ok((addr.to_string(), port));
    }

    // Standard `host:port` vs bare `host`
    if let Some((host, port_str)) = authority.rsplit_once(':')
        && !host.contains(':')
    {
        if host.is_empty() {
            return Err(EtpError::Config(format!(
                "ETP '{id}' endpoint '{raw}' has empty host"
            )));
        }
        let port = parse_nonzero_port(port_str, id, raw)?;
        return Ok((host.to_string(), port));
    }

    let port = default_port_for_scheme_or_kind(scheme_tls, kind, is_tls);
    Ok((authority.to_string(), port))
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

pub(crate) fn extract_endpoint_path(raw: &str) -> String {
    let without_scheme = raw.split_once("://").map(|(_, rest)| rest).unwrap_or(raw);
    match without_scheme.find('/') {
        Some(idx) => {
            let path = without_scheme[idx..].trim_end_matches('/');
            if path.is_empty() {
                String::new()
            } else {
                path.to_string()
            }
        }
        None => String::new(),
    }
}

fn default_port_for_scheme_or_kind(scheme_tls: Option<bool>, kind: EtpKind, is_tls: bool) -> u16 {
    if let Some(tls_scheme) = scheme_tls
        && matches!(kind, EtpKind::S3)
    {
        return if tls_scheme { 443 } else { 80 };
    }
    kind.default_port(is_tls)
}

fn parse_nonzero_port(port_str: &str, id: &str, raw: &str) -> Result<u16, EtpError> {
    let port: u16 = port_str.parse().map_err(|_| {
        EtpError::Config(format!(
            "ETP '{id}' endpoint '{raw}' has invalid port '{port_str}'"
        ))
    })?;
    if port == 0 {
        return Err(EtpError::Config(format!(
            "ETP '{id}' endpoint '{raw}' port must be greater than 0"
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
