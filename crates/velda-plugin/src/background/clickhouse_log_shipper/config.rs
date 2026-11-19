//! ClickHouse Log Shipper Configuration (`clickhouse_log_shipper::config`).
//!
//! # Strict Invariant
//! All configuration fields must be explicitly declared or carry documented default fallbacks.
//! The database must be created by the operator prior to running; the table `velda_access_logs`
//! is automatically ensured by the plugin if absent.

use serde::{Deserialize, Serialize};

use super::format::ClickHouseFormat;
use crate::config::OutboundTlsConfig;
use crate::error::PluginError;

/// Canonical ClickHouse access logs table name managed by Velda Edge.
pub const VELDA_ACCESS_LOGS_TABLE: &str = "velda_access_logs";

/// Transport security mode for outbound connection to ClickHouse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransportSecurity {
    /// Plaintext TCP HTTP connection (requires `http://`).
    #[default]
    Plain,
    /// Standard one-way TLS encryption verifying server certificate (requires `https://`).
    Tls,
    /// Mutual TLS verifying server and presenting client certificate & key (requires `https://`).
    Mtls,
}

impl TransportSecurity {
    /// Parses a string representation or falls back to endpoint scheme.
    pub fn parse(s: &str, endpoint: &str) -> Result<Self, PluginError> {
        let trimmed = s.trim().to_ascii_lowercase();
        if trimmed.is_empty() {
            if endpoint.trim().starts_with("https://") {
                Ok(Self::Tls)
            } else {
                Ok(Self::Plain)
            }
        } else {
            match trimmed.as_str() {
                "plain" | "plaintext" | "none" => Ok(Self::Plain),
                "tls" => Ok(Self::Tls),
                "mtls" => Ok(Self::Mtls),
                other => Err(PluginError::Config(format!(
                    "Invalid transport_security '{other}': must be 'plain', 'tls', or 'mtls'"
                ))),
            }
        }
    }

    /// Returns whether this security mode requires TLS encryption.
    #[inline]
    pub fn is_tls(&self) -> bool {
        matches!(self, Self::Tls | Self::Mtls)
    }

    /// Returns the canonical string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Plain => "plain",
            Self::Tls => "tls",
            Self::Mtls => "mtls",
        }
    }
}

/// Declarative configuration for ClickHouse Log Shipper background plugin (`clickhouse_log_shipper`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClickHouseLogShipperConfig {
    /// ClickHouse HTTP endpoint URL (e.g. `http://127.0.0.1:8123` or `https://clickhouse.internal:8443`).
    pub endpoint: String,
    /// Explicit transport security mode: `"plain"` (HTTP), `"tls"` (HTTPS server auth), or `"mtls"` (HTTPS mutual auth).
    #[serde(default = "default_transport_security")]
    pub transport_security: String,
    /// Target ClickHouse database name (must be pre-created by operator/user).
    pub database: String,
    /// ClickHouse wire format: `"row_binary"` (recommended for HPC) or `"json"` / `"json_each_row"`.
    #[serde(default = "default_format")]
    pub format: String,
    /// ClickHouse authentication username (default: `"default"`).
    #[serde(default = "default_username")]
    pub username: String,
    /// ClickHouse authentication password (default: empty).
    #[serde(default)]
    pub password: String,
    /// Periodic batch flush interval in milliseconds (100 to 60,000 ms, default: 1000).
    #[serde(default = "default_flush_interval")]
    pub flush_interval_ms: u64,
    /// Maximum records grouped into a single HTTP push payload (1 to 50,000, default: 8,000).
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: usize,
    /// TCP connection establishment timeout in milliseconds (100 to 60,000 ms, default: 2,000).
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_ms: u64,
    /// HTTP push request timeout in milliseconds (100 to 60,000 ms, default: 5,000).
    #[serde(default = "default_request_timeout")]
    pub request_timeout_ms: u64,
    /// Outbound TLS settings for connecting to HTTPS ClickHouse endpoints.
    #[serde(default)]
    pub tls: OutboundTlsConfig,
}

fn default_transport_security() -> String {
    "plain".into()
}

fn default_format() -> String {
    "row_binary".into()
}

fn default_username() -> String {
    "default".into()
}

fn default_flush_interval() -> u64 {
    1000
}

fn default_max_batch_size() -> usize {
    8000
}

fn default_connect_timeout() -> u64 {
    2000
}

fn default_request_timeout() -> u64 {
    5000
}

impl ClickHouseLogShipperConfig {
    /// Resolves the parsed wire format.
    pub fn parsed_format(&self) -> ClickHouseFormat {
        match self.format.trim().to_ascii_lowercase().as_str() {
            "json" | "json_each_row" | "jsoneachrow" => ClickHouseFormat::JsonEachRow,
            _ => ClickHouseFormat::RowBinary,
        }
    }

    /// Resolves the parsed transport security mode.
    pub fn parsed_transport_security(&self) -> Result<TransportSecurity, PluginError> {
        TransportSecurity::parse(&self.transport_security, &self.endpoint)
    }
}
