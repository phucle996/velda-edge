//! ClickHouse Log Shipper Configuration (`clickhouse_log_shipper::config`).
//!
//! ```text
//!   plugins.json ("clickhouse_log_shipper")
//!         │
//!         ├─► etp_id ──────────────► resolved from EtpRegistry (ClickHouseEtp)
//!         ├─► database (optional) ─► overrides etp.config.database when non-empty
//!         ├─► format ──────────────► RowBinary | JSONEachRow
//!         └─► batch / timeout ─────► flush_interval_ms | max_batch_size | request_timeout_ms
//!
//!   Plugin Config Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ etp_id: String | database: String | format: String                         │
//!   │ flush_interval_ms: u64 | max_batch_size: usize | request_timeout_ms: u64   │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! Connection, authentication, pool capacity, and TLS/mTLS settings belong strictly to the
//! referenced `clickhouse` entry in `etp.json` / [`velda_etp::EtpRegistry`].

use serde::{Deserialize, Serialize};

use super::format::ClickHouseFormat;

/// Canonical ClickHouse access logs table name managed by Velda Edge.
pub const VELDA_ACCESS_LOGS_TABLE: &str = "velda_access_logs";

/// Declarative configuration for ClickHouse Log Shipper background plugin (`clickhouse_log_shipper`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClickHouseLogShipperConfig {
    /// Identifier of the `clickhouse` ETP entry in [`velda_etp::EtpRegistry`].
    pub etp_id: String,
    /// Optional target ClickHouse database override (defaults to `etp.config.database` when empty).
    #[serde(default)]
    pub database: String,
    /// ClickHouse wire format: `"row_binary"` (recommended for HPC) or `"json"` / `"json_each_row"`.
    #[serde(default = "default_format")]
    pub format: String,
    /// Periodic batch flush interval in milliseconds (100 to 60,000 ms, default: 1000).
    #[serde(default = "default_flush_interval")]
    pub flush_interval_ms: u64,
    /// Maximum records grouped into a single HTTP push payload (1 to 50,000, default: 8,000).
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: usize,
    /// HTTP push request timeout in milliseconds (100 to 60,000 ms, default: 5,000).
    #[serde(default = "default_request_timeout")]
    pub request_timeout_ms: u64,
}

fn default_format() -> String {
    "row_binary".into()
}

fn default_flush_interval() -> u64 {
    1000
}

fn default_max_batch_size() -> usize {
    8000
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
}
