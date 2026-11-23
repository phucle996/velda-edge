//! StatsD / DogStatsD Metrics Exporter Configuration (`statsd_exporter::config`).
//!
//! ```text
//!   plugins.json ("statsd_exporter")
//!         │
//!         ├─► etp_id ─────────────► resolved from EtpRegistry (StatsdEtp)
//!         ├─► prefix (optional) ──► override; falls back to etp.config.prefix | "velda"
//!         ├─► dialect (optional) ─► "dogstatsd" | "standard"; falls back to etp.config.flavor
//!         └─► interval / mtu ─────► interval_secs (1..=3600) | max_packet_size (256..=65507)
//!
//!   Plugin Config Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ etp_id: String | interval_secs: u64 | prefix: String                       │
//!   │ dialect: Option<String> | max_packet_size: usize                           │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! Destination UDP endpoint, socket pool capacity, connect timeout, and idle timeout belong
//! strictly to the referenced `statsd` entry in `etp.json` / [`velda_etp::EtpRegistry`].

use serde::{Deserialize, Serialize};

/// Declarative configuration for [`super::StatsdExporterPlugin`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatsdExporterConfig {
    /// Identifier of the `statsd` ETP entry in [`velda_etp::EtpRegistry`].
    pub etp_id: String,
    /// Periodic push interval in seconds (1..=3600, default: 2).
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,
    /// Optional metric name prefix override (falls back to `etp.config.prefix` or `"velda"`).
    #[serde(default)]
    pub prefix: String,
    /// Optional dialect override (`"dogstatsd"` or `"standard"`; falls back to `etp.config.flavor`).
    #[serde(default)]
    pub dialect: Option<String>,
    /// Maximum UDP packet payload size in bytes before flushing (256..=65507, default: 1432).
    #[serde(default = "default_max_packet_size")]
    pub max_packet_size: usize,
}

fn default_interval_secs() -> u64 {
    2
}

fn default_max_packet_size() -> usize {
    1432
}

impl Default for StatsdExporterConfig {
    fn default() -> Self {
        Self {
            etp_id: String::new(),
            interval_secs: default_interval_secs(),
            prefix: String::new(),
            dialect: None,
            max_packet_size: default_max_packet_size(),
        }
    }
}
