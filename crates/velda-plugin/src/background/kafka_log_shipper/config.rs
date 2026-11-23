//! Kafka Log Shipper Configuration (`kafka_log_shipper::config`).
//!
//! ```text
//!   plugins.json ("kafka_log_shipper")
//!         │
//!         ├─► etp_id ─────────────► resolved from EtpRegistry (KafkaEtp)
//!         ├─► topic ──────────────► target Kafka topic name
//!         ├─► format / compression► json | text | binary, none | gzip | zstd
//!         ├─► acks / partitioner ─► none(0) | leader(1) | all(-1), round_robin | route_id | peer_ip
//!         └─► batch / timers ─────► flush_interval_ms | max_batch_size | request_timeout_ms | metadata_refresh_ms
//!
//!   Plugin Config Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ etp_id: String | topic: String | format | compression | acks | partitioner │
//!   │ flush_interval_ms | max_batch_size | request_timeout_ms | metadata_refresh │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! Broker addresses, `client_id`, SASL authentication, TLS/mTLS, pool capacity, and connect
//! timeouts belong strictly to the referenced `kafka` entry in `etp.json` / [`velda_etp::EtpRegistry`].

use serde::{Deserialize, Serialize};

use crate::error::PluginError;

/// Compression codec for Kafka RecordBatch v2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionCodec {
    /// No compression (codec = 0).
    None,
    /// Gzip / zlib compression (codec = 1).
    Gzip,
    /// Zstandard compression (codec = 4).
    #[default]
    Zstd,
}

impl CompressionCodec {
    /// Strictly parses string representation into [`CompressionCodec`].
    pub fn parse(s: &str) -> Result<Self, PluginError> {
        match s.trim().to_ascii_lowercase().as_str() {
            "none" => Ok(Self::None),
            "gzip" => Ok(Self::Gzip),
            "zstd" => Ok(Self::Zstd),
            other => Err(PluginError::Config(format!(
                "Invalid compression '{other}': supported codecs are 'none', 'gzip', 'zstd'"
            ))),
        }
    }

    /// Returns the Kafka RecordBatch attribute codec bits (bits 0..=2).
    #[inline]
    pub fn attribute_bits(&self) -> i16 {
        match self {
            Self::None => 0,
            Self::Gzip => 1,
            Self::Zstd => 4,
        }
    }
}

/// Partition assignment strategy for incoming access log records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionStrategy {
    /// Uniform round-robin distribution across all available topic partitions.
    #[default]
    RoundRobin,
    /// Stable hash distribution based on `route_id`.
    RouteId,
    /// Stable hash distribution based on client `peer_ip`.
    PeerIp,
}

impl PartitionStrategy {
    /// Strictly parses string representation into [`PartitionStrategy`].
    pub fn parse(s: &str) -> Result<Self, PluginError> {
        match s.trim().to_ascii_lowercase().as_str() {
            "round_robin" => Ok(Self::RoundRobin),
            "route_id" => Ok(Self::RouteId),
            "peer_ip" => Ok(Self::PeerIp),
            other => Err(PluginError::Config(format!(
                "Invalid partitioner '{other}': supported strategies are 'round_robin', 'route_id', 'peer_ip'"
            ))),
        }
    }
}

/// Kafka Produce acknowledgment durability mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum KafkaAckMode {
    /// Acks = 0: Fire and forget.
    None,
    /// Acks = 1: Acknowledged by partition leader.
    #[default]
    Leader,
    /// Acks = -1: Acknowledged by all In-Sync Replicas (ISR).
    All,
}

impl KafkaAckMode {
    /// Strictly parses string representation into [`KafkaAckMode`].
    pub fn parse(s: &str) -> Result<Self, PluginError> {
        match s.trim().to_ascii_lowercase().as_str() {
            "none" | "0" => Ok(Self::None),
            "leader" | "1" => Ok(Self::Leader),
            "all" | "-1" => Ok(Self::All),
            other => Err(PluginError::Config(format!(
                "Invalid acks '{other}': supported values are 'none' (0), 'leader' (1), 'all' (-1)"
            ))),
        }
    }

    /// Kafka wire protocol `acks` integer value.
    #[inline]
    pub fn to_wire_value(&self) -> i16 {
        match self {
            Self::None => 0,
            Self::Leader => 1,
            Self::All => -1,
        }
    }
}

/// Wire payload format for individual Kafka log records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KafkaLogFormat {
    /// Newline-free compact JSON record (default).
    #[default]
    Json,
    /// Single-line Extended Common Log Format (CLF) / Text log.
    Text,
    /// Raw binary layout of TelemetryRecord.
    Binary,
}

impl KafkaLogFormat {
    /// Strictly parses string representation into [`KafkaLogFormat`].
    pub fn parse(s: &str) -> Result<Self, PluginError> {
        match s.trim().to_ascii_lowercase().as_str() {
            "json" => Ok(Self::Json),
            "text" => Ok(Self::Text),
            "binary" => Ok(Self::Binary),
            other => Err(PluginError::Config(format!(
                "Invalid format '{other}': supported formats are 'json', 'text', 'binary'"
            ))),
        }
    }
}

/// Declarative configuration for Kafka Log Shipper background plugin (`kafka_log_shipper`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KafkaLogShipperConfig {
    /// Identifier of the `kafka` ETP entry in [`velda_etp::EtpRegistry`].
    pub etp_id: String,
    /// Target Kafka topic name.
    pub topic: String,
    /// Wire serialization format: `"json"`, `"text"`, or `"binary"`.
    #[serde(default = "default_format")]
    pub format: String,
    /// Compression codec for Kafka RecordBatch v2: `"none"`, `"gzip"`, or `"zstd"`.
    #[serde(default = "default_compression")]
    pub compression: String,
    /// Produce acknowledgment mode: `"none"` (0), `"leader"` (1), or `"all"` (-1).
    #[serde(default = "default_acks")]
    pub acks: String,
    /// Partitioning strategy: `"round_robin"`, `"route_id"`, or `"peer_ip"`.
    #[serde(default = "default_partitioner")]
    pub partitioner: String,
    /// Periodic batch flush interval in milliseconds (100 to 60,000 ms, default: 1000).
    #[serde(default = "default_flush_interval")]
    pub flush_interval_ms: u64,
    /// Maximum records grouped into a single produce batch (1 to 50,000, default: 8000).
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: usize,
    /// Kafka request timeout in milliseconds (100 to 60,000 ms, default: 5000).
    #[serde(default = "default_request_timeout")]
    pub request_timeout_ms: u64,
    /// Topic metadata refresh interval in milliseconds (1,000 to 3,600,000 ms, default: 300,000).
    #[serde(default = "default_metadata_refresh")]
    pub metadata_refresh_ms: u64,
}

fn default_format() -> String {
    "json".into()
}

fn default_compression() -> String {
    "zstd".into()
}

fn default_acks() -> String {
    "leader".into()
}

fn default_partitioner() -> String {
    "round_robin".into()
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

fn default_metadata_refresh() -> u64 {
    300_000
}

impl Default for KafkaLogShipperConfig {
    fn default() -> Self {
        Self {
            etp_id: String::new(),
            topic: String::new(),
            format: default_format(),
            compression: default_compression(),
            acks: default_acks(),
            partitioner: default_partitioner(),
            flush_interval_ms: default_flush_interval(),
            max_batch_size: default_max_batch_size(),
            request_timeout_ms: default_request_timeout(),
            metadata_refresh_ms: default_metadata_refresh(),
        }
    }
}

impl KafkaLogShipperConfig {
    /// Resolves parsed wire format.
    pub fn parsed_format(&self) -> Result<KafkaLogFormat, PluginError> {
        KafkaLogFormat::parse(&self.format)
    }

    /// Resolves parsed compression codec.
    pub fn parsed_compression(&self) -> Result<CompressionCodec, PluginError> {
        CompressionCodec::parse(&self.compression)
    }

    /// Resolves parsed partitioner strategy.
    pub fn parsed_partitioner(&self) -> Result<PartitionStrategy, PluginError> {
        PartitionStrategy::parse(&self.partitioner)
    }

    /// Resolves parsed acks mode.
    pub fn parsed_acks(&self) -> Result<KafkaAckMode, PluginError> {
        KafkaAckMode::parse(&self.acks)
    }
}
