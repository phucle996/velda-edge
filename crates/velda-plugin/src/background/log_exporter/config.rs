//! Unified Access Log Exporter Configuration (`log_exporter::config`).
//!
//! ```text
//!   plugins.json ("log_exporter")
//!         │
//!         └─► targets: Vec<LogTargetConfig>  (tagged by "mode": "local" | "etp")
//!               │
//!               ├─► mode == "local" ──► LocalLogTargetConfig
//!               │     └─► target ("stdout" | "stderr" | /abs/path), format, flush_interval_ms, max_batch_size
//!               │
//!               └─► mode == "etp"   ──► EtpLogTargetConfig
//!                     ├─► etp_id (mandatory, dispatched by EtpBackend at attach_etp time)
//!                     └─► optional backend-specific overrides (database, index, topic, prefix, format, ...)
//! ```
//!
//! # Strict Declarative Contract
//! - `targets` must be non-empty.
//! - Local targets cannot specify `etp_id`; ETP targets must specify a non-empty `etp_id`.
//! - Backend-specific string enums (`format`, `compression`, `acks`, `partitioner`) are strictly
//!   validated against the resolved `EtpBackend` during startup ETP wiring.

use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::providers::clickhouse::format::ClickHouseFormat;
use crate::error::PluginError;

/// Canonical ClickHouse access logs table name managed by Velda Edge.
pub const VELDA_ACCESS_LOGS_TABLE: &str = "velda_access_logs";

/// Top-level configuration for the unified `log_exporter` background plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogExporterConfig {
    /// Ordered list of log export targets (`mode: "local"` or `mode: "etp"`).
    pub targets: Vec<LogTargetConfig>,
}

/// Discriminated target specification routed either to a local stream/file or an ETP sink.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "lowercase")]
pub enum LogTargetConfig {
    /// Local OS stream or file output (`stdout`, `stderr`, or `/abs/path`) without `velda-etp`.
    Local(LocalLogTargetConfig),
    /// External Third-Party sink resolved by `EtpBackend` from `EtpRegistry`.
    Etp(Box<EtpLogTargetConfig>),
}

// ============================================================================
// Local Stream / File Target Config
// ============================================================================

/// Output destination target for local stream/file log records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetStream {
    Stdout,
    Stderr,
    File(PathBuf),
}

impl TargetStream {
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "stdout" => Self::Stdout,
            "stderr" => Self::Stderr,
            _ => Self::File(PathBuf::from(s.trim())),
        }
    }
}

/// Configuration for `mode: "local"` access log targets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalLogTargetConfig {
    /// Destination output stream (`"stdout"`, `"stderr"`, or an absolute filesystem path).
    pub target: String,
    /// Serialization format (`"text"`, `"json"`, or `"clf"`).
    pub format: String,
    /// Periodic batch flush interval in milliseconds (50 to 60,000 ms).
    pub flush_interval_ms: u64,
    /// Maximum records grouped in a single drain cycle (1 to 10,000).
    pub max_batch_size: usize,
}

// ============================================================================
// ETP Target Config & Backend-Specific Resolved Structs
// ============================================================================

/// Declarative configuration for `mode: "etp"` access log targets.
///
/// The concrete backend (`clickhouse`, `elasticsearch`, `loki`, `s3`, `kafka`, `otlp`)
/// is inferred deterministically from the attached [`velda_etp::EtpInstance::backend`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EtpLogTargetConfig {
    /// Identifier of the target entry in `etp.json` / [`velda_etp::EtpRegistry`].
    pub etp_id: String,
    /// Optional explicit ETP kind assertion checked during `velda-sync`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etp_kind: Option<String>,
    /// Periodic batch flush interval in milliseconds (backend-specific bounds).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flush_interval_ms: Option<u64>,
    /// Maximum records grouped into a single push payload (ClickHouse, Loki, Kafka, OTLP).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch_size: Option<usize>,
    /// Maximum telemetry ring batches drained per cycle (Elasticsearch, S3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_drain_batches: Option<usize>,
    /// Network request completion timeout in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_timeout_ms: Option<u64>,

    // ClickHouse
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,

    // Shared formatting / compression overrides (ClickHouse | Kafka | Elasticsearch | S3)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression: Option<String>,

    // Elasticsearch
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op_type: Option<EsBulkOpType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<String>,

    // Loki & OTLP
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,
    #[serde(default)]
    pub labels: HashMap<String, String>,

    // S3
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,

    // Kafka
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acks: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partitioner: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_refresh_ms: Option<u64>,
}

/// Bulk API operation type for Elasticsearch / OpenSearch documents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EsBulkOpType {
    /// Append-only (`{"create":{"_index":"..."}}`), required for ES Data Streams.
    #[default]
    Create,
    /// Standard upsert/index (`{"index":{"_index":"..."}}`).
    Index,
}

/// Wire compression applied to Elasticsearch `_bulk` HTTP request bodies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EsCompression {
    #[default]
    Gzip,
    None,
}

impl EsCompression {
    pub fn parse(s: &str) -> Result<Self, PluginError> {
        match s.trim().to_ascii_lowercase().as_str() {
            "gzip" => Ok(Self::Gzip),
            "none" => Ok(Self::None),
            other => Err(PluginError::Config(format!(
                "Invalid Elasticsearch compression '{other}': supported values are 'gzip', 'none'"
            ))),
        }
    }
}

/// Object compression format for S3 batches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum S3Compression {
    #[default]
    Zstd,
    Gzip,
    None,
}

impl S3Compression {
    pub fn parse(s: &str) -> Result<Self, PluginError> {
        match s.trim().to_ascii_lowercase().as_str() {
            "zstd" => Ok(Self::Zstd),
            "gzip" => Ok(Self::Gzip),
            "none" => Ok(Self::None),
            other => Err(PluginError::Config(format!(
                "Invalid S3 compression '{other}': supported values are 'zstd', 'gzip', 'none'"
            ))),
        }
    }
}

/// Compression codec for Kafka RecordBatch v2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionCodec {
    None,
    Gzip,
    #[default]
    Zstd,
}

impl CompressionCodec {
    pub fn parse(s: &str) -> Result<Self, PluginError> {
        match s.trim().to_ascii_lowercase().as_str() {
            "none" => Ok(Self::None),
            "gzip" => Ok(Self::Gzip),
            "zstd" => Ok(Self::Zstd),
            other => Err(PluginError::Config(format!(
                "Invalid Kafka compression '{other}': supported codecs are 'none', 'gzip', 'zstd'"
            ))),
        }
    }

    #[inline]
    pub fn attribute_bits(&self) -> i16 {
        match self {
            Self::None => 0,
            Self::Gzip => 1,
            Self::Zstd => 4,
        }
    }
}

/// Partition assignment strategy for incoming Kafka access log records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionStrategy {
    #[default]
    RoundRobin,
    RouteId,
    PeerIp,
}

impl PartitionStrategy {
    pub fn parse(s: &str) -> Result<Self, PluginError> {
        match s.trim().to_ascii_lowercase().as_str() {
            "round_robin" => Ok(Self::RoundRobin),
            "route_id" => Ok(Self::RouteId),
            "peer_ip" => Ok(Self::PeerIp),
            other => Err(PluginError::Config(format!(
                "Invalid Kafka partitioner '{other}': supported strategies are 'round_robin', 'route_id', 'peer_ip'"
            ))),
        }
    }
}

/// Kafka Produce acknowledgment durability mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum KafkaAckMode {
    None,
    #[default]
    Leader,
    All,
}

impl KafkaAckMode {
    pub fn parse(s: &str) -> Result<Self, PluginError> {
        match s.trim().to_ascii_lowercase().as_str() {
            "none" | "0" => Ok(Self::None),
            "leader" | "1" => Ok(Self::Leader),
            "all" | "-1" => Ok(Self::All),
            other => Err(PluginError::Config(format!(
                "Invalid Kafka acks '{other}': supported values are 'none' (0), 'leader' (1), 'all' (-1)"
            ))),
        }
    }

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
    #[default]
    Json,
    Text,
    Binary,
}

impl KafkaLogFormat {
    pub fn parse(s: &str) -> Result<Self, PluginError> {
        match s.trim().to_ascii_lowercase().as_str() {
            "json" => Ok(Self::Json),
            "text" => Ok(Self::Text),
            "binary" => Ok(Self::Binary),
            other => Err(PluginError::Config(format!(
                "Invalid Kafka format '{other}': supported formats are 'json', 'text', 'binary'"
            ))),
        }
    }
}

/// Resolved parameters for the ClickHouse log provider.
#[derive(Debug, Clone)]
pub struct ClickHouseTargetConfig {
    pub etp_id: String,
    pub database: String,
    pub format: ClickHouseFormat,
    pub flush_interval_ms: u64,
    pub max_batch_size: usize,
    pub request_timeout_ms: u64,
}

/// Resolved parameters for the Elasticsearch log provider.
#[derive(Debug, Clone)]
pub struct ElasticsearchTargetConfig {
    pub etp_id: String,
    pub index: String,
    pub op_type: EsBulkOpType,
    pub pipeline: Option<String>,
    pub compression: EsCompression,
    pub max_drain_batches: usize,
    pub flush_interval_ms: u64,
    pub request_timeout_ms: u64,
}

/// Resolved parameters for the Loki log provider.
#[derive(Debug, Clone)]
pub struct LokiTargetConfig {
    pub etp_id: String,
    pub service_name: String,
    pub flush_interval_ms: u64,
    pub max_batch_size: usize,
    pub request_timeout_ms: u64,
    pub labels: HashMap<String, String>,
}

/// Resolved parameters for the S3 log provider.
#[derive(Debug, Clone)]
pub struct S3TargetConfig {
    pub etp_id: String,
    pub prefix: String,
    pub compression: S3Compression,
    pub max_drain_batches: usize,
    pub flush_interval_ms: u64,
    pub request_timeout_ms: u64,
}

/// Resolved parameters for the Kafka log provider.
#[derive(Debug, Clone)]
pub struct KafkaTargetConfig {
    pub etp_id: String,
    pub topic: String,
    pub format: KafkaLogFormat,
    pub compression: CompressionCodec,
    pub acks: KafkaAckMode,
    pub partitioner: PartitionStrategy,
    pub flush_interval_ms: u64,
    pub max_batch_size: usize,
    pub request_timeout_ms: u64,
    pub metadata_refresh_ms: u64,
}

/// Resolved parameters for the OTLP log provider.
#[derive(Debug, Clone)]
pub struct OtlpLogTargetConfig {
    pub etp_id: String,
    pub service_name: String,
    pub flush_interval_ms: u64,
    pub max_batch_size: usize,
    pub request_timeout_ms: u64,
}

impl EtpLogTargetConfig {
    pub fn resolve_clickhouse(&self) -> Result<ClickHouseTargetConfig, PluginError> {
        let format = match self.format.as_deref() {
            None => ClickHouseFormat::RowBinary,
            Some(raw) => match raw.trim().to_ascii_lowercase().as_str() {
                "row_binary" | "rowbinary" => ClickHouseFormat::RowBinary,
                "json" | "json_each_row" | "jsoneachrow" => ClickHouseFormat::JsonEachRow,
                other => {
                    return Err(PluginError::Config(format!(
                        "ClickHouse target '{}' invalid format '{other}': expected 'row_binary' or 'json'",
                        self.etp_id
                    )));
                }
            },
        };
        let flush_interval_ms = self.flush_interval_ms.unwrap_or(1000);
        if !(100..=60_000).contains(&flush_interval_ms) {
            return Err(PluginError::Config(format!(
                "ClickHouse target '{}': flush_interval_ms ({flush_interval_ms}) must be in 100..=60_000",
                self.etp_id
            )));
        }
        let max_batch_size = self.max_batch_size.unwrap_or(8000);
        if !(1..=50_000).contains(&max_batch_size) {
            return Err(PluginError::Config(format!(
                "ClickHouse target '{}': max_batch_size ({max_batch_size}) must be in 1..=50_000",
                self.etp_id
            )));
        }
        let request_timeout_ms = self.request_timeout_ms.unwrap_or(5000);
        if !(100..=60_000).contains(&request_timeout_ms) {
            return Err(PluginError::Config(format!(
                "ClickHouse target '{}': request_timeout_ms ({request_timeout_ms}) must be in 100..=60_000",
                self.etp_id
            )));
        }
        Ok(ClickHouseTargetConfig {
            etp_id: self.etp_id.clone(),
            database: self.database.clone().unwrap_or_default(),
            format,
            flush_interval_ms,
            max_batch_size,
            request_timeout_ms,
        })
    }

    pub fn resolve_elasticsearch(&self) -> Result<ElasticsearchTargetConfig, PluginError> {
        let compression = match self.compression.as_deref() {
            None => EsCompression::Gzip,
            Some(raw) => EsCompression::parse(raw)?,
        };
        let max_drain_batches = self.max_drain_batches.unwrap_or(64);
        if !(1..=4096).contains(&max_drain_batches) {
            return Err(PluginError::Config(format!(
                "Elasticsearch target '{}': max_drain_batches ({max_drain_batches}) must be in 1..=4096",
                self.etp_id
            )));
        }
        let flush_interval_ms = self.flush_interval_ms.unwrap_or(1000);
        if !(10..=60_000).contains(&flush_interval_ms) {
            return Err(PluginError::Config(format!(
                "Elasticsearch target '{}': flush_interval_ms ({flush_interval_ms}) must be in 10..=60_000",
                self.etp_id
            )));
        }
        let request_timeout_ms = self.request_timeout_ms.unwrap_or(5000);
        if !(100..=120_000).contains(&request_timeout_ms) {
            return Err(PluginError::Config(format!(
                "Elasticsearch target '{}': request_timeout_ms ({request_timeout_ms}) must be in 100..=120_000",
                self.etp_id
            )));
        }
        Ok(ElasticsearchTargetConfig {
            etp_id: self.etp_id.clone(),
            index: self.index.clone().unwrap_or_default(),
            op_type: self.op_type.unwrap_or_default(),
            pipeline: self.pipeline.clone(),
            compression,
            max_drain_batches,
            flush_interval_ms,
            request_timeout_ms,
        })
    }

    pub fn resolve_loki(&self) -> Result<LokiTargetConfig, PluginError> {
        let service_name = self
            .service_name
            .clone()
            .unwrap_or_else(|| "velda-edge".to_string());
        if service_name.trim().is_empty() {
            return Err(PluginError::Config(format!(
                "Loki target '{}': service_name cannot be empty",
                self.etp_id
            )));
        }
        let flush_interval_ms = self.flush_interval_ms.unwrap_or(1000);
        if !(100..=60_000).contains(&flush_interval_ms) {
            return Err(PluginError::Config(format!(
                "Loki target '{}': flush_interval_ms ({flush_interval_ms}) must be in 100..=60_000",
                self.etp_id
            )));
        }
        let max_batch_size = self.max_batch_size.unwrap_or(1000);
        if !(1..=10_000).contains(&max_batch_size) {
            return Err(PluginError::Config(format!(
                "Loki target '{}': max_batch_size ({max_batch_size}) must be in 1..=10_000",
                self.etp_id
            )));
        }
        let request_timeout_ms = self.request_timeout_ms.unwrap_or(5000);
        if !(100..=60_000).contains(&request_timeout_ms) {
            return Err(PluginError::Config(format!(
                "Loki target '{}': request_timeout_ms ({request_timeout_ms}) must be in 100..=60_000",
                self.etp_id
            )));
        }
        Ok(LokiTargetConfig {
            etp_id: self.etp_id.clone(),
            service_name,
            flush_interval_ms,
            max_batch_size,
            request_timeout_ms,
            labels: self.labels.clone(),
        })
    }

    pub fn resolve_s3(&self) -> Result<S3TargetConfig, PluginError> {
        let compression = match self.compression.as_deref() {
            None => S3Compression::Zstd,
            Some(raw) => S3Compression::parse(raw)?,
        };
        let max_drain_batches = self.max_drain_batches.unwrap_or(256);
        if !(1..=4096).contains(&max_drain_batches) {
            return Err(PluginError::Config(format!(
                "S3 target '{}': max_drain_batches ({max_drain_batches}) must be in 1..=4096",
                self.etp_id
            )));
        }
        let flush_interval_ms = self.flush_interval_ms.unwrap_or(10_000);
        if !(100..=600_000).contains(&flush_interval_ms) {
            return Err(PluginError::Config(format!(
                "S3 target '{}': flush_interval_ms ({flush_interval_ms}) must be in 100..=600_000",
                self.etp_id
            )));
        }
        let request_timeout_ms = self.request_timeout_ms.unwrap_or(10_000);
        if !(100..=120_000).contains(&request_timeout_ms) {
            return Err(PluginError::Config(format!(
                "S3 target '{}': request_timeout_ms ({request_timeout_ms}) must be in 100..=120_000",
                self.etp_id
            )));
        }
        Ok(S3TargetConfig {
            etp_id: self.etp_id.clone(),
            prefix: self
                .prefix
                .clone()
                .unwrap_or_else(|| "velda-logs".to_string()),
            compression,
            max_drain_batches,
            flush_interval_ms,
            request_timeout_ms,
        })
    }

    pub fn resolve_kafka(&self) -> Result<KafkaTargetConfig, PluginError> {
        let topic = self
            .topic
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                PluginError::Config(format!(
                    "Kafka target '{}' requires non-empty 'topic'",
                    self.etp_id
                ))
            })?
            .to_string();
        let format = match self.format.as_deref() {
            None => KafkaLogFormat::Json,
            Some(raw) => KafkaLogFormat::parse(raw)?,
        };
        let compression = match self.compression.as_deref() {
            None => CompressionCodec::Zstd,
            Some(raw) => CompressionCodec::parse(raw)?,
        };
        let acks = match self.acks.as_deref() {
            None => KafkaAckMode::Leader,
            Some(raw) => KafkaAckMode::parse(raw)?,
        };
        let partitioner = match self.partitioner.as_deref() {
            None => PartitionStrategy::RoundRobin,
            Some(raw) => PartitionStrategy::parse(raw)?,
        };
        let flush_interval_ms = self.flush_interval_ms.unwrap_or(1000);
        if !(100..=60_000).contains(&flush_interval_ms) {
            return Err(PluginError::Config(format!(
                "Kafka target '{}': flush_interval_ms ({flush_interval_ms}) must be in 100..=60_000",
                self.etp_id
            )));
        }
        let max_batch_size = self.max_batch_size.unwrap_or(8000);
        if !(1..=50_000).contains(&max_batch_size) {
            return Err(PluginError::Config(format!(
                "Kafka target '{}': max_batch_size ({max_batch_size}) must be in 1..=50_000",
                self.etp_id
            )));
        }
        let request_timeout_ms = self.request_timeout_ms.unwrap_or(5000);
        if !(100..=60_000).contains(&request_timeout_ms) {
            return Err(PluginError::Config(format!(
                "Kafka target '{}': request_timeout_ms ({request_timeout_ms}) must be in 100..=60_000",
                self.etp_id
            )));
        }
        let metadata_refresh_ms = self.metadata_refresh_ms.unwrap_or(300_000);
        if !(1_000..=3_600_000).contains(&metadata_refresh_ms) {
            return Err(PluginError::Config(format!(
                "Kafka target '{}': metadata_refresh_ms ({metadata_refresh_ms}) must be in 1_000..=3_600_000",
                self.etp_id
            )));
        }
        Ok(KafkaTargetConfig {
            etp_id: self.etp_id.clone(),
            topic,
            format,
            compression,
            acks,
            partitioner,
            flush_interval_ms,
            max_batch_size,
            request_timeout_ms,
            metadata_refresh_ms,
        })
    }

    pub fn resolve_otlp(&self) -> Result<OtlpLogTargetConfig, PluginError> {
        let service_name = self
            .service_name
            .clone()
            .unwrap_or_else(|| "velda-edge".to_string());
        if service_name.trim().is_empty() {
            return Err(PluginError::Config(format!(
                "OTLP Logs target '{}': service_name cannot be empty",
                self.etp_id
            )));
        }
        let flush_interval_ms = self.flush_interval_ms.unwrap_or(1000);
        if !(100..=60_000).contains(&flush_interval_ms) {
            return Err(PluginError::Config(format!(
                "OTLP Logs target '{}': flush_interval_ms ({flush_interval_ms}) must be in 100..=60_000",
                self.etp_id
            )));
        }
        let max_batch_size = self.max_batch_size.unwrap_or(1000);
        if !(1..=10_000).contains(&max_batch_size) {
            return Err(PluginError::Config(format!(
                "OTLP Logs target '{}': max_batch_size ({max_batch_size}) must be in 1..=10_000",
                self.etp_id
            )));
        }
        let request_timeout_ms = self.request_timeout_ms.unwrap_or(5000);
        if !(100..=60_000).contains(&request_timeout_ms) {
            return Err(PluginError::Config(format!(
                "OTLP Logs target '{}': request_timeout_ms ({request_timeout_ms}) must be in 100..=60_000",
                self.etp_id
            )));
        }
        Ok(OtlpLogTargetConfig {
            etp_id: self.etp_id.clone(),
            service_name,
            flush_interval_ms,
            max_batch_size,
            request_timeout_ms,
        })
    }
}
