//! Kafka Log Shipper Configuration (`kafka_log_shipper::config`).
//!
//! # Strict Invariants
//! - `client_id` is automatically derived at process start (`velda-edge-{hostname}-{pid}`)
//!   to uniquely distinguish every gateway node across horizontal auto-scaling topologies.
//! - Transport security (`plain`, `tls`, `mtls`) is backed by `velda-tls`.
//! - Zero external C-dependencies: pure Rust Kafka wire protocol.

use serde::{Deserialize, Serialize};

use crate::config::OutboundTlsConfig;
use crate::error::PluginError;

/// Transport security mode for connecting to Kafka brokers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KafkaTransportSecurity {
    /// Plaintext TCP connection (typically port 9092).
    #[default]
    Plain,
    /// Standard one-way TLS encryption verifying server certificate (typically port 9093/9094).
    Tls,
    /// Mutual TLS verifying server and presenting client certificate & key.
    Mtls,
}

impl KafkaTransportSecurity {
    /// Parses string representation into [`KafkaTransportSecurity`].
    pub fn parse(s: &str) -> Result<Self, PluginError> {
        match s.trim().to_ascii_lowercase().as_str() {
            "plain" => Ok(Self::Plain),
            "tls" => Ok(Self::Tls),
            "mtls" => Ok(Self::Mtls),
            other => Err(PluginError::Config(format!(
                "Invalid transport_security '{other}': must be 'plain', 'tls', or 'mtls'"
            ))),
        }
    }

    /// Returns whether this mode requires TLS encryption.
    #[inline]
    pub fn is_tls(&self) -> bool {
        matches!(self, Self::Tls | Self::Mtls)
    }

    /// Returns canonical string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Plain => "plain",
            Self::Tls => "tls",
            Self::Mtls => "mtls",
        }
    }
}

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
    /// Parses string representation into [`CompressionCodec`].
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
    /// Parses string representation into [`PartitionStrategy`].
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

/// SASL authentication mechanism.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SaslMechanism {
    /// No SASL authentication.
    #[default]
    None,
    /// SASL/PLAIN username & password.
    Plain,
    /// SASL/SCRAM-SHA-256 salted challenge-response.
    ScramSha256,
    /// SASL/SCRAM-SHA-512 salted challenge-response.
    ScramSha512,
}

impl SaslMechanism {
    /// Parses string representation into [`SaslMechanism`].
    pub fn parse(s: &str) -> Result<Self, PluginError> {
        match s.trim().to_ascii_lowercase().as_str() {
            "none" => Ok(Self::None),
            "plain" => Ok(Self::Plain),
            "scram_sha_256" => Ok(Self::ScramSha256),
            "scram_sha_512" => Ok(Self::ScramSha512),
            other => Err(PluginError::Config(format!(
                "Invalid sasl_mechanism '{other}': supported mechanisms are 'none', 'plain', 'scram_sha_256', 'scram_sha_512'"
            ))),
        }
    }

    /// Kafka SASL mechanism name string.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Plain => "PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha512 => "SCRAM-SHA-512",
        }
    }

    /// Returns whether authentication credentials are required.
    #[inline]
    pub fn is_auth_required(&self) -> bool {
        !matches!(self, Self::None)
    }
}

/// Kafka Produce acknowledgment durability mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum KafkaAckMode {
    /// Acks = 0: Fire and forget (maximum throughput, potential log loss if broker fails).
    None,
    /// Acks = 1: Acknowledged by partition leader (recommended default).
    #[default]
    Leader,
    /// Acks = -1: Acknowledged by all In-Sync Replicas (ISR).
    All,
}

impl KafkaAckMode {
    /// Parses string representation into [`KafkaAckMode`].
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
    /// Single-line Extended Common Log Format (CLF) / Text log with explicit HTTP/gRPC status.
    Text,
    /// Raw binary layout of TelemetryRecord (zero stringification overhead).
    Binary,
}

impl KafkaLogFormat {
    /// Parses string representation into [`KafkaLogFormat`].
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KafkaLogShipperConfig {
    /// Seed Kafka broker addresses (e.g. `["10.0.1.10:9092", "10.0.1.11:9092"]`).
    pub brokers: Vec<String>,
    /// Target Kafka topic name (must be pre-created by Kafka operator).
    pub topic: String,
    /// Wire serialization format: `"json"` (default) or `"text"`.
    #[serde(default = "default_format")]
    pub format: String,
    /// Explicit transport security mode: `"plain"` (TCP), `"tls"` (TLS server auth), or `"mtls"` (Mutual TLS).
    #[serde(default = "default_transport_security")]
    pub transport_security: String,
    /// Outbound TLS settings (CA certificate, client cert/key for mTLS, skip verify).
    #[serde(default)]
    pub tls: OutboundTlsConfig,
    /// SASL authentication mechanism: `"none"`, `"plain"`, `"scram_sha_256"`, or `"scram_sha_512"`.
    #[serde(default = "default_sasl_mechanism")]
    pub sasl_mechanism: String,
    /// SASL username.
    #[serde(default)]
    pub sasl_username: String,
    /// SASL password.
    #[serde(default)]
    pub sasl_password: String,
    /// Compression codec for Kafka RecordBatch v2: `"none"`, `"gzip"`, or `"zstd"` (default).
    #[serde(default = "default_compression")]
    pub compression: String,
    /// Produce acknowledgment mode: `"none"` (0), `"leader"` (1, default), or `"all"` (-1).
    #[serde(default = "default_acks")]
    pub acks: String,
    /// Partitioning strategy: `"round_robin"` (default), `"route_id"`, or `"peer_ip"`.
    #[serde(default = "default_partitioner")]
    pub partitioner: String,
    /// Periodic batch flush interval in milliseconds (100 to 60,000 ms, default: 1000).
    #[serde(default = "default_flush_interval")]
    pub flush_interval_ms: u64,
    /// Maximum records grouped into a single produce batch (1 to 50,000, default: 8000).
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: usize,
    /// TCP/TLS connection timeout in milliseconds (100 to 60,000 ms, default: 2000).
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_ms: u64,
    /// Kafka request timeout in milliseconds (100 to 60,000 ms, default: 5000).
    #[serde(default = "default_request_timeout")]
    pub request_timeout_ms: u64,
    /// Topic metadata refresh interval in milliseconds (1,000 to 3,600,000 ms, default: 300,000 ms = 5 min).
    #[serde(default = "default_metadata_refresh")]
    pub metadata_refresh_ms: u64,
}

fn default_format() -> String {
    "json".into()
}

fn default_transport_security() -> String {
    "plain".into()
}

fn default_sasl_mechanism() -> String {
    "none".into()
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

fn default_connect_timeout() -> u64 {
    2000
}

fn default_request_timeout() -> u64 {
    5000
}

fn default_metadata_refresh() -> u64 {
    300000
}

impl KafkaLogShipperConfig {
    /// Resolves parsed transport security.
    pub fn parsed_transport_security(&self) -> Result<KafkaTransportSecurity, PluginError> {
        KafkaTransportSecurity::parse(&self.transport_security)
    }

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

    /// Resolves parsed SASL mechanism.
    pub fn parsed_sasl_mechanism(&self) -> Result<SaslMechanism, PluginError> {
        SaslMechanism::parse(&self.sasl_mechanism)
    }

    /// Resolves parsed acks mode.
    pub fn parsed_acks(&self) -> Result<KafkaAckMode, PluginError> {
        KafkaAckMode::parse(&self.acks)
    }
}

/// Automatically derives a unique client ID per gateway process (`velda-edge-{hostname}-{pid}`).
///
/// This guarantees that when gateway pods scale out horizontally, each pod maintains
/// a distinct, identifiable client identity on the Kafka broker without manual configuration.
pub fn generate_client_id() -> String {
    let hostname = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("HOST"))
        .unwrap_or_else(|_| {
            let mut buf = [0u8; 256];
            let res =
                unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) };
            if res == 0 {
                let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
                String::from_utf8_lossy(&buf[..len]).trim().to_string()
            } else {
                "node".to_string()
            }
        });
    let pid = std::process::id();
    format!("velda-edge-{hostname}-{pid}")
}
