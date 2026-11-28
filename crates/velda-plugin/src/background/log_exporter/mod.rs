//! Consolidated Access Log Exporter Background Plugin (`log_exporter`).
//!
//! Exports batches from [`velda_observability::TelemetryStore`] across one or more declared
//! targets routed either locally (`mode: "local"`: stdout/stderr/file) or via `velda-etp`
//! (`mode: "etp"`: ClickHouse, Elasticsearch, Loki, S3, Kafka, OTLP).

pub mod config;
pub mod plugin;
pub mod providers;
pub mod validation;

pub use config::{
    ClickHouseTargetConfig, CompressionCodec, ElasticsearchTargetConfig, EsBulkOpType,
    EsCompression, EtpLogTargetConfig, KafkaAckMode, KafkaLogFormat, KafkaTargetConfig,
    LocalLogTargetConfig, LogExporterConfig, LogTargetConfig, LokiTargetConfig,
    OtlpLogTargetConfig, PartitionStrategy, S3Compression, S3TargetConfig, TargetStream,
    VELDA_ACCESS_LOGS_TABLE,
};
pub use plugin::LogExporterPlugin;
pub use validation::validate_log_exporter_config;
