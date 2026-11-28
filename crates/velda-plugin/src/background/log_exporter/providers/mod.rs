//! Concrete Static-Dispatch Log Provider Engine (`log_exporter::providers`).
//!
//! ```text
//!   EtpInstance.backend (attach_etp) ──► EtpLogAttachment (validated Arc<*Etp> + resolved config)
//!         │
//!         ▼
//!   run(shutdown) ──► LogProvider::build(...)
//!         ├─► Local         (StdLogSink: stdout | stderr | file)
//!         ├─► ClickHouse    (ClickHouseEtp: RowBinary | JSONEachRow)
//!         ├─► Elasticsearch (ElasticsearchEtp: _bulk NDJSON + gzip)
//!         ├─► Loki          (LokiEtp: HTTP JSON | gRPC Proto)
//!         ├─► S3            (S3Etp: SigV4 PUT NDJSON + zstd/gzip/none)
//!         ├─► Kafka         (KafkaEtp: RecordBatch v2 Binary TCP)
//!         └─► Otlp          (OtlpEtp: OTLP HTTP/gRPC Logs)
//! ```

pub mod clickhouse;
pub mod elasticsearch;
pub mod kafka;
pub mod local;
pub mod loki;
pub mod otlp;
pub mod s3;

use std::sync::Arc;

use velda_etp::clickhouse::ClickHouseEtp;
use velda_etp::elasticsearch::ElasticsearchEtp;
use velda_etp::kafka::KafkaEtp;
use velda_etp::loki::LokiEtp;
use velda_etp::otlp::OtlpEtp;
use velda_etp::s3::S3Etp;
use velda_etp::{EtpBackend, EtpInstance};
use velda_observability::TelemetryBatch;

use crate::error::PluginError;

use super::config::{
    ClickHouseTargetConfig, ElasticsearchTargetConfig, EtpLogTargetConfig, KafkaTargetConfig,
    LocalLogTargetConfig, LokiTargetConfig, OtlpLogTargetConfig, S3TargetConfig,
};

use self::clickhouse::ClickHouseLogProvider;
use self::elasticsearch::ElasticsearchLogProvider;
use self::kafka::KafkaLogProvider;
use self::local::LocalLogProvider;
use self::loki::LokiLogProvider;
use self::otlp::OtlpLogProvider;
use self::s3::S3LogProvider;

/// Validated, pre-bound ETP pool handle paired with its resolved target configuration.
#[derive(Clone)]
pub enum EtpLogAttachment {
    ClickHouse {
        etp: Arc<ClickHouseEtp>,
        config: ClickHouseTargetConfig,
    },
    Elasticsearch {
        etp: Arc<ElasticsearchEtp>,
        config: ElasticsearchTargetConfig,
    },
    Loki {
        etp: Arc<LokiEtp>,
        config: LokiTargetConfig,
    },
    S3 {
        etp: Arc<S3Etp>,
        config: S3TargetConfig,
    },
    Kafka {
        etp: Arc<KafkaEtp>,
        config: KafkaTargetConfig,
    },
    Otlp {
        etp: Arc<OtlpEtp>,
        config: OtlpLogTargetConfig,
    },
}

impl EtpLogAttachment {
    /// Resolves target configuration and validates `instance.backend` kind compatibility.
    pub fn from_etp_instance(
        plugin_id: &str,
        cfg: &EtpLogTargetConfig,
        instance: &EtpInstance,
    ) -> Result<Self, PluginError> {
        match &instance.backend {
            EtpBackend::ClickHouse(pool) => Ok(Self::ClickHouse {
                etp: Arc::clone(pool),
                config: cfg.resolve_clickhouse()?,
            }),
            EtpBackend::Elasticsearch(pool) => Ok(Self::Elasticsearch {
                etp: Arc::clone(pool),
                config: cfg.resolve_elasticsearch()?,
            }),
            EtpBackend::Loki(pool) => Ok(Self::Loki {
                etp: Arc::clone(pool),
                config: cfg.resolve_loki()?,
            }),
            EtpBackend::S3(pool) => Ok(Self::S3 {
                etp: Arc::clone(pool),
                config: cfg.resolve_s3()?,
            }),
            EtpBackend::Kafka(pool) => Ok(Self::Kafka {
                etp: Arc::clone(pool),
                config: cfg.resolve_kafka()?,
            }),
            EtpBackend::Otlp(pool) => Ok(Self::Otlp {
                etp: Arc::clone(pool),
                config: cfg.resolve_otlp()?,
            }),
            _ => Err(PluginError::Config(format!(
                "Log plugin '{plugin_id}' requires a supported log ETP (clickhouse, elasticsearch, loki, s3, kafka, otlp) for etp_id '{}', got '{}'",
                cfg.etp_id, instance.kind
            ))),
        }
    }
}

/// Owned, buffer-equipped log provider instance executing inside a target coordinator loop.
pub enum LogProvider {
    Local(LocalLogProvider),
    ClickHouse(ClickHouseLogProvider),
    Elasticsearch(ElasticsearchLogProvider),
    Loki(LokiLogProvider),
    S3(S3LogProvider),
    Kafka(KafkaLogProvider),
    Otlp(OtlpLogProvider),
}

impl LogProvider {
    pub fn from_local(config: LocalLogTargetConfig) -> Self {
        Self::Local(LocalLogProvider::new(config))
    }

    pub fn from_attachment(attachment: &EtpLogAttachment) -> Result<Self, PluginError> {
        match attachment {
            EtpLogAttachment::ClickHouse { etp, config } => Ok(Self::ClickHouse(
                ClickHouseLogProvider::new(Arc::clone(etp), config)?,
            )),
            EtpLogAttachment::Elasticsearch { etp, config } => Ok(Self::Elasticsearch(
                ElasticsearchLogProvider::new(Arc::clone(etp), config),
            )),
            EtpLogAttachment::Loki { etp, config } => {
                Ok(Self::Loki(LokiLogProvider::new(Arc::clone(etp), config)?))
            }
            EtpLogAttachment::S3 { etp, config } => {
                Ok(Self::S3(S3LogProvider::new(Arc::clone(etp), config)))
            }
            EtpLogAttachment::Kafka { etp, config } => {
                Ok(Self::Kafka(KafkaLogProvider::new(Arc::clone(etp), config)?))
            }
            EtpLogAttachment::Otlp { etp, config } => {
                Ok(Self::Otlp(OtlpLogProvider::new(Arc::clone(etp), config)?))
            }
        }
    }

    #[inline]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Local(p) => p.name(),
            Self::ClickHouse(p) => p.name(),
            Self::Elasticsearch(p) => p.name(),
            Self::Loki(p) => p.name(),
            Self::S3(p) => p.name(),
            Self::Kafka(p) => p.name(),
            Self::Otlp(p) => p.name(),
        }
    }

    #[inline]
    pub fn flush_interval_ms(&self) -> u64 {
        match self {
            Self::Local(p) => p.flush_interval_ms(),
            Self::ClickHouse(p) => p.flush_interval_ms,
            Self::Elasticsearch(p) => p.flush_interval_ms,
            Self::Loki(p) => p.flush_interval_ms,
            Self::S3(p) => p.flush_interval_ms,
            Self::Kafka(p) => p.flush_interval_ms,
            Self::Otlp(p) => p.flush_interval_ms,
        }
    }

    #[inline]
    pub fn max_drain_batches(&self) -> usize {
        match self {
            Self::Local(p) => p.max_drain_batches(),
            Self::ClickHouse(_) => 64,
            Self::Elasticsearch(p) => p.max_drain_batches,
            Self::Loki(_) => 64,
            Self::S3(p) => p.max_drain_batches,
            Self::Kafka(_) => 64,
            Self::Otlp(_) => 64,
        }
    }

    #[inline]
    pub fn uses_exponential_backoff(&self) -> bool {
        matches!(self, Self::Otlp(_))
    }

    pub async fn prepare(&mut self) -> Result<(), PluginError> {
        match self {
            Self::Local(p) => p.prepare().await,
            Self::ClickHouse(p) => p.prepare().await,
            Self::Kafka(p) => p.prepare().await,
            _ => Ok(()),
        }
    }

    pub async fn flush_batches(
        &mut self,
        batches: &[TelemetryBatch],
    ) -> Result<usize, PluginError> {
        match self {
            Self::Local(p) => p.flush_batches(batches).await,
            Self::ClickHouse(p) => p.flush_batches(batches).await,
            Self::Elasticsearch(p) => p.flush_batches(batches).await,
            Self::Loki(p) => p.flush_batches(batches).await,
            Self::S3(p) => p.flush_batches(batches).await,
            Self::Kafka(p) => p.flush_batches(batches).await,
            Self::Otlp(p) => p.flush_batches(batches).await,
        }
    }
}
