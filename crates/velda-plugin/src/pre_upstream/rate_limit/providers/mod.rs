//! Pure provider dispatcher for `rate_limit` distributed State-of-Truth (SoT) storage ETPs.
//!
//! ```text
//!   EtpInstance.backend ──► RateLimitSotProvider (pure dispatch across 14 SoT ETPs)
//!         │
//!         ├─► Redis          ──► providers::redis::sync::run_sot_sync_loop
//!         ├─► Valkey         ──► providers::valkey::sync::run_sot_sync_loop
//!         ├─► Dragonfly      ──► providers::dragonfly::sync::run_sot_sync_loop
//!         ├─► AwsElastiCache ──► providers::aws_elasticache::sync::run_sot_sync_loop
//!         ├─► GcpMemorystore ──► providers::gcp_memorystore::sync::run_sot_sync_loop
//!         ├─► Upstash        ──► providers::upstash::sync::run_sot_sync_loop
//!         ├─► Memcached      ──► providers::memcached::sync::run_sot_sync_loop
//!         ├─► DynamoDb       ──► providers::dynamodb::sync::run_sot_sync_loop
//!         ├─► Postgres       ──► providers::postgres::sync::run_sot_sync_loop
//!         ├─► MySql          ──► providers::mysql::sync::run_sot_sync_loop
//!         ├─► Nats           ──► providers::nats::sync::run_sot_sync_loop
//!         ├─► ClickHouse     ──► providers::clickhouse::sync::run_sot_sync_loop
//!         ├─► Elasticsearch  ──► providers::elasticsearch::sync::run_sot_sync_loop
//!         ├─► S3             ──► providers::s3::sync::run_sot_sync_loop
//!         └─► Any other kind (e.g. Kafka, Loki, IdPs) ──► Err(PluginError::Config)
//! ```

pub mod aws_elasticache;
pub mod clickhouse;
pub mod dragonfly;
pub mod dynamodb;
pub mod elasticsearch;
pub mod gcp_memorystore;
pub mod memcached;
pub mod mysql;
pub mod nats;
pub mod postgres;
pub mod redis;
pub mod s3;
pub mod upstash;
pub mod valkey;

use std::sync::Arc;

use tokio::sync::watch;
use velda_etp::aws_elasticache::AwsElastiCacheEtp;
use velda_etp::clickhouse::ClickHouseEtp;
use velda_etp::dragonfly::DragonflyEtp;
use velda_etp::dynamodb::DynamoDbEtp;
use velda_etp::elasticsearch::ElasticsearchEtp;
use velda_etp::gcp_memorystore::GcpMemorystoreEtp;
use velda_etp::memcached::MemcachedEtp;
use velda_etp::mysql::MysqlEtp;
use velda_etp::nats::NatsEtp;
use velda_etp::postgres::PostgresEtp;
use velda_etp::redis::RedisEtp;
use velda_etp::s3::S3Etp;
use velda_etp::upstash::UpstashEtp;
use velda_etp::valkey::ValkeyEtp;
use velda_etp::{EtpBackend, EtpInstance};

use crate::error::PluginError;
use crate::pre_upstream::rate_limit::arena::RateLimitArena;
use crate::pre_upstream::rate_limit::validation::RateLimitConfig;

/// Concrete SoT storage pool handle attached to [`super::RateLimitPlugin`].
#[derive(Debug, Clone)]
pub enum RateLimitSotProvider {
    Redis(Arc<RedisEtp>),
    Valkey(Arc<ValkeyEtp>),
    Dragonfly(Arc<DragonflyEtp>),
    AwsElastiCache(Arc<AwsElastiCacheEtp>),
    GcpMemorystore(Arc<GcpMemorystoreEtp>),
    Upstash(Arc<UpstashEtp>),
    Memcached(Arc<MemcachedEtp>),
    DynamoDb(Arc<DynamoDbEtp>),
    Postgres(Arc<PostgresEtp>),
    MySql(Arc<MysqlEtp>),
    Nats(Arc<NatsEtp>),
    ClickHouse(Arc<ClickHouseEtp>),
    Elasticsearch(Arc<ElasticsearchEtp>),
    S3(Arc<S3Etp>),
}

/// Resolves and validates that the supplied [`EtpInstance`] is one of the 14 supported
/// State-of-Truth storage ETPs for `rate_limit`.
pub fn from_etp_instance(instance: &EtpInstance) -> Result<RateLimitSotProvider, PluginError> {
    match &instance.backend {
        EtpBackend::Redis(pool) => Ok(RateLimitSotProvider::Redis(Arc::clone(pool))),
        EtpBackend::Valkey(pool) => Ok(RateLimitSotProvider::Valkey(Arc::clone(pool))),
        EtpBackend::Dragonfly(pool) => Ok(RateLimitSotProvider::Dragonfly(Arc::clone(pool))),
        EtpBackend::AwsElastiCache(pool) => {
            Ok(RateLimitSotProvider::AwsElastiCache(Arc::clone(pool)))
        }
        EtpBackend::GcpMemorystore(pool) => {
            Ok(RateLimitSotProvider::GcpMemorystore(Arc::clone(pool)))
        }
        EtpBackend::Upstash(pool) => Ok(RateLimitSotProvider::Upstash(Arc::clone(pool))),
        EtpBackend::Memcached(pool) => Ok(RateLimitSotProvider::Memcached(Arc::clone(pool))),
        EtpBackend::DynamoDb(pool) => Ok(RateLimitSotProvider::DynamoDb(Arc::clone(pool))),
        EtpBackend::Postgres(pool) => Ok(RateLimitSotProvider::Postgres(Arc::clone(pool))),
        EtpBackend::MySql(pool) => Ok(RateLimitSotProvider::MySql(Arc::clone(pool))),
        EtpBackend::Nats(pool) => Ok(RateLimitSotProvider::Nats(Arc::clone(pool))),
        EtpBackend::ClickHouse(pool) => Ok(RateLimitSotProvider::ClickHouse(Arc::clone(pool))),
        EtpBackend::Elasticsearch(pool) => {
            Ok(RateLimitSotProvider::Elasticsearch(Arc::clone(pool)))
        }
        EtpBackend::S3(pool) => Ok(RateLimitSotProvider::S3(Arc::clone(pool))),
        other => Err(PluginError::Config(format!(
            "Plugin 'rate_limit' does not support ETP '{}' of non-SoT kind {:?}; \
             supported SoT ETPs: redis, valkey, dragonfly, aws_elasticache, gcp_memorystore, \
             upstash, memcached, dynamodb, postgres, mysql, nats, clickhouse, elasticsearch, s3",
            instance.id, other
        ))),
    }
}

/// Dispatches the Shadow Worker SoT sync loop directly to the resolved provider module.
pub async fn run_sync_loop(
    provider: RateLimitSotProvider,
    arena: Arc<RateLimitArena>,
    config: RateLimitConfig,
    shutdown: watch::Receiver<bool>,
) {
    match provider {
        RateLimitSotProvider::Redis(etp) => {
            redis::sync::run_sot_sync_loop(etp, arena, config, shutdown).await;
        }
        RateLimitSotProvider::Valkey(etp) => {
            valkey::sync::run_sot_sync_loop(etp, arena, config, shutdown).await;
        }
        RateLimitSotProvider::Dragonfly(etp) => {
            dragonfly::sync::run_sot_sync_loop(etp, arena, config, shutdown).await;
        }
        RateLimitSotProvider::AwsElastiCache(etp) => {
            aws_elasticache::sync::run_sot_sync_loop(etp, arena, config, shutdown).await;
        }
        RateLimitSotProvider::GcpMemorystore(etp) => {
            gcp_memorystore::sync::run_sot_sync_loop(etp, arena, config, shutdown).await;
        }
        RateLimitSotProvider::Upstash(etp) => {
            upstash::sync::run_sot_sync_loop(etp, arena, config, shutdown).await;
        }
        RateLimitSotProvider::Memcached(etp) => {
            memcached::sync::run_sot_sync_loop(etp, arena, config, shutdown).await;
        }
        RateLimitSotProvider::DynamoDb(etp) => {
            dynamodb::sync::run_sot_sync_loop(etp, arena, config, shutdown).await;
        }
        RateLimitSotProvider::Postgres(etp) => {
            postgres::sync::run_sot_sync_loop(etp, arena, config, shutdown).await;
        }
        RateLimitSotProvider::MySql(etp) => {
            mysql::sync::run_sot_sync_loop(etp, arena, config, shutdown).await;
        }
        RateLimitSotProvider::Nats(etp) => {
            nats::sync::run_sot_sync_loop(etp, arena, config, shutdown).await;
        }
        RateLimitSotProvider::ClickHouse(etp) => {
            clickhouse::sync::run_sot_sync_loop(etp, arena, config, shutdown).await;
        }
        RateLimitSotProvider::Elasticsearch(etp) => {
            elasticsearch::sync::run_sot_sync_loop(etp, arena, config, shutdown).await;
        }
        RateLimitSotProvider::S3(etp) => {
            s3::sync::run_sot_sync_loop(etp, arena, config, shutdown).await;
        }
    }
}
