//! Pure provider dispatcher for `api_metering` distributed SoT ETPs.
//!
//! ```text
//!   EtpInstance.backend ──► ApiMeteringProvider (pure dispatch across 9 SoT ETPs)
//!         ├─► Redis          ──► providers::redis::sync::run_shadow_sync
//!         ├─► Valkey         ──► providers::valkey::sync::run_shadow_sync
//!         ├─► Dragonfly      ──► providers::dragonfly::sync::run_shadow_sync
//!         ├─► AwsElastiCache ──► providers::aws_elasticache::sync::run_shadow_sync
//!         ├─► GcpMemorystore ──► providers::gcp_memorystore::sync::run_shadow_sync
//!         ├─► Upstash        ──► providers::upstash::sync::run_shadow_sync
//!         ├─► Postgres       ──► providers::postgres::sync::run_shadow_sync
//!         ├─► MySql          ──► providers::mysql::sync::run_shadow_sync
//!         └─► DynamoDb       ──► providers::dynamodb::sync::run_shadow_sync
//! ```

pub mod aws_elasticache;
pub mod dragonfly;
pub mod dynamodb;
pub mod gcp_memorystore;
pub mod mysql;
pub mod postgres;
pub mod redis;
pub mod upstash;
pub mod valkey;

use std::sync::Arc;

use tokio::sync::watch;
use velda_etp::aws_elasticache::AwsElastiCacheEtp;
use velda_etp::dragonfly::DragonflyEtp;
use velda_etp::dynamodb::DynamoDbEtp;
use velda_etp::gcp_memorystore::GcpMemorystoreEtp;
use velda_etp::mysql::MysqlEtp;
use velda_etp::postgres::PostgresEtp;
use velda_etp::redis::RedisEtp;
use velda_etp::upstash::UpstashEtp;
use velda_etp::valkey::ValkeyEtp;
use velda_etp::{EtpBackend, EtpInstance};

use crate::error::PluginError;
use crate::pre_upstream::api_metering::algorithm::ShardedMeteringState;

/// Concrete SoT pool handle attached to [`super::ApiMeteringPlugin`].
#[derive(Debug, Clone)]
pub enum ApiMeteringProvider {
    Redis(Arc<RedisEtp>),
    Valkey(Arc<ValkeyEtp>),
    Dragonfly(Arc<DragonflyEtp>),
    AwsElastiCache(Arc<AwsElastiCacheEtp>),
    GcpMemorystore(Arc<GcpMemorystoreEtp>),
    Upstash(Arc<UpstashEtp>),
    Postgres(Arc<PostgresEtp>),
    MySql(Arc<MysqlEtp>),
    DynamoDb(Arc<DynamoDbEtp>),
}

/// Validates and maps an [`EtpInstance`] onto one of the 9 supported atomic/durable SoT
/// backends for `api_metering`.
pub fn from_etp_instance(
    plugin_id: &str,
    etp_id: Option<&str>,
    instance: &EtpInstance,
) -> Result<ApiMeteringProvider, PluginError> {
    match &instance.backend {
        EtpBackend::Redis(pool) => Ok(ApiMeteringProvider::Redis(Arc::clone(pool))),
        EtpBackend::Valkey(pool) => Ok(ApiMeteringProvider::Valkey(Arc::clone(pool))),
        EtpBackend::Dragonfly(pool) => Ok(ApiMeteringProvider::Dragonfly(Arc::clone(pool))),
        EtpBackend::AwsElastiCache(pool) => {
            Ok(ApiMeteringProvider::AwsElastiCache(Arc::clone(pool)))
        }
        EtpBackend::GcpMemorystore(pool) => {
            Ok(ApiMeteringProvider::GcpMemorystore(Arc::clone(pool)))
        }
        EtpBackend::Upstash(pool) => Ok(ApiMeteringProvider::Upstash(Arc::clone(pool))),
        EtpBackend::Postgres(pool) => Ok(ApiMeteringProvider::Postgres(Arc::clone(pool))),
        EtpBackend::MySql(pool) => Ok(ApiMeteringProvider::MySql(Arc::clone(pool))),
        EtpBackend::DynamoDb(pool) => Ok(ApiMeteringProvider::DynamoDb(Arc::clone(pool))),
        _ => Err(PluginError::Config(format!(
            "Plugin '{}' (api_metering) requires a supported SoT ETP (redis, valkey, dragonfly, aws_elasticache, gcp_memorystore, upstash, postgres, mysql, dynamodb) for etp_id '{}', got '{}'",
            plugin_id,
            etp_id.unwrap_or(""),
            instance.kind
        ))),
    }
}

/// Dispatches the Shadow Worker delta reconciliation loop to the resolved provider module.
pub async fn run_sync_loop(
    provider: ApiMeteringProvider,
    state: Arc<ShardedMeteringState>,
    sync_interval_ms: u64,
    key_prefix: String,
    initial_balance: i64,
    shutdown: watch::Receiver<bool>,
) -> Result<(), PluginError> {
    match provider {
        ApiMeteringProvider::Redis(etp) => {
            redis::sync::run_shadow_sync(
                etp,
                state,
                sync_interval_ms,
                key_prefix,
                initial_balance,
                shutdown,
            )
            .await
        }
        ApiMeteringProvider::Valkey(etp) => {
            valkey::sync::run_shadow_sync(
                etp,
                state,
                sync_interval_ms,
                key_prefix,
                initial_balance,
                shutdown,
            )
            .await
        }
        ApiMeteringProvider::Dragonfly(etp) => {
            dragonfly::sync::run_shadow_sync(
                etp,
                state,
                sync_interval_ms,
                key_prefix,
                initial_balance,
                shutdown,
            )
            .await
        }
        ApiMeteringProvider::AwsElastiCache(etp) => {
            aws_elasticache::sync::run_shadow_sync(
                etp,
                state,
                sync_interval_ms,
                key_prefix,
                initial_balance,
                shutdown,
            )
            .await
        }
        ApiMeteringProvider::GcpMemorystore(etp) => {
            gcp_memorystore::sync::run_shadow_sync(
                etp,
                state,
                sync_interval_ms,
                key_prefix,
                initial_balance,
                shutdown,
            )
            .await
        }
        ApiMeteringProvider::Upstash(etp) => {
            upstash::sync::run_shadow_sync(
                etp,
                state,
                sync_interval_ms,
                key_prefix,
                initial_balance,
                shutdown,
            )
            .await
        }
        ApiMeteringProvider::Postgres(etp) => {
            postgres::sync::run_shadow_sync(
                etp,
                state,
                sync_interval_ms,
                key_prefix,
                initial_balance,
                shutdown,
            )
            .await
        }
        ApiMeteringProvider::MySql(etp) => {
            mysql::sync::run_shadow_sync(
                etp,
                state,
                sync_interval_ms,
                key_prefix,
                initial_balance,
                shutdown,
            )
            .await
        }
        ApiMeteringProvider::DynamoDb(etp) => {
            dynamodb::sync::run_shadow_sync(
                etp,
                state,
                sync_interval_ms,
                key_prefix,
                initial_balance,
                shutdown,
            )
            .await
        }
    }
}
