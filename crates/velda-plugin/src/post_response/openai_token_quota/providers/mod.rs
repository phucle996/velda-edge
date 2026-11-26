//! Pure provider dispatcher for `openai_token_quota` distributed SoT ETPs.
//!
//! ```text
//!   EtpInstance.backend ──► QuotaSyncProvider (pure dispatch across 9 SoT ETPs)
//!         ├─► Redis          ──► providers::redis::sync::run_quota_sync
//!         ├─► Valkey         ──► providers::valkey::sync::run_quota_sync
//!         ├─► Dragonfly      ──► providers::dragonfly::sync::run_quota_sync
//!         ├─► AwsElastiCache ──► providers::aws_elasticache::sync::run_quota_sync
//!         ├─► GcpMemorystore ──► providers::gcp_memorystore::sync::run_quota_sync
//!         ├─► Upstash        ──► providers::upstash::sync::run_quota_sync
//!         ├─► Postgres       ──► providers::postgres::sync::run_quota_sync
//!         ├─► MySql          ──► providers::mysql::sync::run_quota_sync
//!         └─► DynamoDb       ──► providers::dynamodb::sync::run_quota_sync
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
use std::sync::atomic::AtomicBool;

use tokio::sync::{Notify, watch};
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
use crate::post_response::openai_token_quota::local::SharedUsageArena;

/// Concrete SoT pool handle attached to [`super::OpenAiTokenQuotaPlugin`].
#[derive(Debug, Clone)]
pub enum QuotaSyncProvider {
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

/// Parameters required to execute the background quota lease loop on a Shadow Worker.
pub struct QuotaSyncParams {
    pub plugin_id: String,
    pub arena: Arc<SharedUsageArena>,
    pub notify: Arc<Notify>,
    pub connected: Arc<AtomicBool>,
    pub provider: QuotaSyncProvider,
    pub micro_quota_batch: u64,
    pub batch_users: usize,
    pub sync_interval_ms: u64,
    pub pbww: u32,
}

/// Validates and maps an [`EtpInstance`] onto one of the 9 supported atomic/durable SoT
/// backends for `openai_token_quota`.
pub fn from_etp_instance(
    plugin_id: &str,
    etp_id: Option<&str>,
    instance: &EtpInstance,
) -> Result<QuotaSyncProvider, PluginError> {
    match &instance.backend {
        EtpBackend::Redis(pool) => Ok(QuotaSyncProvider::Redis(Arc::clone(pool))),
        EtpBackend::Valkey(pool) => Ok(QuotaSyncProvider::Valkey(Arc::clone(pool))),
        EtpBackend::Dragonfly(pool) => Ok(QuotaSyncProvider::Dragonfly(Arc::clone(pool))),
        EtpBackend::AwsElastiCache(pool) => Ok(QuotaSyncProvider::AwsElastiCache(Arc::clone(pool))),
        EtpBackend::GcpMemorystore(pool) => Ok(QuotaSyncProvider::GcpMemorystore(Arc::clone(pool))),
        EtpBackend::Upstash(pool) => Ok(QuotaSyncProvider::Upstash(Arc::clone(pool))),
        EtpBackend::Postgres(pool) => Ok(QuotaSyncProvider::Postgres(Arc::clone(pool))),
        EtpBackend::MySql(pool) => Ok(QuotaSyncProvider::MySql(Arc::clone(pool))),
        EtpBackend::DynamoDb(pool) => Ok(QuotaSyncProvider::DynamoDb(Arc::clone(pool))),
        _ => Err(PluginError::Config(format!(
            "Plugin '{}' (openai_token_quota) requires a supported SoT ETP (redis, valkey, dragonfly, aws_elasticache, gcp_memorystore, upstash, postgres, mysql, dynamodb) for etp_id '{}', got '{}'",
            plugin_id,
            etp_id.unwrap_or(""),
            instance.kind
        ))),
    }
}

/// Dispatches the cooperative micro-quota sync loop to the resolved provider module.
pub async fn run_sync_loop(
    params: QuotaSyncParams,
    shutdown: watch::Receiver<bool>,
) -> Result<(), PluginError> {
    match params.provider {
        QuotaSyncProvider::Redis(etp) => {
            redis::sync::run_quota_sync(
                params.plugin_id,
                etp,
                params.arena,
                params.notify,
                params.connected,
                params.micro_quota_batch,
                params.batch_users,
                params.sync_interval_ms,
                params.pbww,
                shutdown,
            )
            .await
        }
        QuotaSyncProvider::Valkey(etp) => {
            valkey::sync::run_quota_sync(
                params.plugin_id,
                etp,
                params.arena,
                params.notify,
                params.connected,
                params.micro_quota_batch,
                params.batch_users,
                params.sync_interval_ms,
                params.pbww,
                shutdown,
            )
            .await
        }
        QuotaSyncProvider::Dragonfly(etp) => {
            dragonfly::sync::run_quota_sync(
                params.plugin_id,
                etp,
                params.arena,
                params.notify,
                params.connected,
                params.micro_quota_batch,
                params.batch_users,
                params.sync_interval_ms,
                params.pbww,
                shutdown,
            )
            .await
        }
        QuotaSyncProvider::AwsElastiCache(etp) => {
            aws_elasticache::sync::run_quota_sync(
                params.plugin_id,
                etp,
                params.arena,
                params.notify,
                params.connected,
                params.micro_quota_batch,
                params.batch_users,
                params.sync_interval_ms,
                params.pbww,
                shutdown,
            )
            .await
        }
        QuotaSyncProvider::GcpMemorystore(etp) => {
            gcp_memorystore::sync::run_quota_sync(
                params.plugin_id,
                etp,
                params.arena,
                params.notify,
                params.connected,
                params.micro_quota_batch,
                params.batch_users,
                params.sync_interval_ms,
                params.pbww,
                shutdown,
            )
            .await
        }
        QuotaSyncProvider::Upstash(etp) => {
            upstash::sync::run_quota_sync(
                params.plugin_id,
                etp,
                params.arena,
                params.notify,
                params.connected,
                params.micro_quota_batch,
                params.batch_users,
                params.sync_interval_ms,
                params.pbww,
                shutdown,
            )
            .await
        }
        QuotaSyncProvider::Postgres(etp) => {
            postgres::sync::run_quota_sync(
                params.plugin_id,
                etp,
                params.arena,
                params.notify,
                params.connected,
                params.micro_quota_batch,
                params.batch_users,
                params.sync_interval_ms,
                params.pbww,
                shutdown,
            )
            .await
        }
        QuotaSyncProvider::MySql(etp) => {
            mysql::sync::run_quota_sync(
                params.plugin_id,
                etp,
                params.arena,
                params.notify,
                params.connected,
                params.micro_quota_batch,
                params.batch_users,
                params.sync_interval_ms,
                params.pbww,
                shutdown,
            )
            .await
        }
        QuotaSyncProvider::DynamoDb(etp) => {
            dynamodb::sync::run_quota_sync(
                params.plugin_id,
                etp,
                params.arena,
                params.notify,
                params.connected,
                params.micro_quota_batch,
                params.batch_users,
                params.sync_interval_ms,
                params.pbww,
                shutdown,
            )
            .await
        }
    }
}
