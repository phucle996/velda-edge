//! External Third-Party instance registry (`EtpRegistry`), flat `EtpBackend` enum,
//! standalone lifecycle functions, and Make-Before-Break hot reload.
//!
//! ```text
//!   Sync Reload (etp.bin) calls registry::reconcile(&current, new_configs, prewarm)
//!         │
//!         ▼
//!   [1] For each cfg in new_configs: compute fp = cfg.fingerprint() [64-bit FNV-1a]
//!         │
//!         ├─► current.entries.get(id).fingerprint == fp
//!         │     └──► Arc::clone(existing)  [Zero reconnect, active Arena preserved]
//!         │
//!         └─► New or modified cfg
//!               ├──► build_instance(cfg) ──► allocates new (N+1)*4096B Arena
//!               ├──► if prewarm == true  ──► warm_instance(&instance).await? BEFORE swap
//!               └──► Store in new EtpRegistry; old Arc<EtpInstance> drains as Leases drop
//!
//!   Hot-Path Lookup & Dispatch Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ EtpRegistry { pub entries: HashMap<String, Arc<EtpInstance>> }             │
//!   │   └─► EtpInstance { id, kind, fingerprint: u64, config, backend }          │
//!   │         └─► EtpBackend (flat enum, 8 variants):                            │
//!   │             Redis | ClickHouse | S3 | Loki | Elasticsearch | Otlp |        │
//!   │             Kafka | Statsd  ──► matched directly by callers / plugins      │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::auth0::Auth0Etp;
use crate::authelia::AutheliaEtp;
use crate::aws_elasticache::AwsElastiCacheEtp;
use crate::aws_secrets_manager::AwsSecretsManagerEtp;
use crate::clickhouse::ClickHouseEtp;
use crate::cognito::CognitoEtp;
use crate::config::{EtpEntryConfig, EtpKind};
use crate::dragonfly::DragonflyEtp;
use crate::dynamodb::DynamoDbEtp;
use crate::elasticsearch::ElasticsearchEtp;
use crate::entra_id::EntraIdEtp;
use crate::error::EtpError;
use crate::gcp_memorystore::GcpMemorystoreEtp;
use crate::hydra::HydraEtp;
use crate::kafka::KafkaEtp;
use crate::keycloak::KeycloakEtp;
use crate::loki::LokiEtp;
use crate::memcached::MemcachedEtp;
use crate::mysql::MysqlEtp;
use crate::nats::NatsEtp;
use crate::oidc::OidcEtp;
use crate::okta::OktaEtp;
use crate::otlp::OtlpEtp;
use crate::postgres::PostgresEtp;
use crate::redis::RedisEtp;
use crate::s3::S3Etp;
use crate::statsd::StatsdEtp;
use crate::upstash::UpstashEtp;
use crate::valkey::ValkeyEtp;
use crate::vault::VaultEtp;

/// Concrete, flat enum of per-ETP data source pool engines stored inside [`EtpInstance`].
#[derive(Debug, Clone)]
pub enum EtpBackend {
    Redis(Arc<RedisEtp>),
    Valkey(Arc<ValkeyEtp>),
    Dragonfly(Arc<DragonflyEtp>),
    AwsElastiCache(Arc<AwsElastiCacheEtp>),
    GcpMemorystore(Arc<GcpMemorystoreEtp>),
    Upstash(Arc<UpstashEtp>),
    ClickHouse(Arc<ClickHouseEtp>),
    S3(Arc<S3Etp>),
    Loki(Arc<LokiEtp>),
    Elasticsearch(Arc<ElasticsearchEtp>),
    Otlp(Arc<OtlpEtp>),
    Kafka(Arc<KafkaEtp>),
    Statsd(Arc<StatsdEtp>),
    Vault(Arc<VaultEtp>),
    AwsSecretsManager(Arc<AwsSecretsManagerEtp>),
    Keycloak(Arc<KeycloakEtp>),
    Hydra(Arc<HydraEtp>),
    Authelia(Arc<AutheliaEtp>),
    Okta(Arc<OktaEtp>),
    Auth0(Arc<Auth0Etp>),
    EntraId(Arc<EntraIdEtp>),
    Cognito(Arc<CognitoEtp>),
    Oidc(Arc<OidcEtp>),
    DynamoDb(Arc<DynamoDbEtp>),
    Memcached(Arc<MemcachedEtp>),
    Postgres(Arc<PostgresEtp>),
    MySql(Arc<MysqlEtp>),
    Nats(Arc<NatsEtp>),
}

/// Instantiated External Third-Party provider entry holding its configuration,
/// fingerprint, and concrete per-ETP [`EtpBackend`] pool handle.
#[derive(Debug)]
pub struct EtpInstance {
    pub id: String,
    pub kind: EtpKind,
    pub fingerprint: u64,
    pub config: EtpEntryConfig,
    pub backend: EtpBackend,
}

/// Snapshot-immutable registry of active External Third-Party instances keyed by `id`.
#[derive(Debug, Clone, Default)]
pub struct EtpRegistry {
    pub entries: HashMap<String, Arc<EtpInstance>>,
}

/// Instantiates a single [`EtpInstance`] from a validated [`EtpEntryConfig`].
pub fn build_instance(config: &EtpEntryConfig) -> Result<EtpInstance, EtpError> {
    config.validate()?;
    let backend = match config.kind {
        EtpKind::Redis => EtpBackend::Redis(Arc::new(RedisEtp::from_entry(config)?)),
        EtpKind::ClickHouse => EtpBackend::ClickHouse(Arc::new(ClickHouseEtp::from_entry(config)?)),
        EtpKind::S3 => EtpBackend::S3(Arc::new(S3Etp::from_entry(config)?)),
        EtpKind::Loki => EtpBackend::Loki(Arc::new(LokiEtp::from_entry(config)?)),
        EtpKind::Elasticsearch => {
            EtpBackend::Elasticsearch(Arc::new(ElasticsearchEtp::from_entry(config)?))
        }
        EtpKind::Otlp => EtpBackend::Otlp(Arc::new(OtlpEtp::from_entry(config)?)),
        EtpKind::Kafka => EtpBackend::Kafka(Arc::new(KafkaEtp::from_entry(config)?)),
        EtpKind::Statsd => EtpBackend::Statsd(Arc::new(StatsdEtp::from_entry(config)?)),
        EtpKind::Vault => EtpBackend::Vault(Arc::new(VaultEtp::from_entry(config)?)),
        EtpKind::AwsSecretsManager => {
            EtpBackend::AwsSecretsManager(Arc::new(AwsSecretsManagerEtp::from_entry(config)?))
        }
        EtpKind::Keycloak => EtpBackend::Keycloak(Arc::new(KeycloakEtp::from_entry(config)?)),
        EtpKind::Hydra => EtpBackend::Hydra(Arc::new(HydraEtp::from_entry(config)?)),
        EtpKind::Authelia => EtpBackend::Authelia(Arc::new(AutheliaEtp::from_entry(config)?)),
        EtpKind::Okta => EtpBackend::Okta(Arc::new(OktaEtp::from_entry(config)?)),
        EtpKind::Auth0 => EtpBackend::Auth0(Arc::new(Auth0Etp::from_entry(config)?)),
        EtpKind::EntraId => EtpBackend::EntraId(Arc::new(EntraIdEtp::from_entry(config)?)),
        EtpKind::Cognito => EtpBackend::Cognito(Arc::new(CognitoEtp::from_entry(config)?)),
        EtpKind::Oidc => EtpBackend::Oidc(Arc::new(OidcEtp::from_entry(config)?)),
        EtpKind::DynamoDb => EtpBackend::DynamoDb(Arc::new(DynamoDbEtp::from_entry(config)?)),
        EtpKind::Memcached => EtpBackend::Memcached(Arc::new(MemcachedEtp::from_entry(config)?)),
        EtpKind::Postgres => EtpBackend::Postgres(Arc::new(PostgresEtp::from_entry(config)?)),
        EtpKind::MySql => EtpBackend::MySql(Arc::new(MysqlEtp::from_entry(config)?)),
        EtpKind::Nats => EtpBackend::Nats(Arc::new(NatsEtp::from_entry(config)?)),
        EtpKind::Valkey => EtpBackend::Valkey(Arc::new(ValkeyEtp::from_entry(config)?)),
        EtpKind::Dragonfly => EtpBackend::Dragonfly(Arc::new(DragonflyEtp::from_entry(config)?)),
        EtpKind::AwsElastiCache => {
            EtpBackend::AwsElastiCache(Arc::new(AwsElastiCacheEtp::from_entry(config)?))
        }
        EtpKind::GcpMemorystore => {
            EtpBackend::GcpMemorystore(Arc::new(GcpMemorystoreEtp::from_entry(config)?))
        }
        EtpKind::Upstash => EtpBackend::Upstash(Arc::new(UpstashEtp::from_entry(config)?)),
    };

    Ok(EtpInstance {
        id: config.id.clone(),
        kind: config.kind,
        fingerprint: config.fingerprint(),
        config: config.clone(),
        backend,
    })
}

/// Builds an [`EtpRegistry`] synchronously without pre-warming network connections.
///
/// Suitable for cold-start bootstrap paths executed before an async runtime starts.
pub fn build_registry(configs: &[EtpEntryConfig]) -> Result<EtpRegistry, EtpError> {
    let mut entries = HashMap::with_capacity(configs.len());
    let mut seen = HashSet::with_capacity(configs.len());

    for cfg in configs {
        if !seen.insert(cfg.id.clone()) {
            return Err(EtpError::Config(format!(
                "Duplicate ETP entry id '{}'",
                cfg.id
            )));
        }
        let instance = Arc::new(build_instance(cfg)?);
        entries.insert(cfg.id.clone(), instance);
    }

    Ok(EtpRegistry { entries })
}

/// Pre-warms a single [`EtpInstance`]'s underlying connection pool.
pub async fn warm_instance(inst: &EtpInstance) -> Result<(), EtpError> {
    match &inst.backend {
        EtpBackend::Redis(pool) => pool.warm().await,
        EtpBackend::ClickHouse(pool) => pool.warm().await,
        EtpBackend::S3(pool) => pool.warm().await,
        EtpBackend::Loki(pool) => pool.warm().await,
        EtpBackend::Elasticsearch(pool) => pool.warm().await,
        EtpBackend::Otlp(pool) => pool.warm().await,
        EtpBackend::Kafka(pool) => pool.warm().await,
        EtpBackend::Statsd(pool) => pool.warm().await,
        EtpBackend::Vault(pool) => pool.warm().await,
        EtpBackend::AwsSecretsManager(pool) => pool.warm().await,
        EtpBackend::Keycloak(pool) => pool.warm().await,
        EtpBackend::Hydra(pool) => pool.warm().await,
        EtpBackend::Authelia(pool) => pool.warm().await,
        EtpBackend::Okta(pool) => pool.warm().await,
        EtpBackend::Auth0(pool) => pool.warm().await,
        EtpBackend::EntraId(pool) => pool.warm().await,
        EtpBackend::Cognito(pool) => pool.warm().await,
        EtpBackend::Oidc(pool) => pool.warm().await,
        EtpBackend::DynamoDb(pool) => pool.warm().await,
        EtpBackend::Memcached(pool) => pool.warm().await,
        EtpBackend::Postgres(pool) => pool.warm().await,
        EtpBackend::MySql(pool) => pool.warm().await,
        EtpBackend::Nats(pool) => pool.warm().await,
        EtpBackend::Valkey(pool) => pool.warm().await,
        EtpBackend::Dragonfly(pool) => pool.warm().await,
        EtpBackend::AwsElastiCache(pool) => pool.warm().await,
        EtpBackend::GcpMemorystore(pool) => pool.warm().await,
        EtpBackend::Upstash(pool) => pool.warm().await,
    }
}

/// Pre-warms all registered ETP entries in `registry`.
pub async fn warm_all(registry: &EtpRegistry) -> Result<(), EtpError> {
    for inst in registry.entries.values() {
        warm_instance(inst).await?;
    }
    Ok(())
}

/// Reconciles `current` against `new_configs` using Make-Before-Break semantics:
///
/// 1. Unchanged entries (`existing.fingerprint == cfg.fingerprint()`) are reused via
///    `Arc::clone` with zero reconnection or downtime.
/// 2. New or modified entries are instantiated and (when `prewarm == true`) pre-warmed
///    via `warm_instance(&instance).await?` before returning the candidate `EtpRegistry`.
/// 3. Removed or replaced `Arc<EtpInstance>` handles stay alive until all active
///    typed leases referencing them drop.
pub async fn reconcile(
    current: &EtpRegistry,
    new_configs: &[EtpEntryConfig],
    prewarm: bool,
) -> Result<EtpRegistry, EtpError> {
    let mut new_entries = HashMap::with_capacity(new_configs.len());
    let mut seen = HashSet::with_capacity(new_configs.len());

    for cfg in new_configs {
        if !seen.insert(cfg.id.clone()) {
            return Err(EtpError::Config(format!(
                "Duplicate ETP entry id '{}'",
                cfg.id
            )));
        }

        let fp = cfg.fingerprint();
        if let Some(existing) = current.entries.get(&cfg.id)
            && existing.fingerprint == fp
        {
            new_entries.insert(cfg.id.clone(), Arc::clone(existing));
            continue;
        }

        let instance = Arc::new(build_instance(cfg)?);
        if prewarm {
            warm_instance(&instance).await?;
        }
        new_entries.insert(cfg.id.clone(), instance);
    }

    Ok(EtpRegistry {
        entries: new_entries,
    })
}
