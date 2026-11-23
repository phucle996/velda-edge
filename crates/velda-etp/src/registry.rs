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

use crate::clickhouse::ClickHouseEtp;
use crate::config::{EtpEntryConfig, EtpKind};
use crate::elasticsearch::ElasticsearchEtp;
use crate::error::EtpError;
use crate::kafka::KafkaEtp;
use crate::loki::LokiEtp;
use crate::otlp::OtlpEtp;
use crate::redis::RedisEtp;
use crate::s3::S3Etp;
use crate::statsd::StatsdEtp;

/// Concrete, flat enum of per-ETP data source pool engines stored inside [`EtpInstance`].
#[derive(Debug, Clone)]
pub enum EtpBackend {
    Redis(Arc<RedisEtp>),
    ClickHouse(Arc<ClickHouseEtp>),
    S3(Arc<S3Etp>),
    Loki(Arc<LokiEtp>),
    Elasticsearch(Arc<ElasticsearchEtp>),
    Otlp(Arc<OtlpEtp>),
    Kafka(Arc<KafkaEtp>),
    Statsd(Arc<StatsdEtp>),
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
