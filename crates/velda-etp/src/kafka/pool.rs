//! Lock-free Kafka ETP broker connection pool (`KafkaEtp`) backed by [`EtpPageArena<EtpStream>`]
//! and per-broker [`ColdConnectBlueprint`]s.
//!
//! ```text
//!   warm() / acquire() / try_acquire()
//!         │
//!         ├─► Ready hit (TZCNT on ready_bitmap)
//!         │     └─► prefetch_l1_slot(Slot[idx]) -> borrow in-place -> KafkaLease(idx)
//!         │
//!         └─► Miss / Burst Spillover ──► connect_fresh():
//!               1. start = broker_cursor.fetch_add(1) % blueprints.len()
//!               2. Failover across blueprints[(start + offset) % len].connect_stream()
//!               3. write_leased_slot(idx, stream) -> KafkaLease(idx)
//!
//!   Cold Blueprints & Page Arena Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ blueprints: Vec<ColdConnectBlueprint> (integer SocketAddr per broker)      │
//!   ├────────────────────────────────────────────────────────────────────────────┤
//!   │ Arena: Cache Line 0 (64B) | Slot[idx] at (idx << 6) + 64 | +4096B Spare    │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::Semaphore;

use crate::arena::{EtpPageArena, ReadyAcquire};
use crate::config::EtpEntryConfig;
use crate::connect::{ColdConnectBlueprint, EtpStream, build_tls_target};
use crate::error::EtpError;
use crate::kafka::config::KafkaEtpConfig;
use crate::kafka::lease::KafkaLease;

/// Lock-free bounded pool of connections to Apache Kafka brokers.
pub struct KafkaEtp {
    pub config: KafkaEtpConfig,
    pub blueprints: Vec<ColdConnectBlueprint>,
    pub arena: EtpPageArena<EtpStream>,
    pub semaphore: Semaphore,
    pub connect_timeout: Duration,
    pub idle_timeout_secs: u64,
    broker_cursor: AtomicUsize,
}

impl fmt::Debug for KafkaEtp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaEtp")
            .field("id", &self.config.id)
            .field("brokers", &self.config.brokers)
            .field("client_id", &self.config.client_id)
            .field("baseline_slots", &self.arena.baseline_slots)
            .field("total_slots", &self.arena.total_slots)
            .field("page_count", &self.arena.page_count)
            .field("idle_len", &self.arena.ready_count())
            .finish()
    }
}

impl KafkaEtp {
    /// Creates a new `KafkaEtp` pool from a generic [`EtpEntryConfig`].
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        Self::new(KafkaEtpConfig::from_entry(entry)?)
    }

    /// Creates a new `KafkaEtp` pool from a parsed [`KafkaEtpConfig`].
    pub fn new(config: KafkaEtpConfig) -> Result<Self, EtpError> {
        if config.resolved_brokers.is_empty() {
            return Err(EtpError::Config(format!(
                "Kafka ETP '{}' has no brokers",
                config.id
            )));
        }

        let connect_timeout = Duration::from_millis(config.pool.connect_timeout_ms.max(1));
        let idle_timeout_secs = config.pool.idle_timeout_ms.div_ceil(1000);

        let mut blueprints = Vec::with_capacity(config.resolved_brokers.len());
        for (host, port) in &config.resolved_brokers {
            let tls_target = build_tls_target(&config.id, host, config.tls.as_ref())?;
            blueprints.push(ColdConnectBlueprint::new(
                &config.id,
                host,
                *port,
                tls_target,
                connect_timeout,
            ));
        }

        let arena = EtpPageArena::new(config.pool.pool_size);
        let semaphore = Semaphore::new(arena.total_slots);

        Ok(Self {
            config,
            blueprints,
            arena,
            semaphore,
            connect_timeout,
            idle_timeout_secs,
            broker_cursor: AtomicUsize::new(0),
        })
    }

    pub(crate) async fn connect_fresh(&self) -> Result<EtpStream, EtpError> {
        let count = self.blueprints.len();
        let start = self.broker_cursor.fetch_add(1, Ordering::Relaxed) % count;
        let mut last_err: Option<EtpError> = None;

        for offset in 0..count {
            match self.blueprints[(start + offset) % count]
                .connect_stream()
                .await
            {
                Ok(stream) => return Ok(stream),
                Err(err) => last_err = Some(err),
            }
        }

        Err(last_err.unwrap_or_else(|| {
            EtpError::Config(format!("Kafka ETP '{}' has no brokers", self.config.id))
        }))
    }

    /// Pre-warms up to `baseline_slots` broker connections.
    pub async fn warm(&self) -> Result<(), EtpError> {
        let needed = self
            .arena
            .baseline_slots
            .saturating_sub(self.arena.baseline_active_count());
        if needed == 0 {
            return Ok(());
        }
        let mut permits = Vec::with_capacity(needed);
        for _ in 0..needed {
            match self.semaphore.try_acquire() {
                Ok(p) => permits.push(p),
                Err(_) => break,
            }
        }
        if permits.is_empty() {
            return Ok(());
        }
        let mut first_err: Option<EtpError> = None;
        for _ in 0..permits.len() {
            let Some(guard) = self.arena.claim_empty_slot(false) else {
                break;
            };
            match self.connect_fresh().await {
                Ok(stream) => {
                    guard.commit_warm(stream);
                }
                Err(err) => {
                    drop(guard);
                    if first_err.is_none() {
                        first_err = Some(err);
                    }
                }
            }
        }
        drop(permits);
        if let Some(err) = first_err {
            return Err(err);
        }
        Ok(())
    }

    /// Non-blocking fast path: borrows a warm Kafka lease in-place if available.
    pub fn try_acquire(self: &Arc<Self>) -> Option<KafkaLease> {
        let permit = self.semaphore.try_acquire().ok()?;
        match self.arena.try_acquire_ready_slot(self.idle_timeout_secs) {
            ReadyAcquire::Hit(slot_idx) => {
                permit.forget();
                Some(KafkaLease::new(Arc::clone(self), slot_idx))
            }
            ReadyAcquire::Empty { discarded } => {
                drop(permit);
                if discarded {
                    self.trigger_replenish();
                }
                None
            }
        }
    }

    /// Acquires a Kafka broker connection lease.
    pub async fn acquire(self: &Arc<Self>) -> Result<KafkaLease, EtpError> {
        let permit =
            match tokio::time::timeout(self.connect_timeout, self.semaphore.acquire()).await {
                Ok(Ok(permit)) => permit,
                _ => {
                    return Err(EtpError::PoolExhausted {
                        id: self.config.id.clone(),
                    });
                }
            };

        if let ReadyAcquire::Hit(slot_idx) =
            self.arena.try_acquire_ready_slot(self.idle_timeout_secs)
        {
            permit.forget();
            return Ok(KafkaLease::new(Arc::clone(self), slot_idx));
        }

        let Some(guard) = self.arena.claim_empty_slot(true) else {
            return Err(EtpError::PoolExhausted {
                id: self.config.id.clone(),
            });
        };

        let stream = self.connect_fresh().await?;
        let slot_idx = guard.commit_leased(stream);
        permit.forget();
        Ok(KafkaLease::new(Arc::clone(self), slot_idx))
    }

    /// Lock-free `AtomicBool` CAS background replenishment trigger.
    pub(crate) fn trigger_replenish(self: &Arc<Self>) {
        if !self.arena.try_begin_replenish() {
            return;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            self.arena.abort_replenish();
            return;
        };
        let weak = Arc::downgrade(self);
        handle.spawn(async move {
            loop {
                let Some(pool) = weak.upgrade() else {
                    break;
                };
                pool.arena.clear_replenish_pending();
                let deficit = pool
                    .arena
                    .baseline_slots
                    .saturating_sub(pool.arena.baseline_active_count());
                for _ in 0..deficit {
                    let Ok(permit) = pool.semaphore.try_acquire() else {
                        break;
                    };
                    let Some(guard) = pool.arena.claim_empty_slot(false) else {
                        drop(permit);
                        break;
                    };
                    match pool.connect_fresh().await {
                        Ok(stream) => {
                            guard.commit_warm(stream);
                            drop(permit);
                        }
                        Err(err) => {
                            drop(guard);
                            drop(permit);
                            tracing::warn!(id = %pool.config.id, error = %err, "Kafka ETP background replenishment failed");
                            pool.arena.abort_replenish();
                            return;
                        }
                    }
                }
                if pool.arena.try_finish_replenish() {
                    break;
                }
            }
        });
    }
}
