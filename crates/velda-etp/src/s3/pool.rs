//! Lock-free S3 ETP connection pool (`S3Etp`) backed by [`EtpPageArena<EtpStream>`]
//! and 64B-aligned [`ColdConnectBlueprint`].
//!
//! ```text
//!   warm() / acquire() / try_acquire()
//!         │
//!         ├─► Ready hit (TZCNT on ready_bitmap)
//!         │     └─► prefetch_l1_slot(Slot[idx]) -> borrow in-place -> S3Lease(idx)
//!         │
//!         └─► Miss / Burst Spillover
//!               └─► claim_empty_slot(true) -> blueprint.connect_stream()
//!                     -> write_leased_slot(idx, stream) -> S3Lease(idx)
//!
//!   Page-Aligned Memory & Cold Blueprint Layout ((required_pages + 1) * 4096B):
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ ColdConnectBlueprint (64B aligned, isolated): id | ColdTargetAddr | TLS    │
//!   ├────────────────────────────────────────────────────────────────────────────┤
//!   │ Cache Line 0 (64B, L1D) : ready/busy/init bitmaps | counters | replenishing│
//!   │ Slots [0..baseline)     : L2-warm S3 EtpStream slots at (idx << 6) + 64    │
//!   │ Spare Page (+4096B)     : burst spillover slots [baseline..total_slots)    │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

use crate::arena::{EtpPageArena, ReadyAcquire};
use crate::config::EtpEntryConfig;
use crate::connect::{ColdConnectBlueprint, EtpStream, build_tls_target};
use crate::error::EtpError;
use crate::s3::config::S3EtpConfig;
use crate::s3::lease::S3Lease;

/// Lock-free bounded pool of TCP/TLS connections to an S3-compatible endpoint.
pub struct S3Etp {
    pub config: S3EtpConfig,
    pub blueprint: ColdConnectBlueprint,
    pub arena: EtpPageArena<EtpStream>,
    pub semaphore: Semaphore,
    pub connect_timeout: Duration,
    pub idle_timeout_secs: u64,
}

impl fmt::Debug for S3Etp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Etp")
            .field("id", &self.config.id)
            .field("endpoint", &self.config.endpoint)
            .field("bucket", &self.config.bucket)
            .field("region", &self.config.region)
            .field("baseline_slots", &self.arena.baseline_slots)
            .field("total_slots", &self.arena.total_slots)
            .field("page_count", &self.arena.page_count)
            .field("idle_len", &self.arena.ready_count())
            .finish()
    }
}

impl S3Etp {
    /// Creates a new `S3Etp` pool from a generic [`EtpEntryConfig`].
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        Self::new(S3EtpConfig::from_entry(entry)?)
    }

    /// Creates a new `S3Etp` pool from a parsed [`S3EtpConfig`].
    pub fn new(config: S3EtpConfig) -> Result<Self, EtpError> {
        let tls_target = build_tls_target(&config.id, &config.host, config.tls.as_ref())?;
        let connect_timeout = Duration::from_millis(config.pool.connect_timeout_ms.max(1));
        let idle_timeout_secs = config.pool.idle_timeout_ms.div_ceil(1000);

        let blueprint = ColdConnectBlueprint::new(
            &config.id,
            &config.host,
            config.port,
            tls_target,
            connect_timeout,
        );

        let arena = EtpPageArena::new(config.pool.pool_size);
        let semaphore = Semaphore::new(arena.total_slots);

        Ok(Self {
            config,
            blueprint,
            arena,
            semaphore,
            connect_timeout,
            idle_timeout_secs,
        })
    }

    pub(crate) async fn connect_fresh(&self) -> Result<EtpStream, EtpError> {
        self.blueprint.connect_stream().await
    }

    /// Pre-warms up to `baseline_slots` connections to S3.
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

    /// Non-blocking fast path: borrows a warm S3 lease in-place if available.
    pub fn try_acquire(self: &Arc<Self>) -> Option<S3Lease> {
        let permit = self.semaphore.try_acquire().ok()?;
        match self.arena.try_acquire_ready_slot(self.idle_timeout_secs) {
            ReadyAcquire::Hit(slot_idx) => {
                permit.forget();
                Some(S3Lease::new(Arc::clone(self), slot_idx))
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

    /// Acquires an S3 connection lease.
    pub async fn acquire(self: &Arc<Self>) -> Result<S3Lease, EtpError> {
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
            return Ok(S3Lease::new(Arc::clone(self), slot_idx));
        }

        let Some(guard) = self.arena.claim_empty_slot(true) else {
            return Err(EtpError::PoolExhausted {
                id: self.config.id.clone(),
            });
        };

        let stream = self.connect_fresh().await?;
        let slot_idx = guard.commit_leased(stream);
        permit.forget();
        Ok(S3Lease::new(Arc::clone(self), slot_idx))
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
                            tracing::warn!(id = %pool.config.id, error = %err, "S3 ETP background replenishment failed");
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
