//! Shared lock-free HTTP/TLS stream connection pool (`StreamPool`) and exclusive RAII lease
//! (`StreamLease`) backed by [`EtpPageArena<EtpStream>`] and [`ColdConnectBlueprint`].
//!
//! ```text
//!   warm() / acquire() / try_acquire()
//!         │
//!         ├─► Ready hit (TZCNT on ready_bitmap)
//!         │     └─► prefetch_l1_slot(Slot[idx]) -> borrow in-place -> StreamLease(idx)
//!         │
//!         └─► Miss / Burst Spillover
//!               └─► claim_empty_slot(true) -> blueprint.connect_stream()
//!                     -> commit_leased(stream) -> StreamLease(idx)
//!
//!   StreamLease (24B): Deref/DerefMut(&mut EtpStream) | reconnect() | recreate_in_background()
//!   Drop: healthy baseline -> ready_bitmap = 1 | spare -> decay | broken -> discard + replenish
//! ```

use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

use crate::arena::{EtpPageArena, ReadyAcquire};
use crate::config::EtpPoolSettings;
use crate::connect::{ColdConnectBlueprint, EtpStream, EtpTlsConfig, build_tls_target};
use crate::error::EtpError;

/// Bounded lock-free pool of pre-connected [`EtpStream`] handles for HTTP/TLS ETP providers.
pub struct StreamPool {
    pub id: String,
    pub blueprint: ColdConnectBlueprint,
    pub arena: EtpPageArena<EtpStream>,
    pub semaphore: Semaphore,
    pub connect_timeout: Duration,
    pub idle_timeout_secs: u64,
}

impl fmt::Debug for StreamPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamPool")
            .field("id", &self.id)
            .field("baseline_slots", &self.arena.baseline_slots)
            .field("total_slots", &self.arena.total_slots)
            .field("idle_len", &self.arena.ready_count())
            .finish()
    }
}

impl StreamPool {
    /// Builds a [`StreamPool`] with isolated [`ColdConnectBlueprint`] and page-aligned arena.
    pub fn new(
        id: &str,
        host: &str,
        port: u16,
        tls: Option<&EtpTlsConfig>,
        pool_settings: &EtpPoolSettings,
    ) -> Result<Self, EtpError> {
        let tls_target = build_tls_target(id, host, tls)?;
        let connect_timeout = Duration::from_millis(pool_settings.connect_timeout_ms.max(1));
        let idle_timeout_secs = pool_settings.idle_timeout_ms.div_ceil(1000);
        let blueprint = ColdConnectBlueprint::new(id, host, port, tls_target, connect_timeout);
        let arena = EtpPageArena::new(pool_settings.pool_size);
        let semaphore = Semaphore::new(arena.total_slots);

        Ok(Self {
            id: id.to_string(),
            blueprint,
            arena,
            semaphore,
            connect_timeout,
            idle_timeout_secs,
        })
    }

    #[inline]
    pub(crate) async fn connect_fresh(&self) -> Result<EtpStream, EtpError> {
        self.blueprint.connect_stream().await
    }

    /// Pre-warms up to `baseline_slots` connections into the arena.
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
                Ok(stream) => guard.commit_warm(stream),
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

    /// Non-blocking lease acquisition from warm ready slots.
    pub fn try_acquire(self: &Arc<Self>) -> Option<StreamLease> {
        let permit = self.semaphore.try_acquire().ok()?;
        match self.arena.try_acquire_ready_slot(self.idle_timeout_secs) {
            ReadyAcquire::Hit(slot_idx) => {
                permit.forget();
                Some(StreamLease::new(Arc::clone(self), slot_idx))
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

    /// Acquires an exclusive connection lease, connecting on miss.
    pub async fn acquire(self: &Arc<Self>) -> Result<StreamLease, EtpError> {
        let permit =
            match tokio::time::timeout(self.connect_timeout, self.semaphore.acquire()).await {
                Ok(Ok(permit)) => permit,
                _ => {
                    return Err(EtpError::PoolExhausted {
                        id: self.id.clone(),
                    });
                }
            };

        if let ReadyAcquire::Hit(slot_idx) =
            self.arena.try_acquire_ready_slot(self.idle_timeout_secs)
        {
            permit.forget();
            return Ok(StreamLease::new(Arc::clone(self), slot_idx));
        }

        let Some(guard) = self.arena.claim_empty_slot(true) else {
            return Err(EtpError::PoolExhausted {
                id: self.id.clone(),
            });
        };

        let stream = self.connect_fresh().await?;
        let slot_idx = guard.commit_leased(stream);
        permit.forget();
        Ok(StreamLease::new(Arc::clone(self), slot_idx))
    }

    /// Triggers lock-free CAS background replenishment of broken baseline slots.
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
                            tracing::warn!(id = %pool.id, error = %err, "ETP stream background replenishment failed");
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

/// Exclusive RAII lease borrowing an [`EtpStream`] in-place at `Slot[idx]`.
pub struct StreamLease {
    pub pool: Arc<StreamPool>,
    pub slot_idx: usize,
    pub broken: bool,
}

const _: () = assert!(std::mem::size_of::<StreamLease>() == 24);

impl fmt::Debug for StreamLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamLease")
            .field("id", &self.pool.id)
            .field("slot_idx", &self.slot_idx)
            .field("broken", &self.broken)
            .finish()
    }
}

impl Deref for StreamLease {
    type Target = EtpStream;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        unsafe { self.pool.arena.slot_ref(self.slot_idx) }
    }
}

impl DerefMut for StreamLease {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.pool.arena.slot_mut(self.slot_idx) }
    }
}

impl StreamLease {
    #[inline]
    pub(crate) fn new(pool: Arc<StreamPool>, slot_idx: usize) -> Self {
        Self {
            pool,
            slot_idx,
            broken: false,
        }
    }

    /// Re-establishes a fresh connection and replaces `Slot[idx]` in-place.
    pub async fn reconnect(&mut self) -> Result<(), EtpError> {
        self.broken = true;
        let fresh = self.pool.connect_fresh().await?;
        self.pool.arena.replace_leased_slot(self.slot_idx, fresh);
        self.broken = false;
        Ok(())
    }

    /// Marks this connection broken so `Drop` discards it and triggers replenishment.
    #[inline]
    pub fn recreate_in_background(&mut self) {
        self.broken = true;
    }
}

impl Drop for StreamLease {
    fn drop(&mut self) {
        if !self.broken {
            self.pool.arena.release_healthy_slot(self.slot_idx);
        } else {
            self.pool.arena.discard_broken_slot(self.slot_idx);
            if self.slot_idx < self.pool.arena.baseline_slots {
                self.pool.trigger_replenish();
            }
        }
        self.pool.semaphore.add_permits(1);
    }
}
