//! OTLP connection lease (`OtlpLease`) borrowing `Slot[idx]` in-place inside [`EtpPageArena`]
//! with direct `Deref`/`DerefMut` to [`EtpStream`], in-place `reconnect()`, and
//! `recreate_in_background()`.
//!
//! ```text
//!   Plugin uses OtlpLease (Deref / DerefMut -> &mut EtpStream at Slot[idx])
//!         │
//!         ├─► reconnect().await ──► connect_fresh -> replace_leased_slot(Slot[idx])
//!         ├─► recreate_in_background() ──► sets broken = true
//!         └─► Drop:
//!               ├─ !broken && idx < baseline  ──► L2 prefetch + ready_bitmap[idx] = 1
//!               ├─ !broken && idx >= baseline ──► auto-decay spare slot (init=0, busy=0)
//!               └─  broken                    ──► discard + trigger_replenish()
//!               └─  semaphore.add_permits(1)  ──► return semaphore permit
//!
//!   In-Place Lease Memory Handle (24 Bytes):
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ OtlpLease { pool: Arc<OtlpEtp>, slot_idx: usize, broken: bool }            │
//!   │   ──► &(pool.arena + (slot_idx << 6) + 64) : EtpStream (zero pointer jump) │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use crate::connect::EtpStream;
use crate::error::EtpError;
use crate::otlp::pool::OtlpEtp;

/// Exclusive RAII lease borrowing an OTLP [`EtpStream`] in-place at `Slot[idx]`.
pub struct OtlpLease {
    pub pool: Arc<OtlpEtp>,
    pub slot_idx: usize,
    pub broken: bool,
}

const _: () = assert!(std::mem::size_of::<OtlpLease>() == 24);

impl fmt::Debug for OtlpLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OtlpLease")
            .field("id", &self.pool.config.id)
            .field("slot_idx", &self.slot_idx)
            .field("broken", &self.broken)
            .finish()
    }
}

impl Deref for OtlpLease {
    type Target = EtpStream;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        unsafe { self.pool.arena.slot_ref(self.slot_idx) }
    }
}

impl DerefMut for OtlpLease {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.pool.arena.slot_mut(self.slot_idx) }
    }
}

impl OtlpLease {
    pub(crate) fn new(pool: Arc<OtlpEtp>, slot_idx: usize) -> Self {
        Self {
            pool,
            slot_idx,
            broken: false,
        }
    }

    /// Establishes a fresh OTLP connection and replaces `Slot[idx]` in-place
    /// while retaining the semaphore permit.
    pub async fn reconnect(&mut self) -> Result<(), EtpError> {
        self.broken = true;
        let fresh = self.pool.connect_fresh().await?;
        self.pool.arena.replace_leased_slot(self.slot_idx, fresh);
        self.broken = false;
        Ok(())
    }

    /// Flags this connection as broken so `Drop` discards it, releases the permit,
    /// and triggers background replenishment in [`OtlpEtp`].
    #[inline]
    pub fn recreate_in_background(&mut self) {
        self.broken = true;
    }
}

impl Drop for OtlpLease {
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
