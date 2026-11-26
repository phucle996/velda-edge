//! PostgreSQL session lease (`PostgresLease`) borrowing `Slot[idx]` in-place inside
//! [`EtpPageArena`] with direct `Deref`/`DerefMut` to [`EtpStream`], in-place
//! `reconnect()`, and `recreate_in_background()`.
//!
//! ```text
//!   Plugin uses PostgresLease (Deref / DerefMut -> &mut EtpStream at Slot[idx])
//!         │
//!         ├─► Error -> reconnect().await ──► connect_authenticated -> replace_leased_slot(idx)
//!         ├─► Error -> recreate_in_background() ──► sets broken = true
//!         └─► Drop:
//!               ├─ !broken && idx < baseline  ──► L2 prefetch + ready_bitmap[idx] = 1
//!               ├─ !broken && idx >= baseline ──► auto-decay burst slot
//!               └─  broken                    ──► discard + trigger_replenish()
//!               └─  semaphore.add_permits(1)  ──► return semaphore permit
//! ```

use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use crate::connect::EtpStream;
use crate::error::EtpError;
use crate::postgres::pool::PostgresEtp;

/// Exclusive RAII lease borrowing an authenticated PostgreSQL [`EtpStream`] at `Slot[idx]`.
pub struct PostgresLease {
    pub pool: Arc<PostgresEtp>,
    pub slot_idx: usize,
    pub broken: bool,
}

const _: () = assert!(std::mem::size_of::<PostgresLease>() == 24);

impl fmt::Debug for PostgresLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresLease")
            .field("id", &self.pool.config.id)
            .field("slot_idx", &self.slot_idx)
            .field("broken", &self.broken)
            .finish()
    }
}

impl Deref for PostgresLease {
    type Target = EtpStream;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        unsafe { self.pool.arena.slot_ref(self.slot_idx) }
    }
}

impl DerefMut for PostgresLease {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.pool.arena.slot_mut(self.slot_idx) }
    }
}

impl PostgresLease {
    pub(crate) fn new(pool: Arc<PostgresEtp>, slot_idx: usize) -> Self {
        Self {
            pool,
            slot_idx,
            broken: false,
        }
    }

    /// Establishes and authenticates a fresh PostgreSQL connection and replaces `Slot[idx]`
    /// in-place while retaining the semaphore permit.
    pub async fn reconnect(&mut self) -> Result<(), EtpError> {
        self.broken = true;
        let fresh = self.pool.connect_authenticated().await?;
        self.pool.arena.replace_leased_slot(self.slot_idx, fresh);
        self.broken = false;
        Ok(())
    }

    /// Flags this connection as broken so `Drop` discards it and triggers replenishment.
    #[inline]
    pub fn recreate_in_background(&mut self) {
        self.broken = true;
    }
}

impl Drop for PostgresLease {
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
