//! Redis session lease (`RedisLease`) borrowing `Slot[idx]` in-place inside
//! [`RedisStandalonePool`]'s [`EtpPageArena`] for both Standalone and per-shard Cluster nodes.
//!
//! ```text
//!   Plugin uses RedisLease (Deref / DerefMut -> &mut EtpStream at Slot[idx] inside Arena)
//!         │
//!         ├─► Error -> reconnect().await ──► connect_authenticated -> replace_leased_slot(Slot[idx])
//!         ├─► Error -> recreate_in_background() ──► sets broken = true
//!         └─► Drop:
//!               ├─ !broken && idx < baseline  ──► L2 prefetch + ready_bitmap[idx] = 1
//!               ├─ !broken && idx >= baseline ──► auto-decay burst slot
//!               └─  broken                    ──► discard + trigger_replenish()
//!               └─  semaphore.add_permits(1)  ──► return semaphore permit
//!
//!   In-Place Lease Memory Handle (24 Bytes):
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ RedisLease { pool: Arc<RedisStandalonePool>, slot_idx: usize, broken: bool }│
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use crate::connect::EtpStream;
use crate::error::EtpError;
use crate::redis::standalone::pool::RedisStandalonePool;

/// Exclusive RAII lease borrowing an authenticated Redis [`EtpStream`] in-place at `Slot[idx]`.
pub struct RedisLease {
    pub pool: Arc<RedisStandalonePool>,
    pub slot_idx: usize,
    pub broken: bool,
}

const _: () = assert!(std::mem::size_of::<RedisLease>() == 24);

impl fmt::Debug for RedisLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisLease")
            .field("id", &self.pool.id)
            .field("slot_idx", &self.slot_idx)
            .field("broken", &self.broken)
            .finish()
    }
}

impl Deref for RedisLease {
    type Target = EtpStream;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        unsafe { self.pool.arena.slot_ref(self.slot_idx) }
    }
}

impl DerefMut for RedisLease {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.pool.arena.slot_mut(self.slot_idx) }
    }
}

impl RedisLease {
    pub(crate) fn new(pool: Arc<RedisStandalonePool>, slot_idx: usize) -> Self {
        Self {
            pool,
            slot_idx,
            broken: false,
        }
    }

    /// Establishes and authenticates a fresh Redis connection and replaces `Slot[idx]`
    /// in-place while retaining the semaphore permit.
    pub async fn reconnect(&mut self) -> Result<(), EtpError> {
        self.broken = true;
        let fresh = self.pool.connect_authenticated().await?;
        self.pool.arena.replace_leased_slot(self.slot_idx, fresh);
        self.broken = false;
        Ok(())
    }

    /// Flags this connection as broken so `Drop` discards it, releases the permit,
    /// and triggers background replenishment in [`RedisStandalonePool`].
    #[inline]
    pub fn recreate_in_background(&mut self) {
        self.broken = true;
    }
}

impl Drop for RedisLease {
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
