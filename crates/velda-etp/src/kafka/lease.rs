//! Kafka connection lease (`KafkaLease`) borrowing `Slot[idx]` in-place inside [`EtpPageArena`]
//! with direct `Deref`/`DerefMut` to [`EtpStream`], in-place `reconnect()`, and
//! `recreate_in_background()`.
//!
//! ```text
//!   Plugin uses KafkaLease (Deref / DerefMut -> &mut EtpStream at Slot[idx])
//!         │
//!         ├─► reconnect().await ──► connect_fresh -> replace_leased_slot(Slot[idx])
//!         ├─► recreate_in_background() ──► sets broken = true
//!         └─► Drop ──► baseline: L2 + ready; spare: auto-decay; broken: replenish
//!               └─  semaphore.add_permits(1)  ──► return semaphore permit
//!
//!   In-Place Lease Memory Handle (24 Bytes):
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ KafkaLease { pool: Arc<KafkaEtp>, slot_idx: usize, broken: bool }          │
//!   │   ──► &(pool.arena + (slot_idx << 6) + 64) : EtpStream (zero pointer jump) │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use crate::connect::EtpStream;
use crate::error::EtpError;
use crate::kafka::pool::KafkaEtp;

/// Exclusive RAII lease borrowing a Kafka [`EtpStream`] in-place at `Slot[idx]`.
pub struct KafkaLease {
    pub pool: Arc<KafkaEtp>,
    pub slot_idx: usize,
    pub broken: bool,
}

const _: () = assert!(std::mem::size_of::<KafkaLease>() == 24);

impl fmt::Debug for KafkaLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaLease")
            .field("id", &self.pool.config.id)
            .field("slot_idx", &self.slot_idx)
            .field("broken", &self.broken)
            .finish()
    }
}

impl Deref for KafkaLease {
    type Target = EtpStream;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        unsafe { self.pool.arena.slot_ref(self.slot_idx) }
    }
}

impl DerefMut for KafkaLease {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.pool.arena.slot_mut(self.slot_idx) }
    }
}

impl KafkaLease {
    pub(crate) fn new(pool: Arc<KafkaEtp>, slot_idx: usize) -> Self {
        Self {
            pool,
            slot_idx,
            broken: false,
        }
    }

    /// Establishes a fresh Kafka broker connection and replaces `Slot[idx]` in-place
    /// while retaining the semaphore permit.
    pub async fn reconnect(&mut self) -> Result<(), EtpError> {
        self.broken = true;
        let fresh = self.pool.connect_fresh().await?;
        self.pool.arena.replace_leased_slot(self.slot_idx, fresh);
        self.broken = false;
        Ok(())
    }

    /// Flags this connection as broken so `Drop` discards it, releases the permit,
    /// and triggers background replenishment in [`KafkaEtp`].
    #[inline]
    pub fn recreate_in_background(&mut self) {
        self.broken = true;
    }
}

impl Drop for KafkaLease {
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
