//! Valkey session lease (`ValkeyLease`) borrowing `Slot[idx]` in-place inside [`ValkeyNodePool`].
//!
//! ```text
//!   ValkeyLease (24 bytes) -> Deref/DerefMut(&mut EtpStream) | reconnect() | recreate_in_background()
//! ```

use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use crate::connect::EtpStream;
use crate::error::EtpError;
use crate::valkey::pool::ValkeyNodePool;

pub struct ValkeyLease {
    pub pool: Arc<ValkeyNodePool>,
    pub slot_idx: usize,
    pub broken: bool,
}

const _: () = assert!(std::mem::size_of::<ValkeyLease>() == 24);

impl fmt::Debug for ValkeyLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ValkeyLease")
            .field("id", &self.pool.id)
            .field("slot_idx", &self.slot_idx)
            .field("broken", &self.broken)
            .finish()
    }
}

impl Deref for ValkeyLease {
    type Target = EtpStream;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        unsafe { self.pool.arena.slot_ref(self.slot_idx) }
    }
}

impl DerefMut for ValkeyLease {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.pool.arena.slot_mut(self.slot_idx) }
    }
}

impl ValkeyLease {
    pub(crate) fn new(pool: Arc<ValkeyNodePool>, slot_idx: usize) -> Self {
        Self {
            pool,
            slot_idx,
            broken: false,
        }
    }

    pub async fn reconnect(&mut self) -> Result<(), EtpError> {
        self.broken = true;
        let fresh = self.pool.connect_authenticated().await?;
        self.pool.arena.replace_leased_slot(self.slot_idx, fresh);
        self.broken = false;
        Ok(())
    }

    #[inline]
    pub fn recreate_in_background(&mut self) {
        self.broken = true;
    }
}

impl Drop for ValkeyLease {
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
