//! Lock-free StatsD UDP socket pool (`StatsdEtp`) backed by [`EtpPageArena<UdpSocket>`],
//! integer [`ColdTargetAddr`], `AtomicBool` background replenishment, and `+4096B` burst spillover.
//!
//! ```text
//!   warm() / acquire() / try_acquire()
//!         │
//!         ├─► Ready hit (TZCNT on ready_bitmap)
//!         │     └─► prefetch_l1_slot(Slot[idx]) -> borrow UdpSocket -> StatsdLease(idx)
//!         │
//!         └─► Miss / Burst Spillover ──► connect_fresh():
//!               ├─ ColdTargetAddr::Ip { addr, .. } ──► bind v4/v6 -> socket.connect(*addr)
//!               └─ ColdTargetAddr::Dns { host, p } ──► lookup_host -> bind -> connect
//!                     └─► write_leased_slot(idx, socket) -> StatsdLease(idx)
//!
//!   Page-Aligned UDP Socket Arena Layout ((required_pages + 1) * 4096B):
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ ColdTargetAddr (integer ip_u128 + port_be + SocketAddr for IP literals)    │
//!   ├────────────────────────────────────────────────────────────────────────────┤
//!   │ Cache Line 0 (64B, L1D) : ready/busy/init bitmaps | counters | replenishing│
//!   │ Slots [0..baseline)     : pre-connected UdpSocket (16B) at (idx << 6) + 64 │
//!   │ Spare Page (+4096B)     : burst spillover sockets auto-decay on Drop       │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::sync::Semaphore;

use crate::arena::{EtpPageArena, ReadyAcquire};
use crate::config::EtpEntryConfig;
use crate::connect::{ColdTargetAddr, ip_u128_to_socket_addr};
use crate::error::EtpError;
use crate::statsd::config::StatsdEtpConfig;
use crate::statsd::lease::StatsdLease;

/// Lock-free bounded pool of pre-connected `UdpSocket` instances for StatsD / DogStatsD.
pub struct StatsdEtp {
    pub config: StatsdEtpConfig,
    pub target: ColdTargetAddr,
    pub arena: EtpPageArena<UdpSocket>,
    pub semaphore: Semaphore,
    pub connect_timeout: Duration,
    pub idle_timeout_secs: u64,
}

impl fmt::Debug for StatsdEtp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StatsdEtp")
            .field("id", &self.config.id)
            .field("endpoint", &self.config.endpoint)
            .field("flavor", &self.config.flavor)
            .field("baseline_slots", &self.arena.baseline_slots)
            .field("total_slots", &self.arena.total_slots)
            .field("page_count", &self.arena.page_count)
            .field("idle_len", &self.arena.ready_count())
            .finish()
    }
}

impl StatsdEtp {
    /// Creates a new `StatsdEtp` pool from a generic [`EtpEntryConfig`].
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        Self::new(StatsdEtpConfig::from_entry(entry)?)
    }

    /// Creates a new `StatsdEtp` pool from a parsed [`StatsdEtpConfig`].
    pub fn new(config: StatsdEtpConfig) -> Result<Self, EtpError> {
        let target = ColdTargetAddr::parse(&config.host, config.port);
        let arena = EtpPageArena::new(config.pool.pool_size);
        let semaphore = Semaphore::new(arena.total_slots);
        let connect_timeout = Duration::from_millis(config.pool.connect_timeout_ms.max(1));
        let idle_timeout_secs = config.pool.idle_timeout_ms.div_ceil(1000);

        Ok(Self {
            config,
            target,
            arena,
            semaphore,
            connect_timeout,
            idle_timeout_secs,
        })
    }

    pub(crate) async fn connect_fresh(&self) -> Result<UdpSocket, EtpError> {
        let target_addr: SocketAddr = match &self.target {
            ColdTargetAddr::Ip { addr, .. } => *addr,
            ColdTargetAddr::Dns {
                host,
                port,
                port_be,
                ..
            } => {
                let cached = self.target.cached_ip_u128();
                if cached != 0 {
                    ip_u128_to_socket_addr(cached, *port_be)
                } else {
                    let resolved = tokio::net::lookup_host((&**host, *port))
                        .await?
                        .next()
                        .ok_or_else(|| {
                            EtpError::Config(format!(
                                "StatsD ETP '{}' host '{host}:{port}' resolved no addresses",
                                self.config.id
                            ))
                        })?;
                    self.target.freeze_ip(resolved.ip());
                    resolved
                }
            }
        };

        let bind_addr = match target_addr {
            SocketAddr::V4(_) => "0.0.0.0:0",
            SocketAddr::V6(_) => "[::]:0",
        };
        let socket = UdpSocket::bind(bind_addr).await?;
        socket.connect(target_addr).await?;
        Ok(socket)
    }

    /// Pre-warms up to `baseline_slots` pre-connected UDP sockets.
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
                Ok(socket) => {
                    guard.commit_warm(socket);
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

    /// Non-blocking fast path: borrows a warm StatsD UDP socket lease in-place if available.
    pub fn try_acquire(self: &Arc<Self>) -> Option<StatsdLease> {
        let permit = self.semaphore.try_acquire().ok()?;
        match self.arena.try_acquire_ready_slot(self.idle_timeout_secs) {
            ReadyAcquire::Hit(slot_idx) => {
                permit.forget();
                Some(StatsdLease::new(Arc::clone(self), slot_idx))
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

    /// Acquires a StatsD UDP socket lease.
    pub async fn acquire(self: &Arc<Self>) -> Result<StatsdLease, EtpError> {
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
            return Ok(StatsdLease::new(Arc::clone(self), slot_idx));
        }

        let Some(guard) = self.arena.claim_empty_slot(true) else {
            return Err(EtpError::PoolExhausted {
                id: self.config.id.clone(),
            });
        };

        let socket = self.connect_fresh().await?;
        let slot_idx = guard.commit_leased(socket);
        permit.forget();
        Ok(StatsdLease::new(Arc::clone(self), slot_idx))
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
                        Ok(socket) => {
                            guard.commit_warm(socket);
                            drop(permit);
                        }
                        Err(err) => {
                            drop(guard);
                            drop(permit);
                            tracing::warn!(id = %pool.config.id, error = %err, "StatsD ETP background replenishment failed");
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
