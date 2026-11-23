//! Lock-free Loki ETP connection pool (`LokiEtp`) backed by [`EtpPageArena<LokiConn>`]
//! and 64B-aligned [`ColdConnectBlueprint`], supporting HTTP/1.1 (`EtpStream`) and
//! HTTP/2 gRPC (`h2::client::SendRequest<bytes::Bytes>`).
//!
//! ```text
//!   LokiEtp::new() ──► ColdConnectBlueprint (ALPN "h2" if Grpc) + (N+1)*4096B EtpPageArena<LokiConn>
//!         │
//!         ▼
//!   warm() / acquire() / try_acquire()
//!         ├─► Ready hit (TZCNT) ──► borrow Slot[idx] in-place ──► LokiLease(idx)
//!         └─► Miss / Burst      ──► claim_empty_slot(true) -> connect_fresh() -> LokiLease(idx)
//!                                     ├─ Http -> LokiConn::Http(EtpStream)
//!                                     └─ Grpc -> h2::handshake -> spawn driver -> LokiConn::Grpc(SendRequest)
//!
//!   Cold Blueprint & Page Arena Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ ColdConnectBlueprint (64B aligned, isolated): id | ColdTargetAddr | TLS    │
//!   ├────────────────────────────────────────────────────────────────────────────┤
//!   │ Arena: Cache Line 0 (64B) | Slot[idx]: ArenaSlot<LokiConn> (64B) | +4096B  │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

use crate::arena::{ArenaSlot, EtpPageArena, ReadyAcquire};
use crate::config::EtpEntryConfig;
use crate::connect::{
    ColdConnectBlueprint, EtpStream, build_tls_target, build_tls_target_with_alpn,
};
use crate::error::EtpError;
use crate::loki::config::{LokiEtpConfig, LokiProtocol};
use crate::loki::lease::LokiLease;

/// Protocol-specialized connection stored in-place inside `ArenaSlot<LokiConn>`.
#[derive(Debug)]
pub enum LokiConn {
    Http(EtpStream),
    Grpc(h2::client::SendRequest<bytes::Bytes>),
}

const _: () = assert!(std::mem::size_of::<ArenaSlot<LokiConn>>() == 64);

/// Lock-free bounded pool of HTTP/1.1 or HTTP/2 gRPC connections to Grafana Loki.
pub struct LokiEtp {
    pub config: LokiEtpConfig,
    pub blueprint: ColdConnectBlueprint,
    pub arena: EtpPageArena<LokiConn>,
    pub semaphore: Semaphore,
    pub connect_timeout: Duration,
    pub idle_timeout_secs: u64,
}

impl fmt::Debug for LokiEtp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LokiEtp")
            .field("id", &self.config.id)
            .field("protocol", &self.config.protocol)
            .field("endpoint", &self.config.endpoint)
            .field("baseline_slots", &self.arena.baseline_slots)
            .field("total_slots", &self.arena.total_slots)
            .field("page_count", &self.arena.page_count)
            .field("idle_len", &self.arena.ready_count())
            .finish()
    }
}

impl LokiEtp {
    /// Creates a new `LokiEtp` pool from a generic [`EtpEntryConfig`].
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        Self::new(LokiEtpConfig::from_entry(entry)?)
    }

    /// Creates a new `LokiEtp` pool from a parsed [`LokiEtpConfig`].
    pub fn new(config: LokiEtpConfig) -> Result<Self, EtpError> {
        let tls_target = match config.protocol {
            LokiProtocol::Http => build_tls_target(&config.id, &config.host, config.tls.as_ref())?,
            LokiProtocol::Grpc => {
                build_tls_target_with_alpn(&config.id, &config.host, config.tls.as_ref(), &[b"h2"])?
            }
        };
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

    pub(crate) async fn connect_fresh(&self) -> Result<LokiConn, EtpError> {
        let stream = self.blueprint.connect_stream().await?;
        match self.config.protocol {
            LokiProtocol::Http => Ok(LokiConn::Http(stream)),
            LokiProtocol::Grpc => {
                let timeout_ms =
                    u64::try_from(self.connect_timeout.as_millis()).unwrap_or(u64::MAX);
                let (send_request, connection) =
                    tokio::time::timeout(self.connect_timeout, h2::client::handshake(stream))
                        .await
                        .map_err(|_| EtpError::Timeout {
                            id: self.config.id.clone(),
                            timeout_ms,
                        })?
                        .map_err(|err| {
                            EtpError::Io(std::io::Error::other(format!(
                                "Loki ETP '{}' HTTP/2 handshake failed: {err}",
                                self.config.id
                            )))
                        })?;

                let etp_id = self.config.id.clone();
                tokio::spawn(async move {
                    if let Err(err) = connection.await {
                        tracing::debug!(id = %etp_id, error = %err, "Loki ETP H2 driver exited");
                    }
                });

                let send_request = send_request.ready().await.map_err(|err| {
                    EtpError::Io(std::io::Error::other(format!(
                        "Loki ETP '{}' HTTP/2 session not ready: {err}",
                        self.config.id
                    )))
                })?;

                Ok(LokiConn::Grpc(send_request))
            }
        }
    }

    /// Pre-warms up to `baseline_slots` connections to Loki.
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
                Ok(conn) => {
                    guard.commit_warm(conn);
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

    /// Non-blocking fast path: borrows a warm Loki lease in-place if available.
    pub fn try_acquire(self: &Arc<Self>) -> Option<LokiLease> {
        let permit = self.semaphore.try_acquire().ok()?;
        match self.arena.try_acquire_ready_slot(self.idle_timeout_secs) {
            ReadyAcquire::Hit(slot_idx) => {
                permit.forget();
                Some(LokiLease::new(Arc::clone(self), slot_idx))
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

    /// Acquires a Loki connection lease.
    pub async fn acquire(self: &Arc<Self>) -> Result<LokiLease, EtpError> {
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
            return Ok(LokiLease::new(Arc::clone(self), slot_idx));
        }

        let Some(guard) = self.arena.claim_empty_slot(true) else {
            return Err(EtpError::PoolExhausted {
                id: self.config.id.clone(),
            });
        };

        let conn = self.connect_fresh().await?;
        let slot_idx = guard.commit_leased(conn);
        permit.forget();
        Ok(LokiLease::new(Arc::clone(self), slot_idx))
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
                        Ok(conn) => {
                            guard.commit_warm(conn);
                            drop(permit);
                        }
                        Err(err) => {
                            drop(guard);
                            drop(permit);
                            tracing::warn!(
                                id = %pool.config.id,
                                error = %err,
                                "Loki ETP background replenishment failed"
                            );
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
