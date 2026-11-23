//! Lock-free OTLP ETP connection pool (`OtlpEtp`) backed by [`EtpPageArena<OtlpConn>`]
//! and 64B-aligned [`ColdConnectBlueprint`] supporting HTTP/1.1 (`Http`) and HTTP/2 (`Grpc`).
//!
//! ```text
//!   OtlpEtp::new() ──► ColdConnectBlueprint + (N+1)*4096B EtpPageArena<OtlpConn>
//!         │              └─ Grpc builds TLS target with ALPN ["h2"]
//!         ▼
//!   warm() / acquire() / try_acquire()
//!         ├─► Ready hit (TZCNT) ──► borrow Slot[idx] in-place ──► OtlpLease(idx)
//!         └─► Miss / Burst      ──► connect_fresh():
//!               ├─ HttpProtobuf | HttpJson ──► EtpStream ──► OtlpConn::Http(stream)
//!               └─ Grpc ──► h2::client::handshake + spawn driver + ready() ──► OtlpConn::Grpc
//!
//!   Cache-Line Slot Geometry:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ ArenaSlot<OtlpConn> == 64B (cache-line aligned; compile-time asserted)     │
//!   │ ColdConnectBlueprint (64B aligned, isolated): id | ColdTargetAddr | TLS    │
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
use crate::otlp::config::{OtlpEtpConfig, OtlpProtocol};
use crate::otlp::lease::OtlpLease;

/// Pooled OTLP connection stored in-place inside [`ArenaSlot<OtlpConn>`].
pub enum OtlpConn {
    Http(EtpStream),
    Grpc(h2::client::SendRequest<bytes::Bytes>),
}

const _: () = assert!(std::mem::size_of::<ArenaSlot<OtlpConn>>() == 64);

/// Lock-free bounded pool of connections to an OpenTelemetry OTLP collector.
pub struct OtlpEtp {
    pub config: OtlpEtpConfig,
    pub blueprint: ColdConnectBlueprint,
    pub arena: EtpPageArena<OtlpConn>,
    pub semaphore: Semaphore,
    pub connect_timeout: Duration,
    pub idle_timeout_secs: u64,
}

impl fmt::Debug for OtlpEtp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OtlpEtp")
            .field("id", &self.config.id)
            .field("endpoint", &self.config.endpoint)
            .field("protocol", &self.config.protocol)
            .field("baseline_slots", &self.arena.baseline_slots)
            .field("total_slots", &self.arena.total_slots)
            .field("page_count", &self.arena.page_count)
            .field("idle_len", &self.arena.ready_count())
            .finish()
    }
}

impl OtlpEtp {
    /// Creates a new `OtlpEtp` pool from a generic [`EtpEntryConfig`].
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        Self::new(OtlpEtpConfig::from_entry(entry)?)
    }

    /// Creates a new `OtlpEtp` pool from a parsed [`OtlpEtpConfig`].
    pub fn new(config: OtlpEtpConfig) -> Result<Self, EtpError> {
        let tls_target = match config.protocol {
            OtlpProtocol::HttpProtobuf | OtlpProtocol::HttpJson => {
                build_tls_target(&config.id, &config.host, config.tls.as_ref())?
            }
            OtlpProtocol::Grpc => {
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

    pub(crate) async fn connect_fresh(&self) -> Result<OtlpConn, EtpError> {
        let stream = self.blueprint.connect_stream().await?;
        match self.config.protocol {
            OtlpProtocol::HttpProtobuf | OtlpProtocol::HttpJson => Ok(OtlpConn::Http(stream)),
            OtlpProtocol::Grpc => {
                let (send_request, connection) =
                    match tokio::time::timeout(self.connect_timeout, h2::client::handshake(stream))
                        .await
                    {
                        Ok(Ok(pair)) => pair,
                        Ok(Err(e)) => {
                            return Err(EtpError::Tls(format!(
                                "OTLP ETP '{}' HTTP/2 handshake failed: {e}",
                                self.config.id
                            )));
                        }
                        Err(_) => {
                            return Err(EtpError::Timeout {
                                id: self.config.id.clone(),
                                timeout_ms: u64::try_from(self.connect_timeout.as_millis())
                                    .unwrap_or(u64::MAX),
                            });
                        }
                    };

                tokio::spawn(async move {
                    if let Err(err) = connection.await {
                        tracing::debug!(error = %err, "OTLP H2 connection driver terminated");
                    }
                });

                let ready_req = send_request.ready().await.map_err(|e| {
                    EtpError::Io(std::io::Error::other(format!(
                        "OTLP ETP '{}' HTTP/2 client not ready: {e}",
                        self.config.id
                    )))
                })?;

                Ok(OtlpConn::Grpc(ready_req))
            }
        }
    }

    /// Pre-warms up to `baseline_slots` connections to the OTLP collector.
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

    /// Non-blocking fast path: borrows a warm OTLP lease in-place if available.
    pub fn try_acquire(self: &Arc<Self>) -> Option<OtlpLease> {
        let permit = self.semaphore.try_acquire().ok()?;
        match self.arena.try_acquire_ready_slot(self.idle_timeout_secs) {
            ReadyAcquire::Hit(slot_idx) => {
                permit.forget();
                Some(OtlpLease::new(Arc::clone(self), slot_idx))
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

    /// Acquires an OTLP connection lease.
    pub async fn acquire(self: &Arc<Self>) -> Result<OtlpLease, EtpError> {
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
            return Ok(OtlpLease::new(Arc::clone(self), slot_idx));
        }

        let Some(guard) = self.arena.claim_empty_slot(true) else {
            return Err(EtpError::PoolExhausted {
                id: self.config.id.clone(),
            });
        };

        let conn = self.connect_fresh().await?;
        let slot_idx = guard.commit_leased(conn);
        permit.forget();
        Ok(OtlpLease::new(Arc::clone(self), slot_idx))
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
                            tracing::warn!(id = %pool.config.id, error = %err, "OTLP ETP background replenishment failed");
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
