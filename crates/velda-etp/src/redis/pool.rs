//! Lock-free Redis ETP pool (`RedisEtp`) backed by [`EtpPageArena<EtpStream>`],
//! 64B-aligned [`ColdConnectBlueprint`], and pre-baked pipelined RESP2 `AUTH` / `SELECT` handshake.
//!
//! ```text
//!   acquire() / try_acquire() / warm()
//!         │
//!         ├─► Fast Path (try_acquire_ready_slot):
//!         │     TZCNT(ready_bitmap) -> clear ready[idx], set busy[idx] -> RedisLease(idx)
//!         │
//!         └─► Connect Path (connect_authenticated):
//!               1. claim_empty_slot(allow_spare = true) [spills to +4096B page if full]
//!               2. blueprint.connect_stream().await [integer ColdTargetAddr + TLS]
//!               3. if prebaked_handshake: write_all(frame) + flush() [1 pipelined RTT!]
//!               4. read `handshake_replies` RESP lines, verify each starts with "+OK"
//!               5. write_leased_slot(idx, stream) -> RedisLease(idx)
//!
//!   Pipelined Pre-Baked RESP2 Handshake & Page Arena Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ prebaked_handshake: [*2/3 AUTH ...\r\n][*2 SELECT <db>\r\n] (0 heap allocs)│
//!   │ ColdConnectBlueprint (64B aligned, isolated): id | ColdTargetAddr | TLS    │
//!   ├────────────────────────────────────────────────────────────────────────────┤
//!   │ Hot Arena: Cache Line 0 (64B) | Slot[idx] at (idx << 6) + 64 | +4096B Spare│
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
use crate::redis::config::RedisEtpConfig;
use crate::redis::lease::RedisLease;

/// Lock-free bounded pool of authenticated Redis RESP2 connections stored in-place
/// inside a page-aligned [`EtpPageArena`].
pub struct RedisEtp {
    pub config: RedisEtpConfig,
    pub blueprint: ColdConnectBlueprint,
    pub(crate) prebaked_handshake: Option<Box<[u8]>>,
    pub(crate) handshake_replies: u8,
    pub arena: EtpPageArena<EtpStream>,
    pub semaphore: Semaphore,
    pub connect_timeout: Duration,
    pub idle_timeout_secs: u64,
}

impl fmt::Debug for RedisEtp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisEtp")
            .field("id", &self.config.id)
            .field("host", &self.config.host)
            .field("port", &self.config.port)
            .field("db", &self.config.db)
            .field("baseline_slots", &self.arena.baseline_slots)
            .field("total_slots", &self.arena.total_slots)
            .field("page_count", &self.arena.page_count)
            .field("idle_len", &self.arena.ready_count())
            .finish()
    }
}

impl RedisEtp {
    /// Creates a new `RedisEtp` pool from a generic [`EtpEntryConfig`].
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        let config = RedisEtpConfig::from_entry(entry)?;
        Self::new(config)
    }

    /// Creates a new `RedisEtp` pool from a parsed [`RedisEtpConfig`].
    pub fn new(config: RedisEtpConfig) -> Result<Self, EtpError> {
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

        let mut handshake = Vec::new();
        let mut handshake_replies: u8 = 0;
        if let Some(password) = &config.password {
            handshake.extend_from_slice(&build_auth_command(config.username.as_deref(), password));
            handshake_replies += 1;
        }
        if config.db > 0 {
            handshake.extend_from_slice(&build_select_command(config.db));
            handshake_replies += 1;
        }
        let prebaked_handshake = if handshake.is_empty() {
            None
        } else {
            Some(handshake.into_boxed_slice())
        };

        let arena = EtpPageArena::new(config.pool.pool_size);
        let semaphore = Semaphore::new(arena.total_slots);

        Ok(Self {
            config,
            blueprint,
            prebaked_handshake,
            handshake_replies,
            arena,
            semaphore,
            connect_timeout,
            idle_timeout_secs,
        })
    }

    /// Establishes a fresh TCP/TLS stream from [`ColdConnectBlueprint`] and executes the
    /// pre-baked pipelined RESP2 `AUTH` / `SELECT` handshake in a single RTT.
    pub(crate) async fn connect_authenticated(&self) -> Result<EtpStream, EtpError> {
        let mut stream = self.blueprint.connect_stream().await?;

        let Some(ref frame) = self.prebaked_handshake else {
            return Ok(stream);
        };

        let handshake = async {
            stream.write_all(frame).await?;
            stream.flush().await?;
            for step in 0..self.handshake_replies {
                let reply = read_resp_line(&mut stream).await?;
                if !reply.starts_with("+OK") {
                    return Err(EtpError::Config(format!(
                        "Redis '{}' handshake step {step} failed: {reply}",
                        self.config.id
                    )));
                }
            }
            Ok(stream)
        };

        let timeout_ms = u64::try_from(self.connect_timeout.as_millis()).unwrap_or(u64::MAX);
        match tokio::time::timeout(self.connect_timeout, handshake).await {
            Ok(res) => res,
            Err(_) => Err(EtpError::Timeout {
                id: self.config.id.clone(),
                timeout_ms,
            }),
        }
    }

    /// Pre-warms up to `baseline_slots` authenticated Redis connections upfront into the arena.
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
            match self.connect_authenticated().await {
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

    /// Non-blocking fast path: borrows a warm Redis connection in-place via `TZCNT`.
    pub fn try_acquire(self: &Arc<Self>) -> Option<RedisLease> {
        let permit = self.semaphore.try_acquire().ok()?;
        match self.arena.try_acquire_ready_slot(self.idle_timeout_secs) {
            ReadyAcquire::Hit(slot_idx) => {
                permit.forget();
                Some(RedisLease::new(Arc::clone(self), slot_idx))
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

    /// Acquires an authenticated Redis session lease, borrowing in-place or spilling into the
    /// `+4096B` spare page during traffic bursts.
    pub async fn acquire(self: &Arc<Self>) -> Result<RedisLease, EtpError> {
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
            return Ok(RedisLease::new(Arc::clone(self), slot_idx));
        }

        let Some(guard) = self.arena.claim_empty_slot(true) else {
            return Err(EtpError::PoolExhausted {
                id: self.config.id.clone(),
            });
        };

        let stream = self.connect_authenticated().await?;
        let slot_idx = guard.commit_leased(stream);
        permit.forget();
        Ok(RedisLease::new(Arc::clone(self), slot_idx))
    }

    /// Triggers lock-free background replenishment using Cache Line 0 `AtomicBool` CAS.
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
                    match pool.connect_authenticated().await {
                        Ok(stream) => {
                            guard.commit_warm(stream);
                            drop(permit);
                        }
                        Err(err) => {
                            drop(guard);
                            drop(permit);
                            tracing::warn!(
                                id = %pool.config.id,
                                error = %err,
                                "Redis ETP background replenishment failed"
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

async fn read_resp_line(stream: &mut EtpStream) -> Result<String, EtpError> {
    let mut buf = Vec::with_capacity(64);
    let mut byte = [0u8; 1];
    loop {
        let n = stream.read(&mut byte).await?;
        if n == 0 {
            return Err(EtpError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Redis closed connection before completing RESP line",
            )));
        }
        buf.push(byte[0]);
        if buf.len() >= 2 && buf[buf.len() - 2] == b'\r' && buf[buf.len() - 1] == b'\n' {
            buf.truncate(buf.len() - 2);
            return Ok(String::from_utf8_lossy(&buf).into_owned());
        }
        if buf.len() > 16 * 1024 {
            return Err(EtpError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Redis RESP line exceeded 16 KiB limit",
            )));
        }
    }
}

fn build_auth_command(username: Option<&str>, password: &str) -> Vec<u8> {
    match username {
        Some(user) => format!(
            "*3\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
            user.len(),
            user,
            password.len(),
            password
        )
        .into_bytes(),
        None => format!(
            "*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n",
            password.len(),
            password
        )
        .into_bytes(),
    }
}

fn build_select_command(db: u16) -> Vec<u8> {
    let db_str = db.to_string();
    format!("*2\r\n$6\r\nSELECT\r\n${}\r\n{}\r\n", db_str.len(), db_str).into_bytes()
}
