//! DragonflyDB connection pool (`DragonflyEtp`) backed by [`EtpPageArena<EtpStream>`].
//!
//! ```text
//!   warm() / acquire() / try_acquire() ──► ColdConnectBlueprint + pipelined AUTH/SELECT ──► DragonflyLease
//! ```

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

use crate::arena::{EtpPageArena, ReadyAcquire};
use crate::config::EtpEntryConfig;
use crate::connect::{ColdConnectBlueprint, EtpStream, build_tls_target};
use crate::dragonfly::config::DragonflyEtpConfig;
use crate::dragonfly::lease::DragonflyLease;
use crate::error::EtpError;

pub struct DragonflyEtp {
    pub config: DragonflyEtpConfig,
    pub blueprint: ColdConnectBlueprint,
    prebaked_handshake: Option<Box<[u8]>>,
    handshake_replies: u8,
    pub arena: EtpPageArena<EtpStream>,
    pub semaphore: Semaphore,
    connect_timeout: Duration,
    idle_timeout_secs: u64,
}

impl fmt::Debug for DragonflyEtp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DragonflyEtp")
            .field("id", &self.config.id)
            .field("host", &self.config.host)
            .field("port", &self.config.port)
            .field("idle_len", &self.arena.ready_count())
            .finish()
    }
}

impl DragonflyEtp {
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        let config = DragonflyEtpConfig::from_entry(entry)?;
        Self::new(config)
    }

    pub fn new(config: DragonflyEtpConfig) -> Result<Self, EtpError> {
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
        if let Some(pw) = &config.password {
            handshake.extend_from_slice(&build_auth_command(config.username.as_deref(), pw));
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

    pub async fn connect_authenticated(&self) -> Result<EtpStream, EtpError> {
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
                        "Dragonfly '{}' handshake step {step} failed: {reply}",
                        self.config.id
                    )));
                }
            }
            Ok(stream)
        };

        let timeout_ms = u64::try_from(self.connect_timeout.as_millis()).unwrap_or(u64::MAX);
        tokio::time::timeout(self.connect_timeout, handshake)
            .await
            .map_err(|_| EtpError::Timeout {
                id: self.config.id.clone(),
                timeout_ms,
            })?
    }

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

    pub fn try_acquire(self: &Arc<Self>) -> Option<DragonflyLease> {
        let permit = self.semaphore.try_acquire().ok()?;
        match self.arena.try_acquire_ready_slot(self.idle_timeout_secs) {
            ReadyAcquire::Hit(slot_idx) => {
                permit.forget();
                Some(DragonflyLease::new(Arc::clone(self), slot_idx))
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

    pub async fn acquire(self: &Arc<Self>) -> Result<DragonflyLease, EtpError> {
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
            return Ok(DragonflyLease::new(Arc::clone(self), slot_idx));
        }

        let Some(guard) = self.arena.claim_empty_slot(true) else {
            return Err(EtpError::PoolExhausted {
                id: self.config.id.clone(),
            });
        };

        let stream = self.connect_authenticated().await?;
        let slot_idx = guard.commit_leased(stream);
        permit.forget();
        Ok(DragonflyLease::new(Arc::clone(self), slot_idx))
    }

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
                            tracing::warn!(id = %pool.config.id, error = %err, "Dragonfly replenish failed");
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
                "Dragonfly closed connection before completing RESP line",
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
                "Dragonfly RESP line exceeded 16 KiB limit",
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
