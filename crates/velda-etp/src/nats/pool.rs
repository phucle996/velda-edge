//! NATS connection pool (`NatsEtp`) backed by [`EtpPageArena`] with `INFO`, optional StartTLS
//! upgrade, and pre-baked `CONNECT` + `PING`/`PONG` protocol handshake.
//!
//! ```text
//!   NatsEtp::connect_authenticated()
//!         │
//!         ├─► 1. Plain    ──► connect_stream() -> read INFO
//!         │      StartTls ──► connect_tcp() -> read INFO -> upgrade_tls()
//!         ├─► 2. Send pipelined "CONNECT {...}\r\nPING\r\n"
//!         └─► 3. Expect "PONG\r\n" (reject "-ERR ...") ──► Ok(EtpStream)
//! ```

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::Semaphore;

use crate::arena::{EtpPageArena, ReadyAcquire};
use crate::config::EtpEntryConfig;
use crate::connect::{ColdConnectBlueprint, EtpStream, EtpTransportSecurity, build_tls_target};
use crate::error::EtpError;
use crate::nats::config::NatsEtpConfig;
use crate::nats::lease::NatsLease;

/// NATS / JetStream connection pool with pre-authenticated sessions inside [`EtpPageArena`].
pub struct NatsEtp {
    pub config: NatsEtpConfig,
    pub(crate) blueprint: ColdConnectBlueprint,
    connect_frame: Box<[u8]>,
    pub(crate) arena: EtpPageArena<EtpStream>,
    pub(crate) semaphore: Semaphore,
    connect_timeout: Duration,
    idle_timeout_secs: u64,
}

impl fmt::Debug for NatsEtp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NatsEtp")
            .field("id", &self.config.id)
            .field("host", &self.config.host)
            .field("port", &self.config.port)
            .field("bucket", &self.config.bucket)
            .field("idle_len", &self.arena.ready_count())
            .finish()
    }
}

impl NatsEtp {
    /// Creates a new [`NatsEtp`] pool from a generic [`EtpEntryConfig`].
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        let config = NatsEtpConfig::from_entry(entry)?;
        Self::new(config)
    }

    /// Creates a new [`NatsEtp`] pool from a parsed [`NatsEtpConfig`].
    pub fn new(config: NatsEtpConfig) -> Result<Self, EtpError> {
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

        let connect_frame = build_connect_ping_frame(&config).into_boxed_slice();

        let arena = EtpPageArena::new(config.pool.pool_size);
        let semaphore = Semaphore::new(arena.total_slots);

        Ok(Self {
            config,
            blueprint,
            connect_frame,
            arena,
            semaphore,
            connect_timeout,
            idle_timeout_secs,
        })
    }

    /// Establishes a fresh stream and completes `INFO` -> (optional StartTLS) -> `CONNECT` + `PING`/`PONG`.
    pub(crate) async fn connect_authenticated(&self) -> Result<EtpStream, EtpError> {
        let frame = &self.connect_frame;
        let id = &self.config.id;
        let timeout_ms = u64::try_from(self.connect_timeout.as_millis()).unwrap_or(u64::MAX);

        let mut stream = match self.config.security {
            EtpTransportSecurity::Plain => {
                let mut stream = self.blueprint.connect_stream().await?;
                let info_line =
                    match tokio::time::timeout(self.connect_timeout, read_nats_line(&mut stream))
                        .await
                    {
                        Ok(res) => res?,
                        Err(_) => {
                            return Err(EtpError::Timeout {
                                id: id.clone(),
                                timeout_ms,
                            });
                        }
                    };
                if !info_line.starts_with("INFO ") {
                    return Err(EtpError::Config(format!(
                        "NATS '{id}' expected INFO greeting, got: {info_line}"
                    )));
                }
                stream
            }
            EtpTransportSecurity::StartTls => {
                let mut tcp = self.blueprint.connect_tcp().await?;
                let info_line = match tokio::time::timeout(
                    self.connect_timeout,
                    read_nats_line(&mut tcp),
                )
                .await
                {
                    Ok(res) => res?,
                    Err(_) => {
                        return Err(EtpError::Timeout {
                            id: id.clone(),
                            timeout_ms,
                        });
                    }
                };
                if !info_line.starts_with("INFO ") {
                    return Err(EtpError::Config(format!(
                        "NATS '{id}' expected INFO greeting, got: {info_line}"
                    )));
                }
                self.blueprint.upgrade_tls(tcp).await?
            }
            other => {
                return Err(EtpError::Config(format!(
                    "NATS '{id}' unsupported transport security '{other}'"
                )));
            }
        };

        let handshake = async {
            stream.write_all(frame).await?;
            stream.flush().await?;

            loop {
                let line = read_nats_line(&mut stream).await?;
                if line == "PONG" {
                    return Ok(stream);
                }
                if line == "+OK" {
                    continue;
                }
                if let Some(err) = line.strip_prefix("-ERR") {
                    return Err(EtpError::Config(format!(
                        "NATS '{id}' handshake failed: {}",
                        err.trim()
                    )));
                }
            }
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

    /// Pre-warms up to `baseline_slots` authenticated NATS connections into the arena.
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

    /// Non-blocking fast path: borrows a warm NATS connection in-place via `TZCNT`.
    pub fn try_acquire(self: &Arc<Self>) -> Option<NatsLease> {
        let permit = self.semaphore.try_acquire().ok()?;
        match self.arena.try_acquire_ready_slot(self.idle_timeout_secs) {
            ReadyAcquire::Hit(slot_idx) => {
                permit.forget();
                Some(NatsLease::new(Arc::clone(self), slot_idx))
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

    /// Acquires an authenticated NATS session lease.
    pub async fn acquire(self: &Arc<Self>) -> Result<NatsLease, EtpError> {
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
            return Ok(NatsLease::new(Arc::clone(self), slot_idx));
        }

        let Some(guard) = self.arena.claim_empty_slot(true) else {
            return Err(EtpError::PoolExhausted {
                id: self.config.id.clone(),
            });
        };

        let stream = self.connect_authenticated().await?;
        let slot_idx = guard.commit_leased(stream);
        permit.forget();
        Ok(NatsLease::new(Arc::clone(self), slot_idx))
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
                                "NATS ETP background replenishment failed"
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

fn build_connect_ping_frame(config: &NatsEtpConfig) -> Vec<u8> {
    let mut payload = serde_json::Map::new();
    payload.insert("verbose".to_string(), serde_json::Value::Bool(false));
    payload.insert("pedantic".to_string(), serde_json::Value::Bool(false));
    payload.insert("protocol".to_string(), serde_json::Value::Number(1.into()));
    payload.insert(
        "lang".to_string(),
        serde_json::Value::String("rust".to_string()),
    );
    payload.insert(
        "version".to_string(),
        serde_json::Value::String("velda-etp".to_string()),
    );
    if let Some(ref token) = config.token {
        payload.insert(
            "auth_token".to_string(),
            serde_json::Value::String(token.clone()),
        );
    }
    if let Some(ref user) = config.username {
        payload.insert("user".to_string(), serde_json::Value::String(user.clone()));
    }
    if let Some(ref pass) = config.password {
        payload.insert("pass".to_string(), serde_json::Value::String(pass.clone()));
    }
    let json = serde_json::Value::Object(payload).to_string();
    format!("CONNECT {json}\r\nPING\r\n").into_bytes()
}

async fn read_nats_line<S: AsyncRead + Unpin>(stream: &mut S) -> Result<String, EtpError> {
    let mut buf = Vec::with_capacity(128);
    let mut byte = [0u8; 1];
    loop {
        let n = stream.read(&mut byte).await?;
        if n == 0 {
            return Err(EtpError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "NATS closed connection before completing line",
            )));
        }
        buf.push(byte[0]);
        if buf.len() >= 2 && buf[buf.len() - 2] == b'\r' && buf[buf.len() - 1] == b'\n' {
            buf.truncate(buf.len() - 2);
            return Ok(String::from_utf8_lossy(&buf).into_owned());
        }
        if buf.len() > 64 * 1024 {
            return Err(EtpError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "NATS control line exceeded 64 KiB limit",
            )));
        }
    }
}
