//! Lock-free Kafka ETP broker connection pool (`KafkaEtp`) backed by [`EtpPageArena<EtpStream>`]
//! and per-broker [`ColdConnectBlueprint`]s with optional SASL authentication on connect.
//!
//! ```text
//!   warm() / acquire() / try_acquire()
//!         │
//!         ├─► Ready hit (TZCNT on ready_bitmap)
//!         │     └─► prefetch_l1_slot(Slot[idx]) -> borrow in-place -> KafkaLease(idx)
//!         │
//!         └─► Miss / Burst Spillover ──► connect_fresh():
//!               1. Failover across blueprints[(start + offset) % len].connect_stream()
//!               2. If sasl_mechanism != None:
//!                    SaslHandshake (API 17, v1) -> SaslAuthenticate (API 36, v1)
//!               3. write_leased_slot(idx, stream) -> KafkaLease(idx)
//!
//!   Cold Blueprints & Page Arena Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ blueprints: Vec<ColdConnectBlueprint> (integer SocketAddr per broker)      │
//!   │ prebaked_sasl_handshake / prebaked_sasl_auth (Box<[u8]> if SASL/PLAIN)     │
//!   ├────────────────────────────────────────────────────────────────────────────┤
//!   │ Arena: Cache Line 0 (64B) | Slot[idx] at (idx << 6) + 64 | +4096B Spare    │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::fmt;
use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::Semaphore;

use crate::arena::{EtpPageArena, ReadyAcquire};
use crate::config::EtpEntryConfig;
use crate::connect::{ColdConnectBlueprint, EtpStream, build_tls_target};
use crate::error::EtpError;
use crate::kafka::config::{KafkaEtpConfig, KafkaSaslMechanism};
use crate::kafka::lease::KafkaLease;

const API_KEY_SASL_HANDSHAKE: i16 = 17;
const API_VERSION_SASL_HANDSHAKE: i16 = 1;
const API_KEY_SASL_AUTHENTICATE: i16 = 36;
const API_VERSION_SASL_AUTHENTICATE: i16 = 1;

/// Lock-free bounded pool of authenticated TCP/TLS connections to Apache Kafka brokers.
pub struct KafkaEtp {
    pub config: KafkaEtpConfig,
    pub blueprints: Vec<ColdConnectBlueprint>,
    prebaked_sasl_handshake: Option<Box<[u8]>>,
    prebaked_sasl_auth: Option<Box<[u8]>>,
    pub arena: EtpPageArena<EtpStream>,
    pub semaphore: Semaphore,
    pub connect_timeout: Duration,
    pub idle_timeout_secs: u64,
    broker_cursor: AtomicUsize,
}

impl fmt::Debug for KafkaEtp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaEtp")
            .field("id", &self.config.id)
            .field("brokers", &self.config.brokers)
            .field("client_id", &self.config.client_id)
            .field("sasl_mechanism", &self.config.sasl_mechanism)
            .field("baseline_slots", &self.arena.baseline_slots)
            .field("total_slots", &self.arena.total_slots)
            .field("page_count", &self.arena.page_count)
            .field("idle_len", &self.arena.ready_count())
            .finish()
    }
}

fn encode_kafka_sasl_handshake_frame(client_id: &str, mechanism: &str) -> Box<[u8]> {
    let body_len = 2 + 2 + 4 + 2 + client_id.len() + 2 + mechanism.len();
    let mut frame = Vec::with_capacity(4 + body_len);
    frame.extend_from_slice(&(body_len as i32).to_be_bytes());
    frame.extend_from_slice(&API_KEY_SASL_HANDSHAKE.to_be_bytes());
    frame.extend_from_slice(&API_VERSION_SASL_HANDSHAKE.to_be_bytes());
    frame.extend_from_slice(&1i32.to_be_bytes());
    frame.extend_from_slice(&(client_id.len() as i16).to_be_bytes());
    frame.extend_from_slice(client_id.as_bytes());
    frame.extend_from_slice(&(mechanism.len() as i16).to_be_bytes());
    frame.extend_from_slice(mechanism.as_bytes());
    frame.into_boxed_slice()
}

fn encode_kafka_sasl_plain_auth_frame(
    client_id: &str,
    username: &str,
    password: &str,
) -> Box<[u8]> {
    let auth_len = 2 + username.len() + password.len();
    let body_len = 2 + 2 + 4 + 2 + client_id.len() + 4 + auth_len;
    let mut frame = Vec::with_capacity(4 + body_len);
    frame.extend_from_slice(&(body_len as i32).to_be_bytes());
    frame.extend_from_slice(&API_KEY_SASL_AUTHENTICATE.to_be_bytes());
    frame.extend_from_slice(&API_VERSION_SASL_AUTHENTICATE.to_be_bytes());
    frame.extend_from_slice(&2i32.to_be_bytes());
    frame.extend_from_slice(&(client_id.len() as i16).to_be_bytes());
    frame.extend_from_slice(client_id.as_bytes());
    frame.extend_from_slice(&(auth_len as i32).to_be_bytes());
    frame.push(0);
    frame.extend_from_slice(username.as_bytes());
    frame.push(0);
    frame.extend_from_slice(password.as_bytes());
    frame.into_boxed_slice()
}

async fn read_kafka_frame_error_code(stream: &mut EtpStream) -> Result<i16, EtpError> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let frame_len = i32::from_be_bytes(len_buf);
    if !(6..=16 * 1024 * 1024).contains(&frame_len) {
        return Err(std::io::Error::new(
            ErrorKind::InvalidData,
            format!("Invalid Kafka SASL response frame length: {frame_len}"),
        )
        .into());
    }
    let mut payload = vec![0u8; frame_len as usize];
    stream.read_exact(&mut payload).await?;
    let Some(code_bytes) = payload.get(4..6) else {
        return Err(std::io::Error::new(
            ErrorKind::UnexpectedEof,
            "Truncated Kafka SASL response payload",
        )
        .into());
    };
    Ok(i16::from_be_bytes([code_bytes[0], code_bytes[1]]))
}

impl KafkaEtp {
    /// Creates a new `KafkaEtp` pool from a generic [`EtpEntryConfig`].
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        Self::new(KafkaEtpConfig::from_entry(entry)?)
    }

    /// Creates a new `KafkaEtp` pool from a parsed [`KafkaEtpConfig`].
    pub fn new(config: KafkaEtpConfig) -> Result<Self, EtpError> {
        if config.resolved_brokers.is_empty() {
            return Err(EtpError::Config(format!(
                "Kafka ETP '{}' has no brokers",
                config.id
            )));
        }

        let connect_timeout = Duration::from_millis(config.pool.connect_timeout_ms.max(1));
        let idle_timeout_secs = config.pool.idle_timeout_ms.div_ceil(1000);

        let mut blueprints = Vec::with_capacity(config.resolved_brokers.len());
        for (host, port) in &config.resolved_brokers {
            let tls_target = build_tls_target(&config.id, host, config.tls.as_ref())?;
            blueprints.push(ColdConnectBlueprint::new(
                &config.id,
                host,
                *port,
                tls_target,
                connect_timeout,
            ));
        }

        let (prebaked_sasl_handshake, prebaked_sasl_auth) = match config.sasl_mechanism {
            KafkaSaslMechanism::None => (None, None),
            KafkaSaslMechanism::Plain => (
                Some(encode_kafka_sasl_handshake_frame(
                    &config.client_id,
                    config.sasl_mechanism.as_str(),
                )),
                Some(encode_kafka_sasl_plain_auth_frame(
                    &config.client_id,
                    &config.sasl_username,
                    &config.sasl_password,
                )),
            ),
            KafkaSaslMechanism::ScramSha256 | KafkaSaslMechanism::ScramSha512 => (
                Some(encode_kafka_sasl_handshake_frame(
                    &config.client_id,
                    config.sasl_mechanism.as_str(),
                )),
                None,
            ),
        };

        let arena = EtpPageArena::new(config.pool.pool_size);
        let semaphore = Semaphore::new(arena.total_slots);

        Ok(Self {
            config,
            blueprints,
            prebaked_sasl_handshake,
            prebaked_sasl_auth,
            arena,
            semaphore,
            connect_timeout,
            idle_timeout_secs,
            broker_cursor: AtomicUsize::new(0),
        })
    }

    async fn authenticate_sasl(&self, stream: &mut EtpStream) -> Result<(), EtpError> {
        let Some(ref handshake_frame) = self.prebaked_sasl_handshake else {
            return Ok(());
        };

        let auth_exchange = async {
            stream.write_all(handshake_frame).await?;
            stream.flush().await?;
            let handshake_err = read_kafka_frame_error_code(stream).await?;
            if handshake_err != 0 {
                return Err(EtpError::Config(format!(
                    "Kafka ETP '{}' SaslHandshake rejected mechanism '{}' with error code {handshake_err}",
                    self.config.id,
                    self.config.sasl_mechanism.as_str()
                )));
            }

            match self.config.sasl_mechanism {
                KafkaSaslMechanism::Plain => {
                    let Some(ref auth_frame) = self.prebaked_sasl_auth else {
                        return Err(EtpError::Config(format!(
                            "Kafka ETP '{}' SASL/PLAIN frame not initialized",
                            self.config.id
                        )));
                    };
                    stream.write_all(auth_frame).await?;
                    stream.flush().await?;
                    let auth_err = read_kafka_frame_error_code(stream).await?;
                    if auth_err != 0 {
                        return Err(EtpError::Config(format!(
                            "Kafka ETP '{}' SASL/PLAIN authentication failed for user '{}' with error code {auth_err}",
                            self.config.id, self.config.sasl_username
                        )));
                    }
                    Ok(())
                }
                KafkaSaslMechanism::ScramSha256 | KafkaSaslMechanism::ScramSha512 => {
                    Err(EtpError::Config(format!(
                        "Kafka ETP '{}': SASL mechanism '{}' requires SCRAM challenge-response (unsupported in connect handshake; use 'plain' or TLS client auth)",
                        self.config.id,
                        self.config.sasl_mechanism.as_str()
                    )))
                }
                KafkaSaslMechanism::None => Ok(()),
            }
        };

        let timeout_ms = u64::try_from(self.connect_timeout.as_millis()).unwrap_or(u64::MAX);
        match tokio::time::timeout(self.connect_timeout, auth_exchange).await {
            Ok(res) => res,
            Err(_) => Err(EtpError::Timeout {
                id: self.config.id.clone(),
                timeout_ms,
            }),
        }
    }

    pub(crate) async fn connect_fresh(&self) -> Result<EtpStream, EtpError> {
        let count = self.blueprints.len();
        let start = self.broker_cursor.fetch_add(1, Ordering::Relaxed) % count;
        let mut last_err: Option<EtpError> = None;

        for offset in 0..count {
            match self.blueprints[(start + offset) % count]
                .connect_stream()
                .await
            {
                Ok(mut stream) => match self.authenticate_sasl(&mut stream).await {
                    Ok(()) => return Ok(stream),
                    Err(err) => last_err = Some(err),
                },
                Err(err) => last_err = Some(err),
            }
        }

        Err(last_err.unwrap_or_else(|| {
            EtpError::Config(format!("Kafka ETP '{}' has no brokers", self.config.id))
        }))
    }

    /// Pre-warms up to `baseline_slots` authenticated broker connections.
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

    /// Non-blocking fast path: borrows a warm Kafka lease in-place if available.
    pub fn try_acquire(self: &Arc<Self>) -> Option<KafkaLease> {
        let permit = self.semaphore.try_acquire().ok()?;
        match self.arena.try_acquire_ready_slot(self.idle_timeout_secs) {
            ReadyAcquire::Hit(slot_idx) => {
                permit.forget();
                Some(KafkaLease::new(Arc::clone(self), slot_idx))
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

    /// Acquires a Kafka broker connection lease.
    pub async fn acquire(self: &Arc<Self>) -> Result<KafkaLease, EtpError> {
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
            return Ok(KafkaLease::new(Arc::clone(self), slot_idx));
        }

        let Some(guard) = self.arena.claim_empty_slot(true) else {
            return Err(EtpError::PoolExhausted {
                id: self.config.id.clone(),
            });
        };

        let stream = self.connect_fresh().await?;
        let slot_idx = guard.commit_leased(stream);
        permit.forget();
        Ok(KafkaLease::new(Arc::clone(self), slot_idx))
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
                        Ok(stream) => {
                            guard.commit_warm(stream);
                            drop(permit);
                        }
                        Err(err) => {
                            drop(guard);
                            drop(permit);
                            tracing::warn!(id = %pool.config.id, error = %err, "Kafka ETP background replenishment failed");
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
