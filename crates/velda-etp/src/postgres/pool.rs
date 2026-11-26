//! PostgreSQL connection pool (`PostgresEtp`) backed by [`EtpPageArena`] with wire v3
//! `StartupMessage`, optional `SSLRequest` StartTLS upgrade, and Cleartext / MD5 / SCRAM-SHA-256
//! authentication.
//!
//! ```text
//!   PostgresEtp::connect_authenticated()
//!         │
//!         ├─► 1. security == Plain    ──► ColdConnectBlueprint::connect_stream()
//!         │      security == StartTls ──► connect_tcp() -> SSLRequest(80877103) -> 'S' -> upgrade_tls()
//!         ├─► 2. Send PG v3 StartupMessage (user, database, client_encoding=UTF8)
//!         └─► 3. Backend message loop:
//!               ├─ 'R' (0)  AuthenticationOk          ──► await ReadyForQuery ('Z')
//!               ├─ 'R' (3)  CleartextPassword         ──► send 'p' password\0
//!               ├─ 'R' (5)  MD5Password(salt[4])      ──► send 'p' "md5"+hex
//!               ├─ 'R' (10) SASL (SCRAM-SHA-256)      ──► client-first -> server-first -> proof -> verify 'v'
//!               ├─ 'S'/'K'/'N'                        ──► skip parameter/key/notice frame
//!               ├─ 'E'      ErrorResponse             ──► Err(EtpError::Config)
//!               └─ 'Z'      ReadyForQuery             ──► Ok(EtpStream)
//! ```

use std::fmt;
use std::num::NonZeroU32;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use ring::digest::{SHA256, digest};
use ring::hmac;
use ring::pbkdf2;
use ring::rand::{SecureRandom, SystemRandom};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Semaphore;

use crate::arena::{EtpPageArena, ReadyAcquire};
use crate::config::EtpEntryConfig;
use crate::connect::{ColdConnectBlueprint, EtpStream, EtpTransportSecurity, build_tls_target};
use crate::error::EtpError;
use crate::postgres::config::PostgresEtpConfig;
use crate::postgres::lease::PostgresLease;

/// PostgreSQL connection pool with pre-authenticated wire v3 sessions inside [`EtpPageArena`].
pub struct PostgresEtp {
    pub config: PostgresEtpConfig,
    pub(crate) blueprint: ColdConnectBlueprint,
    startup_packet: Box<[u8]>,
    pub(crate) arena: EtpPageArena<EtpStream>,
    pub(crate) semaphore: Semaphore,
    connect_timeout: Duration,
    idle_timeout_secs: u64,
}

impl fmt::Debug for PostgresEtp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresEtp")
            .field("id", &self.config.id)
            .field("host", &self.config.host)
            .field("port", &self.config.port)
            .field("database", &self.config.database)
            .field("idle_len", &self.arena.ready_count())
            .finish()
    }
}

impl PostgresEtp {
    /// Creates a new [`PostgresEtp`] pool from a generic [`EtpEntryConfig`].
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        let config = PostgresEtpConfig::from_entry(entry)?;
        Self::new(config)
    }

    /// Creates a new [`PostgresEtp`] pool from a parsed [`PostgresEtpConfig`].
    pub fn new(config: PostgresEtpConfig) -> Result<Self, EtpError> {
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

        let startup_packet = build_startup_packet(
            &config.username,
            &config.database,
            config.application_name.as_deref(),
        )
        .into_boxed_slice();

        let arena = EtpPageArena::new(config.pool.pool_size);
        let semaphore = Semaphore::new(arena.total_slots);

        Ok(Self {
            config,
            blueprint,
            startup_packet,
            arena,
            semaphore,
            connect_timeout,
            idle_timeout_secs,
        })
    }

    /// Establishes a fresh stream and completes PostgreSQL StartTLS (when configured) + wire v3
    /// startup + authentication.
    pub(crate) async fn connect_authenticated(&self) -> Result<EtpStream, EtpError> {
        const PG_SSL_REQUEST: [u8; 8] = [0x00, 0x00, 0x00, 0x08, 0x04, 0xd2, 0x16, 0x2f];
        let id = &self.config.id;
        let timeout_ms = u64::try_from(self.connect_timeout.as_millis()).unwrap_or(u64::MAX);

        let mut stream = match self.config.security {
            EtpTransportSecurity::Plain => self.blueprint.connect_stream().await?,
            EtpTransportSecurity::StartTls => {
                let mut tcp = self.blueprint.connect_tcp().await?;
                let negotiate = async {
                    tcp.write_all(&PG_SSL_REQUEST).await?;
                    tcp.flush().await?;
                    let mut reply = [0u8; 1];
                    tcp.read_exact(&mut reply).await?;
                    if reply[0] != b'S' {
                        return Err(EtpError::Tls(format!(
                            "Postgres '{id}' server rejected StartTLS SSLRequest (reply: {:?})",
                            char::from(reply[0])
                        )));
                    }
                    Ok(())
                };
                match tokio::time::timeout(self.connect_timeout, negotiate).await {
                    Ok(res) => res?,
                    Err(_) => {
                        return Err(EtpError::Timeout {
                            id: id.clone(),
                            timeout_ms,
                        });
                    }
                }
                self.blueprint.upgrade_tls(tcp).await?
            }
            other => {
                return Err(EtpError::Config(format!(
                    "Postgres '{id}' unsupported transport security '{other}'"
                )));
            }
        };

        let username = &self.config.username;
        let password = self.config.password.as_deref();
        let startup = &self.startup_packet;

        let handshake = async {
            stream.write_all(startup).await?;
            stream.flush().await?;

            let mut scram_state: Option<ScramClientState> = None;

            loop {
                let (msg_type, payload) = read_pg_message(&mut stream).await?;
                match msg_type {
                    b'R' => {
                        if payload.len() < 4 {
                            return Err(EtpError::Config(format!(
                                "Postgres '{id}' sent truncated Auth packet"
                            )));
                        }
                        let auth_code =
                            u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                        match auth_code {
                            0 => {
                                // AuthenticationOk; wait for ReadyForQuery ('Z').
                            }
                            3 => {
                                let pw = password.ok_or_else(|| {
                                    EtpError::Config(format!(
                                        "Postgres '{id}' requires Cleartext password"
                                    ))
                                })?;
                                let mut body = Vec::with_capacity(pw.len() + 1);
                                body.extend_from_slice(pw.as_bytes());
                                body.push(0);
                                write_pg_message(&mut stream, b'p', &body).await?;
                            }
                            5 => {
                                if payload.len() < 8 {
                                    return Err(EtpError::Config(format!(
                                        "Postgres '{id}' sent truncated MD5 salt"
                                    )));
                                }
                                let pw = password.ok_or_else(|| {
                                    EtpError::Config(format!(
                                        "Postgres '{id}' requires MD5 password"
                                    ))
                                })?;
                                let salt = [payload[4], payload[5], payload[6], payload[7]];
                                let digest_str = pg_md5_password(username, pw, salt);
                                let mut body = Vec::with_capacity(digest_str.len() + 1);
                                body.extend_from_slice(digest_str.as_bytes());
                                body.push(0);
                                write_pg_message(&mut stream, b'p', &body).await?;
                            }
                            10 => {
                                let pw = password.ok_or_else(|| {
                                    EtpError::Config(format!(
                                        "Postgres '{id}' requires SCRAM-SHA-256 password"
                                    ))
                                })?;
                                let mechs = &payload[4..];
                                if !contains_mech(mechs, b"SCRAM-SHA-256") {
                                    return Err(EtpError::Config(format!(
                                        "Postgres '{id}' offered unsupported SASL mechanisms"
                                    )));
                                }
                                let (state, client_first) = ScramClientState::start(pw)?;
                                scram_state = Some(state);

                                let mech_name = b"SCRAM-SHA-256\0";
                                let mut body =
                                    Vec::with_capacity(mech_name.len() + 4 + client_first.len());
                                body.extend_from_slice(mech_name);
                                body.extend_from_slice(&(client_first.len() as i32).to_be_bytes());
                                body.extend_from_slice(client_first.as_bytes());
                                write_pg_message(&mut stream, b'p', &body).await?;
                            }
                            11 => {
                                let state = scram_state.as_mut().ok_or_else(|| {
                                    EtpError::Config(format!(
                                        "Postgres '{id}' sent unexpected SASLContinue"
                                    ))
                                })?;
                                let server_first =
                                    std::str::from_utf8(&payload[4..]).map_err(|_| {
                                        EtpError::Config(format!(
                                            "Postgres '{id}' sent invalid UTF-8 SCRAM challenge"
                                        ))
                                    })?;
                                let client_final = state.handle_server_first(server_first, id)?;
                                write_pg_message(&mut stream, b'p', client_final.as_bytes())
                                    .await?;
                            }
                            12 => {
                                let state = scram_state.take().ok_or_else(|| {
                                    EtpError::Config(format!(
                                        "Postgres '{id}' sent unexpected SASLFinal"
                                    ))
                                })?;
                                let server_final =
                                    std::str::from_utf8(&payload[4..]).map_err(|_| {
                                        EtpError::Config(format!(
                                            "Postgres '{id}' sent invalid UTF-8 SCRAM final"
                                        ))
                                    })?;
                                state.verify_server_final(server_final, id)?;
                            }
                            other => {
                                return Err(EtpError::Config(format!(
                                    "Postgres '{id}' requested unsupported auth method {other}"
                                )));
                            }
                        }
                    }
                    b'Z' => return Ok(stream),
                    b'S' | b'K' | b'N' => {
                        // ParameterStatus, BackendKeyData, NoticeResponse
                    }
                    b'E' => {
                        let msg = parse_pg_error(&payload);
                        return Err(EtpError::Config(format!("Postgres '{id}' error: {msg}")));
                    }
                    other => {
                        return Err(EtpError::Config(format!(
                            "Postgres '{id}' unexpected startup frame type '{}'",
                            char::from(other)
                        )));
                    }
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

    /// Pre-warms up to `baseline_slots` authenticated PostgreSQL connections into the arena.
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

    /// Non-blocking fast path: borrows a warm PostgreSQL connection in-place via `TZCNT`.
    pub fn try_acquire(self: &std::sync::Arc<Self>) -> Option<PostgresLease> {
        let permit = self.semaphore.try_acquire().ok()?;
        match self.arena.try_acquire_ready_slot(self.idle_timeout_secs) {
            ReadyAcquire::Hit(slot_idx) => {
                permit.forget();
                Some(PostgresLease::new(std::sync::Arc::clone(self), slot_idx))
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

    /// Acquires an authenticated PostgreSQL session lease.
    pub async fn acquire(self: &std::sync::Arc<Self>) -> Result<PostgresLease, EtpError> {
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
            return Ok(PostgresLease::new(std::sync::Arc::clone(self), slot_idx));
        }

        let Some(guard) = self.arena.claim_empty_slot(true) else {
            return Err(EtpError::PoolExhausted {
                id: self.config.id.clone(),
            });
        };

        let stream = self.connect_authenticated().await?;
        let slot_idx = guard.commit_leased(stream);
        permit.forget();
        Ok(PostgresLease::new(std::sync::Arc::clone(self), slot_idx))
    }

    /// Triggers lock-free background replenishment using Cache Line 0 `AtomicBool` CAS.
    pub(crate) fn trigger_replenish(self: &std::sync::Arc<Self>) {
        if !self.arena.try_begin_replenish() {
            return;
        }

        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            self.arena.abort_replenish();
            return;
        };

        let weak = std::sync::Arc::downgrade(self);
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
                                "Postgres ETP background replenishment failed"
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

fn build_startup_packet(username: &str, database: &str, application_name: Option<&str>) -> Vec<u8> {
    let mut params = Vec::with_capacity(128);
    // Reserve 4 bytes length + 4 bytes protocol version 3.0 (196608)
    params.extend_from_slice(&[0u8; 4]);
    params.extend_from_slice(&196_608u32.to_be_bytes());

    let push_kv = |buf: &mut Vec<u8>, k: &[u8], v: &[u8]| {
        buf.extend_from_slice(k);
        buf.push(0);
        buf.extend_from_slice(v);
        buf.push(0);
    };
    push_kv(&mut params, b"user", username.as_bytes());
    push_kv(&mut params, b"database", database.as_bytes());
    push_kv(&mut params, b"client_encoding", b"UTF8");
    if let Some(app) = application_name {
        push_kv(&mut params, b"application_name", app.as_bytes());
    }
    params.push(0);

    let len = params.len() as u32;
    params[0..4].copy_from_slice(&len.to_be_bytes());
    params
}

async fn write_pg_message(
    stream: &mut EtpStream,
    msg_type: u8,
    body: &[u8],
) -> Result<(), EtpError> {
    let total_len = (4 + body.len()) as u32;
    let mut header = [0u8; 5];
    header[0] = msg_type;
    header[1..5].copy_from_slice(&total_len.to_be_bytes());
    stream.write_all(&header).await?;
    stream.write_all(body).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_pg_message(stream: &mut EtpStream) -> Result<(u8, Vec<u8>), EtpError> {
    let mut header = [0u8; 5];
    stream.read_exact(&mut header).await?;
    let msg_type = header[0];
    let len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]);
    if !(4..=16 * 1024 * 1024).contains(&len) {
        return Err(EtpError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Postgres frame length out of bounds: {len}"),
        )));
    }
    let payload_len = (len - 4) as usize;
    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        stream.read_exact(&mut payload).await?;
    }
    Ok((msg_type, payload))
}

fn parse_pg_error(payload: &[u8]) -> String {
    let mut idx = 0usize;
    let mut message = None;
    let mut code = None;
    while idx < payload.len() {
        let tag = payload[idx];
        idx += 1;
        if tag == 0 {
            break;
        }
        let end = payload[idx..]
            .iter()
            .position(|&b| b == 0)
            .map(|rel| idx + rel)
            .unwrap_or(payload.len());
        let val = String::from_utf8_lossy(&payload[idx..end]).into_owned();
        idx = (end + 1).min(payload.len());
        match tag {
            b'M' => message = Some(val),
            b'C' => code = Some(val),
            _ => {}
        }
    }
    match (code, message) {
        (Some(c), Some(m)) => format!("[{c}] {m}"),
        (None, Some(m)) => m,
        (Some(c), None) => c,
        (None, None) => "unknown postgresql error".to_string(),
    }
}

fn contains_mech(mut mechs: &[u8], target: &[u8]) -> bool {
    while !mechs.is_empty() {
        if mechs[0] == 0 {
            break;
        }
        let end = mechs.iter().position(|&b| b == 0).unwrap_or(mechs.len());
        if &mechs[..end] == target {
            return true;
        }
        mechs = &mechs[(end + 1).min(mechs.len())..];
    }
    false
}

struct ScramClientState {
    password: String,
    client_nonce: String,
    client_first_bare: String,
    expected_server_sig: Option<Vec<u8>>,
}

impl ScramClientState {
    fn start(password: &str) -> Result<(Self, String), EtpError> {
        let mut nonce_bytes = [0u8; 18];
        SystemRandom::new()
            .fill(&mut nonce_bytes)
            .map_err(|_| EtpError::Config("SCRAM nonce CSPRNG failure".to_string()))?;
        let client_nonce = B64.encode(nonce_bytes);
        let client_first_bare = format!("n=,r={client_nonce}");
        let client_first = format!("n,,{client_first_bare}");
        Ok((
            Self {
                password: password.to_string(),
                client_nonce,
                client_first_bare,
                expected_server_sig: None,
            },
            client_first,
        ))
    }

    fn handle_server_first(&mut self, server_first: &str, id: &str) -> Result<String, EtpError> {
        let mut nonce = None;
        let mut salt_b64 = None;
        let mut iter = None;
        for part in server_first.split(',') {
            if let Some(v) = part.strip_prefix("r=") {
                nonce = Some(v);
            } else if let Some(v) = part.strip_prefix("s=") {
                salt_b64 = Some(v);
            } else if let Some(v) = part.strip_prefix("i=") {
                iter = v.parse::<u32>().ok();
            }
        }
        let full_nonce = nonce.ok_or_else(|| {
            EtpError::Config(format!("Postgres '{id}' SCRAM server-first missing nonce"))
        })?;
        if !full_nonce.starts_with(&self.client_nonce) {
            return Err(EtpError::Config(format!(
                "Postgres '{id}' SCRAM server nonce mismatch"
            )));
        }
        let salt = B64
            .decode(salt_b64.unwrap_or_default())
            .map_err(|_| EtpError::Config(format!("Postgres '{id}' invalid SCRAM salt")))?;
        let iterations = NonZeroU32::new(iter.unwrap_or(0)).ok_or_else(|| {
            EtpError::Config(format!("Postgres '{id}' invalid SCRAM iteration count"))
        })?;

        let mut salted_password = [0u8; 32];
        pbkdf2::derive(
            pbkdf2::PBKDF2_HMAC_SHA256,
            iterations,
            &salt,
            self.password.as_bytes(),
            &mut salted_password,
        );

        let sp_key = hmac::Key::new(hmac::HMAC_SHA256, &salted_password);
        let client_key = hmac::sign(&sp_key, b"Client Key");
        let server_key = hmac::sign(&sp_key, b"Server Key");

        let stored_key = digest(&SHA256, client_key.as_ref());
        let client_final_without_proof = format!("c=biws,r={full_nonce}");
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, server_first, client_final_without_proof
        );

        let sk_hmac = hmac::Key::new(hmac::HMAC_SHA256, stored_key.as_ref());
        let client_sig = hmac::sign(&sk_hmac, auth_message.as_bytes());
        let mut proof = [0u8; 32];
        for (dst, (a, b)) in proof
            .iter_mut()
            .zip(client_key.as_ref().iter().zip(client_sig.as_ref().iter()))
        {
            *dst = a ^ b;
        }

        let srv_hmac = hmac::Key::new(hmac::HMAC_SHA256, server_key.as_ref());
        let expected_srv = hmac::sign(&srv_hmac, auth_message.as_bytes());
        self.expected_server_sig = Some(expected_srv.as_ref().to_vec());

        Ok(format!(
            "{client_final_without_proof},p={}",
            B64.encode(proof)
        ))
    }

    fn verify_server_final(self, server_final: &str, id: &str) -> Result<(), EtpError> {
        let Some(expected) = self.expected_server_sig else {
            return Err(EtpError::Config(format!(
                "Postgres '{id}' invalid SCRAM state on server-final"
            )));
        };
        let sig_b64 = server_final.strip_prefix("v=").ok_or_else(|| {
            EtpError::Config(format!(
                "Postgres '{id}' SCRAM server-final error: {server_final}"
            ))
        })?;
        let actual = B64.decode(sig_b64).map_err(|_| {
            EtpError::Config(format!("Postgres '{id}' invalid SCRAM server signature"))
        })?;
        if actual != expected {
            return Err(EtpError::Config(format!(
                "Postgres '{id}' SCRAM server signature verification failed"
            )));
        }
        Ok(())
    }
}

fn pg_md5_password(username: &str, password: &str, salt: [u8; 4]) -> String {
    let mut inner_in = Vec::with_capacity(password.len() + username.len());
    inner_in.extend_from_slice(password.as_bytes());
    inner_in.extend_from_slice(username.as_bytes());
    let inner_hex = md5_hex(&inner_in);

    let mut outer_in = Vec::with_capacity(36);
    outer_in.extend_from_slice(inner_hex.as_bytes());
    outer_in.extend_from_slice(&salt);
    let outer_hex = md5_hex(&outer_in);
    format!("md5{outer_hex}")
}

fn md5_hex(input: &[u8]) -> String {
    let digest = md5_rfc1321(input);
    let mut hex = String::with_capacity(32);
    for b in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{b:02x}");
    }
    hex
}

// Compact RFC 1321 MD5 digest for PostgreSQL AuthenticationMD5Password compatibility.
#[allow(clippy::many_single_char_names)]
fn md5_rfc1321(data: &[u8]) -> [u8; 16] {
    const S: [u32; 64] = [
        7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20, 5,
        9, 14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 6, 10,
        15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
    ];
    const K: [u32; 64] = [
        0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a, 0xa8304613,
        0xfd469501, 0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be, 0x6b901122, 0xfd987193,
        0xa679438e, 0x49b40821, 0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa, 0xd62f105d,
        0x02441453, 0xd8a1e681, 0xe7d3fbc8, 0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed,
        0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a, 0xfffa3942, 0x8771f681, 0x6d9d6122,
        0xfde5380c, 0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70, 0x289b7ec6, 0xeaa127fa,
        0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665, 0xf4292244,
        0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
        0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1, 0xf7537e82, 0xbd3af235, 0x2ad7d2bb,
        0xeb86d391,
    ];

    let bit_len = (data.len() as u64).wrapping_mul(8);
    let mut padded = data.to_vec();
    padded.push(0x80);
    while (padded.len() % 64) != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_le_bytes());

    let mut a0: u32 = 0x6745_2301;
    let mut b0: u32 = 0xefcd_ab89;
    let mut c0: u32 = 0x98ba_dcfe;
    let mut d0: u32 = 0x1032_5476;

    for chunk in padded.as_chunks::<64>().0 {
        let mut m = [0u32; 16];
        for (i, word) in m.iter_mut().enumerate() {
            let off = i * 4;
            *word =
                u32::from_le_bytes([chunk[off], chunk[off + 1], chunk[off + 2], chunk[off + 3]]);
        }
        let (mut a, mut b, mut c, mut d) = (a0, b0, c0, d0);
        for i in 0..64 {
            let (f, g) = match i {
                0..=15 => ((b & c) | ((!b) & d), i),
                16..=31 => ((d & b) | ((!d) & c), (5 * i + 1) % 16),
                32..=47 => (b ^ c ^ d, (3 * i + 5) % 16),
                _ => (c ^ (b | (!d)), (7 * i) % 16),
            };
            let temp = d;
            d = c;
            c = b;
            b = b.wrapping_add(
                a.wrapping_add(f)
                    .wrapping_add(K[i])
                    .wrapping_add(m[g])
                    .rotate_left(S[i]),
            );
            a = temp;
        }
        a0 = a0.wrapping_add(a);
        b0 = b0.wrapping_add(b);
        c0 = c0.wrapping_add(c);
        d0 = d0.wrapping_add(d);
    }

    let mut out = [0u8; 16];
    out[0..4].copy_from_slice(&a0.to_le_bytes());
    out[4..8].copy_from_slice(&b0.to_le_bytes());
    out[8..12].copy_from_slice(&c0.to_le_bytes());
    out[12..16].copy_from_slice(&d0.to_le_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_md5_rfc_and_pg_vector() {
        assert_eq!(md5_hex(b""), "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(md5_hex(b"abc"), "900150983cd24fb0d6963f7d28e17f72");
        let salt = [0x01, 0x02, 0x03, 0x04];
        let res = pg_md5_password("postgres", "secret", salt);
        assert!(res.starts_with("md5"));
        assert_eq!(res.len(), 35);
    }
}
