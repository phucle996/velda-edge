//! MySQL / MariaDB connection pool (`MysqlEtp`) backed by [`EtpPageArena`] with MySQL
//! `HandshakeV10`, optional `SSLRequest` StartTLS upgrade, and `mysql_native_password` /
//! `caching_sha2_password` / `AuthSwitchRequest` authentication.
//!
//! ```text
//!   MysqlEtp::connect_authenticated()
//!         │
//!         ├─► 1. connect_tcp / connect_stream
//!         ├─► 2. Read Server HandshakeV10 (seq=0) ──► scramble[20] + server_caps + auth_plugin_name
//!         ├─► 3. If StartTls: check CLIENT_SSL -> send 32B SSLRequest (seq=1) -> upgrade_tls()
//!         ├─► 4. Send HandshakeResponse41 (seq=next) ──► username + auth_token + database
//!         └─► 5. Read server reply loop:
//!               ├─ 0x00 OK                      ──► Ok(EtpStream)
//!               ├─ 0xFE AuthSwitchRequest       ──► compute switch response -> send seq+1
//!               ├─ 0x01 AuthMoreData (0x03/0x04)──► fast auth ok OR TLS cleartext password\0
//!               └─ 0xFF ERR                     ──► Err(EtpError::Config)
//! ```

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use ring::digest::{SHA1_FOR_LEGACY_USE_ONLY, SHA256, digest};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::Semaphore;

use crate::arena::{EtpPageArena, ReadyAcquire};
use crate::config::EtpEntryConfig;
use crate::connect::{ColdConnectBlueprint, EtpStream, EtpTransportSecurity, build_tls_target};
use crate::error::EtpError;
use crate::mysql::config::MysqlEtpConfig;
use crate::mysql::lease::MysqlLease;

const CLIENT_CONNECT_WITH_DB: u32 = 0x0000_0008;
const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
const CLIENT_SSL: u32 = 0x0000_0800;
const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;
const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 0x0020_0000;

/// MySQL / MariaDB connection pool with pre-authenticated sessions inside [`EtpPageArena`].
pub struct MysqlEtp {
    pub config: MysqlEtpConfig,
    pub(crate) blueprint: ColdConnectBlueprint,
    pub(crate) arena: EtpPageArena<EtpStream>,
    pub(crate) semaphore: Semaphore,
    connect_timeout: Duration,
    idle_timeout_secs: u64,
    has_tls: bool,
}

impl fmt::Debug for MysqlEtp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MysqlEtp")
            .field("id", &self.config.id)
            .field("host", &self.config.host)
            .field("port", &self.config.port)
            .field("database", &self.config.database)
            .field("idle_len", &self.arena.ready_count())
            .finish()
    }
}

impl MysqlEtp {
    /// Creates a new [`MysqlEtp`] pool from a generic [`EtpEntryConfig`].
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        let config = MysqlEtpConfig::from_entry(entry)?;
        Self::new(config)
    }

    /// Creates a new [`MysqlEtp`] pool from a parsed [`MysqlEtpConfig`].
    pub fn new(config: MysqlEtpConfig) -> Result<Self, EtpError> {
        let tls_target = build_tls_target(&config.id, &config.host, config.tls.as_ref())?;
        let has_tls = tls_target.is_some();
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
            has_tls,
        })
    }

    /// Establishes a fresh stream and completes MySQL HandshakeV10 (with optional StartTLS)
    /// authentication.
    pub(crate) async fn connect_authenticated(&self) -> Result<EtpStream, EtpError> {
        let id = &self.config.id;
        let username = &self.config.username;
        let password = self.config.password.as_deref().unwrap_or("");
        let database = self.config.database.as_deref();
        let has_tls = self.has_tls;
        let timeout_ms = u64::try_from(self.connect_timeout.as_millis()).unwrap_or(u64::MAX);

        let (mut stream, mut seq, greeting) = match self.config.security {
            EtpTransportSecurity::Plain => {
                let mut stream = self.blueprint.connect_stream().await?;
                let (seq, greeting_pkt) = match tokio::time::timeout(
                    self.connect_timeout,
                    read_mysql_packet(&mut stream),
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
                let greeting = parse_handshake_v10(&greeting_pkt, id)?;
                (stream, seq, greeting)
            }
            EtpTransportSecurity::StartTls => {
                let mut tcp = self.blueprint.connect_tcp().await?;
                let mut caps = CLIENT_SSL
                    | CLIENT_PROTOCOL_41
                    | CLIENT_SECURE_CONNECTION
                    | CLIENT_PLUGIN_AUTH
                    | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
                if database.is_some() {
                    caps |= CLIENT_CONNECT_WITH_DB;
                }
                let ssl_req = build_ssl_request(caps);
                let negotiate = async {
                    let (srv_seq, greeting_pkt) = read_mysql_packet(&mut tcp).await?;
                    let greeting = parse_handshake_v10(&greeting_pkt, id)?;
                    if (greeting.server_caps & CLIENT_SSL) == 0 {
                        return Err(EtpError::Tls(format!(
                            "MySQL '{id}' server does not advertise CLIENT_SSL capability for StartTLS"
                        )));
                    }
                    let ssl_seq = srv_seq.wrapping_add(1);
                    write_mysql_packet(&mut tcp, ssl_seq, &ssl_req).await?;
                    Ok((ssl_seq, greeting))
                };
                let (ssl_seq, greeting) =
                    match tokio::time::timeout(self.connect_timeout, negotiate).await {
                        Ok(res) => res?,
                        Err(_) => {
                            return Err(EtpError::Timeout {
                                id: id.clone(),
                                timeout_ms,
                            });
                        }
                    };
                let stream = self.blueprint.upgrade_tls(tcp).await?;
                (stream, ssl_seq, greeting)
            }
            other => {
                return Err(EtpError::Config(format!(
                    "MySQL '{id}' unsupported transport security '{other}'"
                )));
            }
        };

        let handshake = async {
            let mut active_plugin = greeting.auth_plugin_name;

            let auth_token = compute_auth_token(&active_plugin, password, &greeting.scramble, id)?;
            let mut caps = CLIENT_PROTOCOL_41
                | CLIENT_SECURE_CONNECTION
                | CLIENT_PLUGIN_AUTH
                | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
            if has_tls {
                caps |= CLIENT_SSL;
            }
            if database.is_some() {
                caps |= CLIENT_CONNECT_WITH_DB;
            }

            let response =
                build_handshake_response_41(caps, username, &auth_token, database, &active_plugin);
            seq = seq.wrapping_add(1);
            write_mysql_packet(&mut stream, seq, &response).await?;

            loop {
                let (srv_seq, packet) = read_mysql_packet(&mut stream).await?;
                seq = srv_seq;
                if packet.is_empty() {
                    return Err(EtpError::Config(format!(
                        "MySQL '{id}' returned empty handshake packet"
                    )));
                }
                match packet[0] {
                    0x00 => return Ok(stream),
                    0xFF => {
                        let msg = parse_mysql_err(&packet);
                        return Err(EtpError::Config(format!("MySQL '{id}' auth error: {msg}")));
                    }
                    0xFE => {
                        // AuthSwitchRequest: 0xFE | plugin_name\0 | scramble[...]
                        let rest = &packet[1..];
                        let nul = rest.iter().position(|&b| b == 0).unwrap_or(rest.len());
                        active_plugin = String::from_utf8_lossy(&rest[..nul]).into_owned();
                        let scramble_raw = &rest[(nul + 1).min(rest.len())..];
                        let scramble = scramble_raw.strip_suffix(b"\0").unwrap_or(scramble_raw);
                        let reply = compute_auth_token(&active_plugin, password, scramble, id)?;
                        seq = seq.wrapping_add(1);
                        write_mysql_packet(&mut stream, seq, &reply).await?;
                    }
                    0x01 => {
                        // AuthMoreData for caching_sha2_password
                        if packet.len() >= 2 && packet[1] == 0x03 {
                            // fast_auth_success; await final 0x00 OK packet
                            continue;
                        }
                        if packet.len() >= 2 && packet[1] == 0x04 {
                            if !has_tls {
                                return Err(EtpError::Config(format!(
                                    "MySQL '{id}' requires TLS for caching_sha2_password full auth"
                                )));
                            }
                            let mut pw_buf = Vec::with_capacity(password.len() + 1);
                            pw_buf.extend_from_slice(password.as_bytes());
                            pw_buf.push(0);
                            seq = seq.wrapping_add(1);
                            write_mysql_packet(&mut stream, seq, &pw_buf).await?;
                            continue;
                        }
                        return Err(EtpError::Config(format!(
                            "MySQL '{id}' sent unsupported AuthMoreData payload"
                        )));
                    }
                    other => {
                        return Err(EtpError::Config(format!(
                            "MySQL '{id}' unexpected handshake header 0x{other:02x}"
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

    /// Pre-warms up to `baseline_slots` authenticated MySQL connections into the arena.
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

    /// Non-blocking fast path: borrows a warm MySQL connection in-place via `TZCNT`.
    pub fn try_acquire(self: &Arc<Self>) -> Option<MysqlLease> {
        let permit = self.semaphore.try_acquire().ok()?;
        match self.arena.try_acquire_ready_slot(self.idle_timeout_secs) {
            ReadyAcquire::Hit(slot_idx) => {
                permit.forget();
                Some(MysqlLease::new(Arc::clone(self), slot_idx))
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

    /// Acquires an authenticated MySQL session lease.
    pub async fn acquire(self: &Arc<Self>) -> Result<MysqlLease, EtpError> {
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
            return Ok(MysqlLease::new(Arc::clone(self), slot_idx));
        }

        let Some(guard) = self.arena.claim_empty_slot(true) else {
            return Err(EtpError::PoolExhausted {
                id: self.config.id.clone(),
            });
        };

        let stream = self.connect_authenticated().await?;
        let slot_idx = guard.commit_leased(stream);
        permit.forget();
        Ok(MysqlLease::new(Arc::clone(self), slot_idx))
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
                                "MySQL ETP background replenishment failed"
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

struct HandshakeV10 {
    scramble: Vec<u8>,
    server_caps: u32,
    auth_plugin_name: String,
}

fn parse_handshake_v10(pkt: &[u8], id: &str) -> Result<HandshakeV10, EtpError> {
    if pkt.is_empty() || pkt[0] != 0x0A {
        return Err(EtpError::Config(format!(
            "MySQL '{id}' expected protocol v10 greeting (0x0a)"
        )));
    }
    let mut pos = 1usize;
    let ver_end = pkt[pos..]
        .iter()
        .position(|&b| b == 0)
        .map(|rel| pos + rel)
        .ok_or_else(|| EtpError::Config(format!("MySQL '{id}' malformed greeting version")))?;
    pos = ver_end + 1;

    // connection_id (4) + auth_plugin_data_part_1 (8) + filler (1) + cap_low (2) + charset (1)
    // + status (2) + cap_high (2) + auth_plugin_data_len (1) + reserved (10) = 31 bytes
    if pkt.len() < pos + 31 {
        return Err(EtpError::Config(format!(
            "MySQL '{id}' truncated HandshakeV10 packet"
        )));
    }
    let part1 = &pkt[pos + 4..pos + 12];
    let cap_low = u16::from_le_bytes([pkt[pos + 13], pkt[pos + 14]]) as u32;
    let cap_high = u16::from_le_bytes([pkt[pos + 18], pkt[pos + 19]]) as u32;
    let caps = cap_low | (cap_high << 16);
    let auth_len = pkt[pos + 20] as usize;
    pos += 31;

    let mut scramble = Vec::with_capacity(20);
    scramble.extend_from_slice(part1);

    let part2_len = auth_len.saturating_sub(8).max(13);
    if pkt.len() < pos + part2_len {
        return Err(EtpError::Config(format!(
            "MySQL '{id}' truncated HandshakeV10 scramble"
        )));
    }
    let part2 = &pkt[pos..pos + part2_len];
    let part2_clean = part2.strip_suffix(b"\0").unwrap_or(part2);
    scramble.extend_from_slice(part2_clean);
    pos += part2_len;

    let auth_plugin_name = if (caps & CLIENT_PLUGIN_AUTH) != 0 && pos < pkt.len() {
        let name_end = pkt[pos..]
            .iter()
            .position(|&b| b == 0)
            .map(|rel| pos + rel)
            .unwrap_or(pkt.len());
        String::from_utf8_lossy(&pkt[pos..name_end]).into_owned()
    } else {
        "mysql_native_password".to_string()
    };

    Ok(HandshakeV10 {
        scramble,
        server_caps: caps,
        auth_plugin_name,
    })
}

fn compute_auth_token(
    plugin: &str,
    password: &str,
    scramble: &[u8],
    id: &str,
) -> Result<Vec<u8>, EtpError> {
    if password.is_empty() {
        return Ok(Vec::new());
    }
    match plugin {
        "mysql_native_password" => {
            let s1 = digest(&SHA1_FOR_LEGACY_USE_ONLY, password.as_bytes());
            let s2 = digest(&SHA1_FOR_LEGACY_USE_ONLY, s1.as_ref());
            let mut ctx = ring::digest::Context::new(&SHA1_FOR_LEGACY_USE_ONLY);
            ctx.update(scramble);
            ctx.update(s2.as_ref());
            let mixed = ctx.finish();
            let mut out = vec![0u8; 20];
            for (dst, (a, b)) in out
                .iter_mut()
                .zip(s1.as_ref().iter().zip(mixed.as_ref().iter()))
            {
                *dst = a ^ b;
            }
            Ok(out)
        }
        "caching_sha2_password" => {
            let s1 = digest(&SHA256, password.as_bytes());
            let s2 = digest(&SHA256, s1.as_ref());
            let mut ctx = ring::digest::Context::new(&SHA256);
            ctx.update(s2.as_ref());
            ctx.update(scramble);
            let mixed = ctx.finish();
            let mut out = vec![0u8; 32];
            for (dst, (a, b)) in out
                .iter_mut()
                .zip(s1.as_ref().iter().zip(mixed.as_ref().iter()))
            {
                *dst = a ^ b;
            }
            Ok(out)
        }
        "mysql_clear_password" => {
            let mut out = Vec::with_capacity(password.len() + 1);
            out.extend_from_slice(password.as_bytes());
            out.push(0);
            Ok(out)
        }
        other => Err(EtpError::Config(format!(
            "MySQL '{id}' unsupported auth plugin '{other}'"
        ))),
    }
}

fn build_ssl_request(caps: u32) -> [u8; 32] {
    let mut buf = [0u8; 32];
    buf[0..4].copy_from_slice(&caps.to_le_bytes());
    buf[4..8].copy_from_slice(&(16 * 1024 * 1024u32).to_le_bytes());
    buf[8] = 45; // utf8mb4_general_ci
    buf
}

fn build_handshake_response_41(
    caps: u32,
    username: &str,
    auth_token: &[u8],
    database: Option<&str>,
    plugin_name: &str,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(128);
    buf.extend_from_slice(&caps.to_le_bytes());
    buf.extend_from_slice(&(16 * 1024 * 1024u32).to_le_bytes());
    buf.push(45); // utf8mb4_general_ci
    buf.extend_from_slice(&[0u8; 23]);
    buf.extend_from_slice(username.as_bytes());
    buf.push(0);
    // Lenenc int for auth_token (0..250 fits in 1 byte)
    buf.push(auth_token.len() as u8);
    buf.extend_from_slice(auth_token);
    if let Some(db) = database {
        buf.extend_from_slice(db.as_bytes());
        buf.push(0);
    }
    buf.extend_from_slice(plugin_name.as_bytes());
    buf.push(0);
    buf
}

async fn write_mysql_packet<S: AsyncWrite + Unpin>(
    stream: &mut S,
    seq: u8,
    payload: &[u8],
) -> Result<(), EtpError> {
    let len = payload.len() as u32;
    let header = [
        (len & 0xff) as u8,
        ((len >> 8) & 0xff) as u8,
        ((len >> 16) & 0xff) as u8,
        seq,
    ];
    stream.write_all(&header).await?;
    stream.write_all(payload).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_mysql_packet<S: AsyncRead + Unpin>(
    stream: &mut S,
) -> Result<(u8, Vec<u8>), EtpError> {
    let mut header = [0u8; 4];
    stream.read_exact(&mut header).await?;
    let len = (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
    let seq = header[3];
    let mut payload = vec![0u8; len];
    if len > 0 {
        stream.read_exact(&mut payload).await?;
    }
    Ok((seq, payload))
}

fn parse_mysql_err(packet: &[u8]) -> String {
    if packet.len() < 3 {
        return "unknown mysql error".to_string();
    }
    let code = u16::from_le_bytes([packet[1], packet[2]]);
    let msg_start = if packet.len() > 9 && packet[3] == b'#' {
        9
    } else {
        3
    };
    let msg = String::from_utf8_lossy(&packet[msg_start..]);
    format!("[{code}] {msg}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mysql_native_and_caching_sha2_tokens() {
        let scramble = [0x11u8; 20];
        let native = compute_auth_token("mysql_native_password", "pw123", &scramble, "m").unwrap();
        assert_eq!(native.len(), 20);
        let sha2 = compute_auth_token("caching_sha2_password", "pw123", &scramble, "m").unwrap();
        assert_eq!(sha2.len(), 32);
        let empty = compute_auth_token("mysql_native_password", "", &scramble, "m").unwrap();
        assert!(empty.is_empty());
    }
}
