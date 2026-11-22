//! Persistent Dual-Connection Redis Client & Background Sync Loop (`openai_token_quota::redis`).
//!
//! ```text
//!   Assigned Shadow Worker (`velda-shadow-{i}` scheduled by PBWW LPT)
//!     ├── Opens & owns Primary + Standby connections directly inside Shadow Worker loop
//!     ├── Supports Plaintext (`redis://`), TLS (`rediss://`), and mTLS (`OutboundTlsConfig`)
//!     ├── Failover: if Primary drops/errors, promote Standby in 0 RTT & re-warm Standby
//!     └── Batch Multi-Key Lua `EVAL`: leases up to `batch_users` (64) keys in 1 RTT
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use rustls::pki_types::ServerName;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{Notify, watch};
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::config::OutboundTlsConfig;
use crate::post_response::openai_token_quota::local::{
    FLAG_EXHAUSTED, FLAG_NEEDS_REFILL, MAX_CONSUMER_KEY_LEN, SharedUsageArena,
};

const LUA_BATCH_LEASE_MICRO_QUOTA: &str = "\
local res = {} \
local batch = tonumber(ARGV[1]) \
for i = 1, #KEYS do \
  local avail = tonumber(redis.call('GET', KEYS[i]) or '0') \
  local lease = 0 \
  if avail > 0 then \
    lease = avail < batch and avail or batch \
    redis.call('DECRBY', KEYS[i], lease) \
  end \
  res[i] = lease \
end \
return res";

/// Parsed Redis endpoint target (`host:port`, `sni_host`, `use_tls`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RedisTarget {
    pub addr: String,
    pub host: String,
    pub use_tls: bool,
}

/// Parses `"redis://host:port"` or `"rediss://host:port"`.
pub fn parse_redis_addr(url: &str) -> Option<&str> {
    let rest = url
        .strip_prefix("redis://")
        .or_else(|| url.strip_prefix("rediss://"))?;
    let host_port = rest.split('/').next()?.trim();
    if host_port.is_empty() {
        None
    } else {
        Some(host_port)
    }
}

fn parse_redis_target(url: &str, tls: Option<&OutboundTlsConfig>) -> Option<RedisTarget> {
    let use_tls = url.starts_with("rediss://") || tls.is_some();
    let addr = parse_redis_addr(url)?.to_string();
    let host = addr
        .rsplit_once(':')
        .map(|(h, _)| h.trim_matches(['[', ']']))
        .unwrap_or(&addr)
        .to_string();
    Some(RedisTarget {
        addr,
        host,
        use_tls,
    })
}

enum RespStream {
    Plain(BufReader<TcpStream>),
    Tls(Box<BufReader<TlsStream<TcpStream>>>),
}

impl RespStream {
    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            Self::Plain(r) => r.get_mut().write_all(buf).await,
            Self::Tls(r) => r.get_mut().write_all(buf).await,
        }
    }

    async fn read_line(&mut self, line: &mut String) -> std::io::Result<usize> {
        match self {
            Self::Plain(r) => r.read_line(line).await,
            Self::Tls(r) => r.read_line(line).await,
        }
    }
}

fn encode_resp_cmd(args: &[&str], out: &mut Vec<u8>) {
    out.clear();
    out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for arg in args {
        out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        out.extend_from_slice(arg.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
}

async fn read_resp_int_array(stream: &mut RespStream, expected_len: usize) -> Option<Vec<i64>> {
    let mut line = String::new();
    let n = stream.read_line(&mut line).await.ok()?;
    if n == 0 {
        return None;
    }
    let header = line.trim_end_matches(&['\r', '\n'][..]);
    let count: usize = header.strip_prefix('*')?.parse().ok()?;
    if count != expected_len {
        return None;
    }

    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        line.clear();
        if stream.read_line(&mut line).await.ok()? == 0 {
            return None;
        }
        let item = line.trim_end_matches(&['\r', '\n'][..]);
        let val = item.strip_prefix(':')?.parse::<i64>().ok()?;
        out.push(val);
    }
    Some(out)
}

async fn open_stream(
    target: &RedisTarget,
    password: Option<&str>,
    tls_connector: Option<&TlsConnector>,
) -> Option<RespStream> {
    let tcp = tokio::time::timeout(Duration::from_millis(500), TcpStream::connect(&target.addr))
        .await
        .ok()?
        .ok()?;
    let _ = tcp.set_nodelay(true);

    let mut stream = if let Some(connector) = tls_connector {
        let sni = ServerName::try_from(target.host.clone()).ok()?;
        let tls_stream =
            tokio::time::timeout(Duration::from_millis(500), connector.connect(sni, tcp))
                .await
                .ok()?
                .ok()?;
        RespStream::Tls(Box::new(BufReader::new(tls_stream)))
    } else {
        RespStream::Plain(BufReader::new(tcp))
    };

    if let Some(pw) = password {
        let mut buf = Vec::with_capacity(64);
        encode_resp_cmd(&["AUTH", pw], &mut buf);
        stream.write_all(&buf).await.ok()?;
        let mut line = String::new();
        stream.read_line(&mut line).await.ok()?;
        if !line.starts_with('+') {
            return None;
        }
    }

    Some(stream)
}

/// Dual-connection (`Primary` + `Standby`) Redis client supporting Plaintext, TLS, and mTLS.
pub struct RedisQuotaClient {
    target: RedisTarget,
    password: Option<String>,
    tls_connector: Option<TlsConnector>,
    primary: Option<RespStream>,
    standby: Option<RespStream>,
    cmd_buf: Vec<u8>,
}

impl RedisQuotaClient {
    pub fn new(
        redis_url: &str,
        password: Option<String>,
        tls: Option<OutboundTlsConfig>,
    ) -> Option<Self> {
        let target = parse_redis_target(redis_url, tls.as_ref())?;
        let tls_connector = if target.use_tls {
            let tls_cfg = tls.unwrap_or_default().build_client_config(&[]).ok()?;
            Some(TlsConnector::from(tls_cfg))
        } else {
            None
        };

        Some(Self {
            target,
            password,
            tls_connector,
            primary: None,
            standby: None,
            cmd_buf: Vec::with_capacity(2048),
        })
    }

    async fn acquire_primary(&mut self) -> Option<&mut RespStream> {
        if self.primary.is_none() {
            if let Some(backup) = self.standby.take() {
                self.primary = Some(backup);
            } else {
                self.primary = open_stream(
                    &self.target,
                    self.password.as_deref(),
                    self.tls_connector.as_ref(),
                )
                .await;
            }
        }
        self.primary.as_mut()
    }

    async fn ensure_standby_warm(&mut self) {
        if self.standby.is_none() {
            self.standby = open_stream(
                &self.target,
                self.password.as_deref(),
                self.tls_connector.as_ref(),
            )
            .await;
        }
    }

    /// Sweeps active slots in batches of up to `batch_users` per Multi-Key Lua `EVAL` round-trip.
    pub async fn sync_arena_batch(
        &mut self,
        plugin_id: &str,
        arena: &SharedUsageArena,
        batch_str: &str,
        batch_users: usize,
        connected: &AtomicBool,
    ) {
        let mut key_buf = [0u8; MAX_CONSUMER_KEY_LEN];
        let mut pending: Vec<(usize, String)> = Vec::with_capacity(batch_users);
        let slots = arena.slots();

        for (idx, slot) in slots.iter().enumerate() {
            if slot.consumer_hash.load(Ordering::Relaxed) == 0 {
                continue;
            }
            let rem = slot.local_remaining.load(Ordering::Relaxed);
            let flags = slot.flags.load(Ordering::Relaxed);
            if (flags & FLAG_NEEDS_REFILL) == 0 && rem > arena.refill_watermark {
                continue;
            }

            let consumer_key = SharedUsageArena::read_slot_key(slot, &mut key_buf);
            pending.push((idx, format!("velda:plugin:{plugin_id}:{consumer_key}")));

            if pending.len() >= batch_users {
                if !self
                    .flush_batch(slots, &pending, batch_str, connected)
                    .await
                {
                    return;
                }
                pending.clear();
            }
        }

        if !pending.is_empty() {
            let _ = self
                .flush_batch(slots, &pending, batch_str, connected)
                .await;
        }

        if self.primary.is_some() {
            self.ensure_standby_warm().await;
        }
    }

    async fn flush_batch(
        &mut self,
        slots: &[crate::post_response::openai_token_quota::local::UsageSlot],
        batch: &[(usize, String)],
        batch_str: &str,
        connected: &AtomicBool,
    ) -> bool {
        let num_keys_str = batch.len().to_string();
        let mut args: Vec<&str> = Vec::with_capacity(4 + batch.len());
        args.push("EVAL");
        args.push(LUA_BATCH_LEASE_MICRO_QUOTA);
        args.push(&num_keys_str);
        for (_, key) in batch {
            args.push(key.as_str());
        }
        args.push(batch_str);
        encode_resp_cmd(&args, &mut self.cmd_buf);

        // Attempt on Primary; on failure, promote warm Standby in 0 RTT and retry once.
        for _attempt in 0..2 {
            let cmd = &self.cmd_buf;
            let Some(stream) = (if self.primary.is_none() {
                if let Some(backup) = self.standby.take() {
                    self.primary = Some(backup);
                } else {
                    self.primary = open_stream(
                        &self.target,
                        self.password.as_deref(),
                        self.tls_connector.as_ref(),
                    )
                    .await;
                }
                self.primary.as_mut()
            } else {
                self.primary.as_mut()
            }) else {
                connected.store(false, Ordering::Relaxed);
                return false;
            };

            if stream.write_all(cmd).await.is_err() {
                self.primary = None;
                continue;
            }

            match tokio::time::timeout(
                Duration::from_millis(500),
                read_resp_int_array(stream, batch.len()),
            )
            .await
            {
                Ok(Some(leases)) => {
                    connected.store(true, Ordering::Relaxed);
                    for ((slot_idx, _), leased) in batch.iter().zip(leases) {
                        let slot = &slots[*slot_idx];
                        if leased > 0 {
                            slot.local_remaining.fetch_add(leased, Ordering::Relaxed);
                            slot.pending_delta.store(0, Ordering::Relaxed);
                            slot.flags.fetch_and(
                                !(FLAG_NEEDS_REFILL | FLAG_EXHAUSTED),
                                Ordering::Relaxed,
                            );
                        } else {
                            slot.flags.fetch_or(FLAG_EXHAUSTED, Ordering::Relaxed);
                            slot.flags.fetch_and(!FLAG_NEEDS_REFILL, Ordering::Relaxed);
                        }
                    }
                    return true;
                }
                _ => {
                    self.primary = None;
                }
            }
        }

        // Keep track of whether at least one connection remains alive.
        let still_up = self.acquire_primary().await.is_some();
        connected.store(still_up, Ordering::Relaxed);
        false
    }
}

/// Configuration parameters for a dedicated Redis micro-quota background sync loop.
pub struct RedisSyncParams {
    pub plugin_id: String,
    pub arena: Arc<SharedUsageArena>,
    pub notify: Arc<Notify>,
    pub connected: Arc<AtomicBool>,
    pub redis_url: String,
    pub redis_password: Option<String>,
    pub redis_tls: Option<OutboundTlsConfig>,
    pub micro_quota_batch: u64,
    pub batch_users: usize,
    pub sync_interval_ms: u64,
    pub pbww: u32,
}

/// Runs the cooperative Redis micro-quota sync loop inside the assigned Shadow Worker until shutdown.
///
/// Connections to Redis are opened lazily inside this async task on the assigned Shadow Worker
/// thread rather than during config compilation.
pub async fn run_redis_sync_loop(
    params: RedisSyncParams,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), crate::error::PluginError> {
    let mut client = RedisQuotaClient::new(
        &params.redis_url,
        params.redis_password.clone(),
        params.redis_tls.clone(),
    )
    .ok_or_else(|| {
        crate::error::PluginError::Config(format!(
            "Plugin '{}': invalid redis_url '{}'",
            params.plugin_id, params.redis_url
        ))
    })?;
    let batch_str = params.micro_quota_batch.to_string();
    let interval_dur = Duration::from_millis(params.sync_interval_ms.max(10));

    loop {
        tokio::select! {
            _ = params.notify.notified() => {}
            _ = tokio::time::sleep(interval_dur) => {}
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
            }
        }

        if *shutdown.borrow() {
            break;
        }

        client
            .sync_arena_batch(
                &params.plugin_id,
                &params.arena,
                &batch_str,
                params.batch_users,
                &params.connected,
            )
            .await;

        if params.pbww < 50 {
            tokio::task::yield_now().await;
        }
    }

    Ok(())
}
