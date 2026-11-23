//! RESP2 Multi-Key Lua `EVAL` Micro-Quota Sync Loop (`openai_token_quota::redis::client`).
//!
//! ```text
//!   Shadow Worker (`velda-shadow-{i}`) ──► notify / sleep(sync_interval_ms)
//!         │
//!         ▼
//!   sync_arena_batch(&etp, &arena, batch_str, batch_users)
//!         │  collects slots with FLAG_NEEDS_REFILL or rem <= refill_watermark
//!         ▼
//!   flush_batch(&etp, &batch, &mut cmd_buf, &mut resp_buf)
//!         ├─► encode RESP2: EVAL <LUA_BATCH_LEASE_MICRO_QUOTA> <N> <keys...> <batch_str>
//!         ├─► etp.acquire().await ──► RedisLease (DerefMut -> EtpStream at Slot[idx])
//!         ├─► attempt 0..2: write_all(cmd) + flush + read RESP2 *N\r\n :val\r\n...
//!         ├─► Io EOF/reset (attempt 0) ─► lease.reconnect().await + retry
//!         └─► error / timeout          ─► lease.recreate_in_background()
//! ```

use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::{Notify, watch};
use velda_core::lut::decimal::fast_write_u64;
use velda_etp::EtpError;
use velda_etp::redis::RedisEtp;

use crate::post_response::openai_token_quota::local::{
    FLAG_EXHAUSTED, FLAG_NEEDS_REFILL, MAX_CONSUMER_KEY_LEN, SharedUsageArena, UsageSlot,
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

fn encode_resp_cmd(args: &[&str], out: &mut Vec<u8>) {
    out.clear();
    out.push(b'*');
    fast_write_u64(out, args.len() as u64);
    out.extend_from_slice(b"\r\n");
    for arg in args {
        out.push(b'$');
        fast_write_u64(out, arg.len() as u64);
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(arg.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
}

fn parse_resp_int_array(buf: &[u8], expected_len: usize) -> Option<Result<Vec<i64>, ()>> {
    let first_nl = buf.windows(2).position(|w| w == b"\r\n")?;
    let header = std::str::from_utf8(buf.get(..first_nl)?).ok()?;
    let Some(count_str) = header.strip_prefix('*') else {
        return Some(Err(()));
    };
    let Ok(count) = count_str.parse::<usize>() else {
        return Some(Err(()));
    };
    if count != expected_len {
        return Some(Err(()));
    }

    let mut pos = first_nl + 2;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let tail = buf.get(pos..)?;
        let rel_nl = tail.windows(2).position(|w| w == b"\r\n")?;
        let line = std::str::from_utf8(tail.get(..rel_nl)?).ok()?;
        let Some(val_str) = line.strip_prefix(':') else {
            return Some(Err(()));
        };
        let Ok(val) = val_str.parse::<i64>() else {
            return Some(Err(()));
        };
        out.push(val);
        pos += rel_nl + 2;
    }
    Some(Ok(out))
}

async fn flush_batch(
    etp: &Arc<RedisEtp>,
    slots: &[UsageSlot],
    batch: &[(usize, String)],
    batch_str: &str,
    cmd_buf: &mut Vec<u8>,
    resp_buf: &mut Vec<u8>,
    connected: &AtomicBool,
) -> bool {
    let mut num_keys_buf = itoa::Buffer::new();
    let num_keys_str = num_keys_buf.format(batch.len());
    let mut args: Vec<&str> = Vec::with_capacity(4 + batch.len());
    args.push("EVAL");
    args.push(LUA_BATCH_LEASE_MICRO_QUOTA);
    args.push(num_keys_str);
    for (_, key) in batch {
        args.push(key.as_str());
    }
    args.push(batch_str);
    encode_resp_cmd(&args, cmd_buf);

    let Ok(mut lease) = etp.acquire().await else {
        connected.store(false, Ordering::Relaxed);
        return false;
    };

    for attempt in 0..2 {
        let exchange: Result<Result<Vec<i64>, EtpError>, _> =
            tokio::time::timeout(Duration::from_millis(500), async {
                lease.write_all(cmd_buf).await?;
                lease.flush().await?;

                resp_buf.clear();
                let mut tmp = [0u8; 2048];
                loop {
                    let n = lease.read(&mut tmp).await?;
                    if n == 0 {
                        return Err(std::io::Error::new(
                            ErrorKind::UnexpectedEof,
                            "Redis closed before RESP2 array",
                        )
                        .into());
                    }
                    if let Some(chunk) = tmp.get(..n) {
                        resp_buf.extend_from_slice(chunk);
                    }
                    if resp_buf.len() > 64 * 1024 {
                        return Err(std::io::Error::new(
                            ErrorKind::InvalidData,
                            "Redis RESP2 response > 64 KiB",
                        )
                        .into());
                    }
                    match parse_resp_int_array(resp_buf, batch.len()) {
                        Some(Ok(leases)) => return Ok(leases),
                        Some(Err(())) => {
                            return Err(std::io::Error::new(
                                ErrorKind::InvalidData,
                                "Malformed Redis RESP2 integer array",
                            )
                            .into());
                        }
                        None => continue,
                    }
                }
            })
            .await;

        match exchange {
            Ok(Ok(leases)) => {
                connected.store(true, Ordering::Relaxed);
                for ((slot_idx, _), leased) in batch.iter().zip(leases) {
                    if let Some(slot) = slots.get(*slot_idx) {
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
                }
                return true;
            }
            Ok(Err(EtpError::Io(ref err)))
                if attempt == 0 && err.kind() != ErrorKind::InvalidData =>
            {
                if lease.reconnect().await.is_err() {
                    lease.recreate_in_background();
                    connected.store(false, Ordering::Relaxed);
                    return false;
                }
            }
            _ => {
                lease.recreate_in_background();
                connected.store(false, Ordering::Relaxed);
                return false;
            }
        }
    }

    connected.store(false, Ordering::Relaxed);
    false
}

/// Configuration parameters for a dedicated Redis micro-quota background sync loop.
pub struct RedisSyncParams {
    pub plugin_id: String,
    pub arena: Arc<SharedUsageArena>,
    pub notify: Arc<Notify>,
    pub connected: Arc<AtomicBool>,
    pub etp: Arc<RedisEtp>,
    pub micro_quota_batch: u64,
    pub batch_users: usize,
    pub sync_interval_ms: u64,
    pub pbww: u32,
}

/// Runs the cooperative Redis micro-quota sync loop inside the assigned Shadow Worker until shutdown.
pub async fn run_redis_sync_loop(
    params: RedisSyncParams,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), crate::error::PluginError> {
    let batch_str = params.micro_quota_batch.to_string();
    let interval_dur = Duration::from_millis(params.sync_interval_ms.max(10));
    let mut cmd_buf = Vec::with_capacity(2048);
    let mut resp_buf = Vec::with_capacity(1024);
    let mut key_buf = [0u8; MAX_CONSUMER_KEY_LEN];
    let mut pending: Vec<(usize, String)> = Vec::with_capacity(params.batch_users);

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

        pending.clear();
        let slots = params.arena.slots();
        let mut aborted = false;

        for (idx, slot) in slots.iter().enumerate() {
            if slot.consumer_hash.load(Ordering::Relaxed) == 0 {
                continue;
            }
            let rem = slot.local_remaining.load(Ordering::Relaxed);
            let flags = slot.flags.load(Ordering::Relaxed);
            if (flags & FLAG_NEEDS_REFILL) == 0 && rem > params.arena.refill_watermark {
                continue;
            }

            let consumer_key = SharedUsageArena::read_slot_key(slot, &mut key_buf);
            pending.push((
                idx,
                format!("velda:plugin:{}:{consumer_key}", params.plugin_id),
            ));

            if pending.len() >= params.batch_users {
                if !flush_batch(
                    &params.etp,
                    slots,
                    &pending,
                    &batch_str,
                    &mut cmd_buf,
                    &mut resp_buf,
                    &params.connected,
                )
                .await
                {
                    aborted = true;
                    break;
                }
                pending.clear();
            }
        }

        if !aborted && !pending.is_empty() {
            let _ = flush_batch(
                &params.etp,
                slots,
                &pending,
                &batch_str,
                &mut cmd_buf,
                &mut resp_buf,
                &params.connected,
            )
            .await;
        }

        if params.pbww < 50 {
            tokio::task::yield_now().await;
        }
    }

    Ok(())
}
