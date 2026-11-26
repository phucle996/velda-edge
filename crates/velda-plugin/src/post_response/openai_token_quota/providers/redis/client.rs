//! Lua-Free RESP2 Pipelined Micro-Quota Lease (`DECRBY` + Exact-Conservation `INCRBY`
//! Compensation) coordinator supporting `standalone` and `cluster` [`RedisEtp`] (`client.rs`).
//!
//! ```text
//!   flush_batch(&etp, slots, batch, batch_i64)
//!     ├─► !is_cluster() ──► standalone::flush_standalone()
//!     └─►  is_cluster() ──► cluster::flush_cluster()
//!            └──► update UsageSlot atomics (local_remaining, pending_delta, flags)
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use velda_etp::redis::RedisEtp;

use super::cluster::flush_cluster;
use super::codec::ShardItemOutcome;
use super::standalone::flush_standalone;
use crate::post_response::openai_token_quota::local::{
    FLAG_EXHAUSTED, FLAG_NEEDS_REFILL, UsageSlot,
};

#[allow(clippy::too_many_arguments)]
pub(crate) async fn flush_batch(
    etp: &Arc<RedisEtp>,
    slots: &[UsageSlot],
    batch: &[(usize, String)],
    batch_i64: i64,
    batch_str: &str,
    cmd_buf: &mut Vec<u8>,
    resp_buf: &mut Vec<u8>,
    connected: &AtomicBool,
) -> bool {
    if batch.is_empty() {
        return true;
    }

    let outcomes = if !etp.is_cluster() {
        let Some(res) = flush_standalone(
            etp, batch, batch_i64, batch_str, cmd_buf, resp_buf, connected,
        )
        .await
        else {
            return false;
        };
        res
    } else {
        let Some(res) = flush_cluster(
            etp, batch, batch_i64, batch_str, cmd_buf, resp_buf, connected,
        )
        .await
        else {
            return false;
        };
        res
    };

    connected.store(true, Ordering::Relaxed);
    for item in outcomes {
        if let ShardItemOutcome::Leased { slot_idx, leased } = item
            && let Some(slot) = slots.get(slot_idx)
        {
            if leased > 0 {
                slot.local_remaining.fetch_add(leased, Ordering::Relaxed);
                slot.pending_delta.store(0, Ordering::Relaxed);
                slot.flags
                    .fetch_and(!(FLAG_NEEDS_REFILL | FLAG_EXHAUSTED), Ordering::Relaxed);
            } else {
                slot.flags.fetch_or(FLAG_EXHAUSTED, Ordering::Relaxed);
                slot.flags.fetch_and(!FLAG_NEEDS_REFILL, Ordering::Relaxed);
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::post_response::openai_token_quota::providers::redis::codec::compute_lease_and_refund;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use velda_core::clock::init_clock;
    use velda_etp::redis::crc16_slot;
    use velda_etp::{EtpEntryConfig, EtpKind, EtpPoolSettings};

    use crate::post_response::openai_token_quota::local::{SharedUsageArena, fnv1a_64};
    use crate::post_response::openai_token_quota::scanner::TokenUsage;

    #[test]
    fn test_compute_lease_and_refund_conservation() {
        assert_eq!(compute_lease_and_refund(3000, 5000), (5000, 0));
        assert_eq!(compute_lease_and_refund(0, 5000), (5000, 0));
        assert_eq!(compute_lease_and_refund(-2000, 5000), (3000, 2000));
        assert_eq!(compute_lease_and_refund(-5000, 5000), (0, 5000));
        assert_eq!(compute_lease_and_refund(-9000, 5000), (0, 5000));
    }

    #[tokio::test]
    async fn test_cluster_decrby_moved_redirect_and_compensating_incrby_refund() {
        init_clock();
        let arena = Arc::new(SharedUsageArena::new(64, 0, 10));
        let usage = TokenUsage {
            prompt_tokens: 1,
            completion_tokens: 1,
            total_tokens: 2,
            cached_prompt_tokens: 0,
        };
        arena.record(fnv1a_64(b"tenant-rich"), b"tenant-rich", usage, 1);
        arena.record(fnv1a_64(b"tenant-partial"), b"tenant-partial", usage, 1);

        let mut pending = Vec::new();
        let mut kb = [0u8; 62];
        for (i, slot) in arena.slots().iter().enumerate() {
            if slot.consumer_hash.load(Ordering::Relaxed) != 0 {
                let k = SharedUsageArena::read_slot_key(slot, &mut kb).to_string();
                pending.push((i, format!("velda:plugin:openai_token_quota:{k}")));
            }
        }
        assert_eq!(pending.len(), 2);

        let l0 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let p0 = l0.local_addr().unwrap().port();
        let l1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let p1 = l1.local_addr().unwrap().port();

        let partial_slot = crc16_slot(b"velda:plugin:openai_token_quota:tenant-partial");
        let saw_refund = Arc::new(AtomicBool::new(false));
        let saw_refund_c = Arc::clone(&saw_refund);

        let srv0 = tokio::spawn(async move {
            let slots_resp =
                format!("*1\r\n*3\r\n:0\r\n:16383\r\n*2\r\n$9\r\n127.0.0.1\r\n:{p0}\r\n");
            while let Ok((mut sock, _)) = l0.accept().await {
                let slots_resp = slots_resp.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    while let Ok(n) = sock.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                        let wire = String::from_utf8_lossy(&buf[..n]);
                        if wire.contains("SLOTS") {
                            let _ = sock.write_all(slots_resp.as_bytes()).await;
                            continue;
                        }
                        let mut reply = String::new();
                        let mut rest = wire.as_ref();
                        while let Some(pos) = rest.find("$6\r\nDECRBY\r\n") {
                            let after = &rest[pos + 12..];
                            let end = after.find("$6\r\nDECRBY\r\n").unwrap_or(after.len());
                            if after[..end].contains("tenant-partial") {
                                reply
                                    .push_str(&format!("-MOVED {partial_slot} 127.0.0.1:{p1}\r\n"));
                            } else {
                                reply.push_str(":500\r\n");
                            }
                            rest = &after[end..];
                        }
                        let _ = sock.write_all(reply.as_bytes()).await;
                    }
                });
            }
        });

        let srv1 = tokio::spawn(async move {
            let slots_resp =
                format!("*1\r\n*3\r\n:0\r\n:16383\r\n*2\r\n$9\r\n127.0.0.1\r\n:{p1}\r\n");
            while let Ok((mut sock, _)) = l1.accept().await {
                let slots_resp = slots_resp.clone();
                let saw_refund_c = Arc::clone(&saw_refund_c);
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    while let Ok(n) = sock.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                        let wire = String::from_utf8_lossy(&buf[..n]);
                        if wire.contains("SLOTS") {
                            let _ = sock.write_all(slots_resp.as_bytes()).await;
                        } else if wire.contains("$6\r\nDECRBY\r\n") {
                            let _ = sock.write_all(b":-20\r\n").await;
                        } else if wire.contains("$6\r\nINCRBY\r\n") && wire.contains("$2\r\n20\r\n")
                        {
                            saw_refund_c.store(true, Ordering::Release);
                            let _ = sock.write_all(b":0\r\n").await;
                        }
                    }
                });
            }
        });

        let entry = EtpEntryConfig {
            id: "redis-quota-cluster".into(),
            kind: EtpKind::Redis,
            security: velda_etp::EtpTransportSecurity::Plain,
            pool: EtpPoolSettings {
                pool_size: 2,
                connect_timeout_ms: 500,
                idle_timeout_ms: 5000,
            },
            tls: None,
            settings: serde_json::json!({
                "mode": "cluster",
                "seed_nodes": [format!("127.0.0.1:{p0}")]
            }),
        };
        let etp = Arc::new(RedisEtp::from_entry(&entry).unwrap());
        let connected = AtomicBool::new(false);
        let mut cbuf = Vec::new();
        let mut rbuf = Vec::new();

        let ok = flush_batch(
            &etp,
            arena.slots(),
            &pending,
            50,
            "50",
            &mut cbuf,
            &mut rbuf,
            &connected,
        )
        .await;
        assert!(ok);
        assert!(connected.load(Ordering::Relaxed));
        assert!(
            saw_refund.load(Ordering::Acquire),
            "must refund INCRBY 20 after -20 overshoot"
        );

        let (_, _, _, _, rich_rem) = arena.read_consumer("tenant-rich").unwrap();
        let (_, _, _, _, partial_rem) = arena.read_consumer("tenant-partial").unwrap();
        assert_eq!(rich_rem, 48);
        assert_eq!(partial_rem, 28);

        srv0.abort();
        srv1.abort();
    }
}
