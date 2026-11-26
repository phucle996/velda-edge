//! Upstash HTTPS REST (`/pipeline`) Micro-Quota Lease (`DECRBY` + compensating `INCRBY` refund)
//! client for `openai_token_quota` (`client.rs`).
//!
//! ```text
//!   flush_batch(&etp, slots, batch, batch_i64)
//!     ├─► 1. BakedWire SIMD POST /pipeline `[["DECRBY","<k>",<batch>],...]` -> Vec<Value>
//!     ├─► 2. compute_lease_and_refund(after, batch)
//!     └─► 3. BakedWire SIMD POST /pipeline `[["INCRBY","<k>",<refund>],...]`
//! ```

use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use velda_etp::EtpError;
use velda_etp::upstash::UpstashEtp;

use super::http::read_http1_response;
use crate::post_response::openai_token_quota::local::{
    FLAG_EXHAUSTED, FLAG_NEEDS_REFILL, UsageSlot,
};
use crate::wire_baked::{BakedWire, json_escape_bytes};

#[inline(always)]
fn compute_lease_and_refund(after: i64, batch_i64: i64) -> (i64, i64) {
    if after >= 0 {
        (batch_i64, 0)
    } else {
        let prev = after.saturating_add(batch_i64);
        if prev > 0 {
            let leased = prev.min(batch_i64);
            (leased, batch_i64 - leased)
        } else {
            (0, batch_i64)
        }
    }
}

async fn post_upstash_pipeline(
    etp: &Arc<UpstashEtp>,
    body: &[u8],
    req_buf: &mut Vec<u8>,
    resp_buf: &mut Vec<u8>,
) -> Result<Vec<serde_json::Value>, EtpError> {
    {
        let mut rw = BakedWire::cleared(req_buf, 192 + body.len());
        rw.push_chunk(b"POST ");
        rw.push_chunk(etp.config.base_path.as_bytes());
        rw.push_chunk(b"/pipeline HTTP/1.1\r\nHost: ");
        rw.push_chunk(etp.config.host_header.as_bytes());
        rw.push_chunk(b"\r\nAuthorization: Bearer ");
        rw.push_chunk(etp.config.token.as_bytes());
        rw.push_chunk(
            b"\r\nContent-Type: application/json\r\nConnection: keep-alive\r\nContent-Length: ",
        );
        rw.push_u64(body.len() as u64);
        rw.push_chunk(b"\r\n\r\n");
        rw.push_chunk(body);
    }

    let mut lease = etp.acquire().await?;
    for attempt in 0..2 {
        let exchange: Result<Result<Vec<serde_json::Value>, EtpError>, _> =
            tokio::time::timeout(Duration::from_millis(1000), async {
                lease.write_all(req_buf).await?;
                lease.flush().await?;
                let (status, resp_body) = read_http1_response(&mut lease, resp_buf).await?;
                if !(200..300).contains(&status) {
                    return Err(EtpError::Io(Error::other(format!(
                        "Upstash /pipeline HTTP {status}: {}",
                        String::from_utf8_lossy(&resp_body)
                    ))));
                }
                serde_json::from_slice::<Vec<serde_json::Value>>(&resp_body).map_err(|e| {
                    EtpError::Io(Error::new(
                        ErrorKind::InvalidData,
                        format!("Upstash JSON parse failed: {e}"),
                    ))
                })
            })
            .await;

        match exchange {
            Ok(Ok(arr)) => return Ok(arr),
            Ok(Err(EtpError::Io(io_err)))
                if attempt == 0
                    && matches!(
                        io_err.kind(),
                        ErrorKind::UnexpectedEof
                            | ErrorKind::BrokenPipe
                            | ErrorKind::ConnectionReset
                    ) =>
            {
                if lease.reconnect().await.is_err() {
                    lease.recreate_in_background();
                    return Err(EtpError::Io(io_err));
                }
            }
            Ok(Err(err)) => {
                lease.recreate_in_background();
                return Err(err);
            }
            Err(_) => {
                lease.recreate_in_background();
                return Err(EtpError::Timeout {
                    id: etp.config.id.clone(),
                    timeout_ms: 1000,
                });
            }
        }
    }

    Err(EtpError::Timeout {
        id: etp.config.id.clone(),
        timeout_ms: 1000,
    })
}

pub(crate) async fn flush_batch(
    etp: &Arc<UpstashEtp>,
    slots: &[UsageSlot],
    batch: &[(usize, String)],
    batch_i64: i64,
    resp_buf: &mut Vec<u8>,
    connected: &AtomicBool,
) -> bool {
    if batch.is_empty() {
        return true;
    }

    let mut body_buf = Vec::with_capacity(batch.len() * 64);
    let mut req_buf = Vec::with_capacity(256 + batch.len() * 64);
    let mut batch_tail = Vec::with_capacity(24);
    {
        let mut tw = BakedWire::new(&mut batch_tail);
        tw.push_chunk(br#"","#);
        tw.push_i64(batch_i64);
        tw.push_byte(b']');
    }

    {
        let mut bw = BakedWire::cleared(&mut body_buf, 4 + batch.len() * 64);
        bw.push_byte(b'[');
        for (idx, (_, key)) in batch.iter().enumerate() {
            if idx > 0 {
                bw.push_byte(b',');
            }
            bw.push_chunk(br#"["DECRBY",""#);
            bw.push_json_escaped_str(key);
            bw.push_chunk(&batch_tail);
        }
        bw.push_byte(b']');
    }

    let parsed = match post_upstash_pipeline(etp, &body_buf, &mut req_buf, resp_buf).await {
        Ok(p) => p,
        Err(err) => {
            tracing::warn!(id = %etp.config.id, error = %err, "Upstash quota DECRBY batch failed");
            connected.store(false, Ordering::Relaxed);
            return false;
        }
    };

    let mut has_refunds = false;
    {
        let mut rw = BakedWire::cleared(&mut body_buf, 128);
        rw.push_byte(b'[');
        let mut updates: Vec<(usize, i64)> = Vec::with_capacity(batch.len());

        for (i, (slot_idx, key)) in batch.iter().enumerate() {
            let Some(item) = parsed.get(i) else {
                updates.push((*slot_idx, 0));
                continue;
            };
            if let Some(err) = item.get("error").and_then(|v| v.as_str()) {
                tracing::warn!(key = %key, error = %err, "Upstash DECRBY item error");
                updates.push((*slot_idx, 0));
                continue;
            }
            let Some(after) = item.get("result").and_then(|v| v.as_i64()) else {
                updates.push((*slot_idx, 0));
                continue;
            };
            let (leased, refund) = compute_lease_and_refund(after, batch_i64);
            if refund > 0 {
                if has_refunds {
                    rw.push_byte(b',');
                }
                let esc = json_escape_bytes(key);
                rw.push_chunk(br#"["INCRBY",""#);
                rw.push_chunk(&esc);
                rw.push_chunk(br#"","#);
                rw.push_i64(refund);
                rw.push_byte(b']');
                has_refunds = true;
            }
            updates.push((*slot_idx, leased));
        }
        rw.push_byte(b']');

        if has_refunds
            && let Err(err) = post_upstash_pipeline(etp, &body_buf, &mut req_buf, resp_buf).await
        {
            tracing::warn!(
                id = %etp.config.id,
                error = %err,
                "Failed to flush compensating Upstash INCRBY refunds"
            );
        }

        connected.store(true, Ordering::Relaxed);
        for (slot_idx, leased) in updates {
            if let Some(slot) = slots.get(slot_idx) {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use velda_core::clock::init_clock;
    use velda_etp::{EtpEntryConfig, EtpKind, EtpPoolSettings, EtpTransportSecurity};

    use crate::post_response::openai_token_quota::local::{SharedUsageArena, fnv1a_64};
    use crate::post_response::openai_token_quota::scanner::TokenUsage;

    #[tokio::test]
    async fn test_upstash_quota_decrby_and_compensating_incrby_refund() {
        init_clock();
        let arena = Arc::new(SharedUsageArena::new(64, 0, 10));
        let usage = TokenUsage {
            prompt_tokens: 1,
            completion_tokens: 1,
            total_tokens: 2,
            cached_prompt_tokens: 0,
        };
        arena.record(
            fnv1a_64(b"tenant-up-partial"),
            b"tenant-up-partial",
            usage,
            1,
        );

        let mut pending = Vec::new();
        let mut kb = [0u8; 62];
        for (i, slot) in arena.slots().iter().enumerate() {
            if slot.consumer_hash.load(Ordering::Relaxed) != 0 {
                let k = SharedUsageArena::read_slot_key(slot, &mut kb).to_string();
                pending.push((i, format!("velda:plugin:openai_token_quota:{k}")));
            }
        }

        let saw_refund = Arc::new(AtomicBool::new(false));
        let saw_refund_c = Arc::clone(&saw_refund);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let srv = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            while let Ok(n) = sock.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                let req = String::from_utf8_lossy(&buf[..n]);
                let body = if req.contains("\"DECRBY\"") {
                    r#"[{"result":-20}]"#
                } else if req.contains("\"INCRBY\"") && req.contains(",20]") {
                    saw_refund_c.store(true, Ordering::Release);
                    r#"[{"result":0}]"#
                } else {
                    "[]"
                };
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            }
        });

        let entry = EtpEntryConfig {
            id: "up-quota-test".into(),
            kind: EtpKind::Upstash,
            security: EtpTransportSecurity::Plain,
            pool: EtpPoolSettings {
                pool_size: 2,
                connect_timeout_ms: 500,
                idle_timeout_ms: 5000,
            },
            tls: None,
            settings: serde_json::json!({
                "host": "127.0.0.1",
                "port": port,
                "token": "up-secret"
            }),
        };
        let etp = Arc::new(UpstashEtp::from_entry(&entry).unwrap());
        let connected = AtomicBool::new(false);
        let mut rbuf = Vec::new();
        let ok = flush_batch(&etp, arena.slots(), &pending, 50, &mut rbuf, &connected).await;
        assert!(ok);
        assert!(saw_refund.load(Ordering::Acquire));

        let (_, _, _, _, rem) = arena.read_consumer("tenant-up-partial").unwrap();
        assert_eq!(rem, 28);
        srv.abort();
    }
}
