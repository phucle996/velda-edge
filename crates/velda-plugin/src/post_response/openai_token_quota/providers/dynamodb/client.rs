//! AWS DynamoDB SigV4 `UpdateItem` (`ADD remaining :-batch` + overshoot `ADD remaining :refund`)
//! micro-quota lease on dedicated table `velda_openai_token_quota` (`client.rs`).
//!
//! ```text
//!   flush_batch(&etp, slots, batch, batch_i64)
//!     └──► UpdateItem(ADD #r :-batch, ALL_NEW) -> refund if after < 0 -> update UsageSlot
//! ```

use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use velda_core::lut::decimal::fast_write_u64_slice;
use velda_etp::EtpError;
use velda_etp::dynamodb::DynamoDbEtp;

use super::sigv4::{DynamoDbSigV4Signer, read_http1_response};
use crate::post_response::openai_token_quota::local::{
    FLAG_EXHAUSTED, FLAG_NEEDS_REFILL, UsageSlot,
};
use crate::wire_baked::BakedWire;

const DDB_DECR_PREFIX: &[u8] = br#"{"TableName":"velda_openai_token_quota","Key":{"pk":{"S":""#;
const DDB_DECR_MID: &[u8] = br##"","}},"UpdateExpression":"ADD #r :d SET #u = :u","ExpressionAttributeNames":{"#r":"remaining","#u":"updated_at"},"ExpressionAttributeValues":{":d":{"N":""##;
const DDB_DECR_TAIL_PRE: &[u8] = br#"","},":u":{"N":""#;
const DDB_DECR_TAIL_POST: &[u8] = br#""}},"ReturnValues":"ALL_NEW"}"#;
const DDB_REFUND_U_PRE: &[u8] = br#"","},":u":{"N":""#;
const DDB_REFUND_TAIL: &[u8] = br#""}}}"#;

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

pub(crate) async fn flush_batch(
    etp: &Arc<DynamoDbEtp>,
    slots: &[UsageSlot],
    batch: &[(usize, String)],
    batch_i64: i64,
    io_timeout: Duration,
    connected: &AtomicBool,
) -> bool {
    if batch.is_empty() {
        return true;
    }

    let now_unix = velda_core::clock::coarse_sec();
    let mut now_buf = [0u8; 24];
    let now_len = fast_write_u64_slice(&mut now_buf, now_unix);
    let now_bytes = &now_buf[..now_len];

    // Pre-bake the invariant UpdateItem suffix (`:d = -batch`, `:u = now`, `ReturnValues = ALL_NEW`) once per batch.
    let mut decr_tail = Vec::with_capacity(64 + DDB_DECR_TAIL_PRE.len() + DDB_DECR_TAIL_POST.len());
    {
        let mut tw = BakedWire::new(&mut decr_tail);
        tw.push_chunk(DDB_DECR_MID);
        tw.push_i64(-batch_i64);
        tw.push_chunk(DDB_DECR_TAIL_PRE);
        tw.push_chunk(now_bytes);
        tw.push_chunk(DDB_DECR_TAIL_POST);
    }

    let Ok(mut lease) = etp.acquire().await else {
        connected.store(false, Ordering::Relaxed);
        return false;
    };

    let mut signer = DynamoDbSigV4Signer::new(&etp.config);
    let mut resp_buf = Vec::with_capacity(2048);
    let mut body_buf = Vec::with_capacity(384);
    let mut creq_buf = Vec::with_capacity(320);
    let mut req_buf = Vec::with_capacity(896);
    let mut updates = None;

    for attempt in 0..2 {
        let res: Result<Result<Vec<(usize, i64)>, EtpError>, _> =
            tokio::time::timeout(io_timeout, async {
                let mut out = Vec::with_capacity(batch.len());
                for (slot_idx, key) in batch {
                    {
                        let mut bw = BakedWire::cleared(&mut body_buf, 384);
                        bw.push_chunk(DDB_DECR_PREFIX);
                        bw.push_json_escaped_str(key);
                        bw.push_chunk(&decr_tail);
                    }

                    signer.write_signed_request(
                        b"DynamoDB_20120810.UpdateItem",
                        &body_buf,
                        now_unix,
                        &mut creq_buf,
                        &mut req_buf,
                    );
                    lease.write_all(&req_buf).await?;
                    lease.flush().await?;
                    let (st, resp_body) = read_http1_response(&mut lease, &mut resp_buf).await?;
                    if !(200..300).contains(&st) {
                        return Err(EtpError::Io(Error::other(format!(
                            "DynamoDB UpdateItem returned HTTP {st}: {}",
                            String::from_utf8_lossy(&resp_body)
                        ))));
                    }
                    let parsed: serde_json::Value =
                        serde_json::from_slice(&resp_body).map_err(|e| {
                            EtpError::Io(Error::new(
                                ErrorKind::InvalidData,
                                format!("DynamoDB invalid JSON: {e}"),
                            ))
                        })?;
                    let after = parsed
                        .get("Attributes")
                        .and_then(|a| a.get("remaining"))
                        .and_then(|c| c.get("N"))
                        .and_then(|n| n.as_str())
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(-batch_i64);

                    let (leased, refund) = compute_lease_and_refund(after, batch_i64);
                    if refund > 0 {
                        {
                            let mut bw = BakedWire::cleared(&mut body_buf, 384);
                            bw.push_chunk(DDB_DECR_PREFIX);
                            bw.push_json_escaped_str(key);
                            bw.push_chunk(br##"","}},"UpdateExpression":"ADD #r :ref SET #u = :u","ExpressionAttributeNames":{"#r":"remaining","#u":"updated_at"},"ExpressionAttributeValues":{":ref":{"N":""##);
                            bw.push_i64(refund);
                            bw.push_chunk(DDB_REFUND_U_PRE);
                            bw.push_chunk(now_bytes);
                            bw.push_chunk(DDB_REFUND_TAIL);
                        }
                        signer.write_signed_request(
                            b"DynamoDB_20120810.UpdateItem",
                            &body_buf,
                            now_unix,
                            &mut creq_buf,
                            &mut req_buf,
                        );
                        lease.write_all(&req_buf).await?;
                        lease.flush().await?;
                        let _ = read_http1_response(&mut lease, &mut resp_buf).await?;
                    }
                    out.push((*slot_idx, leased));
                }
                Ok(out)
            })
            .await;

        match res {
            Ok(Ok(vals)) => {
                updates = Some(vals);
                break;
            }
            Ok(Err(EtpError::Io(_))) if attempt == 0 && lease.reconnect().await.is_ok() => {}
            Ok(Err(err)) => {
                lease.recreate_in_background();
                tracing::warn!(id = %etp.config.id, error = %err, "DynamoDB quota lease failed");
                connected.store(false, Ordering::Relaxed);
                return false;
            }
            Err(_) => {
                lease.recreate_in_background();
                connected.store(false, Ordering::Relaxed);
                return false;
            }
        }
    }

    let Some(updates) = updates else {
        connected.store(false, Ordering::Relaxed);
        return false;
    };

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::post_response::openai_token_quota::providers::dynamodb::schema::ensure_table;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use velda_core::clock::init_clock;
    use velda_etp::{EtpEntryConfig, EtpKind, EtpPoolSettings, EtpTransportSecurity};

    use crate::post_response::openai_token_quota::local::{SharedUsageArena, fnv1a_64};
    use crate::post_response::openai_token_quota::scanner::TokenUsage;

    #[tokio::test]
    async fn test_dynamodb_quota_auto_create_table_and_overshoot_refund() {
        init_clock();
        let arena = Arc::new(SharedUsageArena::new(64, 0, 10));
        let usage = TokenUsage {
            prompt_tokens: 1,
            completion_tokens: 1,
            total_tokens: 2,
            cached_prompt_tokens: 0,
        };
        arena.record(fnv1a_64(b"ddb-tenant"), b"ddb-tenant", usage, 1);

        let mut pending = Vec::new();
        let mut kb = [0u8; 62];
        for (i, slot) in arena.slots().iter().enumerate() {
            if slot.consumer_hash.load(Ordering::Relaxed) != 0 {
                let k = SharedUsageArena::read_slot_key(slot, &mut kb).to_string();
                pending.push((i, format!("velda:plugin:openai_token_quota:{k}")));
            }
        }

        let created = Arc::new(AtomicBool::new(false));
        let refunded = Arc::new(AtomicBool::new(false));
        let created_c = Arc::clone(&created);
        let refunded_c = Arc::clone(&refunded);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let srv = tokio::spawn(async move {
            while let Ok((mut s, _)) = listener.accept().await {
                let cr = Arc::clone(&created_c);
                let rf = Arc::clone(&refunded_c);
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    while let Ok(n) = s.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                        let req = String::from_utf8_lossy(&buf[..n]);
                        let (status, body) = if req.contains("DescribeTable") {
                            (
                                "400 Bad Request",
                                r#"{"__type":"ResourceNotFoundException"}"#,
                            )
                        } else if req.contains("CreateTable") {
                            assert!(req.contains("velda_openai_token_quota"));
                            cr.store(true, Ordering::Release);
                            ("200 OK", r#"{"TableDescription":{"TableStatus":"ACTIVE"}}"#)
                        } else if req.contains("\"-50\"") {
                            // 30 remaining -> -20 after subtracting 50
                            ("200 OK", r#"{"Attributes":{"remaining":{"N":"-20"}}}"#)
                        } else if req.contains("\"20\"") {
                            rf.store(true, Ordering::Release);
                            ("200 OK", "{}")
                        } else {
                            ("200 OK", "{}")
                        };
                        let resp = format!(
                            "HTTP/1.1 {status}\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\n\r\n{body}",
                            body.len()
                        );
                        let _ = s.write_all(resp.as_bytes()).await;
                    }
                });
            }
        });

        let entry = EtpEntryConfig {
            id: "ddb-quota-test".into(),
            kind: EtpKind::DynamoDb,
            security: EtpTransportSecurity::Plain,
            pool: EtpPoolSettings {
                pool_size: 2,
                connect_timeout_ms: 500,
                idle_timeout_ms: 5000,
            },
            tls: None,
            settings: serde_json::json!({
                "region": "us-east-1",
                "host": "127.0.0.1",
                "port": port,
                "access_key_id": "AKIA",
                "secret_access_key": "SEC"
            }),
        };
        let ddb = Arc::new(DynamoDbEtp::from_entry(&entry).unwrap());
        ensure_table(&ddb, Duration::from_secs(1)).await.unwrap();
        assert!(created.load(Ordering::Acquire));

        let connected = AtomicBool::new(false);
        assert!(
            flush_batch(
                &ddb,
                arena.slots(),
                &pending,
                50,
                Duration::from_secs(1),
                &connected
            )
            .await
        );
        assert!(refunded.load(Ordering::Acquire));
        let (_, _, _, _, rem) = arena.read_consumer("ddb-tenant").unwrap();
        assert_eq!(rem, 28);
        srv.abort();
    }
}
