//! DynamoDB `UpdateItem` atomic counter upsert client (`client.rs`).
//!
//! ```text
//!   BakedWire chunked UpdateItem JSON + DynamoDbSigV4Signer (kSigning daily cache):
//!     CHUNK_0 ("velda_rate_limit") + escaped_pk + CHUNK_1 + delta + CHUNK_2 + ttl + CHUNK_3
//! ```

use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinSet;
use velda_etp::EtpError;
use velda_etp::dynamodb::DynamoDbEtp;

use super::sigv4::{DynamoDbSigV4Signer, read_http1_response};
use crate::pre_upstream::rate_limit::arena::{DirtyKeyDelta, ReconciledClusterTotal};
use crate::wire_baked::{BakedWire, json_escape_bytes};

const CHUNK_0: &[u8] = b"{\"TableName\":\"velda_rate_limit\",\"Key\":{\"pk\":{\"S\":\"";
const CHUNK_1: &[u8] = b"\"}},\"UpdateExpression\":\"ADD #c :d SET #ttl = if_not_exists(#ttl, :t)\",\"ExpressionAttributeNames\":{\"#c\":\"cnt\",\"#ttl\":\"ttl\"},\"ExpressionAttributeValues\":{\":d\":{\"N\":\"";
const CHUNK_2: &[u8] = b"\"},\":t\":{\"N\":\"";
const CHUNK_3: &[u8] = b"\"}},\"ReturnValues\":\"UPDATED_NEW\"}";

/// Pushes counter increments to DynamoDB `UpdateItem` in parallel across pooled connections.
pub async fn push_and_read_totals(
    etp: &Arc<DynamoDbEtp>,
    key_prefix: &str,
    deltas: &[DirtyKeyDelta],
    io_timeout: Duration,
) -> Result<Vec<ReconciledClusterTotal>, EtpError> {
    if deltas.is_empty() {
        return Ok(Vec::new());
    }

    let escaped_prefix = Arc::new(json_escape_bytes(key_prefix));
    let now_unix = velda_core::clock::coarse_sec();
    let ttl_epoch = now_unix + 300;
    let concurrency = etp.config.pool.pool_size.clamp(1, 8).min(deltas.len());
    let chunk_size = deltas.len().div_ceil(concurrency);

    let mut set = JoinSet::new();
    for (chunk_idx, chunk) in deltas.chunks(chunk_size).enumerate() {
        let base_idx = chunk_idx * chunk_size;
        let sub_deltas = chunk.to_vec();
        let etp_c = Arc::clone(etp);
        let prefix_c = Arc::clone(&escaped_prefix);
        set.spawn(async move {
            push_chunk(
                &etp_c,
                &prefix_c,
                ttl_epoch,
                now_unix,
                base_idx,
                &sub_deltas,
                io_timeout,
            )
            .await
        });
    }

    let mut ordered: Vec<Option<ReconciledClusterTotal>> = vec![None; deltas.len()];
    while let Some(joined) = set.join_next().await {
        let pairs = joined.map_err(|e| {
            EtpError::Io(Error::other(format!("DynamoDB sync worker panicked: {e}")))
        })??;
        for (idx, item) in pairs {
            ordered[idx] = Some(item);
        }
    }

    Ok(ordered.into_iter().flatten().collect())
}

async fn push_chunk(
    etp: &Arc<DynamoDbEtp>,
    escaped_prefix: &[u8],
    ttl_epoch: u64,
    now_unix: u64,
    base_idx: usize,
    deltas: &[DirtyKeyDelta],
    io_timeout: Duration,
) -> Result<Vec<(usize, ReconciledClusterTotal)>, EtpError> {
    let mut lease = etp.acquire().await?;
    let mut signer = DynamoDbSigV4Signer::new(&etp.config);
    let mut body_buf = Vec::with_capacity(384);
    let mut creq_buf = Vec::with_capacity(320);
    let mut req_buf = Vec::with_capacity(1024);
    let mut resp_buf = Vec::with_capacity(2048);
    let mut out = Vec::with_capacity(deltas.len());

    let mut last_err = EtpError::Timeout {
        id: etp.config.id.clone(),
        timeout_ms: u64::try_from(io_timeout.as_millis()).unwrap_or(u64::MAX),
    };

    for attempt in 0..2 {
        let done = out.len();
        let pending = &deltas[done..];
        let res: Result<Result<(), EtpError>, _> = tokio::time::timeout(io_timeout, async {
            for (rel_i, d) in pending.iter().enumerate() {
                {
                    let mut bw = BakedWire::cleared(&mut body_buf, 384);
                    bw.push_chunk(CHUNK_0);
                    bw.push_chunk(escaped_prefix);
                    bw.push_u64(u64::from(d.tier));
                    bw.push_byte(b':');
                    bw.push_hex16(d.key_hash);
                    bw.push_byte(b':');
                    bw.push_u64(d.window_epoch);
                    bw.push_chunk(CHUNK_1);
                    bw.push_u64(d.delta);
                    bw.push_chunk(CHUNK_2);
                    bw.push_u64(ttl_epoch);
                    bw.push_chunk(CHUNK_3);
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

                let (status, resp_body) = read_http1_response(&mut lease, &mut resp_buf).await?;
                if !(200..300).contains(&status) {
                    return Err(EtpError::Io(Error::other(format!(
                        "DynamoDB UpdateItem returned HTTP {status}: {}",
                        String::from_utf8_lossy(&resp_body)
                    ))));
                }

                let parsed: serde_json::Value =
                    serde_json::from_slice(&resp_body).map_err(|e| {
                        EtpError::Io(Error::new(
                            ErrorKind::InvalidData,
                            format!("DynamoDB invalid JSON response: {e}"),
                        ))
                    })?;
                let total = parsed
                    .get("Attributes")
                    .and_then(|a| a.get("cnt"))
                    .and_then(|c| c.get("N"))
                    .and_then(|n| n.as_str())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(d.delta);

                out.push((
                    base_idx + done + rel_i,
                    ReconciledClusterTotal {
                        key_hash: d.key_hash,
                        tier: d.tier,
                        pushed_delta: d.delta,
                        window_epoch: d.window_epoch,
                        cluster_total: total,
                    },
                ));
            }
            Ok(())
        })
        .await;

        match res {
            Ok(Ok(())) => return Ok(out),
            Ok(Err(err)) => {
                last_err = err;
                if attempt == 0 && lease.reconnect().await.is_ok() {
                    continue;
                }
                lease.recreate_in_background();
                return Err(last_err);
            }
            Err(_) => {
                lease.recreate_in_background();
                return Err(last_err);
            }
        }
    }

    Err(last_err)
}
