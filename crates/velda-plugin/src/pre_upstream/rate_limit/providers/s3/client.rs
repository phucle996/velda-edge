//! S3 / MinIO / R2 SigV4 HTTP/1.1 `GET` + `PUT` object ledger client (`client.rs`).
//!
//! ```text
//!   DirtyKeyDelta -> BakedWire uri_path + S3SigV4Signer (kSigning cache)
//!     ├─► 1. SigV4 GET -> parse {"cnt": prev} + ETag
//!     └─► 2. SigV4 PUT (If-Match: ETag) BakedWire {"cnt":prev+delta,"epoch":epoch}
//! ```

use std::io::Error;
use std::sync::Arc;
use std::time::Duration;

use velda_etp::EtpError;
use velda_etp::s3::S3Etp;

use super::http::read_http1_response;
use super::sigv4::S3SigV4Signer;
use crate::pre_upstream::rate_limit::arena::{DirtyKeyDelta, ReconciledClusterTotal};
use crate::wire_baked::BakedWire;

/// Reads existing epoch object from S3 (if any), merges delta, and puts updated object with SigV4.
pub async fn push_and_read_totals(
    etp: &Arc<S3Etp>,
    key_prefix: &str,
    deltas: &[DirtyKeyDelta],
    io_timeout: Duration,
) -> Result<Vec<ReconciledClusterTotal>, EtpError> {
    if deltas.is_empty() {
        return Ok(Vec::new());
    }

    let safe_prefix: String = key_prefix
        .trim_matches('/')
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '/') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let uri_prefix: Vec<u8> = if etp.config.force_path_style {
        if safe_prefix.is_empty() {
            format!("/{}/", etp.config.bucket).into_bytes()
        } else {
            format!("/{}/{safe_prefix}/", etp.config.bucket).into_bytes()
        }
    } else if safe_prefix.is_empty() {
        b"/".to_vec()
    } else {
        format!("/{safe_prefix}/").into_bytes()
    };

    let now_unix = velda_core::clock::coarse_sec();
    let mut lease = etp.acquire().await?;
    let mut signer = S3SigV4Signer::new(&etp.config);
    let mut uri_buf = Vec::with_capacity(uri_prefix.len() + 48);
    let mut body_buf = Vec::with_capacity(64);
    let mut creq_buf = Vec::with_capacity(320);
    let mut req_buf = Vec::with_capacity(768);
    let mut resp_buf = Vec::with_capacity(4096);

    let mut last_err = EtpError::Timeout {
        id: etp.config.id.clone(),
        timeout_ms: u64::try_from(io_timeout.as_millis()).unwrap_or(u64::MAX),
    };

    for attempt in 0..2 {
        let res: Result<Result<Vec<ReconciledClusterTotal>, EtpError>, _> =
            tokio::time::timeout(io_timeout, async {
                let mut out = Vec::with_capacity(deltas.len());
                for d in deltas {
                    {
                        let mut uw = BakedWire::cleared(&mut uri_buf, uri_prefix.len() + 48);
                        uw.push_chunk(&uri_prefix);
                        uw.push_u64(u64::from(d.tier));
                        uw.push_byte(b'_');
                        uw.push_hex16(d.key_hash);
                        uw.push_byte(b'_');
                        uw.push_u64(d.window_epoch);
                        uw.push_chunk(b".json");
                    }

                    signer.write_signed_request(
                        b"GET",
                        &uri_buf,
                        &[],
                        None,
                        now_unix,
                        &mut creq_buf,
                        &mut req_buf,
                    );
                    lease.write_all(&req_buf).await?;
                    lease.flush().await?;
                    let (get_st, etag, get_body) =
                        read_http1_response(&mut lease, &mut resp_buf).await?;

                    let existing = if get_st == 200 {
                        serde_json::from_slice::<serde_json::Value>(&get_body)
                            .ok()
                            .and_then(|v| v.get("cnt").and_then(|c| c.as_u64()))
                            .unwrap_or(0)
                    } else if get_st == 404 {
                        0
                    } else {
                        return Err(EtpError::Io(Error::other(format!(
                            "S3 GET {} returned HTTP {get_st}",
                            String::from_utf8_lossy(&uri_buf)
                        ))));
                    };

                    let total = existing.saturating_add(d.delta);
                    {
                        let mut bw = BakedWire::cleared(&mut body_buf, 64);
                        bw.push_chunk(b"{\"cnt\":");
                        bw.push_u64(total);
                        bw.push_chunk(b",\"epoch\":");
                        bw.push_u64(d.window_epoch);
                        bw.push_byte(b'}');
                    }

                    signer.write_signed_request(
                        b"PUT",
                        &uri_buf,
                        &body_buf,
                        etag.as_deref(),
                        now_unix,
                        &mut creq_buf,
                        &mut req_buf,
                    );
                    lease.write_all(&req_buf).await?;
                    lease.flush().await?;
                    let (put_st, _, put_resp) =
                        read_http1_response(&mut lease, &mut resp_buf).await?;
                    if !(200..300).contains(&put_st) && put_st != 412 {
                        return Err(EtpError::Io(Error::other(format!(
                            "S3 PUT {} returned HTTP {put_st}: {}",
                            String::from_utf8_lossy(&uri_buf),
                            String::from_utf8_lossy(&put_resp)
                        ))));
                    }

                    out.push(ReconciledClusterTotal {
                        key_hash: d.key_hash,
                        tier: d.tier,
                        pushed_delta: d.delta,
                        window_epoch: d.window_epoch,
                        cluster_total: total,
                    });
                }
                Ok(out)
            })
            .await;

        match res {
            Ok(Ok(totals)) => return Ok(totals),
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
