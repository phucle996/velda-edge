//! Elasticsearch / OpenSearch `_bulk` scripted atomic upsert & `_mget` readback (`client.rs`).
//!
//! ```text
//!   BakedWire chunked NDJSON & _mget JSON assembly (0 serde_json::json! on send):
//!     bulk_head <- prebaked_bulk_head + fast_write_u64(bulk_body.len()) + "\r\n\r\n"
//!     bulk_body <- [ {"update":{"_id":"..."}}\n{"script":{"source":...},"upsert":...}\n ]
//!     mget_head <- prebaked_mget_head + fast_write_u64(mget_body.len()) + "\r\n\r\n"
//!     mget_body <- {"ids":["...","..."]}
//! ```

use std::io::Error;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use velda_etp::EtpError;
use velda_etp::elasticsearch::{ElasticsearchEtp, ElasticsearchEtpConfig};

use super::http::read_http1_response;
use crate::pre_upstream::rate_limit::arena::{DirtyKeyDelta, ReconciledClusterTotal};
use crate::wire_baked::{BakedWire, json_escape_bytes};

const RL_INDEX: &str = "velda_rate_limits";
const ACTION_HEAD: &[u8] = b"{\"update\":{\"_id\":\"";
const SCRIPT_HEAD: &[u8] = b"\"}}\n{\"script\":{\"source\":\"ctx._source.cnt = (ctx._source.cnt ?: 0) + params.delta\",\"lang\":\"painless\",\"params\":{\"delta\":";
const UPSERT_CNT: &[u8] = b"}},\"upsert\":{\"cnt\":";
const UPSERT_EPOCH: &[u8] = b",\"epoch\":";
const ACTION_TAIL: &[u8] = b"},\"scripted_upsert\":false}\n";

/// Executes scripted atomic counter increments in Elasticsearch/OpenSearch `_bulk`
/// and reads updated cluster totals via `_mget`.
pub async fn push_and_read_totals(
    etp: &Arc<ElasticsearchEtp>,
    key_prefix: &str,
    deltas: &[DirtyKeyDelta],
    io_timeout: Duration,
) -> Result<Vec<ReconciledClusterTotal>, EtpError> {
    if deltas.is_empty() {
        return Ok(Vec::new());
    }

    let escaped_prefix = json_escape_bytes(key_prefix);
    let mut bulk_body = Vec::with_capacity(deltas.len() * (escaped_prefix.len() + 240));
    let mut mget_body = Vec::with_capacity(16 + deltas.len() * (escaped_prefix.len() + 40));

    {
        let mut bw = BakedWire::new(&mut bulk_body);
        let mut mw = BakedWire::new(&mut mget_body);
        mw.push_chunk(b"{\"ids\":[");

        for (i, d) in deltas.iter().enumerate() {
            if i > 0 {
                mw.push_byte(b',');
            }
            bw.push_chunk(ACTION_HEAD);
            bw.push_chunk(&escaped_prefix);
            bw.push_u64(u64::from(d.tier));
            bw.push_byte(b':');
            bw.push_hex16(d.key_hash);
            bw.push_byte(b':');
            bw.push_u64(d.window_epoch);
            bw.push_chunk(SCRIPT_HEAD);
            bw.push_u64(d.delta);
            bw.push_chunk(UPSERT_CNT);
            bw.push_u64(d.delta);
            bw.push_chunk(UPSERT_EPOCH);
            bw.push_u64(d.window_epoch);
            bw.push_chunk(ACTION_TAIL);

            mw.push_byte(b'"');
            mw.push_chunk(&escaped_prefix);
            mw.push_u64(u64::from(d.tier));
            mw.push_byte(b':');
            mw.push_hex16(d.key_hash);
            mw.push_byte(b':');
            mw.push_u64(d.window_epoch);
            mw.push_byte(b'"');
        }
        mw.push_chunk(b"]}");
    }

    let auth = format_auth_headers(&etp.config);
    let bulk_head_prefix = format!(
        "POST {}/{RL_INDEX}/_bulk?refresh=wait_for HTTP/1.1\r\nHost: {}\r\n\
         Content-Type: application/x-ndjson\r\n{auth}Connection: keep-alive\r\nContent-Length: ",
        etp.config.base_path, etp.config.host_header
    );
    let mget_head_prefix = format!(
        "POST {}/{RL_INDEX}/_mget HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\n\
         {auth}Connection: keep-alive\r\nContent-Length: ",
        etp.config.base_path, etp.config.host_header
    );

    let mut bulk_head = Vec::with_capacity(bulk_head_prefix.len() + 24);
    {
        let mut hw = BakedWire::new(&mut bulk_head);
        hw.push_chunk(bulk_head_prefix.as_bytes());
        hw.push_u64(bulk_body.len() as u64);
        hw.push_chunk(b"\r\n\r\n");
    }
    let mut mget_head = Vec::with_capacity(mget_head_prefix.len() + 24);
    {
        let mut hw = BakedWire::new(&mut mget_head);
        hw.push_chunk(mget_head_prefix.as_bytes());
        hw.push_u64(mget_body.len() as u64);
        hw.push_chunk(b"\r\n\r\n");
    }

    let mut lease = etp.acquire().await?;
    let mut resp_buf = Vec::with_capacity(4096);
    let mut last_err = EtpError::Timeout {
        id: etp.config.id.clone(),
        timeout_ms: u64::try_from(io_timeout.as_millis()).unwrap_or(u64::MAX),
    };

    for attempt in 0..2 {
        let res: Result<Result<Vec<ReconciledClusterTotal>, EtpError>, _> =
            tokio::time::timeout(io_timeout, async {
                lease.write_all(&bulk_head).await?;
                lease.write_all(&bulk_body).await?;
                lease.flush().await?;
                let (st1, b1) = read_http1_response(&mut lease, &mut resp_buf).await?;
                if !(200..300).contains(&st1) {
                    return Err(EtpError::Io(Error::other(format!(
                        "Elasticsearch _bulk HTTP {st1}: {}",
                        String::from_utf8_lossy(&b1)
                    ))));
                }

                lease.write_all(&mget_head).await?;
                lease.write_all(&mget_body).await?;
                lease.flush().await?;
                let (st2, b2) = read_http1_response(&mut lease, &mut resp_buf).await?;
                if !(200..300).contains(&st2) {
                    return Err(EtpError::Io(Error::other(format!(
                        "Elasticsearch _mget HTTP {st2}: {}",
                        String::from_utf8_lossy(&b2)
                    ))));
                }

                let json: serde_json::Value = serde_json::from_slice(&b2).unwrap_or_default();
                let docs = json.get("docs").and_then(|v| v.as_array());
                let mut out = Vec::with_capacity(deltas.len());
                for (i, d) in deltas.iter().enumerate() {
                    let cluster_total = docs
                        .and_then(|arr| arr.get(i))
                        .and_then(|doc| doc.get("_source"))
                        .and_then(|src| src.get("cnt"))
                        .and_then(|c| c.as_u64())
                        .unwrap_or(d.delta);
                    out.push(ReconciledClusterTotal {
                        key_hash: d.key_hash,
                        tier: d.tier,
                        pushed_delta: d.delta,
                        window_epoch: d.window_epoch,
                        cluster_total,
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

fn format_auth_headers(cfg: &ElasticsearchEtpConfig) -> String {
    if let Some(key) = &cfg.api_key {
        format!("Authorization: ApiKey {key}\r\n")
    } else if let Some(basic) = &cfg.basic_auth {
        if basic.contains(':') {
            let encoded = base64::engine::general_purpose::STANDARD.encode(basic);
            format!("Authorization: Basic {encoded}\r\n")
        } else {
            format!("Authorization: Basic {basic}\r\n")
        }
    } else if let Some(token) = &cfg.bearer_token {
        format!("Authorization: Bearer {token}\r\n")
    } else {
        String::new()
    }
}
