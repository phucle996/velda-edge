//! Upstash HTTPS REST (`/pipeline`) atomic counter upsert & readback (`client.rs`).
//!
//! ```text
//!   BakedWire chunked JSON pipeline assembly (0 serde_json::json!, 0 format! body copy):
//!     head_buf <- prebaked_head + fast_write_u64(body.len()) + "\r\n\r\n"
//!     body_buf <- [ ["SET", "<prefix><tier>:<16hex>:<epoch>", "0", "EX", 15, "NX"],
//!                   ["INCRBY", "<prefix><tier>:<16hex>:<epoch>", <delta>], ... ]
//! ```

use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::time::Duration;

use velda_etp::EtpError;
use velda_etp::upstash::UpstashEtp;

use super::http::read_http1_response;
use crate::pre_upstream::rate_limit::arena::{DirtyKeyDelta, ReconciledClusterTotal};
use crate::wire_baked::{BakedWire, json_escape_bytes};

const SET_PREFIX: &[u8] = b"[\"SET\",\"";
const SET_TAIL_AND_INCR_PREFIX: &[u8] = b"\",\"0\",\"EX\",15,\"NX\"],[\"INCRBY\",\"";
const INCR_MID: &[u8] = b"\",";

/// Atomically initializes (if absent with TTL) and increments each key's epoch counter via
/// Upstash REST `POST /pipeline` over keep-alive HTTPS/HTTP.
pub async fn push_and_read_totals(
    etp: &Arc<UpstashEtp>,
    key_prefix: &str,
    deltas: &[DirtyKeyDelta],
    io_timeout: Duration,
) -> Result<Vec<ReconciledClusterTotal>, EtpError> {
    if deltas.is_empty() {
        return Ok(Vec::new());
    }

    let effective_prefix = if key_prefix.is_empty() {
        "velda:rl:"
    } else {
        key_prefix
    };
    let escaped_prefix = json_escape_bytes(effective_prefix);

    let mut body_buf = Vec::with_capacity(2 + deltas.len() * (escaped_prefix.len() + 112));
    {
        let mut w = BakedWire::new(&mut body_buf);
        w.push_byte(b'[');
        for (i, d) in deltas.iter().enumerate() {
            if i > 0 {
                w.push_byte(b',');
            }
            w.push_chunk(SET_PREFIX);
            w.push_chunk(&escaped_prefix);
            w.push_u64(u64::from(d.tier));
            w.push_byte(b':');
            w.push_hex16(d.key_hash);
            w.push_byte(b':');
            w.push_u64(d.window_epoch);
            w.push_chunk(SET_TAIL_AND_INCR_PREFIX);
            w.push_chunk(&escaped_prefix);
            w.push_u64(u64::from(d.tier));
            w.push_byte(b':');
            w.push_hex16(d.key_hash);
            w.push_byte(b':');
            w.push_u64(d.window_epoch);
            w.push_chunk(INCR_MID);
            w.push_u64(d.delta);
            w.push_byte(b']');
        }
        w.push_byte(b']');
    }

    let head_prefix = format!(
        "POST {}/pipeline HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {}\r\n\
         Content-Type: application/json\r\nConnection: keep-alive\r\nContent-Length: ",
        etp.config.base_path, etp.config.host_header, etp.config.token
    );
    let mut head_buf = Vec::with_capacity(head_prefix.len() + 24);
    {
        let mut hw = BakedWire::new(&mut head_buf);
        hw.push_chunk(head_prefix.as_bytes());
        hw.push_u64(body_buf.len() as u64);
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
                lease.write_all(&head_buf).await?;
                lease.write_all(&body_buf).await?;
                lease.flush().await?;
                let (status, resp_body) = read_http1_response(&mut lease, &mut resp_buf).await?;
                if !(200..300).contains(&status) {
                    return Err(EtpError::Io(Error::other(format!(
                        "Upstash /pipeline HTTP {status}: {}",
                        String::from_utf8_lossy(&resp_body)
                    ))));
                }
                let parsed: Vec<serde_json::Value> =
                    serde_json::from_slice(&resp_body).map_err(|e| {
                        EtpError::Io(Error::new(
                            ErrorKind::InvalidData,
                            format!("Upstash JSON parse failed: {e}"),
                        ))
                    })?;

                let mut out = Vec::with_capacity(deltas.len());
                for (i, d) in deltas.iter().enumerate() {
                    let incr_item = parsed.get(i * 2 + 1).ok_or_else(|| {
                        EtpError::Io(Error::new(
                            ErrorKind::InvalidData,
                            "Upstash pipeline response truncated",
                        ))
                    })?;
                    if let Some(err) = incr_item.get("error").and_then(|v| v.as_str()) {
                        return Err(EtpError::Io(Error::new(
                            ErrorKind::InvalidData,
                            format!("Upstash INCRBY error: {err}"),
                        )));
                    }
                    let total = incr_item
                        .get("result")
                        .and_then(|v| v.as_u64())
                        .ok_or_else(|| {
                            EtpError::Io(Error::new(
                                ErrorKind::InvalidData,
                                format!("Unexpected Upstash INCRBY result: {incr_item}"),
                            ))
                        })?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use velda_etp::{EtpEntryConfig, EtpKind, EtpPoolSettings, EtpTransportSecurity};

    #[tokio::test]
    async fn test_upstash_pipeline_post_parses_increment_totals() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let srv = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 2048];
            let mut total_read = 0usize;
            loop {
                let n = sock.read(&mut buf[total_read..]).await.unwrap();
                if n == 0 {
                    break;
                }
                total_read += n;
                if String::from_utf8_lossy(&buf[..total_read]).contains("]]") {
                    break;
                }
            }
            let req = String::from_utf8_lossy(&buf[..total_read]);
            assert!(req.starts_with("POST /pipeline HTTP/1.1"));
            assert!(req.contains("Authorization: Bearer test-token"));
            assert!(req.contains("\"SET\""));
            assert!(req.contains("\"INCRBY\""));
            let body = r#"[{"result":"OK"},{"result":11},{"result":null},{"result":27}]"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            let _ = sock.write_all(resp.as_bytes()).await;
        });

        let entry = EtpEntryConfig {
            id: "up-rl-test".into(),
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
                "token": "test-token"
            }),
        };
        let pool = Arc::new(UpstashEtp::from_entry(&entry).unwrap());
        let deltas = vec![
            DirtyKeyDelta {
                key_hash: 1,
                tier: 0,
                delta: 3,
                window_epoch: 10,
            },
            DirtyKeyDelta {
                key_hash: 2,
                tier: 1,
                delta: 4,
                window_epoch: 10,
            },
        ];
        let totals = push_and_read_totals(&pool, "velda:rl:", &deltas, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(totals.len(), 2);
        assert_eq!(totals[0].cluster_total, 11);
        assert_eq!(totals[1].cluster_total, 27);
        srv.abort();
    }
}
