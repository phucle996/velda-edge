//! Upstash HTTPS REST (`/pipeline`) `HINCRBY` client for `api_metering` (`client.rs`).
//!
//! ```text
//!   flush_deltas(&etp, key, deltas)
//!     ├─► pre-bake `["HINCRBY","<escaped_key>","` chunk + HTTP/1.1 head prefix
//!     └─► BakedWire SIMD body + single write_all(&req_buf) ──► Vec<i64> consumed totals
//! ```

use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::time::Duration;

use velda_etp::EtpError;
use velda_etp::upstash::UpstashEtp;

use super::http::read_http1_response;
use crate::wire_baked::{BakedWire, json_escape_bytes};

/// Pushes account credit deltas via Upstash REST `POST /pipeline` (`HINCRBY`).
pub async fn flush_deltas(
    etp: &Arc<UpstashEtp>,
    key: &str,
    deltas: &[(u64, i64)],
    resp_buf: &mut Vec<u8>,
) -> Result<Vec<i64>, EtpError> {
    let escaped_key = json_escape_bytes(key);
    let mut cmd_prefix = Vec::with_capacity(14 + escaped_key.len());
    {
        let mut pw = BakedWire::new(&mut cmd_prefix);
        pw.push_chunk(br#"["HINCRBY",""#);
        pw.push_chunk(&escaped_key);
        pw.push_chunk(br#"",""#);
    }

    let mut body_buf = Vec::new();
    {
        let mut bw = BakedWire::cleared(&mut body_buf, 4 + deltas.len() * (cmd_prefix.len() + 36));
        bw.push_byte(b'[');
        for (idx, &(acc_hash, delta)) in deltas.iter().enumerate() {
            if idx > 0 {
                bw.push_byte(b',');
            }
            bw.push_chunk(&cmd_prefix);
            bw.push_u64(acc_hash);
            bw.push_chunk(br#"","#);
            bw.push_i64(delta);
            bw.push_byte(b']');
        }
        bw.push_byte(b']');
    }

    let mut req_buf = Vec::new();
    {
        let mut rw = BakedWire::cleared(&mut req_buf, 192 + body_buf.len());
        rw.push_chunk(b"POST ");
        rw.push_chunk(etp.config.base_path.as_bytes());
        rw.push_chunk(b"/pipeline HTTP/1.1\r\nHost: ");
        rw.push_chunk(etp.config.host_header.as_bytes());
        rw.push_chunk(b"\r\nAuthorization: Bearer ");
        rw.push_chunk(etp.config.token.as_bytes());
        rw.push_chunk(
            b"\r\nContent-Type: application/json\r\nConnection: keep-alive\r\nContent-Length: ",
        );
        rw.push_u64(body_buf.len() as u64);
        rw.push_chunk(b"\r\n\r\n");
        rw.push_chunk(&body_buf);
    }

    let mut lease = etp.acquire().await?;
    for attempt in 0..2 {
        let exchange: Result<Result<Vec<i64>, EtpError>, _> =
            tokio::time::timeout(Duration::from_millis(1000), async {
                lease.write_all(&req_buf).await?;
                lease.flush().await?;
                let (status, resp_body) = read_http1_response(&mut lease, resp_buf).await?;
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
                for (i, _) in deltas.iter().enumerate() {
                    let item = parsed.get(i).ok_or_else(|| {
                        EtpError::Io(Error::new(
                            ErrorKind::InvalidData,
                            "Upstash HINCRBY pipeline response truncated",
                        ))
                    })?;
                    if let Some(err) = item.get("error").and_then(|v| v.as_str()) {
                        tracing::warn!(error = %err, "Upstash returned error for HINCRBY in batch");
                        out.push(0);
                    } else {
                        out.push(item.get("result").and_then(|v| v.as_i64()).unwrap_or(0));
                    }
                }
                Ok(out)
            })
            .await;

        match exchange {
            Ok(Ok(vals)) => return Ok(vals),
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use velda_etp::{EtpEntryConfig, EtpKind, EtpPoolSettings, EtpTransportSecurity};

    #[tokio::test]
    async fn test_api_metering_upstash_hincrby_pipeline() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let srv = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut req_bytes = Vec::with_capacity(2048);
            let mut chunk = [0u8; 1024];
            loop {
                let n = sock.read(&mut chunk).await.unwrap();
                if n == 0 {
                    break;
                }
                req_bytes.extend_from_slice(&chunk[..n]);
                if req_bytes.windows(2).any(|w| w == b"]]") {
                    break;
                }
            }
            let req = String::from_utf8_lossy(&req_bytes);
            assert!(req.starts_with("POST /pipeline HTTP/1.1"));
            assert!(req.contains("Authorization: Bearer up-secret"));
            assert!(req.contains("\"HINCRBY\""));
            let body = r#"[{"result":120},{"result":350}]"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            let _ = sock.write_all(resp.as_bytes()).await;
        });

        let entry = EtpEntryConfig {
            id: "up-meter-test".into(),
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
        let mut resp_buf = Vec::new();
        let out = flush_deltas(
            &etp,
            "velda:metering:consumed",
            &[(101, 20), (202, 50)],
            &mut resp_buf,
        )
        .await
        .unwrap();
        assert_eq!(out, vec![120, 350]);
        srv.abort();
    }
}
