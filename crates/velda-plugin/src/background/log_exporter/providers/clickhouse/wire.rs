//! ClickHouse HTTP/1.1 Wire Framing & Keep-Alive Exchange (`clickhouse_log_shipper::wire`).
//!
//! ```text
//!   Startup:
//!     bake_insert_prefix(&etp.config, db, format) ──► immutable INSERT HTTP/1.1 header prefix
//!     bake_ddl_request(&etp.config, db)           ──► complete CREATE TABLE HTTP/1.1 request
//!
//!   Per-Flush Hot Path:
//!     head_buf <- prebaked_prefix + fast_write_u64(payload.len()) + "\r\n\r\n"
//!         │
//!         ▼
//!     etp.acquire().await ──► ClickHouseLease (Slot[idx] in-place)
//!         │
//!         ├─► attempt 0..2: write head + body + flush -> httparse::Response + drain body
//!         ├─► Io error (attempt 0) ─► lease.reconnect().await (in-place Slot[idx]) + retry
//!         ├─► >= 500 / Connection: close / timeout ─► lease.recreate_in_background()
//!         └─► Ok(status)
//!
//!   HTTP/1.1 Request Frame Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ POST {base_path}/?query=INSERT... HTTP/1.1\r\nHost: {host_header}\r\n      │
//!   │ Content-Type: {content_type}\r\nConnection: keep-alive\r\n                 │
//!   │ {X-ClickHouse-User\r\n}{X-ClickHouse-Key\r\n}Content-Length: {N}\r\n\r\n   │
//!   │ {RowBinary | JSONEachRow payload bytes}                                    │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::time::Duration;

use velda_etp::EtpError;
use velda_etp::clickhouse::{ClickHouseEtp, ClickHouseEtpConfig};

use super::format::ClickHouseFormat;
use crate::background::log_exporter::config::VELDA_ACCESS_LOGS_TABLE;

fn format_auth_headers(etp_cfg: &ClickHouseEtpConfig) -> String {
    let mut auth = String::new();
    if let Some(user) = &etp_cfg.user {
        auth.push_str("X-ClickHouse-User: ");
        auth.push_str(user);
        auth.push_str("\r\n");
    }
    if let Some(password) = &etp_cfg.password {
        auth.push_str("X-ClickHouse-Key: ");
        auth.push_str(password);
        auth.push_str("\r\n");
    }
    auth
}

/// Pre-bakes the immutable `INSERT INTO ... FORMAT ...` header prefix ending at `Content-Length: `.
pub fn bake_insert_prefix(
    etp_cfg: &ClickHouseEtpConfig,
    database: &str,
    format: ClickHouseFormat,
) -> Vec<u8> {
    let auth = format_auth_headers(etp_cfg);
    format!(
        "POST {}/?query=INSERT%20INTO%20{}.{}%20FORMAT%20{} HTTP/1.1\r\n\
         Host: {}\r\nContent-Type: {}\r\nConnection: keep-alive\r\n{}Content-Length: ",
        etp_cfg.base_path,
        database,
        VELDA_ACCESS_LOGS_TABLE,
        format.as_clickhouse_format_str(),
        etp_cfg.host_header,
        format.content_type(),
        auth,
    )
    .into_bytes()
}

/// Pre-bakes the complete idempotent `CREATE TABLE IF NOT EXISTS` HTTP/1.1 request.
pub fn bake_ddl_request(etp_cfg: &ClickHouseEtpConfig, database: &str) -> Vec<u8> {
    let auth = format_auth_headers(etp_cfg);
    let ddl_body = format!(
        "CREATE TABLE IF NOT EXISTS {database}.{VELDA_ACCESS_LOGS_TABLE} (\
            timestamp DateTime64(9) CODEC(DoubleDelta, ZSTD(1)),\
            request_id UInt64 CODEC(DoubleDelta, ZSTD(1)),\
            duration_ns UInt64 CODEC(T64, ZSTD(1)),\
            peer_ip String CODEC(ZSTD(1)),peer_port UInt16 CODEC(T64, ZSTD(1)),\
            upstream_ip String CODEC(ZSTD(1)),upstream_port UInt16 CODEC(T64, ZSTD(1)),\
            route_id UInt16 CODEC(T64, ZSTD(1)),\
            pipe LowCardinality(String) CODEC(ZSTD(1)),\
            method LowCardinality(String) CODEC(ZSTD(1)),url String CODEC(ZSTD(3)),\
            bytes_rx UInt32 CODEC(T64, ZSTD(1)),bytes_tx UInt32 CODEC(T64, ZSTD(1)),\
            internal_status LowCardinality(String) CODEC(ZSTD(1)),\
            user_agent_family LowCardinality(String) CODEC(ZSTD(1)),\
            user_agent_os LowCardinality(String) CODEC(ZSTD(1))\
        ) ENGINE = MergeTree() PARTITION BY toYYYYMM(timestamp) \
        ORDER BY (route_id, internal_status, timestamp) SETTINGS index_granularity = 8192"
    );
    let mut ddl_request = format!(
        "POST {}/ HTTP/1.1\r\nHost: {}\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: keep-alive\r\n{}Content-Length: ",
        etp_cfg.base_path,
        etp_cfg.host_header,
        auth,
    )
    .into_bytes();
    velda_core::lut::decimal::fast_write_u64(&mut ddl_request, ddl_body.len() as u64);
    ddl_request.extend_from_slice(b"\r\n\r\n");
    ddl_request.extend_from_slice(ddl_body.as_bytes());
    ddl_request
}

/// Executes a pre-baked ClickHouse DDL HTTP/1.1 request over a pooled keep-alive lease.
pub async fn ensure_table(
    etp: &Arc<ClickHouseEtp>,
    ddl_request: &[u8],
    resp_buf: &mut Vec<u8>,
    request_timeout: Duration,
) -> Result<u16, EtpError> {
    send_clickhouse_request(etp, ddl_request, &[], resp_buf, request_timeout).await
}

/// Stamps `Content-Length` onto `head_buf` from `prebaked_prefix` and transmits `payload`.
pub async fn push_batch(
    etp: &Arc<ClickHouseEtp>,
    prebaked_prefix: &[u8],
    head_buf: &mut Vec<u8>,
    payload: &[u8],
    resp_buf: &mut Vec<u8>,
    request_timeout: Duration,
) -> Result<u16, EtpError> {
    head_buf.clear();
    head_buf.extend_from_slice(prebaked_prefix);
    velda_core::lut::decimal::fast_write_u64(head_buf, payload.len() as u64);
    head_buf.extend_from_slice(b"\r\n\r\n");
    send_clickhouse_request(etp, head_buf, payload, resp_buf, request_timeout).await
}

/// Acquires an in-place `ClickHouseLease`, writes `req_head` (+ `req_body`), parses and drains
/// the HTTP/1.1 response, and retries once via `lease.reconnect()` on initial I/O disconnect.
pub async fn send_clickhouse_request(
    etp: &Arc<ClickHouseEtp>,
    req_head: &[u8],
    req_body: &[u8],
    resp_buf: &mut Vec<u8>,
    request_timeout: Duration,
) -> Result<u16, EtpError> {
    let mut lease = etp.acquire().await?;
    let timeout_ms = u64::try_from(request_timeout.as_millis()).unwrap_or(u64::MAX);
    let mut outcome = Err(EtpError::Timeout {
        id: etp.config.id.clone(),
        timeout_ms,
    });

    for attempt in 0..2 {
        let exchange: Result<Result<(u16, bool), EtpError>, _> =
            tokio::time::timeout(request_timeout, async {
                lease.write_all(req_head).await?;
                if !req_body.is_empty() {
                    lease.write_all(req_body).await?;
                }
                lease.flush().await?;

                resp_buf.clear();
                let mut tmp = [0u8; 4096];
                let (status, hdr_len, content_length, chunked, connection_close) = loop {
                    let n = lease.read(&mut tmp).await?;
                    if n == 0 {
                        return Err(Error::new(
                            ErrorKind::UnexpectedEof,
                            "ClickHouse closed before HTTP headers",
                        )
                        .into());
                    }
                    if let Some(chunk) = tmp.get(..n) {
                        resp_buf.extend_from_slice(chunk);
                    }
                    if resp_buf.len() > 32 * 1024 {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "ClickHouse HTTP headers > 32 KiB",
                        )
                        .into());
                    }

                    let mut headers = [httparse::EMPTY_HEADER; 64];
                    let mut resp = httparse::Response::new(&mut headers);
                    match resp.parse(resp_buf) {
                        Ok(httparse::Status::Complete(hdr_len)) => {
                            let code = resp.code.ok_or_else(|| {
                                Error::new(ErrorKind::InvalidData, "Missing HTTP status")
                            })?;
                            let mut cl: Option<usize> = None;
                            let mut is_chunked = false;
                            let mut is_close = false;
                            for h in resp.headers.iter() {
                                if h.name.eq_ignore_ascii_case("content-length") {
                                    cl = std::str::from_utf8(h.value)
                                        .ok()
                                        .and_then(|s| s.trim().parse().ok());
                                } else if h.name.eq_ignore_ascii_case("transfer-encoding")
                                    && std::str::from_utf8(h.value)
                                        .is_ok_and(|s| s.to_ascii_lowercase().contains("chunked"))
                                {
                                    is_chunked = true;
                                } else if h.name.eq_ignore_ascii_case("connection")
                                    && std::str::from_utf8(h.value)
                                        .is_ok_and(|s| s.trim().eq_ignore_ascii_case("close"))
                                {
                                    is_close = true;
                                }
                            }
                            break (code, hdr_len, cl, is_chunked, is_close);
                        }
                        Ok(httparse::Status::Partial) => continue,
                        Err(err) => {
                            return Err(Error::new(
                                ErrorKind::InvalidData,
                                format!("Invalid HTTP response: {err}"),
                            )
                            .into());
                        }
                    }
                };

                if chunked {
                    let mut pos = hdr_len;
                    loop {
                        let crlf_rel = loop {
                            if let Some(rel) = resp_buf
                                .get(pos..)
                                .and_then(|t| t.windows(2).position(|w| w == b"\r\n"))
                            {
                                break rel;
                            }
                            if resp_buf.len().saturating_sub(pos) > 8192 {
                                return Err(Error::new(
                                    ErrorKind::InvalidData,
                                    "Chunk size line > 8 KiB",
                                )
                                .into());
                            }
                            let n = lease.read(&mut tmp).await?;
                            if n == 0 {
                                return Err(Error::new(
                                    ErrorKind::UnexpectedEof,
                                    "EOF reading chunked body",
                                )
                                .into());
                            }
                            if let Some(chunk) = tmp.get(..n) {
                                resp_buf.extend_from_slice(chunk);
                            }
                        };

                        let size_slice = pos
                            .checked_add(crlf_rel)
                            .and_then(|end| resp_buf.get(pos..end))
                            .ok_or_else(|| {
                                Error::new(ErrorKind::InvalidData, "Invalid chunk size range")
                            })?;
                        let size_line = std::str::from_utf8(size_slice).map_err(|_| {
                            Error::new(ErrorKind::InvalidData, "Invalid UTF-8 chunk size")
                        })?;
                        let hex_part = size_line.split(';').next().unwrap_or("").trim();
                        let chunk_size = usize::from_str_radix(hex_part, 16).map_err(|_| {
                            Error::new(
                                ErrorKind::InvalidData,
                                format!("Bad chunk size '{size_line}'"),
                            )
                        })?;
                        pos += crlf_rel + 2;

                        if chunk_size == 0 {
                            loop {
                                let trailer_rel = loop {
                                    if let Some(rel) = resp_buf
                                        .get(pos..)
                                        .and_then(|t| t.windows(2).position(|w| w == b"\r\n"))
                                    {
                                        break rel;
                                    }
                                    if resp_buf.len().saturating_sub(pos) > 8192 {
                                        return Err(Error::new(
                                            ErrorKind::InvalidData,
                                            "Chunk trailer > 8 KiB",
                                        )
                                        .into());
                                    }
                                    let n = lease.read(&mut tmp).await?;
                                    if n == 0 {
                                        return Err(Error::new(
                                            ErrorKind::UnexpectedEof,
                                            "EOF reading chunk trailer",
                                        )
                                        .into());
                                    }
                                    if let Some(chunk) = tmp.get(..n) {
                                        resp_buf.extend_from_slice(chunk);
                                    }
                                };
                                pos += trailer_rel + 2;
                                if trailer_rel == 0 {
                                    break;
                                }
                            }
                            break;
                        }

                        let need = chunk_size + 2;
                        let avail = resp_buf.len().saturating_sub(pos);
                        if avail >= need {
                            pos += need;
                            if pos >= resp_buf.len() {
                                resp_buf.clear();
                                pos = 0;
                            } else if pos > 4096 {
                                let tail_len = resp_buf.len().saturating_sub(pos);
                                for i in 0..tail_len {
                                    if let Some(&b) = resp_buf.get(pos + i)
                                        && let Some(dst) = resp_buf.get_mut(i)
                                    {
                                        *dst = b;
                                    }
                                }
                                resp_buf.truncate(tail_len);
                                pos = 0;
                            }
                        } else {
                            let mut remaining = need - avail;
                            resp_buf.clear();
                            pos = 0;
                            let mut sink = [0u8; 8192];
                            while remaining > 0 {
                                let to_read = remaining.min(sink.len());
                                if let Some(buf) = sink.get_mut(..to_read) {
                                    lease.read_exact(buf).await?;
                                }
                                remaining -= to_read;
                            }
                        }
                    }
                } else if let Some(len) = content_length
                    && len > 0
                {
                    let already = resp_buf.len().saturating_sub(hdr_len);
                    let mut remaining = len.saturating_sub(already);
                    let mut sink = [0u8; 8192];
                    while remaining > 0 {
                        let to_read = remaining.min(sink.len());
                        if let Some(buf) = sink.get_mut(..to_read) {
                            lease.read_exact(buf).await?;
                        }
                        remaining -= to_read;
                    }
                }

                Ok((status, connection_close))
            })
            .await;

        match exchange {
            Ok(Ok((status, connection_close))) => {
                if connection_close || status >= 500 {
                    lease.recreate_in_background();
                }
                outcome = Ok(status);
                break;
            }
            Ok(Err(EtpError::Io(ref err)))
                if attempt == 0 && err.kind() != ErrorKind::InvalidData =>
            {
                if let Err(err) = lease.reconnect().await {
                    lease.recreate_in_background();
                    outcome = Err(err);
                    break;
                }
            }
            Ok(Err(err)) => {
                lease.recreate_in_background();
                outcome = Err(err);
                break;
            }
            Err(_) => {
                lease.recreate_in_background();
                outcome = Err(EtpError::Timeout {
                    id: etp.config.id.clone(),
                    timeout_ms,
                });
                break;
            }
        }
    }

    outcome
}
