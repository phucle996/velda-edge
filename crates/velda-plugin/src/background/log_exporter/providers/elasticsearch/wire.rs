//! Elasticsearch / OpenSearch HTTP/1.1 `_bulk` Wire Exchange (`elasticsearch_log_shipper::wire`).
//!
//! ```text
//!   Startup:
//!     bake_bulk_prefix(&etp.config, &plugin_cfg) ──► immutable POST _bulk HTTP/1.1 header prefix
//!
//!   Per-Flush Hot Path:
//!     head_buf <- prebaked_prefix + fast_write_u64(payload.len()) + "\r\n\r\n"
//!         │
//!         ▼
//!     etp.acquire().await ──► ElasticsearchLease (Slot[idx] in-place)
//!         │
//!         ├─► attempt 0..2: write head + body + flush
//!         ├─► httparse::Response + drain body (streaming "\"errors\":true" scan)
//!         ├─► Io error (attempt 0) ─► lease.reconnect().await (in-place Slot[idx]) + retry
//!         ├─► >= 500 / Connection: close / unframed / timeout ─► lease.recreate_in_background()
//!         └─► Ok((status, has_errors))
//!
//!   HTTP/1.1 `_bulk` Request Frame Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ POST {base_path}/_bulk{?pipeline=...} HTTP/1.1\r\n                         │
//!   │ Host: {host_header}\r\nContent-Type: application/x-ndjson\r\n              │
//!   │ {Content-Encoding: gzip\r\n}{Authorization: ApiKey|Basic|Bearer\r\n}       │
//!   │ Connection: keep-alive\r\nContent-Length: {N}\r\n\r\n{NDJSON | Gzip body}  │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use velda_etp::EtpError;
use velda_etp::elasticsearch::{ElasticsearchEtp, ElasticsearchEtpConfig};

use crate::background::log_exporter::config::{ElasticsearchTargetConfig, EsCompression};

/// Pre-bakes the immutable HTTP/1.1 `_bulk` header prefix once from ETP and plugin config.
pub fn bake_bulk_prefix(
    etp_cfg: &ElasticsearchEtpConfig,
    plugin_cfg: &ElasticsearchTargetConfig,
) -> Vec<u8> {
    let bulk_path = match &plugin_cfg.pipeline {
        Some(p) => format!("{}/_bulk?pipeline={p}", etp_cfg.base_path),
        None => format!("{}/_bulk", etp_cfg.base_path),
    };
    let encoding_hdr = match plugin_cfg.compression {
        EsCompression::Gzip => "Content-Encoding: gzip\r\n",
        EsCompression::None => "",
    };
    let auth_header = if let Some(key) = &etp_cfg.api_key {
        format!("Authorization: ApiKey {key}\r\n")
    } else if let Some(basic) = &etp_cfg.basic_auth {
        if basic.contains(':') {
            let encoded = base64::engine::general_purpose::STANDARD.encode(basic);
            format!("Authorization: Basic {encoded}\r\n")
        } else {
            format!("Authorization: Basic {basic}\r\n")
        }
    } else if let Some(token) = &etp_cfg.bearer_token {
        format!("Authorization: Bearer {token}\r\n")
    } else {
        String::new()
    };

    format!(
        "POST {bulk_path} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/x-ndjson\r\n\
         {encoding_hdr}{auth_header}Connection: keep-alive\r\nContent-Length: ",
        etp_cfg.host_header,
    )
    .into_bytes()
}

/// Stamps `Content-Length`, acquires an in-place `ElasticsearchLease`, writes and drains the
/// HTTP/1.1 `_bulk` exchange while scanning for `"\"errors\":true"`, and retries once on I/O EOF.
pub async fn push_bulk_payload(
    etp: &Arc<ElasticsearchEtp>,
    prebaked_prefix: &[u8],
    head_buf: &mut Vec<u8>,
    wire_payload: &[u8],
    resp_buf: &mut Vec<u8>,
    request_timeout: Duration,
) -> Result<(u16, bool), EtpError> {
    head_buf.clear();
    head_buf.extend_from_slice(prebaked_prefix);
    velda_core::lut::decimal::fast_write_u64(head_buf, wire_payload.len() as u64);
    head_buf.extend_from_slice(b"\r\n\r\n");

    let mut lease = etp.acquire().await?;
    let timeout_ms = u64::try_from(request_timeout.as_millis()).unwrap_or(u64::MAX);
    let mut outcome = Err(EtpError::Timeout {
        id: etp.config.id.clone(),
        timeout_ms,
    });

    for attempt in 0..2 {
        let exchange: Result<Result<(u16, bool, bool), EtpError>, _> =
            tokio::time::timeout(request_timeout, async {
                lease.write_all(head_buf).await?;
                lease.write_all(wire_payload).await?;
                lease.flush().await?;

                resp_buf.clear();
                let mut tmp = [0u8; 4096];
                let (status, hdr_len, content_length, chunked, connection_close) = loop {
                    let n = lease.read(&mut tmp).await?;
                    if n == 0 {
                        return Err(Error::new(
                            ErrorKind::UnexpectedEof,
                            "Elasticsearch closed before HTTP headers",
                        )
                        .into());
                    }
                    if let Some(chunk) = tmp.get(..n) {
                        resp_buf.extend_from_slice(chunk);
                    }
                    if resp_buf.len() > 32 * 1024 {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Elasticsearch HTTP headers > 32 KiB",
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

                let mut err_tail = [0u8; 12];
                let mut err_tail_len = 0usize;
                let mut has_errors = false;
                let mut scan_bulk_errors = |chunk: &[u8]| {
                    if has_errors || chunk.is_empty() {
                        return;
                    }
                    const NEEDLE: &[u8; 13] = b"\"errors\":true";
                    let mut seam = [0u8; 24];
                    let tail_keep = err_tail_len.min(12);
                    let head_take = chunk.len().min(12);
                    if let (Some(dst), Some(src)) =
                        (seam.get_mut(..tail_keep), err_tail.get(..tail_keep))
                    {
                        dst.copy_from_slice(src);
                    }
                    if let (Some(dst), Some(src)) = (
                        seam.get_mut(tail_keep..tail_keep + head_take),
                        chunk.get(..head_take),
                    ) {
                        dst.copy_from_slice(src);
                    }
                    if let Some(seam_slice) = seam.get(..tail_keep + head_take)
                        && seam_slice.windows(13).any(|w| w == NEEDLE)
                    {
                        has_errors = true;
                        return;
                    }
                    if chunk.len() >= 13 {
                        if chunk.windows(13).any(|w| w == NEEDLE) {
                            has_errors = true;
                            return;
                        }
                        if let (Some(dst), Some(src)) =
                            (err_tail.get_mut(..12), chunk.get(chunk.len() - 12..))
                        {
                            dst.copy_from_slice(src);
                            err_tail_len = 12;
                        }
                    } else if let Some(seam_slice) = seam.get(..tail_keep + head_take) {
                        let keep = seam_slice.len().min(12);
                        if let (Some(dst), Some(src)) = (
                            err_tail.get_mut(..keep),
                            seam_slice.get(seam_slice.len() - keep..),
                        ) {
                            dst.copy_from_slice(src);
                            err_tail_len = keep;
                        }
                    }
                };

                let mut close_after = connection_close;
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
                            if let Some(s) = pos
                                .checked_add(chunk_size)
                                .and_then(|e| resp_buf.get(pos..e))
                            {
                                scan_bulk_errors(s);
                            }
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
                            let scan_in_buf = avail.min(chunk_size);
                            if let Some(s) = pos
                                .checked_add(scan_in_buf)
                                .and_then(|e| resp_buf.get(pos..e))
                            {
                                scan_bulk_errors(s);
                            }
                            let mut rem_body = chunk_size.saturating_sub(avail);
                            let crlf_rem = need.saturating_sub(avail + rem_body);
                            resp_buf.clear();
                            pos = 0;

                            let mut sink = [0u8; 8192];
                            while rem_body > 0 {
                                let to_read = rem_body.min(sink.len());
                                if let Some(buf) = sink.get_mut(..to_read) {
                                    lease.read_exact(buf).await?;
                                    scan_bulk_errors(buf);
                                }
                                rem_body -= to_read;
                            }
                            if crlf_rem > 0
                                && let Some(crlf_buf) = sink.get_mut(..crlf_rem)
                            {
                                lease.read_exact(crlf_buf).await?;
                            }
                        }
                    }
                } else if let Some(len) = content_length {
                    if len > 0 {
                        let already = resp_buf.len().saturating_sub(hdr_len);
                        if let Some(s) = hdr_len
                            .checked_add(already.min(len))
                            .and_then(|e| resp_buf.get(hdr_len..e))
                        {
                            scan_bulk_errors(s);
                        }
                        let mut remaining = len.saturating_sub(already);
                        let mut sink = [0u8; 8192];
                        while remaining > 0 {
                            let to_read = remaining.min(sink.len());
                            if let Some(buf) = sink.get_mut(..to_read) {
                                lease.read_exact(buf).await?;
                                scan_bulk_errors(buf);
                            }
                            remaining -= to_read;
                        }
                    }
                } else {
                    if let Some(body_slice) = resp_buf.get(hdr_len..) {
                        scan_bulk_errors(body_slice);
                    }
                    close_after = true;
                }

                Ok((status, has_errors, close_after))
            })
            .await;

        match exchange {
            Ok(Ok((status, has_errors, close_after))) => {
                if close_after || status >= 500 {
                    lease.recreate_in_background();
                }
                outcome = Ok((status, has_errors));
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
