//! S3 / MinIO / R2 HTTP/1.1 `PUT` & SigV4 Wire Exchange (`s3_log_shipper::wire`).
//!
//! ```text
//!   Startup (once per run):
//!     S3WireState::new(etp, &config) ──► pre-built SigV4Signer + reusable scratch buffers
//!
//!   Per-Flush Hot Path:
//!     compress_object() ─► build URI in uri_buf (LUT) ─► SHA-256 ─► SigV4 PUT head (LUT)
//!         │
//!         ▼
//!     etp.acquire().await ──► S3Lease (Slot[idx] in-place)
//!         │
//!         ├─► attempt 0..2: write head + body + flush
//!         ├─► httparse::Response + drain body (chunked / Content-Length / close-delimited)
//!         ├─► Io error (attempt 0) ─► lease.reconnect().await (in-place Slot[idx]) + retry
//!         ├─► >= 500 / Connection: close / unframed / timeout ─► lease.recreate_in_background()
//!         └─► Ok(status)
//!
//!   HTTP/1.1 S3 `PUT` + AWS SigV4 Request Frame Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ PUT {[/bucket][/prefix]/year=YYYY/month=MM/day=DD/hour=HH/velda-T-S.ext}   │
//!   │ Host: {etp.host_header}\r\n                                                │
//!   │ x-amz-date: {YYYYMMDDTHHMMSSZ}\r\n                                         │
//!   │ x-amz-content-sha256: {64-hex}\r\n                                         │
//!   │ {x-amz-security-token: {STS}\r\n}                                          │
//!   │ Authorization: AWS4-HMAC-SHA256 Credential=..., SignedHeaders=..., Sig=... │
//!   │ Content-Length: {fast_write_u64}\r\nConnection: keep-alive\r\n\r\n{body}   │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ring::digest;
use velda_core::lut::decimal::fast_write_u64;
use velda_etp::EtpError;
use velda_etp::s3::S3Etp;

use crate::error::PluginError;

use super::format::compress_object;
use super::sigv4::{SigV4Signer, unix_secs_to_utc_ymdhms, write_hex_lower};
use crate::background::log_exporter::config::{S3Compression, S3TargetConfig};

/// Pre-initialized SigV4 signer, routing metadata, and reusable buffers for an S3 uploader task.
pub struct S3WireState {
    pub etp: Arc<S3Etp>,
    pub signer: SigV4Signer,
    pub bucket: String,
    pub host_header: String,
    pub prefix: String,
    pub force_path_style: bool,
    pub session_token: Option<String>,
    pub compression: S3Compression,
    pub request_timeout: Duration,
    pub seq: u64,
    pub uri_buf: Vec<u8>,
    pub head_buf: Vec<u8>,
    pub compress_buf: Vec<u8>,
    pub resp_buf: Vec<u8>,
}

impl S3WireState {
    /// Constructs wire state from an attached [`S3Etp`] pool and validated [`S3TargetConfig`].
    pub fn new(etp: Arc<S3Etp>, cfg: &S3TargetConfig) -> Self {
        let signer = SigV4Signer::new(
            &etp.config.access_key_id,
            &etp.config.secret_access_key,
            &etp.config.region,
            &etp.config.host_header,
        );
        Self {
            bucket: etp.config.bucket.clone(),
            host_header: etp.config.host_header.clone(),
            prefix: cfg.prefix.trim_matches('/').to_string(),
            force_path_style: etp.config.force_path_style,
            session_token: etp.config.session_token.clone(),
            compression: cfg.compression,
            request_timeout: Duration::from_millis(cfg.request_timeout_ms),
            etp,
            signer,
            seq: 0,
            uri_buf: Vec::with_capacity(128),
            head_buf: Vec::with_capacity(1024),
            compress_buf: Vec::with_capacity(128 * 1024),
            resp_buf: Vec::with_capacity(2048),
        }
    }
}

/// Compresses `ndjson_payload`, builds the UTC partitioned object key, signs with SigV4,
/// acquires an in-place [`S3Lease`](velda_etp::s3::S3Lease), executes the `PUT` exchange,
/// and drains the HTTP/1.1 response cleanly for keep-alive reuse.
pub async fn upload_batch(
    state: &mut S3WireState,
    ndjson_payload: &[u8],
) -> Result<u16, PluginError> {
    let unix_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let (y, m, d, hh, _, _) = unix_secs_to_utc_ymdhms(unix_secs);
    state.seq = state.seq.wrapping_add(1);
    let seq = state.seq;

    let (wire_len, ext) = {
        let (compressed, ext) =
            compress_object(ndjson_payload, state.compression, &mut state.compress_buf)
                .map_err(|e| PluginError::Http(format!("S3 compression error: {e}")))?;
        (compressed.len(), ext)
    };
    let wire_bytes: &[u8] = match state.compression {
        S3Compression::None => ndjson_payload,
        S3Compression::Zstd | S3Compression::Gzip => state.compress_buf.as_slice(),
    };

    state.uri_buf.clear();
    if state.force_path_style && !state.bucket.is_empty() {
        state.uri_buf.push(b'/');
        state.uri_buf.extend_from_slice(state.bucket.as_bytes());
    }
    if !state.prefix.is_empty() {
        state.uri_buf.push(b'/');
        state.uri_buf.extend_from_slice(state.prefix.as_bytes());
    }
    state.uri_buf.extend_from_slice(b"/year=");
    state.uri_buf.extend_from_slice(&[
        b'0' + ((y / 1000) % 10) as u8,
        b'0' + ((y / 100) % 10) as u8,
        b'0' + ((y / 10) % 10) as u8,
        b'0' + (y % 10) as u8,
    ]);
    state.uri_buf.extend_from_slice(b"/month=");
    state
        .uri_buf
        .extend_from_slice(&[b'0' + ((m / 10) % 10) as u8, b'0' + (m % 10) as u8]);
    state.uri_buf.extend_from_slice(b"/day=");
    state
        .uri_buf
        .extend_from_slice(&[b'0' + ((d / 10) % 10) as u8, b'0' + (d % 10) as u8]);
    state.uri_buf.extend_from_slice(b"/hour=");
    state
        .uri_buf
        .extend_from_slice(&[b'0' + ((hh / 10) % 10) as u8, b'0' + (hh % 10) as u8]);
    state.uri_buf.extend_from_slice(b"/velda-");
    fast_write_u64(&mut state.uri_buf, unix_secs);
    state.uri_buf.push(b'-');
    fast_write_u64(&mut state.uri_buf, seq);
    state.uri_buf.push(b'.');
    state.uri_buf.extend_from_slice(ext.as_bytes());

    let canonical_uri = std::str::from_utf8(&state.uri_buf)
        .map_err(|e| PluginError::Config(format!("Invalid S3 URI UTF-8: {e}")))?;

    let mut payload_hex = [0u8; 64];
    let payload_digest = digest::digest(&digest::SHA256, wire_bytes);
    write_hex_lower(payload_digest.as_ref(), &mut payload_hex);

    state.signer.write_signed_put_headers(
        canonical_uri,
        wire_len,
        &payload_hex,
        unix_secs,
        state.session_token.as_deref(),
        &mut state.head_buf,
    );

    let mut lease = state
        .etp
        .acquire()
        .await
        .map_err(|e| PluginError::Http(format!("S3 ETP acquire error: {e}")))?;
    let timeout_ms = u64::try_from(state.request_timeout.as_millis()).unwrap_or(u64::MAX);

    for attempt in 0..2 {
        let exchange: Result<Result<(u16, bool), EtpError>, _> =
            tokio::time::timeout(state.request_timeout, async {
                lease.write_all(&state.head_buf).await?;
                lease.write_all(wire_bytes).await?;
                lease.flush().await?;

                state.resp_buf.clear();
                let mut tmp = [0u8; 4096];
                let (status, hdr_len, content_length, chunked, connection_close) = loop {
                    let n = lease.read(&mut tmp).await?;
                    if n == 0 {
                        return Err(Error::new(
                            ErrorKind::UnexpectedEof,
                            "S3 closed before HTTP headers",
                        )
                        .into());
                    }
                    if let Some(chunk) = tmp.get(..n) {
                        state.resp_buf.extend_from_slice(chunk);
                    }
                    if state.resp_buf.len() > 32 * 1024 {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "S3 HTTP headers exceeded 32 KiB",
                        )
                        .into());
                    }

                    let mut headers = [httparse::EMPTY_HEADER; 64];
                    let mut resp = httparse::Response::new(&mut headers);
                    match resp.parse(&state.resp_buf) {
                        Ok(httparse::Status::Complete(hdr_len)) => {
                            let code = resp.code.ok_or_else(|| {
                                Error::new(ErrorKind::InvalidData, "Missing S3 HTTP status")
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
                                format!("Invalid S3 HTTP response: {err}"),
                            )
                            .into());
                        }
                    }
                };

                let mut close_after = connection_close;
                if chunked {
                    let mut pos = hdr_len;
                    loop {
                        let crlf_rel = loop {
                            if let Some(rel) = state
                                .resp_buf
                                .get(pos..)
                                .and_then(|t| t.windows(2).position(|w| w == b"\r\n"))
                            {
                                break rel;
                            }
                            if state.resp_buf.len().saturating_sub(pos) > 8192 {
                                return Err(Error::new(
                                    ErrorKind::InvalidData,
                                    "S3 chunk size line > 8 KiB",
                                )
                                .into());
                            }
                            let n = lease.read(&mut tmp).await?;
                            if n == 0 {
                                return Err(Error::new(
                                    ErrorKind::UnexpectedEof,
                                    "EOF reading S3 chunked body",
                                )
                                .into());
                            }
                            if let Some(chunk) = tmp.get(..n) {
                                state.resp_buf.extend_from_slice(chunk);
                            }
                        };

                        let size_slice = pos
                            .checked_add(crlf_rel)
                            .and_then(|end| state.resp_buf.get(pos..end))
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
                                    if let Some(rel) = state
                                        .resp_buf
                                        .get(pos..)
                                        .and_then(|t| t.windows(2).position(|w| w == b"\r\n"))
                                    {
                                        break rel;
                                    }
                                    if state.resp_buf.len().saturating_sub(pos) > 8192 {
                                        return Err(Error::new(
                                            ErrorKind::InvalidData,
                                            "S3 chunk trailer > 8 KiB",
                                        )
                                        .into());
                                    }
                                    let n = lease.read(&mut tmp).await?;
                                    if n == 0 {
                                        return Err(Error::new(
                                            ErrorKind::UnexpectedEof,
                                            "EOF reading S3 chunk trailer",
                                        )
                                        .into());
                                    }
                                    if let Some(chunk) = tmp.get(..n) {
                                        state.resp_buf.extend_from_slice(chunk);
                                    }
                                };
                                pos += trailer_rel + 2;
                                if trailer_rel == 0 {
                                    break;
                                }
                            }
                            break;
                        }

                        let need = chunk_size.checked_add(2).ok_or_else(|| {
                            Error::new(ErrorKind::InvalidData, "S3 chunk size overflow")
                        })?;
                        let avail = state.resp_buf.len().saturating_sub(pos);
                        if avail >= need {
                            pos += need;
                            if pos >= state.resp_buf.len() {
                                state.resp_buf.clear();
                                pos = 0;
                            } else if pos > 4096 {
                                let tail_len = state.resp_buf.len().saturating_sub(pos);
                                for i in 0..tail_len {
                                    if let Some(&b) = state.resp_buf.get(pos + i)
                                        && let Some(dst) = state.resp_buf.get_mut(i)
                                    {
                                        *dst = b;
                                    }
                                }
                                state.resp_buf.truncate(tail_len);
                                pos = 0;
                            }
                        } else {
                            let mut remaining = need - avail;
                            state.resp_buf.clear();
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
                } else if let Some(len) = content_length {
                    if len > 0 {
                        let already = state.resp_buf.len().saturating_sub(hdr_len);
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
                } else if status != 204 {
                    close_after = true;
                }

                Ok((status, close_after))
            })
            .await;

        match exchange {
            Ok(Ok((status, close_after))) => {
                if close_after || status >= 500 {
                    lease.recreate_in_background();
                }
                return Ok(status);
            }
            Ok(Err(EtpError::Io(ref err)))
                if attempt == 0 && err.kind() != ErrorKind::InvalidData =>
            {
                if let Err(err) = lease.reconnect().await {
                    lease.recreate_in_background();
                    return Err(PluginError::Http(format!("S3 reconnect failed: {err}")));
                }
            }
            Ok(Err(err)) => {
                lease.recreate_in_background();
                return Err(PluginError::Http(format!("S3 PUT error: {err}")));
            }
            Err(_) => {
                lease.recreate_in_background();
                return Err(PluginError::Http(format!(
                    "S3 PUT timed out after {timeout_ms} ms"
                )));
            }
        }
    }

    lease.recreate_in_background();
    Err(PluginError::Http(format!(
        "S3 PUT failed after reconnect retry on ETP '{}'",
        state.etp.config.id
    )))
}
