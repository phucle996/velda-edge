//! Microsoft Entra ID ETP Keep-Alive HTTP Client (`providers::entra_id::client`).
//!
//! ```text
//!   EntraIdEtp::acquire() ──► StreamLease (Keep-Alive; 1-shot reconnect() on stale socket)
//!         ├─► Default: GET {path | /oidc/userinfo} w/ Authorization: Bearer <token>
//!         └─► Custom `path` + creds: POST {path} (RFC 7662 form-encoded)
//!               └─► Returns `(status, payload, used_rfc7662_post)`
//! ```

use std::fmt::Write as _;
use std::time::Duration;

use base64::prelude::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use velda_etp::entra_id::EntraIdEtp;

use super::super::{AuthMethod, IntrospectionCallParams};
use crate::error::PluginError;

const DEFAULT_PATH: &str = "/oidc/userinfo";
const MAX_RESP_BYTES: usize = 64 * 1024;

/// Executes an Entra ID `/oidc/userinfo` GET or custom RFC 7662 POST request against [`EntraIdEtp`].
pub async fn request_raw(
    entra: &EntraIdEtp,
    params: &IntrospectionCallParams,
    token: &str,
) -> Result<(u16, Vec<u8>, bool), PluginError> {
    let raw_path = params
        .path_override
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_PATH);
    let path = if raw_path.starts_with('/') {
        raw_path.to_string()
    } else {
        format!("/{raw_path}")
    };

    let eff_cid = params
        .client_id
        .as_deref()
        .or(entra.config.client_id.as_deref());
    let eff_csec = params
        .client_secret
        .as_deref()
        .or(entra.config.client_secret.as_deref());

    let use_rfc7662_post = params.path_override.is_some()
        && params.auth_method != AuthMethod::None
        && eff_cid.is_some()
        && eff_csec.is_some();

    let req = if use_rfc7662_post {
        let mut body = format!("token={}", url_encode(token));
        if let Some(hint) = params.token_type_hint.as_deref() {
            body.push_str("&token_type_hint=");
            body.push_str(hint);
        }
        if params.auth_method == AuthMethod::Post
            && let (Some(cid), Some(csec)) = (eff_cid, eff_csec)
        {
            body.push_str("&client_id=");
            body.push_str(&url_encode(cid));
            body.push_str("&client_secret=");
            body.push_str(&url_encode(csec));
        }

        let mut r = format!(
            "POST {path} HTTP/1.1\r\nHost: {}\r\nUser-Agent: velda-edge/oauth2-introspection\r\nAccept: application/json\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\nConnection: keep-alive\r\n",
            entra.config.host_header,
            body.len()
        );
        if params.auth_method == AuthMethod::Basic
            && let (Some(cid), Some(csec)) = (eff_cid, eff_csec)
        {
            let b64 = BASE64_STANDARD.encode(format!("{cid}:{csec}").as_bytes());
            let _ = write!(r, "Authorization: Basic {b64}\r\n");
        }
        for (k, v) in &entra.config.custom_headers {
            let _ = write!(r, "{k}: {v}\r\n");
        }
        r.push_str("\r\n");
        r.push_str(&body);
        r
    } else {
        let mut r = format!(
            "GET {path} HTTP/1.1\r\nHost: {}\r\nUser-Agent: velda-edge/oauth2-introspection\r\nAccept: application/json\r\nAuthorization: Bearer {token}\r\nConnection: keep-alive\r\n",
            entra.config.host_header
        );
        for (k, v) in &entra.config.custom_headers {
            let _ = write!(r, "{k}: {v}\r\n");
        }
        r.push_str("\r\n");
        r
    };

    let mut lease = entra
        .acquire()
        .await
        .map_err(|e| PluginError::Http(format!("Entra ID '{}': {e}", entra.config.id)))?;

    for attempt in 0..2 {
        match send_and_read(
            &mut *lease,
            req.as_bytes(),
            params.request_timeout,
            &entra.config.id,
        )
        .await
        {
            Ok((status, payload, keep_alive)) => {
                if !keep_alive {
                    lease.recreate_in_background();
                }
                return Ok((status, payload, use_rfc7662_post));
            }
            Err((err, true)) if attempt == 0 => {
                if lease.reconnect().await.is_err() {
                    return Err(err);
                }
            }
            Err((err, _)) => {
                lease.recreate_in_background();
                return Err(err);
            }
        }
    }

    unreachable!()
}

async fn send_and_read<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    stream: &mut S,
    req: &[u8],
    timeout: Duration,
    label: &str,
) -> Result<(u16, Vec<u8>, bool), (PluginError, bool)> {
    tokio::time::timeout(timeout, stream.write_all(req))
        .await
        .map_err(|_| {
            (
                PluginError::Http(format!("Entra ID '{label}': write timeout")),
                true,
            )
        })?
        .map_err(|e| {
            (
                PluginError::Http(format!("Entra ID '{label}': write failed: {e}")),
                true,
            )
        })?;

    tokio::time::timeout(timeout, stream.flush())
        .await
        .map_err(|_| {
            (
                PluginError::Http(format!("Entra ID '{label}': flush timeout")),
                true,
            )
        })?
        .map_err(|e| {
            (
                PluginError::Http(format!("Entra ID '{label}': flush failed: {e}")),
                true,
            )
        })?;

    let mut raw = Vec::with_capacity(4096);
    let mut buf = [0u8; 4096];
    let mut headers_parsed = false;
    let mut status = 0u16;
    let mut body_start = 0usize;
    let mut content_length: Option<usize> = None;
    let mut is_chunked = false;
    let mut connection_close = false;

    loop {
        let n = tokio::time::timeout(timeout, stream.read(&mut buf))
            .await
            .map_err(|_| {
                (
                    PluginError::Http(format!("Entra ID '{label}': read timeout")),
                    !headers_parsed && raw.is_empty(),
                )
            })?
            .map_err(|e| {
                (
                    PluginError::Http(format!("Entra ID '{label}': read failed: {e}")),
                    !headers_parsed && raw.is_empty(),
                )
            })?;

        if n == 0 {
            if !headers_parsed {
                return Err((
                    PluginError::Http(format!("Entra ID '{label}': EOF before headers")),
                    raw.is_empty(),
                ));
            }
            let body = decode_on_eof(&raw[body_start..], is_chunked, content_length, label)
                .map_err(|e| (e, false))?;
            return Ok((status, body, false));
        }

        raw.extend_from_slice(&buf[..n]);
        if raw.len() > MAX_RESP_BYTES {
            return Err((
                PluginError::Http(format!("Entra ID '{label}': response exceeded 64 KiB")),
                false,
            ));
        }

        if !headers_parsed {
            let mut headers = [httparse::EMPTY_HEADER; 64];
            let mut parsed = httparse::Response::new(&mut headers);
            match parsed.parse(&raw) {
                Ok(httparse::Status::Complete(off)) => {
                    headers_parsed = true;
                    status = parsed.code.unwrap_or(500);
                    body_start = off;
                    for h in parsed.headers.iter() {
                        if h.name.eq_ignore_ascii_case("content-length")
                            && let Ok(s) = std::str::from_utf8(h.value)
                        {
                            let cl = s.trim().parse::<usize>().map_err(|_| {
                                (
                                    PluginError::Http(format!(
                                        "Entra ID '{label}': invalid Content-Length"
                                    )),
                                    false,
                                )
                            })?;
                            if cl > MAX_RESP_BYTES {
                                return Err((
                                    PluginError::Http(format!(
                                        "Entra ID '{label}': Content-Length > 64 KiB"
                                    )),
                                    false,
                                ));
                            }
                            content_length = Some(cl);
                        } else if h.name.eq_ignore_ascii_case("transfer-encoding")
                            && let Ok(s) = std::str::from_utf8(h.value)
                            && s.split(',')
                                .any(|t| t.trim().eq_ignore_ascii_case("chunked"))
                        {
                            is_chunked = true;
                        } else if h.name.eq_ignore_ascii_case("connection")
                            && let Ok(s) = std::str::from_utf8(h.value)
                            && s.trim().eq_ignore_ascii_case("close")
                        {
                            connection_close = true;
                        }
                    }
                }
                Ok(httparse::Status::Partial) => continue,
                Err(e) => {
                    return Err((
                        PluginError::Http(format!("Entra ID '{label}': HTTP parse error: {e}")),
                        false,
                    ));
                }
            }
        }

        if headers_parsed {
            let body_slice = &raw[body_start..];
            if is_chunked {
                if let Some(decoded) =
                    try_decode_chunked(body_slice, label).map_err(|e| (e, false))?
                {
                    return Ok((status, decoded, !connection_close));
                }
            } else if let Some(cl) = content_length
                && body_slice.len() >= cl
            {
                return Ok((status, body_slice[..cl].to_vec(), !connection_close));
            }
        }
    }
}

fn decode_on_eof(
    body_slice: &[u8],
    is_chunked: bool,
    content_length: Option<usize>,
    label: &str,
) -> Result<Vec<u8>, PluginError> {
    if is_chunked {
        try_decode_chunked(body_slice, label)?
            .ok_or_else(|| PluginError::Http(format!("Entra ID '{label}': truncated chunked body")))
    } else if let Some(cl) = content_length {
        if body_slice.len() < cl {
            return Err(PluginError::Http(format!(
                "Entra ID '{label}': truncated body ({}/{cl})",
                body_slice.len()
            )));
        }
        Ok(body_slice[..cl].to_vec())
    } else {
        Ok(body_slice.to_vec())
    }
}

fn try_decode_chunked(mut input: &[u8], label: &str) -> Result<Option<Vec<u8>>, PluginError> {
    let mut out = Vec::with_capacity(input.len());
    loop {
        let Some(crlf) = input.windows(2).position(|w| w == b"\r\n") else {
            return Ok(None);
        };
        let line = std::str::from_utf8(&input[..crlf])
            .map_err(|_| PluginError::Http(format!("Entra ID '{label}': invalid chunk header")))?;
        let hex = line.split(';').next().unwrap_or("").trim();
        let sz = usize::from_str_radix(hex, 16).map_err(|_| {
            PluginError::Http(format!("Entra ID '{label}': invalid hex chunk '{hex}'"))
        })?;
        input = &input[crlf + 2..];
        if sz == 0 {
            if input.windows(2).any(|w| w == b"\r\n") || input.is_empty() {
                return Ok(Some(out));
            }
            return Ok(None);
        }
        if input.len() < sz + 2 {
            return Ok(None);
        }
        if &input[sz..sz + 2] != b"\r\n" {
            return Err(PluginError::Http(format!(
                "Entra ID '{label}': malformed chunk frame"
            )));
        }
        out.extend_from_slice(&input[..sz]);
        input = &input[sz + 2..];
    }
}

fn url_encode(input: &str) -> String {
    let mut out = String::with_capacity(input.len() * 2);
    for b in input.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}
