//! Authelia ETP Keep-Alive HTTP Client (`providers::authelia::client`).
//!
//! ```text
//!   AutheliaEtp::acquire() ──► StreamLease (Keep-Alive; 1-shot reconnect() on stale socket)
//!         ├─► POST {path | /api/oidc/introspection}
//!         ├─► Auth: Basic | Post | ETP bearer_token + custom_headers
//!         └─► Read Content-Length / Chunked / EOF (<= 64 KiB) -> `(path, status, body_bytes)`
//! ```

use std::fmt::Write as _;
use std::time::Duration;

use base64::prelude::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use velda_etp::authelia::AutheliaEtp;

use super::super::{AuthMethod, IntrospectionCallParams};
use crate::error::PluginError;

const DEFAULT_PATH: &str = "/api/oidc/introspection";
const MAX_RESP_BYTES: usize = 64 * 1024;

/// Executes an HTTP/1.1 RFC 7662 POST request against [`AutheliaEtp`] returning raw body bytes.
pub async fn request_raw(
    authelia: &AutheliaEtp,
    params: &IntrospectionCallParams,
    token: &str,
) -> Result<(String, u16, Vec<u8>), PluginError> {
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
        .or(authelia.config.client_id.as_deref());
    let eff_csec = params
        .client_secret
        .as_deref()
        .or(authelia.config.client_secret.as_deref());

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

    let mut req = format!(
        "POST {path} HTTP/1.1\r\nHost: {}\r\nUser-Agent: velda-edge/oauth2-introspection\r\nAccept: application/json\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\nConnection: keep-alive\r\n",
        authelia.config.host_header,
        body.len()
    );
    match params.auth_method {
        AuthMethod::Basic => {
            if let (Some(cid), Some(csec)) = (eff_cid, eff_csec) {
                let b64 = BASE64_STANDARD.encode(format!("{cid}:{csec}").as_bytes());
                let _ = write!(req, "Authorization: Basic {b64}\r\n");
            }
        }
        AuthMethod::Post | AuthMethod::None => {
            if let Some(tok) = authelia.config.bearer_token.as_deref() {
                let _ = write!(req, "Authorization: Bearer {tok}\r\n");
            }
        }
    }
    for (k, v) in &authelia.config.custom_headers {
        let _ = write!(req, "{k}: {v}\r\n");
    }
    req.push_str("\r\n");
    req.push_str(&body);

    let mut lease = authelia
        .acquire()
        .await
        .map_err(|e| PluginError::Http(format!("Authelia '{}': {e}", authelia.config.id)))?;

    for attempt in 0..2 {
        match send_and_read(
            &mut *lease,
            req.as_bytes(),
            params.request_timeout,
            &authelia.config.id,
        )
        .await
        {
            Ok((status, payload, keep_alive)) => {
                if !keep_alive {
                    lease.recreate_in_background();
                }
                return Ok((path, status, payload));
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
                PluginError::Http(format!("Authelia '{label}': write timeout")),
                true,
            )
        })?
        .map_err(|e| {
            (
                PluginError::Http(format!("Authelia '{label}': write failed: {e}")),
                true,
            )
        })?;

    tokio::time::timeout(timeout, stream.flush())
        .await
        .map_err(|_| {
            (
                PluginError::Http(format!("Authelia '{label}': flush timeout")),
                true,
            )
        })?
        .map_err(|e| {
            (
                PluginError::Http(format!("Authelia '{label}': flush failed: {e}")),
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
                    PluginError::Http(format!("Authelia '{label}': read timeout")),
                    !headers_parsed && raw.is_empty(),
                )
            })?
            .map_err(|e| {
                (
                    PluginError::Http(format!("Authelia '{label}': read failed: {e}")),
                    !headers_parsed && raw.is_empty(),
                )
            })?;

        if n == 0 {
            if !headers_parsed {
                return Err((
                    PluginError::Http(format!("Authelia '{label}': EOF before headers")),
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
                PluginError::Http(format!("Authelia '{label}': response exceeded 64 KiB")),
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
                                        "Authelia '{label}': invalid Content-Length"
                                    )),
                                    false,
                                )
                            })?;
                            if cl > MAX_RESP_BYTES {
                                return Err((
                                    PluginError::Http(format!(
                                        "Authelia '{label}': Content-Length > 64 KiB"
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
                        PluginError::Http(format!("Authelia '{label}': HTTP parse error: {e}")),
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
            .ok_or_else(|| PluginError::Http(format!("Authelia '{label}': truncated chunked body")))
    } else if let Some(cl) = content_length {
        if body_slice.len() < cl {
            return Err(PluginError::Http(format!(
                "Authelia '{label}': truncated body ({}/{cl})",
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
            .map_err(|_| PluginError::Http(format!("Authelia '{label}': invalid chunk header")))?;
        let hex = line.split(';').next().unwrap_or("").trim();
        let sz = usize::from_str_radix(hex, 16).map_err(|_| {
            PluginError::Http(format!("Authelia '{label}': invalid hex chunk '{hex}'"))
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
                "Authelia '{label}': malformed chunk frame"
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
