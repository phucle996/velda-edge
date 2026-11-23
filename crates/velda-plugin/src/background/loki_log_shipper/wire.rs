//! Grafana Loki HTTP/1.1 & HTTP/2 gRPC Wire Exchange (`loki_log_shipper::wire`).
//!
//! ```text
//!   Startup:
//!     bake_wire_template(&etp.config)
//!         ├─ LokiProtocol::Http ──► HTTP/1.1 POST {base_path}/loki/api/v1/push header prefix
//!         └─ LokiProtocol::Grpc ──► HTTP/2 POST /logproto.Pusher/Push Uri + HeaderMap
//!
//!   Per-Flush Hot Path:
//!     etp.acquire().await ──► LokiLease (Slot[idx] in-place)
//!         ├─ Http(stream)  ──► stamp Content-Length + write + flush -> httparse::Response + drain
//!         └─ Grpc(send_req)──► 5B frame [0x00, len_be32] -> H2 send_request + trailers grpc-status==0
//!               ├─ transport Io/H2 error (attempt 0) ─► lease.reconnect().await + retry once
//!               └─ 5xx / Connection: close / timeout / persistent err ─► lease.recreate_in_background()
//!
//!   Wire Frame Layouts:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ HTTP/1.1: POST {path} HTTP/1.1\r\nHost: {host_header}\r\n                  │
//!   │   Content-Type: application/json\r\n{X-Scope-OrgID\r\n}{Authorization\r\n} │
//!   │   Connection: keep-alive\r\nContent-Length: {N}\r\n\r\n{JSON payload}      │
//!   ├────────────────────────────────────────────────────────────────────────────┤
//!   │ gRPC H2: POST /logproto.Pusher/Push | content-type: application/grpc       │
//!   │   Frame: [ 0x00 (1B) | len_u32_be (4B) | PushRequest protobuf payload ]    │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use bytes::BytesMut;
use http::header::{HeaderMap, HeaderName, HeaderValue};
use velda_etp::EtpError;
use velda_etp::loki::{LokiConn, LokiEtp, LokiEtpConfig, LokiProtocol};

use crate::error::PluginError;

/// Canonical Loki Pusher gRPC RPC path.
pub const LOKI_GRPC_PATH: &str = "/logproto.Pusher/Push";

/// Pre-baked immutable transport header/URI template for a Loki ETP connection.
pub enum LokiWireTemplate {
    Http { prefix: Vec<u8> },
    Grpc { uri: http::Uri, headers: HeaderMap },
}

enum GrpcExchangeErr {
    Transport(String),
    Rejected { http_status_5xx: bool, msg: String },
}

fn encode_auth_header_value(etp_cfg: &LokiEtpConfig) -> Option<String> {
    if let Some(token) = &etp_cfg.bearer_token {
        Some(format!("Bearer {token}"))
    } else if let Some(basic) = &etp_cfg.basic_auth {
        if basic.contains(':') {
            let encoded = base64::engine::general_purpose::STANDARD.encode(basic);
            Some(format!("Basic {encoded}"))
        } else {
            Some(format!("Basic {basic}"))
        }
    } else {
        None
    }
}

/// Pre-bakes HTTP/1.1 header prefix or HTTP/2 URI + HeaderMap once at plugin startup.
pub fn bake_wire_template(etp_cfg: &LokiEtpConfig) -> Result<LokiWireTemplate, PluginError> {
    let auth_val = encode_auth_header_value(etp_cfg);

    match etp_cfg.protocol {
        LokiProtocol::Http => {
            let path = if etp_cfg.base_path.ends_with("/loki/api/v1/push") {
                etp_cfg.base_path.clone()
            } else {
                format!("{}/loki/api/v1/push", etp_cfg.base_path)
            };
            let mut extra = String::new();
            if let Some(tid) = &etp_cfg.tenant_id {
                extra.push_str("X-Scope-OrgID: ");
                extra.push_str(tid);
                extra.push_str("\r\n");
            }
            if let Some(auth) = &auth_val {
                extra.push_str("Authorization: ");
                extra.push_str(auth);
                extra.push_str("\r\n");
            }
            for (k, v) in &etp_cfg.custom_headers {
                extra.push_str(k);
                extra.push_str(": ");
                extra.push_str(v);
                extra.push_str("\r\n");
            }
            let prefix = format!(
                "POST {path} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\n\
                 {extra}Connection: keep-alive\r\nContent-Length: ",
                etp_cfg.host_header,
            )
            .into_bytes();
            Ok(LokiWireTemplate::Http { prefix })
        }
        LokiProtocol::Grpc => {
            let ep_lower = etp_cfg.endpoint.to_ascii_lowercase();
            let is_tls = etp_cfg.tls.is_some()
                || ep_lower.starts_with("grpcs://")
                || ep_lower.starts_with("https://");
            let uri = http::Uri::builder()
                .scheme(if is_tls { "https" } else { "http" })
                .authority(etp_cfg.host_header.as_str())
                .path_and_query(LOKI_GRPC_PATH)
                .build()
                .map_err(|e| PluginError::Config(format!("Invalid Loki gRPC URI: {e}")))?;

            let mut headers = HeaderMap::new();
            headers.insert(
                http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/grpc"),
            );
            headers.insert(http::header::TE, HeaderValue::from_static("trailers"));
            headers.insert(
                http::header::USER_AGENT,
                HeaderValue::from_static("velda-edge/0.1.0"),
            );
            if let Some(tid) = &etp_cfg.tenant_id {
                let val = HeaderValue::from_str(tid).map_err(|e| {
                    PluginError::Config(format!("Invalid Loki X-Scope-OrgID header value: {e}"))
                })?;
                headers.insert(HeaderName::from_static("x-scope-orgid"), val);
            }
            if let Some(auth) = &auth_val {
                let val = HeaderValue::from_str(auth).map_err(|e| {
                    PluginError::Config(format!("Invalid Loki Authorization header: {e}"))
                })?;
                headers.insert(http::header::AUTHORIZATION, val);
            }
            for (k, v) in &etp_cfg.custom_headers {
                let name = HeaderName::from_bytes(k.as_bytes()).map_err(|e| {
                    PluginError::Config(format!("Invalid Loki custom header name '{k}': {e}"))
                })?;
                let val = HeaderValue::from_str(v).map_err(|e| {
                    PluginError::Config(format!("Invalid Loki custom header '{k}' value: {e}"))
                })?;
                headers.insert(name, val);
            }
            Ok(LokiWireTemplate::Grpc { uri, headers })
        }
    }
}

/// Acquires an in-place [`LokiLease`] and ships a serialized JSON or Protobuf log batch.
pub async fn push_logs(
    etp: &Arc<LokiEtp>,
    template: &LokiWireTemplate,
    head_buf: &mut Vec<u8>,
    payload: &[u8],
    resp_buf: &mut Vec<u8>,
    request_timeout: Duration,
) -> Result<(), PluginError> {
    let mut lease = etp
        .acquire()
        .await
        .map_err(|e| PluginError::Http(format!("Loki ETP acquire failed: {e}")))?;

    match template {
        LokiWireTemplate::Http { prefix } => {
            head_buf.clear();
            head_buf.extend_from_slice(prefix);
            velda_core::lut::decimal::fast_write_u64(head_buf, payload.len() as u64);
            head_buf.extend_from_slice(b"\r\n\r\n");

            for attempt in 0..2 {
                let LokiConn::Http(ref mut stream) = *lease else {
                    return Err(PluginError::Config(
                        "Loki ETP connection kind mismatch (expected HTTP)".into(),
                    ));
                };

                let exchange: Result<Result<(u16, bool), EtpError>, _> =
                    tokio::time::timeout(request_timeout, async {
                        stream.write_all(head_buf).await?;
                        stream.write_all(payload).await?;
                        stream.flush().await?;

                        resp_buf.clear();
                        let mut tmp = [0u8; 4096];
                        let (status, hdr_len, content_length, chunked, connection_close) = loop {
                            let n = stream.read(&mut tmp).await?;
                            if n == 0 {
                                return Err(Error::new(
                                    ErrorKind::UnexpectedEof,
                                    "Loki closed before HTTP response headers",
                                )
                                .into());
                            }
                            if let Some(chunk) = tmp.get(..n) {
                                resp_buf.extend_from_slice(chunk);
                            }
                            if resp_buf.len() > 32 * 1024 {
                                return Err(Error::new(
                                    ErrorKind::InvalidData,
                                    "Loki HTTP response headers > 32 KiB",
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
                                            && std::str::from_utf8(h.value).is_ok_and(|s| {
                                                s.to_ascii_lowercase().contains("chunked")
                                            })
                                        {
                                            is_chunked = true;
                                        } else if h.name.eq_ignore_ascii_case("connection")
                                            && std::str::from_utf8(h.value).is_ok_and(|s| {
                                                s.trim().eq_ignore_ascii_case("close")
                                            })
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
                                        format!("Invalid Loki HTTP response: {err}"),
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
                                    let n = stream.read(&mut tmp).await?;
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
                                        Error::new(
                                            ErrorKind::InvalidData,
                                            "Invalid chunk size range",
                                        )
                                    })?;
                                let size_line = std::str::from_utf8(size_slice).map_err(|_| {
                                    Error::new(ErrorKind::InvalidData, "Invalid UTF-8 chunk size")
                                })?;
                                let hex_part = size_line.split(';').next().unwrap_or("").trim();
                                let chunk_size =
                                    usize::from_str_radix(hex_part, 16).map_err(|_| {
                                        Error::new(
                                            ErrorKind::InvalidData,
                                            format!("Bad chunk size '{size_line}'"),
                                        )
                                    })?;
                                pos += crlf_rel + 2;

                                if chunk_size == 0 {
                                    loop {
                                        let trailer_rel = loop {
                                            if let Some(rel) = resp_buf.get(pos..).and_then(|t| {
                                                t.windows(2).position(|w| w == b"\r\n")
                                            }) {
                                                break rel;
                                            }
                                            if resp_buf.len().saturating_sub(pos) > 8192 {
                                                return Err(Error::new(
                                                    ErrorKind::InvalidData,
                                                    "Chunk trailer > 8 KiB",
                                                )
                                                .into());
                                            }
                                            let n = stream.read(&mut tmp).await?;
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
                                            stream.read_exact(buf).await?;
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
                                    stream.read_exact(buf).await?;
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
                        if (200..300).contains(&status) {
                            return Ok(());
                        }
                        return Err(PluginError::Http(format!(
                            "Loki HTTP push returned status {status}"
                        )));
                    }
                    Ok(Err(EtpError::Io(ref err)))
                        if attempt == 0 && err.kind() != ErrorKind::InvalidData =>
                    {
                        if let Err(err) = lease.reconnect().await {
                            lease.recreate_in_background();
                            return Err(PluginError::Io(Error::other(err.to_string())));
                        }
                    }
                    Ok(Err(err)) => {
                        lease.recreate_in_background();
                        return Err(PluginError::Http(format!("Loki HTTP push failed: {err}")));
                    }
                    Err(_) => {
                        lease.recreate_in_background();
                        return Err(PluginError::Io(Error::new(
                            ErrorKind::TimedOut,
                            "Loki HTTP push request timed out",
                        )));
                    }
                }
            }
            Err(PluginError::Http("Loki HTTP push retries exhausted".into()))
        }
        LokiWireTemplate::Grpc { uri, headers } => {
            let payload_len = u32::try_from(payload.len())
                .map_err(|_| PluginError::Http("Loki gRPC payload exceeds u32 length".into()))?;
            let mut frame_buf = BytesMut::with_capacity(5 + payload.len());
            frame_buf.extend_from_slice(&[0u8]);
            frame_buf.extend_from_slice(&payload_len.to_be_bytes());
            frame_buf.extend_from_slice(payload);
            let frame = frame_buf.freeze();

            for attempt in 0..2 {
                let LokiConn::Grpc(ref mut send_req) = *lease else {
                    return Err(PluginError::Config(
                        "Loki ETP connection kind mismatch (expected gRPC)".into(),
                    ));
                };

                let mut req = http::Request::builder()
                    .method(http::Method::POST)
                    .uri(uri.clone())
                    .body(())
                    .map_err(|e| PluginError::Config(format!("Invalid Loki gRPC request: {e}")))?;
                *req.headers_mut() = headers.clone();

                let send_req_clone = send_req.clone();
                let frame_clone = frame.clone();
                let exchange = tokio::time::timeout(request_timeout, async move {
                    let mut ready_req = send_req_clone.ready().await.map_err(|e| {
                        GrpcExchangeErr::Transport(format!("HTTP/2 not ready: {e}"))
                    })?;
                    let (response_future, mut send_stream) =
                        ready_req.send_request(req, false).map_err(|e| {
                            GrpcExchangeErr::Transport(format!("Failed to start H2 stream: {e}"))
                        })?;
                    send_stream.send_data(frame_clone, true).map_err(|e| {
                        GrpcExchangeErr::Transport(format!("Failed to send gRPC frame: {e}"))
                    })?;

                    let response = response_future.await.map_err(|e| {
                        GrpcExchangeErr::Transport(format!("Loki gRPC response error: {e}"))
                    })?;

                    let http_status = response.status();
                    if http_status != http::StatusCode::OK {
                        return Err(GrpcExchangeErr::Rejected {
                            http_status_5xx: http_status.is_server_error(),
                            msg: format!("Loki gRPC export failed with HTTP status: {http_status}"),
                        });
                    }

                    if let Some(status_str) = response
                        .headers()
                        .get("grpc-status")
                        .and_then(|v| v.to_str().ok())
                    {
                        if status_str != "0" {
                            let msg = response
                                .headers()
                                .get("grpc-message")
                                .and_then(|v| v.to_str().ok())
                                .unwrap_or("Unknown error");
                            return Err(GrpcExchangeErr::Rejected {
                                http_status_5xx: false,
                                msg: format!(
                                    "Loki gRPC export rejected with status {status_str}: {msg}"
                                ),
                            });
                        }
                        return Ok(());
                    }

                    let mut body = response.into_body();
                    while let Some(chunk) = body.data().await {
                        let _ = chunk.map_err(|e| {
                            GrpcExchangeErr::Transport(format!("Loki gRPC read chunk error: {e}"))
                        })?;
                    }
                    if let Some(trailers) = body.trailers().await.map_err(|e| {
                        GrpcExchangeErr::Transport(format!("Loki gRPC read trailers error: {e}"))
                    })? && let Some(status_str) =
                        trailers.get("grpc-status").and_then(|h| h.to_str().ok())
                        && status_str != "0"
                    {
                        let msg = trailers
                            .get("grpc-message")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("Unknown error");
                        return Err(GrpcExchangeErr::Rejected {
                            http_status_5xx: false,
                            msg: format!(
                                "Loki gRPC export rejected with status {status_str}: {msg}"
                            ),
                        });
                    }
                    Ok(())
                })
                .await;

                match exchange {
                    Ok(Ok(())) => return Ok(()),
                    Ok(Err(GrpcExchangeErr::Rejected {
                        http_status_5xx,
                        msg,
                    })) => {
                        if http_status_5xx {
                            lease.recreate_in_background();
                        }
                        return Err(PluginError::Http(msg));
                    }
                    Ok(Err(GrpcExchangeErr::Transport(err_msg))) => {
                        if attempt == 0 {
                            if let Err(err) = lease.reconnect().await {
                                lease.recreate_in_background();
                                return Err(PluginError::Http(err.to_string()));
                            }
                            continue;
                        }
                        lease.recreate_in_background();
                        return Err(PluginError::Http(err_msg));
                    }
                    Err(_) => {
                        lease.recreate_in_background();
                        return Err(PluginError::Io(Error::new(
                            ErrorKind::TimedOut,
                            "Loki gRPC push timed out",
                        )));
                    }
                }
            }
            Err(PluginError::Http("Loki gRPC push retries exhausted".into()))
        }
    }
}
