//! OpenTelemetry Logs HTTP/1.1 & HTTP/2 gRPC Wire Exchange (`otlp_log_shipper::wire`).
//!
//! ```text
//!   Startup:
//!     bake_wire_template(&etp.config)
//!         ├─ OtlpProtocol::HttpJson     ──► HTTP/1.1 POST {base_path}/v1/logs (application/json)
//!         ├─ OtlpProtocol::HttpProtobuf ──► HTTP/1.1 POST {base_path}/v1/logs (application/x-protobuf)
//!         └─ OtlpProtocol::Grpc         ──► HTTP/2 POST /opentelemetry.proto.collector.logs.v1.LogsService/Export
//!
//!   Per-Flush Hot Path:
//!     etp.acquire().await ──► OtlpLease (Slot[idx] in-place)
//!         ├─ Http(stream)  ──► stamp Content-Length (LUT) + write + flush -> httparse::Response + drain
//!         └─ Grpc(send_req)──► 5B frame [0x00, len_be32] -> H2 send_request + trailers grpc-status==0
//!               ├─ transport Io/H2 error (attempt 0) ─► lease.reconnect().await + retry once
//!               └─ 5xx / Connection: close / timeout / persistent err ─► lease.recreate_in_background()
//!
//!   Wire Frame Layouts:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ HTTP/1.1: POST {path} HTTP/1.1\r\nHost: {host_header}\r\n                  │
//!   │   Content-Type: application/json | application/x-protobuf\r\n              │
//!   │   {Custom Headers\r\n}Connection: keep-alive\r\nContent-Length: {N}\r\n\r\n│
//!   ├────────────────────────────────────────────────────────────────────────────┤
//!   │ gRPC H2: POST /opentelemetry.proto.collector.logs.v1.LogsService/Export    │
//!   │   Frame: [ 0x00 (1B) | len_u32_be (4B) | ExportLogsServiceRequest bytes ]  │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use http::header::{HeaderMap, HeaderName, HeaderValue};
use velda_etp::EtpError;
use velda_etp::otlp::{OtlpConn, OtlpEtp, OtlpEtpConfig, OtlpProtocol};

use crate::error::PluginError;

/// Canonical OpenTelemetry Logs gRPC RPC path.
pub const OTLP_LOGS_GRPC_PATH: &str = "/opentelemetry.proto.collector.logs.v1.LogsService/Export";

/// Pre-baked immutable transport header/URI template for an OTLP Logs ETP connection.
pub enum OtlpLogsWireTemplate {
    Http { prefix: Vec<u8> },
    Grpc { uri: http::Uri, headers: HeaderMap },
}

enum GrpcExchangeErr {
    Transport(String),
    Rejected { http_status_5xx: bool, msg: String },
}

/// Pre-bakes HTTP/1.1 header prefix or HTTP/2 URI + HeaderMap once at plugin startup.
pub fn bake_wire_template(etp_cfg: &OtlpEtpConfig) -> Result<OtlpLogsWireTemplate, PluginError> {
    match etp_cfg.protocol {
        OtlpProtocol::HttpJson | OtlpProtocol::HttpProtobuf => {
            let path = if etp_cfg.base_path.ends_with("/v1/logs") {
                etp_cfg.base_path.clone()
            } else {
                format!("{}/v1/logs", etp_cfg.base_path)
            };
            let content_type = match etp_cfg.protocol {
                OtlpProtocol::HttpJson => "application/json",
                OtlpProtocol::HttpProtobuf => "application/x-protobuf",
                OtlpProtocol::Grpc => unreachable!(),
            };
            let mut extra = String::new();
            for (k, v) in &etp_cfg.headers {
                extra.push_str(k);
                extra.push_str(": ");
                extra.push_str(v);
                extra.push_str("\r\n");
            }
            let prefix = format!(
                "POST {path} HTTP/1.1\r\nHost: {}\r\nContent-Type: {content_type}\r\n\
                 {extra}Connection: keep-alive\r\nContent-Length: ",
                etp_cfg.host_header,
            )
            .into_bytes();
            Ok(OtlpLogsWireTemplate::Http { prefix })
        }
        OtlpProtocol::Grpc => {
            let ep_lower = etp_cfg.endpoint.to_ascii_lowercase();
            let is_tls = etp_cfg.tls.is_some()
                || ep_lower.starts_with("grpcs://")
                || ep_lower.starts_with("https://");
            let uri = http::Uri::builder()
                .scheme(if is_tls { "https" } else { "http" })
                .authority(etp_cfg.host_header.as_str())
                .path_and_query(OTLP_LOGS_GRPC_PATH)
                .build()
                .map_err(|e| PluginError::Config(format!("Invalid OTLP Logs gRPC URI: {e}")))?;

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
            for (k, v) in &etp_cfg.headers {
                let name = HeaderName::from_bytes(k.as_bytes()).map_err(|e| {
                    PluginError::Config(format!("Invalid OTLP custom header name '{k}': {e}"))
                })?;
                let val = HeaderValue::from_str(v).map_err(|e| {
                    PluginError::Config(format!("Invalid OTLP custom header '{k}' value: {e}"))
                })?;
                headers.insert(name, val);
            }
            Ok(OtlpLogsWireTemplate::Grpc { uri, headers })
        }
    }
}

/// Acquires an in-place [`OtlpLease`](velda_etp::otlp::OtlpLease) and exports a serialized OTLP
/// JSON or Protobuf logs payload over HTTP/1.1 or HTTP/2 gRPC.
pub async fn push_logs(
    etp: &Arc<OtlpEtp>,
    template: &OtlpLogsWireTemplate,
    head_buf: &mut Vec<u8>,
    payload: &[u8],
    resp_buf: &mut Vec<u8>,
    request_timeout: Duration,
) -> Result<(), PluginError> {
    let mut lease = etp
        .acquire()
        .await
        .map_err(|e| PluginError::Http(format!("OTLP Logs ETP acquire failed: {e}")))?;

    match template {
        OtlpLogsWireTemplate::Http { prefix } => {
            head_buf.clear();
            head_buf.extend_from_slice(prefix);
            velda_core::lut::decimal::fast_write_u64(head_buf, payload.len() as u64);
            head_buf.extend_from_slice(b"\r\n\r\n");

            for attempt in 0..2 {
                let OtlpConn::Http(ref mut stream) = *lease else {
                    return Err(PluginError::Config(
                        "OTLP ETP connection kind mismatch (expected HTTP)".into(),
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
                                    "OTLP collector closed before HTTP headers",
                                )
                                .into());
                            }
                            if let Some(chunk) = tmp.get(..n) {
                                resp_buf.extend_from_slice(chunk);
                            }
                            if resp_buf.len() > 32 * 1024 {
                                return Err(Error::new(
                                    ErrorKind::InvalidData,
                                    "OTLP HTTP response headers > 32 KiB",
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
                                        format!("Invalid OTLP HTTP response: {err}"),
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

                                let need = chunk_size.checked_add(2).ok_or_else(|| {
                                    Error::new(ErrorKind::InvalidData, "Chunk size overflow")
                                })?;
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
                        } else if let Some(len) = content_length {
                            if len > 0 {
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
                        if (200..300).contains(&status) {
                            return Ok(());
                        }
                        return Err(PluginError::Http(format!(
                            "OTLP Logs HTTP export returned status {status}"
                        )));
                    }
                    Ok(Err(EtpError::Io(ref err)))
                        if attempt == 0 && err.kind() != ErrorKind::InvalidData =>
                    {
                        if let Err(err) = lease.reconnect().await {
                            lease.recreate_in_background();
                            return Err(PluginError::Http(format!(
                                "OTLP Logs reconnect failed: {err}"
                            )));
                        }
                    }
                    Ok(Err(err)) => {
                        lease.recreate_in_background();
                        return Err(PluginError::Http(format!("OTLP Logs HTTP error: {err}")));
                    }
                    Err(_) => {
                        lease.recreate_in_background();
                        return Err(PluginError::Http(format!(
                            "OTLP Logs HTTP request timed out after {} ms",
                            request_timeout.as_millis()
                        )));
                    }
                }
            }

            lease.recreate_in_background();
            Err(PluginError::Http(
                "OTLP Logs HTTP export failed after reconnect retry".into(),
            ))
        }
        OtlpLogsWireTemplate::Grpc { uri, headers } => {
            let len_u32 = u32::try_from(payload.len())
                .map_err(|_| PluginError::Http("OTLP gRPC frame exceeds 4 GiB".into()))?;
            let mut framed = BytesMut::with_capacity(5 + payload.len());
            framed.extend_from_slice(&[0u8]);
            framed.extend_from_slice(&len_u32.to_be_bytes());
            framed.extend_from_slice(payload);
            let frame_bytes = framed.freeze();

            for attempt in 0..2 {
                let OtlpConn::Grpc(ref mut send_req) = *lease else {
                    return Err(PluginError::Config(
                        "OTLP ETP connection kind mismatch (expected gRPC)".into(),
                    ));
                };

                let mut req = http::Request::new(());
                *req.method_mut() = http::Method::POST;
                *req.uri_mut() = uri.clone();
                *req.headers_mut() = headers.clone();

                let rpc_exchange = async {
                    let mut ready_client =
                        send_req.clone().ready().await.map_err(|e| {
                            GrpcExchangeErr::Transport(format!("H2 not ready: {e}"))
                        })?;
                    let (response_fut, mut send_stream) = ready_client
                        .send_request(req, false)
                        .map_err(|e| GrpcExchangeErr::Transport(format!("H2 send_request: {e}")))?;
                    send_stream
                        .send_data(frame_bytes.clone(), true)
                        .map_err(|e| GrpcExchangeErr::Transport(format!("H2 send_data: {e}")))?;

                    let response = response_fut
                        .await
                        .map_err(|e| GrpcExchangeErr::Transport(format!("H2 response: {e}")))?;
                    let status = response.status();
                    if status != http::StatusCode::OK {
                        return Err(GrpcExchangeErr::Rejected {
                            http_status_5xx: status.as_u16() >= 500,
                            msg: format!("OTLP Logs gRPC HTTP status {status}"),
                        });
                    }

                    // Trailers-Only response: `grpc-status` arrives in initial HEADERS.
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
                                msg: format!("OTLP Logs gRPC rejected status {status_str}: {msg}"),
                            });
                        }
                        return Ok(());
                    }

                    let mut body = response.into_body();
                    while let Some(chunk) = body.data().await {
                        let data = chunk
                            .map_err(|e| GrpcExchangeErr::Transport(format!("H2 body: {e}")))?;
                        let _ = body.flow_control().release_capacity(data.len());
                    }

                    if let Some(trailers) = body
                        .trailers()
                        .await
                        .map_err(|e| GrpcExchangeErr::Transport(format!("H2 trailers: {e}")))?
                        && let Some(status_str) =
                            trailers.get("grpc-status").and_then(|h| h.to_str().ok())
                        && status_str != "0"
                    {
                        let msg = trailers
                            .get("grpc-message")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("Unknown error");
                        return Err(GrpcExchangeErr::Rejected {
                            http_status_5xx: false,
                            msg: format!("OTLP Logs gRPC rejected status {status_str}: {msg}"),
                        });
                    }

                    Ok(())
                };

                match tokio::time::timeout(request_timeout, rpc_exchange).await {
                    Ok(Ok(())) => return Ok(()),
                    Ok(Err(GrpcExchangeErr::Transport(msg))) if attempt == 0 => {
                        if let Err(err) = lease.reconnect().await {
                            lease.recreate_in_background();
                            return Err(PluginError::Http(format!(
                                "OTLP Logs gRPC reconnect failed after {msg}: {err}"
                            )));
                        }
                    }
                    Ok(Err(GrpcExchangeErr::Transport(msg))) => {
                        lease.recreate_in_background();
                        return Err(PluginError::Http(format!(
                            "OTLP Logs gRPC transport error: {msg}"
                        )));
                    }
                    Ok(Err(GrpcExchangeErr::Rejected {
                        http_status_5xx,
                        msg,
                    })) => {
                        if http_status_5xx {
                            lease.recreate_in_background();
                        }
                        return Err(PluginError::Http(msg));
                    }
                    Err(_) => {
                        lease.recreate_in_background();
                        return Err(PluginError::Io(Error::new(
                            ErrorKind::TimedOut,
                            format!(
                                "OTLP Logs gRPC request timed out after {} ms",
                                request_timeout.as_millis()
                            ),
                        )));
                    }
                }
            }

            lease.recreate_in_background();
            Err(PluginError::Http(
                "OTLP Logs gRPC export failed after reconnect retry".into(),
            ))
        }
    }
}
