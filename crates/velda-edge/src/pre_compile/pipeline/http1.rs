//! Layer 7 HTTP/1.1 pipeline: downstream stream worker, route evaluation, and upstream connection handoff.
//!
//! Operates strictly for listeners explicitly configured with `protocol = "http1"`.
//! Routes matched via `velda-router::Http1Router`, protocol lifecycle and wire streaming
//! piped via `velda-http1`.
//!
//! Follows strict architectural boundary: `velda-edge` orchestrates the ingress pipeline
//! (`accept head -> route -> target -> enrich -> connect -> pipe`), while all HTTP/1.1
//! wire protocol mechanics, hop-by-hop sanitization, and progressive streaming pumps
//! belong entirely inside `velda-http1`.

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderName, HeaderValue};
use tokio::io::{AsyncRead, AsyncWrite};
use velda_http1::{
    Http1BodyFraming, Http1Config, Http1PipeStrategy, Http1Response, Http1ServerConnection,
    pipe_buffered, pipe_client_stream, pipe_duplex, pipe_server_stream,
};
use velda_router::Http1RouteRequest;
use velda_tls::TlsServerEngine;
use velda_transport::Connection;

use crate::pipeline::context::{IngressContext, TlsMetadata};
use crate::runtime::SharedRuntime;

pub use crate::runtime::upstream::UpstreamHttp1Stream;

/// Enriches HTTP request headers with standard proxy forwarding metadata.
///
/// Implements both the IETF official standard ([RFC 7239]) and the de-facto
/// industry standards (`X-Forwarded-*`, `X-Real-IP`) used by modern reverse proxies
/// (Nginx, Envoy, HAProxy, AWS ALB).
///
/// ### Standards Complied:
/// 1. **RFC 7239 (Forwarded HTTP Extension)**:
///    - Injects `Forwarded: for=<client>;proto=<http|https>;by=<local>;host=<host>`.
///    - RFC 7239 §5.2: IPv6 addresses are explicitly wrapped in quotes and square brackets `"[...]"`
///      to distinguish colons from port/parameter delimiters.
///    - Chains with existing downstream `Forwarded` headers via comma separation.
/// 2. **X-Forwarded-For (RFC 7239 §5.2 reference / Squid / Nginx)**:
///    - Appends downstream client IP to existing list or initializes new header.
/// 3. **X-Forwarded-Proto (De-facto industry standard / Envoy / Nginx / AWS ALB)**:
///    - Authoritative downstream protocol (`"https"` if TLS terminated, else `"http"`).
///    - Security Invariant: Overwrites any forged downstream `X-Forwarded-Proto` to prevent
///      protocol spoofing and security bypasses on internal backends.
/// 4. **X-Forwarded-Port (RFC 7239 §5.4 reference / Spring Boot / ASP.NET)**:
///    - Sets the ingress listener port (`context.local_addr.port()`).
///    - Enables upstream services to construct accurate absolute redirect URLs (301/302 `Location`).
/// 5. **X-Forwarded-Host (RFC 7239 §5.3 reference / Apache / Envoy)**:
///    - Injects original downstream `Host` if present and not already defined.
/// 6. **X-Real-IP (Nginx `proxy_set_header` standard)**:
///    - Sets direct peer client IP address without comma-separated multi-hop traversal.
pub fn enrich_forwarded_headers(
    headers: &mut http::HeaderMap,
    context: &IngressContext,
    host: Option<&str>,
) {
    let client_ip = context.peer.ip();
    let client_ip_str = client_ip.to_string();
    let is_tls = context.tls.is_some();
    let proto = if is_tls { "https" } else { "http" };

    // 1. Standard: X-Forwarded-For (De-facto industry standard / RFC 7239 §5.2)
    // Preserves proxy traversal chain by appending immediate peer IP.
    let x_forwarded_for = HeaderName::from_static("x-forwarded-for");
    if let Some(existing) = headers.get(&x_forwarded_for) {
        if let Ok(existing_str) = existing.to_str() {
            let combined = format!("{existing_str}, {client_ip_str}");
            if let Ok(val) = HeaderValue::from_str(&combined) {
                headers.insert(x_forwarded_for, val);
            }
        }
    } else if let Ok(val) = HeaderValue::from_str(&client_ip_str) {
        headers.insert(x_forwarded_for, val);
    }

    // 2. Standard: X-Forwarded-Proto (De-facto industry standard / Envoy / Nginx)
    // Security Invariant: As an edge proxy terminating downstream connections,
    // we authoritatively declare the ingress transport protocol to prevent client spoofing.
    let x_forwarded_proto = HeaderName::from_static("x-forwarded-proto");
    headers.insert(x_forwarded_proto, HeaderValue::from_static(proto));

    // 3. Standard: X-Forwarded-Port (De-facto industry standard / RFC 7239 §5.4)
    // Advertises the ingress listener port so backends can generate correct 301/302 redirects.
    let x_forwarded_port = HeaderName::from_static("x-forwarded-port");
    let port_str = context.local_addr.port().to_string();
    if let Ok(val) = HeaderValue::from_str(&port_str) {
        headers.insert(x_forwarded_port, val);
    }

    // 4. Standard: X-Forwarded-Host (De-facto industry standard / RFC 7239 §5.3)
    // Advertises the original Host requested by the client if not already present.
    let x_forwarded_host = HeaderName::from_static("x-forwarded-host");
    if !headers.contains_key(&x_forwarded_host)
        && let Some(h) = host
        && let Ok(val) = HeaderValue::from_str(h)
    {
        headers.insert(x_forwarded_host, val);
    }

    // 5. Standard: X-Real-IP (Nginx de-facto standard)
    // Supplies immediate client IP directly without requiring upstream to parse CSV chains.
    let x_real_ip = HeaderName::from_static("x-real-ip");
    if let Ok(val) = HeaderValue::from_str(&client_ip_str) {
        headers.insert(x_real_ip, val);
    }

    // 6. Standard: RFC 7239 (Official IETF "Forwarded" HTTP Extension)
    // Format: for=<client>;proto=<http|https>;by=<local>;host=<host>
    // RFC 7239 §5.2: IPv6 addresses must be quoted with square brackets: "[...]"
    let forwarded = HeaderName::from_static("forwarded");
    let rfc_for = match client_ip {
        std::net::IpAddr::V4(v4) => v4.to_string(),
        std::net::IpAddr::V6(v6) => format!("\"[{v6}]\""),
    };
    let rfc_by = match context.local_addr.ip() {
        std::net::IpAddr::V4(v4) => v4.to_string(),
        std::net::IpAddr::V6(v6) => format!("\"[{v6}]\""),
    };

    let mut entry = format!("for={rfc_for};proto={proto};by={rfc_by}");
    if let Some(h) = host {
        entry.push_str(&format!(";host=\"{h}\""));
    }

    if let Some(existing) = headers.get(&forwarded) {
        if let Ok(existing_str) = existing.to_str() {
            let combined = format!("{existing_str}, {entry}");
            if let Ok(val) = HeaderValue::from_str(&combined) {
                headers.insert(forwarded, val);
            }
        }
    } else if let Ok(val) = HeaderValue::from_str(&entry) {
        headers.insert(forwarded, val);
    }
}

/// Asynchronous stream worker dispatching incoming HTTP/1.1 connections.
///
/// Inspects the pre-compiled listener metadata to determine whether downstream
/// TLS termination is active. If TLS is required, negotiates the handshake, validates
/// ALPN against the declared protocol, and passes the encrypted stream to the HTTP/1.1 loop.
pub async fn handle_http1_stream(
    connection: Connection,
    context: IngressContext,
    config: Http1Config,
    runtime: SharedRuntime,
) {
    let rt = runtime.load();

    if context.tls_enabled {
        let Some(tls_server) = rt.tls_server.as_ref() else {
            tracing::error!(
                listener = %context.listener_id,
                peer = %context.peer,
                "TLS required for listener, but no TLS server engine is compiled; dropping connection"
            );
            return;
        };

        match tls_server.accept_with_timeout(connection).await {
            Ok(tls_stream) => {
                let handshake_info = TlsServerEngine::extract_handshake_info(&tls_stream);
                tracing::debug!(
                    listener = %context.listener_id,
                    peer = %context.peer,
                    sni = ?handshake_info.sni,
                    alpn = ?handshake_info.alpn,
                    "Downstream TLS handshake succeeded"
                );
                let enriched_context =
                    context.with_tls_metadata(TlsMetadata::from_handshake(handshake_info));

                if let Err(err) = enriched_context.validate_alpn("http1") {
                    tracing::warn!(
                        error = %err,
                        listener = %enriched_context.listener_id,
                        peer = %enriched_context.peer,
                        "Dropping connection due to protocol ALPN mismatch"
                    );
                    return;
                }

                run_http1_loop(tls_stream, enriched_context, config, runtime).await;
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    listener = %context.listener_id,
                    peer = %context.peer,
                    "Downstream TLS handshake failed"
                );
            }
        }
    } else {
        run_http1_loop(connection, context, config, runtime).await;
    }
}

/// Core HTTP/1.1 downstream request-response loop decoupled from transport layer.
pub async fn run_http1_loop<IO>(
    stream: IO,
    context: IngressContext,
    config: Http1Config,
    runtime: SharedRuntime,
) where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let header_timeout =
        std::time::Duration::from_millis(config.header_read_timeout_ms.min(config.idle_timeout_ms));
    let mut conn = Http1ServerConnection::new(stream, config);
    let mut requests_served: u32 = 0;

    loop {
        // Phase 1: decode request head (fail-fast, header inspection, Slowloris protection)
        let (mut head, framing) =
            match tokio::time::timeout(header_timeout, conn.next_request_head()).await {
                Ok(Ok(Some(parts))) => parts,
                Ok(Ok(None)) => break,
                Ok(Err(e)) => {
                    tracing::debug!(error = %e, "HTTP/1.1 request head decode error");
                    break;
                }
                Err(_) => {
                    tracing::debug!(
                        listener = %context.listener_id,
                        timeout_ms = config.header_read_timeout_ms,
                        "HTTP/1.1 request head read timed out"
                    );
                    break;
                }
            };

        requests_served += 1;
        let reach_max_keepalive = requests_served >= config.max_keepalive_requests;
        let rt = runtime.load();

        // Enforce Ingress Streaming Policy (Option A):
        // If client sends chunked upload but listener has streaming.client disabled, reject immediately!
        if framing == Http1BodyFraming::Chunked && !context.streaming.client {
            tracing::warn!(
                listener = %context.listener_id,
                "Rejecting chunked request: streaming upload is disabled on this listener"
            );
            let rejected = Http1Response::from_bytes(
                StatusCode::FORBIDDEN,
                b"403 Forbidden: streaming upload is disabled on this listener\n".to_vec(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            let _ = conn.send_response(&rejected).await;
            break;
        }

        let host_str: Option<String> = head
            .headers
            .get(http::header::HOST)
            .and_then(|h| h.to_str().ok())
            .or_else(|| head.uri.host())
            .map(|s| s.to_string());

        let mut http_req = Http1RouteRequest::new(head.path());
        if let Some(ref h) = host_str {
            http_req = http_req.with_host(h);
        }
        http_req = http_req.with_method(head.method.as_str());

        let Some(route) = rt.router.route_http1(&context.listener_id, &http_req) else {
            tracing::debug!(
                listener = %context.listener_id,
                path = %head.path(),
                host = ?host_str,
                method = %head.method,
                "No HTTP/1.1 route matched"
            );
            let not_found = Http1Response::from_bytes(
                StatusCode::NOT_FOUND,
                b"404 Not Found: no matching route\n".to_vec(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            let _ = conn.send_response(&not_found).await;
            break;
        };

        let Some(upstream) = rt.upstreams.http1.get(&route.upstream_name) else {
            tracing::error!(
                listener = %context.listener_id,
                route = %route.id,
                upstream = %route.upstream_name,
                "No HTTP/1.1 upstream configured"
            );
            let no_backend = Http1Response::from_bytes(
                StatusCode::SERVICE_UNAVAILABLE,
                b"503 Service Unavailable: upstream not configured\n".to_vec(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            let _ = conn.send_response(&no_backend).await;
            break;
        };

        let strategy = upstream.strategy;
        enrich_forwarded_headers(&mut head.headers, &context, host_str.as_deref());
        let cfg = *conn.config();

        // HTTP/1.1 Pipeline Invariant (RFC 9112):
        // The Edge pipeline hands off the request processing closure directly to the upstream.
        // The upstream manages single-round Load Balancing selection, idle connection pooling,
        // pre-compiled TLS handshakes, error recovery, and streaming pipe execution.
        let pipe_result = upstream
            .dispatch_pipe(host_str.as_deref(), |mut upstream_stream| {
                let conn_ref = &mut conn;
                async move {
                    match strategy {
                        Http1PipeStrategy::Buffered => {
                            pipe_buffered(conn_ref, head, framing, &mut upstream_stream, &cfg).await
                        }
                        Http1PipeStrategy::ServerStream => {
                            pipe_server_stream(conn_ref, head, framing, &mut upstream_stream, &cfg)
                                .await
                        }
                        Http1PipeStrategy::ClientStream => {
                            pipe_client_stream(conn_ref, head, &mut upstream_stream, &cfg).await
                        }
                        Http1PipeStrategy::Duplex => {
                            pipe_duplex(conn_ref, head, &mut upstream_stream, &cfg).await
                        }
                    }
                }
            })
            .await;

        if let Err(e) = pipe_result {
            tracing::warn!(
                error = %e,
                upstream = %route.upstream_name,
                strategy = ?strategy,
                "HTTP/1.1 upstream stream pipe failed"
            );
            let err_resp = Http1Response::from_bytes(
                StatusCode::BAD_GATEWAY,
                format!("502 Bad Gateway: {e}\n").into_bytes(),
            )
            .with_header(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            let _ = conn.send_response(&err_resp).await;
            break;
        }

        if reach_max_keepalive || conn.is_closed() {
            tracing::debug!(
                listener = %context.listener_id,
                requests_served,
                max = config.max_keepalive_requests,
                "Closing HTTP/1.1 connection (reached max keepalive requests or connection closed)"
            );
            break;
        }
    }
}
