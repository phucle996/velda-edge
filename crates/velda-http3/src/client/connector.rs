//! Upstream HTTP/3 Client Multiplexed Connection & Request Forwarding (RFC 9114).
//!
//! Provides [`Http3Client`], an asynchronous, persistent QUIC / HTTP/3 client connection
//! capable of multiplexing concurrent bidirectional request-response streams over a single
//! QUIC connection without Head-of-Line blocking.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use quinn_proto::{ClientConfig, Dir, Endpoint, EndpointConfig, Event};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};
use velda_core::{L7Request, L7Response};

use super::driver::{ClientCommand, run_client_driver};
use crate::config::Http3Config;
use crate::error::Http3Error;
use crate::frame::{Http3Frame, encode_frame, encode_varint, settings_id, stream_type};

/// An active, persistent multiplexed HTTP/3 upstream client connection.
///
/// Can be cheaply cloned and shared across Tokio tasks to dispatch concurrent
/// requests onto the same underlying QUIC connection without Head-of-Line blocking.
#[derive(Clone)]
pub struct Http3Client {
    command_tx: mpsc::Sender<ClientCommand>,
    target: SocketAddr,
}

impl Http3Client {
    /// Dispatches an owned [`L7Request`] over a new multiplexed bidirectional stream on this connection.
    ///
    /// Consumes `req` with **ZERO cloning and ZERO heap boxing**, transferring ownership
    /// directly to the background QUIC connection driver loop.
    pub async fn send_request(&self, req: L7Request) -> Result<L7Response, Http3Error> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.command_tx
            .send(ClientCommand::SendRequest {
                request: req,
                reply_tx,
            })
            .await
            .map_err(|_| Http3Error::ConnectionClosed)?;

        reply_rx.await.map_err(|_| Http3Error::ConnectionClosed)?
    }

    /// Dispatches a borrowed [`L7Request`] by cloning it, useful when the caller retains ownership.
    #[inline]
    pub async fn send_request_ref(&self, req: &L7Request) -> Result<L7Response, Http3Error> {
        self.send_request(req.clone()).await
    }

    /// Returns the target physical socket address of this client connection.
    #[inline]
    pub fn target(&self) -> SocketAddr {
        self.target
    }

    /// Returns `true` if the background driver loop has terminated or connection closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.command_tx.is_closed()
    }

    /// Gracefully closes the underlying QUIC connection.
    pub async fn close(&self) {
        let _ = self.command_tx.send(ClientCommand::Close).await;
    }

    #[cfg(test)]
    pub(crate) fn mock_for_test(target: SocketAddr) -> (Self, mpsc::Receiver<ClientCommand>) {
        let (cmd_tx, cmd_rx) = mpsc::channel(1);
        (
            Self {
                command_tx: cmd_tx,
                target,
            },
            cmd_rx,
        )
    }
}

/// Establishes a new persistent, multiplexed HTTP/3 client connection to the target backend endpoint.
///
/// Drives the initial QUIC handshake to completion and spawns the background connection driver.
pub async fn connect(
    target: SocketAddr,
    server_name: &str,
    config: &Http3Config,
) -> Result<Http3Client, Http3Error> {
    // 1. Bind local UDP egress socket
    let bind_addr: SocketAddr = if target.is_ipv4() {
        "0.0.0.0:0".parse().unwrap()
    } else {
        "[::]:0".parse().unwrap()
    };
    let socket = Arc::new(UdpSocket::bind(bind_addr).await.map_err(Http3Error::Io)?);

    // 2. Build QUIC client endpoint and config
    let endpoint_config = Arc::new(EndpointConfig::default());
    let mut endpoint = Endpoint::new(endpoint_config, None, false, None);

    let mut client_cfg = default_client_config()?;
    client_cfg.transport_config(Arc::new(config.build_transport_config()));
    let now = Instant::now();
    let (handle, mut conn) = endpoint
        .connect(now, client_cfg, target, server_name)
        .map_err(|e| Http3Error::H3(format!("Failed to initiate connect to {target}: {e}")))?;

    let mut transmit_buf = Vec::with_capacity(65535);
    let mut socket_recv_buf = vec![0u8; 65535];

    // 3. Drive QUIC handshake loop to completion
    let handshake_start = Instant::now();
    let handshake_timeout = Duration::from_millis(config.idle_timeout_ms.max(5000));

    while conn.is_handshaking() {
        if handshake_start.elapsed() > handshake_timeout {
            return Err(Http3Error::H3(format!(
                "HTTP/3 QUIC handshake to {target} timed out"
            )));
        }

        // Drain pending transmits
        transmit_buf.clear();
        while let Some(transmit) = conn.poll_transmit(Instant::now(), 1, &mut transmit_buf) {
            if transmit.size > 0 {
                let _ = socket
                    .send_to(&transmit_buf[..transmit.size], transmit.destination)
                    .await;
            }
            transmit_buf.clear();
        }

        tokio::select! {
            res = socket.recv_from(&mut socket_recv_buf) => {
                let (len, from_addr) = res.map_err(Http3Error::Io)?;
                let now = Instant::now();
                let payload = BytesMut::from(&socket_recv_buf[..len]);
                let mut resp_buf = Vec::new();
                if let Some(quinn_proto::DatagramEvent::ConnectionEvent(_, ce)) =
                    endpoint.handle(now, from_addr, None, None, payload, &mut resp_buf)
                {
                    conn.handle_event(ce);
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(5)) => {
                let now = Instant::now();
                if let Some(timeout) = conn.poll_timeout()
                    && timeout <= now
                {
                    conn.handle_timeout(now);
                }
            }
        }

        // Poll connection events during handshake to catch connection failure fast
        while let Some(event) = conn.poll() {
            if let Event::ConnectionLost { reason } = event {
                return Err(Http3Error::H3(format!(
                    "HTTP/3 QUIC connection to {target} failed during handshake: {reason}"
                )));
            }
        }
    }

    if conn.is_drained() {
        return Err(Http3Error::ConnectionClosed);
    }

    // 4. RFC 9114 Section 6.2.1: Open client unidirectional Control Stream and transmit mandatory SETTINGS frame.
    if let Some(ctrl_stream_id) = conn.streams().open(Dir::Uni) {
        let mut ctrl_buf = BytesMut::new();
        // Stream Type 0x00 = Control Stream
        encode_varint(stream_type::CONTROL, &mut ctrl_buf);
        let settings = vec![
            (
                settings_id::QPACK_MAX_TABLE_CAPACITY,
                config.max_qpack_table_capacity as u64,
            ),
            (
                settings_id::MAX_FIELD_SECTION_SIZE,
                config.max_header_size as u64,
            ),
            (settings_id::QPACK_BLOCKED_STREAMS, 0),
        ];
        encode_frame(&Http3Frame::Settings(settings), &mut ctrl_buf);
        let _ = conn.send_stream(ctrl_stream_id).write(&ctrl_buf);

        // Flush initial control stream packet
        transmit_buf.clear();
        while let Some(transmit) = conn.poll_transmit(Instant::now(), 1, &mut transmit_buf) {
            if transmit.size > 0 {
                let _ = socket
                    .send_to(&transmit_buf[..transmit.size], transmit.destination)
                    .await;
            }
            transmit_buf.clear();
        }
    }

    // 5. Spawn background connection driver task
    let (command_tx, command_rx) = mpsc::channel(256);
    tokio::spawn(run_client_driver(
        socket,
        endpoint,
        conn,
        handle,
        command_rx,
        config.max_body_size,
    ));

    Ok(Http3Client { command_tx, target })
}

/// Forwards an HTTP/3 [`L7Request`] to the target backend endpoint over a dedicated HTTP/3 connection.
///
/// Convenience entrypoint for single-shot or non-pooled egress workflows.
pub async fn forward_request(
    req: &L7Request,
    target: SocketAddr,
) -> Result<L7Response, Http3Error> {
    let server_name = extract_sni(req, &target);
    let config = Http3Config::auto();
    let client = connect(target, &server_name, &config).await?;
    client.send_request_ref(req).await
}

/// Extracts the server name (SNI) from an HTTP request or falls back to the target IP address.
///
/// Precedence:
/// 1. Request URI host (`req.uri.host()`)
/// 2. Request `Host` header (stripping `:port` if present)
/// 3. Target IP address string (`target.ip().to_string()`)
pub fn extract_sni<'a>(req: &'a L7Request, target: &SocketAddr) -> std::borrow::Cow<'a, str> {
    if let Some(host) = req.uri.host().filter(|h| !h.trim().is_empty()) {
        return std::borrow::Cow::Borrowed(host.trim());
    }

    if let Some(host_hdr) = req
        .headers
        .get(http::header::HOST)
        .and_then(|h| h.to_str().ok())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        return std::borrow::Cow::Borrowed(strip_port(host_hdr));
    }

    std::borrow::Cow::Owned(target.ip().to_string())
}

/// Strips the port component from a host string (e.g., `example.com:8443` -> `example.com`,
/// `[::1]:8443` -> `::1`).
#[inline]
pub fn strip_port(host: &str) -> &str {
    if let Some(stripped) = host.strip_prefix('[')
        && let Some(end_bracket) = stripped.find(']')
    {
        return &stripped[..end_bracket];
    }

    if let Some(colon_idx) = host.rfind(':') {
        if host[..colon_idx].contains(':') {
            host
        } else {
            &host[..colon_idx]
        }
    } else {
        host
    }
}

/// Resolves the upstream TLS SNI / ServerName for an HTTP/3 outbound connection.
///
/// Precedence:
/// 1. Explicit upstream configuration `explicit_sni` (e.g. from upstream TLS target_sni)
/// 2. Request URI host (`req.uri.host()`)
/// 3. Request `Host` header (stripping `:port` if present)
/// 4. Target IP address string (`target.ip().to_string()`)
pub fn resolve_sni<'a>(
    req: &'a L7Request,
    target: &SocketAddr,
    explicit_sni: Option<&'a str>,
) -> std::borrow::Cow<'a, str> {
    if let Some(sni) = explicit_sni.filter(|s| !s.trim().is_empty()) {
        return std::borrow::Cow::Borrowed(sni.trim());
    }
    extract_sni(req, target)
}

/// Builds default QUIC client configuration for HTTP/3 upstream.
pub fn default_client_config() -> Result<ClientConfig, Http3Error> {
    let rustls_client = velda_tls::client::build_insecure_tls13_client_config(vec![b"h3".to_vec()]);
    velda_tls::client::build_quic_client_config(rustls_client)
        .map_err(|e| Http3Error::H3(format!("QUIC crypto config error: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, Method, Uri, Version};
    use velda_core::Body;

    #[test]
    fn test_strip_port() {
        assert_eq!(strip_port("example.com"), "example.com");
        assert_eq!(strip_port("example.com:8443"), "example.com");
        assert_eq!(strip_port("192.168.1.100"), "192.168.1.100");
        assert_eq!(strip_port("192.168.1.100:443"), "192.168.1.100");
        assert_eq!(strip_port("[::1]:8443"), "::1");
        assert_eq!(strip_port("[2001:db8::1]:443"), "2001:db8::1");
        assert_eq!(strip_port("::1"), "::1");
    }

    #[test]
    fn test_resolve_sni_precedence() {
        let target: SocketAddr = "10.0.0.1:4433".parse().unwrap();

        // 1. Explicit SNI overrides everything
        let req1 = L7Request::new(
            Method::GET,
            "https://uri-host.internal/api".parse::<Uri>().unwrap(),
            Version::HTTP_3,
            {
                let mut h = HeaderMap::new();
                h.insert(http::header::HOST, "header-host.com:8443".parse().unwrap());
                h
            },
            Body::Empty,
        );
        assert_eq!(
            resolve_sni(&req1, &target, Some("explicit-backend.internal")),
            "explicit-backend.internal"
        );

        // 2. URI host takes precedence when explicit SNI is None
        assert_eq!(resolve_sni(&req1, &target, None), "uri-host.internal");

        // 3. Host header with port stripped takes precedence when URI host is absent
        let req2 = L7Request::new(
            Method::GET,
            "/api/v1/orders".parse::<Uri>().unwrap(),
            Version::HTTP_3,
            {
                let mut h = HeaderMap::new();
                h.insert(
                    http::header::HOST,
                    "my-service.internal:9443".parse().unwrap(),
                );
                h
            },
            Body::Empty,
        );
        assert_eq!(resolve_sni(&req2, &target, None), "my-service.internal");

        // 4. Target IP fallback when no SNI, URI host, or Host header exists (no hardcoded "localhost")
        let req3 = L7Request::new(
            Method::GET,
            "/health".parse::<Uri>().unwrap(),
            Version::HTTP_3,
            HeaderMap::new(),
            Body::Empty,
        );
        assert_eq!(resolve_sni(&req3, &target, None), "10.0.0.1");
    }
}
