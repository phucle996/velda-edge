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
use super::response::Http3StreamResponse;
use crate::config::Http3Config;
use crate::error::Http3Error;
use crate::frame::{Http3Frame, encode_frame, encode_varint, settings_id, stream_type};

/// Upstream HTTP/3 progressive request chunk streaming handle.
#[derive(Clone)]
pub struct Http3StreamSender {
    pub(crate) stream_id: quinn_proto::StreamId,
    pub(crate) command_tx: mpsc::Sender<ClientCommand>,
}

impl Http3StreamSender {
    /// Progressively sends a chunk of request DATA frame to the upstream HTTP/3 stream.
    #[inline]
    pub async fn send_chunk(&self, chunk: bytes::Bytes) -> Result<(), Http3Error> {
        self.command_tx
            .send(ClientCommand::SendChunk {
                stream_id: self.stream_id,
                chunk,
                is_fin: false,
            })
            .await
            .map_err(|_| Http3Error::ConnectionClosed)
    }

    /// Finishes the upstream HTTP/3 request stream (emits FIN bit).
    #[inline]
    pub async fn finish(&self) -> Result<(), Http3Error> {
        self.command_tx
            .send(ClientCommand::SendChunk {
                stream_id: self.stream_id,
                chunk: bytes::Bytes::new(),
                is_fin: true,
            })
            .await
            .map_err(|_| Http3Error::ConnectionClosed)
    }
}

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

    /// Dispatches an owned [`L7Request`] over a new multiplexed bidirectional stream, returning
    /// a progressive streaming response handle [`Http3StreamResponse`].
    pub async fn send_stream_request(
        &self,
        req: L7Request,
    ) -> Result<Http3StreamResponse, Http3Error> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.command_tx
            .send(ClientCommand::SendStreamRequest {
                request: req,
                reply_tx,
            })
            .await
            .map_err(|_| Http3Error::ConnectionClosed)?;

        reply_rx.await.map_err(|_| Http3Error::ConnectionClosed)?
    }

    /// Initiates a progressive bidirectional stream on this HTTP/3 connection.
    ///
    /// Sends the request headers frame upstream immediately and returns a pair:
    /// `(Http3StreamSender, oneshot::Receiver<Result<Http3StreamResponse, Http3Error>>)`
    /// - `Http3StreamSender` allows streaming request body chunks to upstream immediately.
    /// - The receiver yields the upstream [`Http3StreamResponse`] once response headers arrive.
    pub async fn open_stream(
        &self,
        method: http::Method,
        uri: http::Uri,
        headers: http::HeaderMap,
    ) -> Result<
        (
            Http3StreamSender,
            oneshot::Receiver<Result<Http3StreamResponse, Http3Error>>,
        ),
        Http3Error,
    > {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.command_tx
            .send(ClientCommand::OpenStream {
                method,
                uri,
                headers,
                reply_tx,
            })
            .await
            .map_err(|_| Http3Error::ConnectionClosed)?;

        reply_rx.await.map_err(|_| Http3Error::ConnectionClosed)?
    }

    /// Dispatches an upstream [`Http3ClientRequest`] over a new multiplexed bidirectional stream on this connection.
    pub async fn send_client_request(
        &self,
        req: super::request::Http3ClientRequest,
    ) -> Result<super::response::Http3ClientResponse, Http3Error> {
        let l7_req = req.into_l7();
        let l7_resp = self.send_request(l7_req).await?;
        Ok(super::response::Http3ClientResponse::from_l7(l7_resp))
    }

    /// Dispatches a borrowed [`L7Request`] by cloning it, useful when the caller retains ownership.
    #[inline]
    pub async fn send_request_ref(&self, req: &L7Request) -> Result<L7Response, Http3Error> {
        self.send_request(req.clone()).await
    }

    /// Dispatches a borrowed [`L7Request`] for streaming response by cloning it.
    #[inline]
    pub async fn send_stream_request_ref(
        &self,
        req: &L7Request,
    ) -> Result<Http3StreamResponse, Http3Error> {
        self.send_stream_request(req.clone()).await
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
    #[allow(dead_code)]
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

/// Configures tuned kernel buffers and opportunistic acceleration flags on an egress UDP socket.
///
/// Implements continuous safe memory budgeting (clamped between 512KB and 16MB) based on
/// host/cgroup RAM, avoiding rigid tier classification and adapting seamlessly to any VPS or bare-metal host.
///
/// Opportunistically enables Linux kernel UDP acceleration (`UDP_GRO`, `SO_RCVBUFFORCE`).
/// In all cases, failure (e.g. unprivileged container, older kernel) gracefully falls back
/// to standard unaccelerated socket options without interrupting connection establishment.
fn tune_udp_socket(sock: &socket2::Socket) {
    let topology = velda_core::global_hardware_topology();
    let total_ram = topology.memory.total_bytes;
    // 1/64 of available host/cgroup RAM, clamped between 512KB and 16MB.
    let desired_buf = ((total_ram / 64) as usize).clamp(512 * 1024, 16 * 1024 * 1024);

    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let fd = sock.as_raw_fd();

        // 1. Opportunistic privileged force-buffers (SO_RCVBUFFORCE / SO_SNDBUFFORCE)
        // If CAP_NET_ADMIN is present, bypasses sysctl rmem_max / wmem_max constraints.
        // Otherwise gracefully falls back to standard SO_RCVBUF / SO_SNDBUF (kernel auto-clamps to rmem_max).
        let buf_size_c = desired_buf as libc::c_int;
        unsafe {
            let ret_rcv = libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_RCVBUFFORCE,
                &buf_size_c as *const _ as *const libc::c_void,
                std::mem::size_of_val(&buf_size_c) as libc::socklen_t,
            );
            if ret_rcv != 0 {
                let _ = sock.set_recv_buffer_size(desired_buf);
            }

            let ret_snd = libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_SNDBUFFORCE,
                &buf_size_c as *const _ as *const libc::c_void,
                std::mem::size_of_val(&buf_size_c) as libc::socklen_t,
            );
            if ret_snd != 0 {
                let _ = sock.set_send_buffer_size(desired_buf);
            }
        }

        // 2. Opportunistic UDP Generic Receive Offload (UDP_GRO, Linux 5.0+)
        // Fast-path: Coalesces multiple incoming UDP datagrams into single batches, dramatically reducing
        // userspace recvfrom syscall rate.
        // Fallback: If unsupported by kernel or denied by permissions, gracefully falls back to disabled.
        if topology.kernel.supports_udp_gro() {
            const UDP_GRO: libc::c_int = 104;
            let gro_val: libc::c_int = 1;
            unsafe {
                let ret = libc::setsockopt(
                    fd,
                    libc::IPPROTO_UDP,
                    UDP_GRO,
                    &gro_val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&gro_val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(
                        errno = std::io::Error::last_os_error().raw_os_error(),
                        "Kernel UDP_GRO not supported or denied, gracefully fell back to disabled"
                    );
                }
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = sock.set_recv_buffer_size(desired_buf);
        let _ = sock.set_send_buffer_size(desired_buf);
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
    // 1. Bind local UDP egress socket with tuned kernel buffers and graceful acceleration
    let domain = if target.is_ipv4() {
        socket2::Domain::IPV4
    } else {
        socket2::Domain::IPV6
    };
    let sock = socket2::Socket::new(domain, socket2::Type::DGRAM, None).map_err(Http3Error::Io)?;
    tune_udp_socket(&sock);
    sock.set_nonblocking(true).map_err(Http3Error::Io)?;
    let bind_addr: SocketAddr = if target.is_ipv4() {
        "0.0.0.0:0".parse().unwrap()
    } else {
        "[::]:0".parse().unwrap()
    };
    sock.bind(&bind_addr.into()).map_err(Http3Error::Io)?;
    let std_socket: std::net::UdpSocket = sock.into();
    let socket = Arc::new(UdpSocket::from_std(std_socket).map_err(Http3Error::Io)?);

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
    let (command_tx, command_rx) = mpsc::channel(4096);
    tokio::spawn(run_client_driver(
        socket,
        endpoint,
        conn,
        handle,
        command_rx,
        command_tx.clone(),
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

pub use super::header::{extract_sni, resolve_sni, strip_port};

/// Builds default QUIC client configuration for HTTP/3 upstream.
pub fn default_client_config() -> Result<ClientConfig, Http3Error> {
    let rustls_client = velda_tls::client::build_insecure_tls13_client_config(vec![b"h3".to_vec()]);
    velda_tls::client::build_quic_client_config(rustls_client)
        .map_err(|e| Http3Error::H3(format!("QUIC crypto config error: {e}")))
}
