//! Upstream gRPC client connector for `velda-upstream` and connection pool.
//!
//! Owns connection establishment, HTTP/2 client framing, and both Unary
//! and Streaming request dispatch to physical backend endpoints.

use bytes::{Bytes, BytesMut};
use h2::SendStream;
use h2::client::{ResponseFuture, SendRequest};
use http::Version;
use std::net::SocketAddr;
use velda_core::{Body, L7Request, L7Response};
use velda_tls::TlsClientEngine;

use crate::config::GrpcConfig;
use crate::error::GrpcError;

/// Pre-compiled Linux socket acceleration path for gRPC backend client streams.
///
/// Pre-computed at bootstrap to keep connection establishment on a branchless hot path
/// without repeated hardware or kernel capability checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrpcAccelerationPath {
    /// Disables Nagle's algorithm (`TCP_NODELAY`) to eliminate 40ms delayed-ACK packet coalescing delays.
    pub nodelay: bool,
    /// Sends ClientHello directly in SYN packet (`TCP_FASTOPEN_CONNECT`), shaving 1 full RTT
    /// off TLS handshakes. Only enabled for TLS where replay protection is cryptographic.
    pub fastopen: bool,
    /// Caps unsent bytes in the socket write queue (`TCP_NOTSENT_LOWAT`).
    ///
    /// In gRPC, multiple RPC streams multiplex over a single HTTP/2 connection. Capping unsent
    /// bytes ensures kernel buffers do not inflate, preserving user-space RPC deadline and priority ordering.
    pub notsent_lowat: Option<u32>,
    /// Aborts stuck TCP streams (`TCP_USER_TIMEOUT`) when cloud NAT gateways or firewalls silently
    /// drop packets, replacing the default 15-minute OS retransmit timeout with upstream deadline.
    pub user_timeout: Option<std::time::Duration>,
    /// Periodically probes idle connections (`SO_KEEPALIVE` + `TCP_KEEPIDLE`) to keep stateful
    /// middlebox / NAT table entries warm and detect silent peer reboots.
    pub keepalive: Option<std::time::Duration>,
    /// Low-latency socket polling in kernel space (`SO_BUSY_POLL`) to bypass epoll sleep/wake
    /// context-switch latency spikes on high-core server tiers.
    pub busy_poll_us: Option<u32>,
}

impl Default for GrpcAccelerationPath {
    fn default() -> Self {
        Self {
            nodelay: true,
            fastopen: false,
            notsent_lowat: None,
            user_timeout: None,
            keepalive: None,
            busy_poll_us: None,
        }
    }
}

impl GrpcAccelerationPath {
    /// Pre-compiles gRPC socket acceleration path from hardware topology, timeouts, and TLS status.
    ///
    /// Fast Open is only enabled for TLS endpoints because raw HTTP requests (like POST) are not
    /// idempotent and could suffer from TCP retransmission replays.
    pub fn for_topology(
        topo: &velda_core::HardwareTopology,
        connect_timeout: std::time::Duration,
        idle_timeout: std::time::Duration,
        is_tls: bool,
    ) -> Self {
        let fastopen = is_tls && topo.kernel.supports_tcp_fastopen_connect();

        let notsent_lowat = if topo.kernel.supports_tcp_notsent_lowat() {
            Some(notsent_lowat_for_mem_tier(topo.memory_tier()))
        } else {
            None
        };

        let user_timeout = if topo.kernel.supports_tcp_user_timeout() {
            let candidate = connect_timeout.saturating_mul(3);
            let cap = idle_timeout.min(std::time::Duration::from_secs(30));
            Some(candidate.max(cap).max(std::time::Duration::from_secs(10)))
        } else {
            None
        };

        let keepalive = Some(idle_timeout / 2);

        // Enable SO_BUSY_POLL only on large multi-core hardware tiers where dedicated core polling
        // yields sub-millisecond tail latency wins without starving small-tier CPU budgets.
        let busy_poll_us = if topo.kernel.supports_busy_poll()
            && matches!(
                topo.cpu_tier(),
                velda_core::CpuTier::Large
                    | velda_core::CpuTier::XLarge
                    | velda_core::CpuTier::TwoXLarge
                    | velda_core::CpuTier::Ultra
            ) {
            Some(50)
        } else {
            None
        };

        Self {
            nodelay: true,
            fastopen,
            notsent_lowat,
            user_timeout,
            keepalive,
            busy_poll_us,
        }
    }

    /// Applies pre-connect socket acceleration options (e.g. `TCP_FASTOPEN_CONNECT`).
    ///
    /// Unprivileged containers (Docker default seccomp, K8s unprivileged pods) often block
    /// Fast Open with `EPERM` or `ENOPROTOOPT`. Logging at trace level and proceeding ensures
    /// traffic still flows without hard connection failures.
    pub fn apply_pre_connect(&self, fd: std::os::unix::io::RawFd) {
        #[cfg(target_os = "linux")]
        if self.fastopen {
            let val: libc::c_int = 1;
            unsafe {
                let ret = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_FASTOPEN_CONNECT,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(
                        errno = std::io::Error::last_os_error().raw_os_error(),
                        "TCP_FASTOPEN_CONNECT not supported by kernel or denied in container; skipping"
                    );
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        let _ = fd;
    }

    /// Applies post-connect socket acceleration options (`TCP_NODELAY`, `TCP_NOTSENT_LOWAT`,
    /// `TCP_USER_TIMEOUT`, `SO_KEEPALIVE`, `TCP_KEEPIDLE`, `SO_BUSY_POLL`).
    ///
    /// Invoked immediately after connection handshake. Any option rejected by host seccomp
    /// or legacy kernel is skipped at trace level rather than dropping an established backend stream.
    pub fn apply_post_connect(&self, fd: std::os::unix::io::RawFd) {
        #[cfg(target_os = "linux")]
        unsafe {
            if let Some(lowat) = self.notsent_lowat {
                let val = lowat as libc::c_uint;
                let ret = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_NOTSENT_LOWAT,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(error = %std::io::Error::last_os_error(), "TCP_NOTSENT_LOWAT skipped");
                }
            }

            if let Some(user_timeout) = self.user_timeout {
                let val = user_timeout.as_millis() as libc::c_uint;
                let ret = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_USER_TIMEOUT,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(error = %std::io::Error::last_os_error(), "TCP_USER_TIMEOUT skipped");
                }
            }

            if let Some(keepalive) = self.keepalive {
                let val: libc::c_int = 1;
                let _ = libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_KEEPALIVE,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );

                let idle_secs = keepalive.as_secs().max(1) as libc::c_int;
                let _ = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_KEEPIDLE,
                    &idle_secs as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&idle_secs) as libc::socklen_t,
                );
            }

            if let Some(busy_poll) = self.busy_poll_us {
                let val = busy_poll as libc::c_int;
                let ret = libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_BUSY_POLL,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(error = %std::io::Error::last_os_error(), "SO_BUSY_POLL skipped (unprivileged container)");
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        let _ = fd;
    }
}

/// Maps a [`velda_core::MemoryTier`] to a `TCP_NOTSENT_LOWAT` byte threshold.
///
/// Thresholds are scaled to memory tier with a 16KB minimum:
/// - Setting the threshold too low (< 16KB) starves NIC TCP Segmentation Offload (TSO) and
///   causes epoll to wake on almost every single packet, degrading CPU cache locality.
/// - Setting it too high (> 128KB) causes bufferbloat in the kernel socket buffer and defeats
///   multiplexed stream prioritization.
pub const fn notsent_lowat_for_mem_tier(tier: velda_core::MemoryTier) -> u32 {
    use velda_core::MemoryTier;
    const KB: u32 = 1024;
    match tier {
        MemoryTier::Constrained | MemoryTier::Small => 16 * KB,
        MemoryTier::Medium => 32 * KB,
        MemoryTier::Large | MemoryTier::XLarge => 64 * KB,
        MemoryTier::TwoXLarge | MemoryTier::Ultra => 128 * KB,
    }
}

/// Active gRPC client connector to an upstream backend endpoint.
#[derive(Clone)]
pub struct GrpcUpstreamConnector {
    send_request: SendRequest<Bytes>,
}

impl GrpcUpstreamConnector {
    /// Polls or awaits readiness of the underlying HTTP/2 connection to accept a new request stream.
    pub async fn ready(&mut self) -> Result<(), GrpcError> {
        let ready_send = self
            .send_request
            .clone()
            .ready()
            .await
            .map_err(GrpcError::H2)?;
        self.send_request = ready_send;
        Ok(())
    }

    /// Establishes a new HTTP/2 connection to the upstream target backend endpoint
    /// applying limits from [`GrpcConfig`], optional socket acceleration path, optional TLS,
    /// and connection timeout.
    ///
    /// `tls` is `Some((engine, sni))` for TLS upstreams; the SNI is the upstream's pre-compiled
    /// static property. gRPC is always HTTP/2 as declared by the upstream: ALPN is only validated
    /// (a negotiated value other than `h2` is rejected), never used to choose the protocol.
    pub async fn connect(
        target: SocketAddr,
        tls: Option<(&TlsClientEngine, &str)>,
        config: &GrpcConfig,
        acceleration: Option<&GrpcAccelerationPath>,
        timeout: Option<std::time::Duration>,
    ) -> Result<Self, GrpcError> {
        let socket = if target.is_ipv4() {
            tokio::net::TcpSocket::new_v4().map_err(GrpcError::Io)?
        } else {
            tokio::net::TcpSocket::new_v6().map_err(GrpcError::Io)?
        };

        #[cfg(target_os = "linux")]
        if let Some(accel) = acceleration {
            use std::os::unix::io::AsRawFd;
            accel.apply_pre_connect(socket.as_raw_fd());
        }

        let connect_fut = socket.connect(target);
        let stream = if let Some(to) = timeout {
            tokio::time::timeout(to, connect_fut)
                .await
                .map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::TimedOut, "TCP connect timed out")
                })
                .map_err(GrpcError::Io)?
                .map_err(GrpcError::Io)?
        } else {
            connect_fut.await.map_err(GrpcError::Io)?
        };

        if let Some(accel) = acceleration {
            let _ = stream.set_nodelay(accel.nodelay);
            #[cfg(target_os = "linux")]
            {
                use std::os::unix::io::AsRawFd;
                accel.apply_post_connect(stream.as_raw_fd());
            }
        } else {
            let _ = stream.set_nodelay(true);
        }

        let mut builder = h2::client::Builder::default();
        builder.max_header_list_size(config.max_header_size as u32);

        let send_request = if let Some((engine, sni)) = tls {
            let tls_stream = engine
                .connect(sni, stream)
                .await
                .map_err(|e| GrpcError::Tls(e.to_string()))?;
            if let Some(alpn) = tls_stream.get_ref().1.alpn_protocol()
                && alpn != b"h2"
            {
                return Err(GrpcError::Tls(format!(
                    "upstream negotiated ALPN {:?}, expected \"h2\"",
                    String::from_utf8_lossy(alpn)
                )));
            }
            let (send_request, connection) = builder
                .handshake::<_, Bytes>(tls_stream)
                .await
                .map_err(GrpcError::H2)?;
            // Drive background H2 connection management
            tokio::spawn(async move {
                let _ = connection.await;
            });
            send_request
        } else {
            let (send_request, connection) = builder
                .handshake::<_, Bytes>(stream)
                .await
                .map_err(GrpcError::H2)?;
            // Drive background H2 connection management
            tokio::spawn(async move {
                let _ = connection.await;
            });
            send_request
        };

        Ok(Self { send_request })
    }

    /// Submits a raw streaming gRPC request to the upstream backend.
    ///
    /// Returns the upstream `ResponseFuture` and an active `SendStream` for streaming data chunks.
    pub fn open_stream(
        &mut self,
        request: http::Request<()>,
        end_of_stream: bool,
    ) -> Result<(ResponseFuture, SendStream<Bytes>), GrpcError> {
        self.send_request
            .send_request(request, end_of_stream)
            .map_err(GrpcError::H2)
    }

    /// Invokes a Unary gRPC request and returns the upstream `L7Response`.
    ///
    /// Seamlessly forwards request payload, waits for response headers, reads response LPM frame,
    /// and collects trailers (`grpc-status`). Enforces `max_message_size` from [`GrpcConfig`]
    /// on the upstream response body.
    pub async fn invoke_unary(
        &mut self,
        req: &L7Request,
        config: &GrpcConfig,
    ) -> Result<L7Response, GrpcError> {
        let mut request_builder = http::Request::builder()
            .method(&req.method)
            .uri(&req.uri)
            .version(Version::HTTP_2);

        for (name, val) in &req.headers {
            request_builder = request_builder.header(name, val);
        }

        let end_of_stream = !req.has_body();
        let http_req = request_builder.body(()).map_err(GrpcError::Http)?;

        let (response_future, mut send_stream) = self.open_stream(http_req, end_of_stream)?;

        if let Body::Bytes(ref data) = req.body {
            send_stream
                .send_data(data.clone(), true)
                .map_err(GrpcError::H2)?;
        }

        let response = response_future.await.map_err(GrpcError::H2)?;
        let (parts, mut body_stream) = response.into_parts();
        let max_body = config.max_message_size;

        // Zero-allocation fast path for single-chunk Unary responses
        let body = if body_stream.is_end_stream() {
            Body::Empty
        } else if let Some(first_chunk) = body_stream.data().await {
            let chunk = first_chunk.map_err(GrpcError::H2)?;
            let len = chunk.len();
            if len > max_body {
                let _ = body_stream.flow_control().release_capacity(len);
                return Err(GrpcError::PayloadTooLarge(len));
            }
            let _ = body_stream.flow_control().release_capacity(len);

            if body_stream.is_end_stream() {
                Body::Bytes(chunk)
            } else {
                let mut resp_body = BytesMut::with_capacity(len * 2);
                resp_body.extend_from_slice(&chunk);

                while let Some(chunk_res) = body_stream.data().await {
                    let chunk = chunk_res.map_err(GrpcError::H2)?;
                    let len = chunk.len();
                    if resp_body.len() + len > max_body {
                        let _ = body_stream.flow_control().release_capacity(len);
                        return Err(GrpcError::PayloadTooLarge(resp_body.len() + len));
                    }
                    resp_body.extend_from_slice(&chunk);
                    let _ = body_stream.flow_control().release_capacity(len);
                }

                Body::Bytes(resp_body.freeze())
            }
        } else {
            Body::Empty
        };

        let mut headers = parts.headers;
        if let Some(trailers) = body_stream.trailers().await.map_err(GrpcError::H2)? {
            for (name, val) in trailers {
                if let Some(name) = name {
                    headers.append(name, val);
                }
            }
        }

        Ok(L7Response::new(
            parts.status,
            Version::HTTP_2,
            headers,
            body,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_notsent_lowat_scaling() {
        use velda_core::MemoryTier;
        assert_eq!(
            notsent_lowat_for_mem_tier(MemoryTier::Constrained),
            16 * 1024
        );
        assert_eq!(notsent_lowat_for_mem_tier(MemoryTier::Medium), 32 * 1024);
        assert_eq!(notsent_lowat_for_mem_tier(MemoryTier::Large), 64 * 1024);
        assert_eq!(notsent_lowat_for_mem_tier(MemoryTier::Ultra), 128 * 1024);
    }

    #[test]
    fn test_grpc_acceleration_path_for_topology() {
        let topo = velda_core::global_hardware_topology();
        let connect_to = std::time::Duration::from_millis(500);
        let idle_to = std::time::Duration::from_secs(30);

        let accel_plain = GrpcAccelerationPath::for_topology(topo, connect_to, idle_to, false);
        assert!(accel_plain.nodelay);
        assert!(!accel_plain.fastopen);

        let accel_tls = GrpcAccelerationPath::for_topology(topo, connect_to, idle_to, true);
        assert!(accel_tls.nodelay);
        #[cfg(target_os = "linux")]
        {
            if topo.kernel.supports_tcp_fastopen_connect() {
                assert!(accel_tls.fastopen);
            }
        }
    }
}
