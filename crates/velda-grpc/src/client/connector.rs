//! Upstream gRPC client connector for `velda-upstream` and connection pool.
//!
//! Owns connection establishment, HTTP/2 client framing, and both Unary
//! and Streaming request dispatch to physical backend endpoints.

use bytes::{Bytes, BytesMut};
use h2::SendStream;
use h2::client::{Connection, ResponseFuture, SendRequest};
use http::Version;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use velda_core::{Body, L7Request, L7Response};

use crate::config::GrpcConfig;
use crate::error::GrpcError;

/// Upstream gRPC socket acceleration path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrpcAccelerationPath {
    /// Disable Nagle's algorithm (`TCP_NODELAY`). Defaults to `true`.
    pub nodelay: bool,
    /// Enable TCP Fast Open Connect (`TCP_FASTOPEN_CONNECT`).
    pub fastopen: bool,
    /// Sndbuf low-watermark threshold (`TCP_NOTSENT_LOWAT`) in bytes.
    pub notsent_lowat: Option<u32>,
    /// Maximum time that transmitted data may remain unacknowledged (`TCP_USER_TIMEOUT`).
    pub user_timeout: Option<std::time::Duration>,
    /// TCP Keep-Alive interval (`SO_KEEPALIVE` + `TCP_KEEPIDLE`).
    pub keepalive: Option<std::time::Duration>,
}

impl Default for GrpcAccelerationPath {
    fn default() -> Self {
        Self {
            nodelay: true,
            fastopen: false,
            notsent_lowat: None,
            user_timeout: None,
            keepalive: None,
        }
    }
}

impl GrpcAccelerationPath {
    /// Pre-compiles gRPC socket acceleration path from hardware topology, timeouts, and TLS status.
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

        Self {
            nodelay: true,
            fastopen,
            notsent_lowat,
            user_timeout,
            keepalive,
        }
    }
}

const fn notsent_lowat_for_mem_tier(tier: velda_core::MemoryTier) -> u32 {
    use velda_core::MemoryTier;
    const KB: u32 = 1024;
    match tier {
        MemoryTier::Constrained | MemoryTier::Small => 16 * KB,
        MemoryTier::Medium => 32 * KB,
        MemoryTier::Large | MemoryTier::XLarge => 64 * KB,
        MemoryTier::TwoXLarge | MemoryTier::Ultra => 128 * KB,
    }
}

#[cfg(target_os = "linux")]
fn apply_tcp_fastopen(fd: std::os::unix::io::RawFd) {
    let val: libc::c_int = 1;
    unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_FASTOPEN_CONNECT,
            &val as *const _ as *const libc::c_void,
            std::mem::size_of_val(&val) as libc::socklen_t,
        );
    }
}

#[cfg(target_os = "linux")]
fn apply_post_connect_options(fd: std::os::unix::io::RawFd, accel: &GrpcAccelerationPath) {
    unsafe {
        if let Some(lowat) = accel.notsent_lowat {
            let val = lowat as libc::c_uint;
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_NOTSENT_LOWAT,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of_val(&val) as libc::socklen_t,
            );
        }

        if let Some(user_timeout) = accel.user_timeout {
            let val = user_timeout.as_millis() as libc::c_uint;
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_USER_TIMEOUT,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of_val(&val) as libc::socklen_t,
            );
        }

        if let Some(keepalive) = accel.keepalive {
            let val: libc::c_int = 1;
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_KEEPALIVE,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of_val(&val) as libc::socklen_t,
            );

            let idle_secs = keepalive.as_secs().max(1) as libc::c_int;
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_KEEPIDLE,
                &idle_secs as *const _ as *const libc::c_void,
                std::mem::size_of_val(&idle_secs) as libc::socklen_t,
            );
        }
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
    /// applying limits from [`GrpcConfig`], optional socket acceleration path, and connection timeout.
    pub async fn connect(
        target: SocketAddr,
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
        if acceleration.is_some_and(|accel| accel.fastopen) {
            use std::os::unix::io::AsRawFd;
            apply_tcp_fastopen(socket.as_raw_fd());
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
                apply_post_connect_options(stream.as_raw_fd(), accel);
            }
        } else {
            let _ = stream.set_nodelay(true);
        }

        let mut builder = h2::client::Builder::default();
        builder.max_header_list_size(config.max_header_size as u32);

        let (send_request, connection): (SendRequest<Bytes>, Connection<TcpStream, Bytes>) =
            builder.handshake(stream).await.map_err(GrpcError::H2)?;

        // Drive background H2 connection management
        tokio::spawn(async move {
            let _ = connection.await;
        });

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
