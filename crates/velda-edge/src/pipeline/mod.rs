//! Flat protocol pipelines: each module represents an isolated, self-contained protocol pipeline.

pub mod grpc;
pub mod http1;
pub mod http2;
pub mod http3;
pub mod l4;
pub mod mirror;

pub use l4::{tcp, udp};

/// Canonical request correlation ID header propagated across downstream pipelines.
pub use velda_core::buffer::header::X_REQUEST_ID;

/// Zero-allocation scalar transfer summary returned on stack by isolated protocol pipes
/// to their `downstream.rs` workflow owner for metrics and access log emission.
#[derive(Debug, Clone, Copy)]
pub struct PipeOutcome {
    pub bytes_rx: u64,
    pub bytes_tx: u64,
    pub upstream_addr: Option<(u128, u16)>,
    pub grpc_status: velda_grpc::GrpcStatus,
    pub internal_status: velda_core::InternalStatus,
}

impl PipeOutcome {
    /// Constructs a standard HTTP pipe outcome.
    #[inline(always)]
    pub const fn http(
        bytes_rx: u64,
        bytes_tx: u64,
        upstream_addr: Option<(u128, u16)>,
        internal_status: velda_core::InternalStatus,
    ) -> Self {
        Self {
            bytes_rx,
            bytes_tx,
            upstream_addr,
            grpc_status: velda_grpc::GrpcStatus::Ok,
            internal_status,
        }
    }

    /// Constructs a gRPC pipe outcome carrying canonical wire `grpc-status`.
    #[inline(always)]
    pub const fn grpc(
        grpc_status: velda_grpc::GrpcStatus,
        bytes_rx: u64,
        bytes_tx: u64,
        upstream_addr: Option<(u128, u16)>,
        internal_status: velda_core::InternalStatus,
    ) -> Self {
        Self {
            bytes_rx,
            bytes_tx,
            upstream_addr,
            grpc_status,
            internal_status,
        }
    }
}

/// Maps an HTTP response status (e.g., from a plugin `Action::Respond`) to canonical [`velda_core::InternalStatus`].
#[inline(always)]
pub const fn internal_status_from_http(status: http::StatusCode) -> velda_core::InternalStatus {
    use http::StatusCode;
    use velda_core::InternalStatus;

    if status.as_u16() < 400 {
        return InternalStatus::Completed;
    }
    match status {
        StatusCode::BAD_REQUEST => InternalStatus::BadRequest,
        StatusCode::UNAUTHORIZED => InternalStatus::Unauthorized,
        StatusCode::FORBIDDEN => InternalStatus::Forbidden,
        StatusCode::NOT_FOUND => InternalStatus::RouteNotFound,
        StatusCode::METHOD_NOT_ALLOWED => InternalStatus::MethodNotAllowed,
        StatusCode::REQUEST_TIMEOUT => InternalStatus::RequestTimeout,
        StatusCode::CONFLICT => InternalStatus::Conflict,
        StatusCode::PAYLOAD_TOO_LARGE => InternalStatus::PayloadTooLarge,
        StatusCode::URI_TOO_LONG => InternalStatus::UriTooLong,
        StatusCode::UNSUPPORTED_MEDIA_TYPE => InternalStatus::UnsupportedMediaType,
        StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE => InternalStatus::HeaderFieldsTooLarge,
        StatusCode::TOO_MANY_REQUESTS => InternalStatus::RateLimited,
        StatusCode::BAD_GATEWAY => InternalStatus::UpstreamError,
        StatusCode::SERVICE_UNAVAILABLE => InternalStatus::ServiceUnavailable,
        StatusCode::GATEWAY_TIMEOUT => InternalStatus::UpstreamTimeout,
        _ if status.as_u16() < 500 => InternalStatus::BadRequest,
        _ => InternalStatus::InternalError,
    }
}

/// Maps a canonical gRPC status (e.g., from a plugin `Action::Respond`) to [`velda_core::InternalStatus`].
#[inline(always)]
pub const fn internal_status_from_grpc(
    status: velda_grpc::GrpcStatus,
) -> velda_core::InternalStatus {
    use velda_core::InternalStatus;
    use velda_grpc::GrpcStatus;

    match status {
        GrpcStatus::Ok => InternalStatus::Completed,
        GrpcStatus::Cancelled => InternalStatus::ClientCanceled,
        GrpcStatus::InvalidArgument | GrpcStatus::OutOfRange => InternalStatus::BadRequest,
        GrpcStatus::DeadlineExceeded => InternalStatus::RequestTimeout,
        GrpcStatus::NotFound => InternalStatus::RouteNotFound,
        GrpcStatus::AlreadyExists | GrpcStatus::FailedPrecondition | GrpcStatus::Aborted => {
            InternalStatus::Conflict
        }
        GrpcStatus::PermissionDenied => InternalStatus::Forbidden,
        GrpcStatus::ResourceExhausted => InternalStatus::RateLimited,
        GrpcStatus::Unimplemented => InternalStatus::MethodNotAllowed,
        GrpcStatus::Unavailable => InternalStatus::ServiceUnavailable,
        GrpcStatus::Unauthenticated => InternalStatus::Unauthorized,
        GrpcStatus::Unknown | GrpcStatus::Internal | GrpcStatus::DataLoss => {
            InternalStatus::InternalError
        }
    }
}

/// Zero-cost RAII guard tracking global L4 TCP connection open/close lifecycle.
pub struct ConnMetricGuard;

impl ConnMetricGuard {
    #[inline(always)]
    pub fn enter() -> Self {
        velda_observability::global_metrics()
            .global()
            .connection_opened();
        Self
    }
}

impl Drop for ConnMetricGuard {
    #[inline(always)]
    fn drop(&mut self) {
        velda_observability::global_metrics()
            .global()
            .connection_closed();
    }
}

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use velda_sock::{Connection, Datagram, UdpSocket};

use crate::runtime::SharedRuntime;
use crate::runtime::pipeline::{TcpPipeline, UdpPipeline};

/// Downstream connection metadata capturing network endpoints and listener identity.
#[derive(Debug, Clone)]
pub struct DownstreamMeta {
    pub listener_id: Arc<str>,
    pub peer_ip: u128,
    pub peer_port_be: u16,
    pub local_ip: u128,
    pub local_port_be: u16,
    pub is_tls: bool,
    pub ja4: u64,
    pub tls_traits: u16,
    pub client_ip_header: http::HeaderValue,
    pub listener_port_header: http::HeaderValue,
    pub alt_svc_header: Option<http::HeaderValue>,
    pub slab_pool: Arc<velda_core::buffer::slab::PayloadSlabPool>,
}

impl DownstreamMeta {
    /// Creates and pre-computes downstream metadata and static header values once per TCP connection.
    pub fn new(
        listener_id: Arc<str>,
        peer: SocketAddr,
        local_addr: SocketAddr,
        is_tls: bool,
        alt_svc_header: Option<http::HeaderValue>,
        buffer_pool: &velda_core::buffer::pool::ShardedBufferPool,
    ) -> Self {
        let (peer_ip, peer_port_be) = velda_sock::socket_addr_to_raw(peer);
        let (local_ip, local_port_be) = velda_sock::socket_addr_to_raw(local_addr);
        let (ip_buf, ip_len) = velda_core::lut::ip::format_ip_bytes(peer_ip);
        let client_ip_header = http::HeaderValue::from_bytes(&ip_buf[..ip_len])
            .unwrap_or_else(|_| http::HeaderValue::from_static("127.0.0.1"));
        let listener_port_header = http::HeaderValue::from(local_addr.port());
        let slab_pool = Arc::clone(
            &buffer_pool
                .core_pool(velda_connection_pool::current_thread_lane())
                .slabs,
        );
        Self {
            listener_id,
            peer_ip,
            peer_port_be,
            local_ip,
            local_port_be,
            is_tls,
            ja4: 0,
            tls_traits: 0,
            client_ip_header,
            listener_port_header,
            alt_svc_header,
            slab_pool,
        }
    }

    /// Attaches 64-bit JA4 fingerprint and 16-bit structural `tls_traits` extracted from ClientHello.
    #[inline(always)]
    pub fn with_tls_signals(mut self, ja4: u64, tls_traits: u16) -> Self {
        self.ja4 = ja4;
        self.tls_traits = tls_traits;
        self
    }

    /// Injects downstream connection-level headers (e.g. Alt-Svc) into response headers if not already present.
    #[inline]
    pub fn inject_response_headers(&self, headers: &mut velda_core::HeaderWorkspace) {
        if let Some(ref alt_svc) = self.alt_svc_header
            && !headers.contains_key(&http::header::ALT_SVC)
        {
            headers.insert_bypass(http::header::ALT_SVC, alt_svc.clone());
        }
    }
}

/// Boxed thread-safe future alias for pre-bound pipeline runners.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Dedicated, specialized pipeline runner pre-bound to a specific TCP listener.
pub type DedicatedTcpRunner = Arc<dyn Fn(Connection) -> BoxFuture<'static, ()> + Send + Sync>;

/// Dedicated, specialized pipeline runner pre-bound to a specific UDP listener.
pub type DedicatedUdpRunner =
    Arc<dyn Fn(Arc<str>, Arc<UdpSocket>, Datagram) -> BoxFuture<'static, ()> + Send + Sync>;

/// Pre-resolves and compiles a dedicated TCP pipeline runner for the specified listener.
///
/// Executed strictly on the cold configuration path (during listener bind / reconcile),
/// completely eliminating runtime HashMap lookups, Arc cloning, and enum pattern matching
/// from the connection accept loop.
pub fn build_tcp_pipeline_runner(listener_id: &str, runtime: SharedRuntime) -> DedicatedTcpRunner {
    let lid: Arc<str> = Arc::from(listener_id);
    let pipeline = {
        let rt = runtime.load();
        rt.pipelines
            .tcp_pipeline(velda_core::fnv1a_hash_32(listener_id))
            .cloned()
    };

    match pipeline {
        Some(TcpPipeline::Http1 {
            config,
            tls_enabled,
            streaming,
            alt_svc,
            sampling,
        }) => {
            let lid = Arc::clone(&lid);
            Arc::new(move |connection: Connection| {
                let rt = runtime.clone();
                let lid = Arc::clone(&lid);
                let alt_svc = alt_svc.clone();
                Box::pin(async move {
                    http1::downstream::handle_http1_stream(
                        connection,
                        lid,
                        http1::downstream::Http1IngressConfig {
                            config,
                            tls_enabled,
                            streaming,
                            alt_svc,
                            sampling,
                        },
                        rt,
                    )
                    .await;
                })
            })
        }
        Some(TcpPipeline::Http2 {
            config,
            tls_enabled,
            streaming: _,
            alt_svc,
            sampling,
        }) => {
            let lid = Arc::clone(&lid);
            Arc::new(move |connection: Connection| {
                let rt = runtime.clone();
                let lid = Arc::clone(&lid);
                let alt_svc = alt_svc.clone();
                Box::pin(async move {
                    http2::handle_http2_stream(
                        connection,
                        lid,
                        config,
                        tls_enabled,
                        alt_svc,
                        sampling,
                        rt,
                    )
                    .await;
                })
            })
        }
        Some(TcpPipeline::HttpAuto {
            tls_enabled,
            streaming,
            http1_config,
            http2_config,
            alt_svc,
            sampling,
        }) => {
            let lid = Arc::clone(&lid);
            let auto_config = HttpAutoConfig {
                http1_config,
                http2_config,
                tls_enabled,
                streaming,
                alt_svc,
                sampling,
            };
            Arc::new(move |connection: Connection| {
                let rt = runtime.clone();
                let lid = Arc::clone(&lid);
                let auto_config = auto_config.clone();
                Box::pin(async move {
                    handle_http_auto_stream(connection, lid, auto_config, rt).await;
                })
            })
        }
        Some(TcpPipeline::Grpc {
            config,
            tls_enabled,
            streaming: _,
            sampling,
        }) => {
            let lid = Arc::clone(&lid);
            Arc::new(move |connection: Connection| {
                let rt = runtime.clone();
                let lid = Arc::clone(&lid);
                Box::pin(async move {
                    grpc::handle_grpc_tcp(connection, lid, config, tls_enabled, sampling, rt).await;
                })
            })
        }
        Some(TcpPipeline::Raw { config }) => {
            let lid = Arc::clone(&lid);
            Arc::new(move |connection: Connection| {
                let rt = runtime.load();
                let lid = Arc::clone(&lid);
                Box::pin(async move {
                    tcp::handle_l4_tcp(connection, lid, config, &rt).await;
                })
            })
        }
        None => {
            let lid = Arc::clone(&lid);
            Arc::new(move |connection: Connection| {
                let peer = velda_sock::format_raw_addr(connection.peer_ip, connection.peer_port_be);
                tracing::warn!(
                    listener = %lid,
                    peer = %peer,
                    "Unregistered TCP listener dropped (Fail-Closed)"
                );
                Box::pin(async move {})
            })
        }
    }
}

/// Pre-resolves and compiles a dedicated UDP pipeline runner for the specified listener.
///
/// Executed strictly on the cold configuration path (during socket bind / reconcile),
/// completely eliminating runtime HashMap lookups, Arc cloning, and enum pattern matching
/// from line-rate UDP datagram ingestion.
pub fn build_udp_pipeline_runner(listener_id: &str, runtime: SharedRuntime) -> DedicatedUdpRunner {
    let lid: Arc<str> = Arc::from(listener_id);
    let pipeline = {
        let rt = runtime.load();
        rt.pipelines
            .udp_pipeline(velda_core::fnv1a_hash_32(listener_id))
            .cloned()
    };

    match pipeline {
        Some(UdpPipeline::Http3 {
            config, sampling, ..
        }) => {
            let lid = Arc::clone(&lid);
            let workers = http3::get_or_init_h3_workers(&lid, &runtime);
            Arc::new(
                move |_id: Arc<str>, socket: Arc<UdpSocket>, datagram: Datagram| {
                    let rt = runtime.clone();
                    let lid = Arc::clone(&lid);
                    let workers = workers.clone();
                    Box::pin(async move {
                        http3::handle_http3_udp(
                            lid,
                            socket,
                            datagram,
                            config,
                            sampling,
                            &rt,
                            workers.as_ref(),
                        )
                        .await;
                    })
                },
            )
        }
        Some(UdpPipeline::Grpc {
            config, sampling, ..
        }) => {
            let lid = Arc::clone(&lid);
            Arc::new(
                move |_id: Arc<str>, socket: Arc<UdpSocket>, datagram: Datagram| {
                    let rt = runtime.clone();
                    let lid = Arc::clone(&lid);
                    Box::pin(async move {
                        grpc::handle_grpc_udp(lid, socket, datagram, config, sampling, &rt).await;
                    })
                },
            )
        }
        Some(UdpPipeline::Raw { config }) => {
            let lid = Arc::clone(&lid);
            Arc::new(
                move |_id: Arc<str>, socket: Arc<UdpSocket>, datagram: Datagram| {
                    let rt = runtime.clone();
                    let lid = Arc::clone(&lid);
                    Box::pin(async move {
                        let rt_guard = rt.load();
                        udp::handle_l4_udp(lid, socket, datagram, config, &rt_guard).await;
                    })
                },
            )
        }
        None => {
            let lid = Arc::clone(&lid);
            Arc::new(
                move |_id: Arc<str>, _socket: Arc<UdpSocket>, datagram: Datagram| {
                    let peer = velda_sock::format_raw_addr(datagram.peer_ip, datagram.peer_port_be);
                    tracing::warn!(
                        listener = %lid,
                        peer = %peer,
                        "Unregistered UDP listener datagram dropped (Fail-Closed)"
                    );
                    Box::pin(async move {})
                },
            )
        }
    }
}

/// Internal configuration bundle for the unified HTTP auto stream worker.
#[derive(Clone)]
struct HttpAutoConfig {
    http1_config: velda_http1::config::Http1Config,
    http2_config: velda_http2::config::Http2Config,
    tls_enabled: bool,
    streaming: velda_core::StreamingMode,
    alt_svc: Option<http::HeaderValue>,
    sampling: velda_observability::sampling::status::StatusSampling,
}

/// Unified HTTP stream worker for listeners declaring the `http` family.
///
/// Under TLS, negotiates ALPN strictly within the HTTP family ("h2" -> HTTP/2, "http/1.1" -> HTTP/1.1).
/// Under cleartext, falls back directly to the HTTP/1.1 stream loop.
async fn handle_http_auto_stream(
    connection: Connection,
    listener_id: Arc<str>,
    config: HttpAutoConfig,
    runtime: SharedRuntime,
) {
    if !config.tls_enabled {
        http1::downstream::handle_http1_stream(
            connection,
            listener_id,
            http1::downstream::Http1IngressConfig {
                config: config.http1_config,
                tls_enabled: false,
                streaming: config.streaming,
                alt_svc: config.alt_svc,
                sampling: config.sampling,
            },
            runtime,
        )
        .await;
        return;
    }

    let _conn_guard = ConnMetricGuard::enter();
    let rt = runtime.load();
    let peer = velda_sock::raw_to_socket_addr(connection.peer_ip, connection.peer_port_be);
    let local_addr = velda_sock::raw_to_socket_addr(connection.local_ip, connection.local_port_be);

    let Some(tls_server) = rt.tls_server.as_ref() else {
        tracing::error!(
            listener = %listener_id,
            peer = %peer,
            "TLS required for HTTP family listener, but no TLS server engine is compiled; dropping connection"
        );
        return;
    };

    match tls_server.accept(connection).await {
        Ok(velda_tls::AcceptedTlsStream {
            stream: tls_stream,
            ja4,
            tls_traits,
        }) => {
            velda_observability::global_metrics()
                .global()
                .tls_handshake_success();
            // Zero-allocation ALPN extraction avoiding SNI String and peer certificate cloning
            let alpn = velda_tls::TlsServerEngine::extract_alpn(&tls_stream);
            tracing::debug!(
                listener = %listener_id,
                peer = %peer,
                alpn = ?alpn,
                "Downstream TLS handshake succeeded for HTTP family"
            );

            let meta = DownstreamMeta::new(
                listener_id,
                peer,
                local_addr,
                true,
                config.alt_svc,
                &rt.buffer_pool,
            )
            .with_tls_signals(ja4, tls_traits);

            // ALPN demuxing strictly within the HTTP protocol family.
            // When ALPN is omitted by the client, default to HTTP/1.1 as per RFC 7301.
            match alpn.as_deref() {
                Some("h2") => {
                    http2::run_http2_loop(
                        tls_stream,
                        meta,
                        config.http2_config,
                        config.sampling,
                        runtime,
                    )
                    .await;
                }
                None | Some("http/1.1") => {
                    http1::downstream::run_http1_loop(
                        tls_stream,
                        meta,
                        config.streaming,
                        config.http1_config,
                        config.sampling,
                        runtime,
                    )
                    .await;
                }
                other_alpn => {
                    tracing::warn!(
                        listener = %meta.listener_id,
                        peer = %peer,
                        actual = ?other_alpn,
                        "Dropping connection: ALPN negotiated outside the HTTP protocol family or missing"
                    );
                }
            }
        }
        Err(e) => {
            velda_observability::global_metrics()
                .global()
                .tls_handshake_error();
            tracing::debug!(
                listener = %listener_id,
                peer = %peer,
                error = %e,
                "Downstream TLS handshake failed for HTTP family"
            );
        }
    }
}
