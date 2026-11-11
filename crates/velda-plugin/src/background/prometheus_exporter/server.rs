//! Prometheus Exporter HTTP/1.1 & HTTPS Server (`prometheus_exporter::server`).
//!
//! Exposes OpenMetrics / Prometheus scrape endpoints (`GET /metrics`) and liveness probes (`GET /healthz`)
//! on a management network interface strictly separated from Data Plane traffic workers.
//!
//! # Microarchitectural Invariants
//! 1. **Zero Heap Allocation on Hot Scrape Loop**: Uses pre-allocated [`BufferPool`] and pre-baked
//!    [`PrometheusTemplate`] skeletons. Scrape bodies and headers require 0 dynamic heap allocations.
//! 2. **Shadow Worker Execution**: Operates strictly on Shadow Worker runtimes under `SCHED_IDLE`,
//!    never touching frontline traffic worker pins or causing cacheline invalidations.
//! 3. **Non-blocking Telemetry**: Extracts point-in-time atomics via lock-free [`MetricsStore::snapshot`].
//! 4. **Socket Safety Invariant**: Strictly uses [`velda_core::global_socket_manager`] via
//!    `acquire_shadow_tcp` and `release_shadow_tcp`.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use base64::prelude::*;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, watch};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;

use velda_observability::MetricsStore;
use velda_sync::PrometheusExporterConfig;
use velda_tls::ServerTlsConfig;

use super::lut::fast_write_u64_slice;
use super::template::PrometheusTemplate;
use crate::error::PluginError;

// ============================================================================
// Zero-Allocation Scratch Buffer Pool
// ============================================================================

/// Pre-allocated reusable scratch buffer pool for zero-allocation scrape processing.
pub struct BufferPool {
    pool: std::sync::Mutex<Vec<Vec<u8>>>,
    initial_capacity: usize,
}

impl BufferPool {
    /// Pre-allocates `capacity_count` buffers of `buffer_bytes` size each.
    pub fn new(capacity_count: usize, buffer_bytes: usize) -> Self {
        let pool = (0..capacity_count)
            .map(|_| Vec::with_capacity(buffer_bytes))
            .collect();
        Self {
            pool: std::sync::Mutex::new(pool),
            initial_capacity: buffer_bytes,
        }
    }

    /// Acquires an exclusive scratch buffer from the pool, creating one if empty.
    pub fn acquire(&self) -> PooledBuffer<'_> {
        let mut guard = self.pool.lock().unwrap();
        let buf = guard
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(self.initial_capacity));
        PooledBuffer {
            buf: Some(buf),
            pool: self,
        }
    }
}

/// RAII handle that automatically clears and returns a buffer to its owning [`BufferPool`] on drop.
pub struct PooledBuffer<'a> {
    buf: Option<Vec<u8>>,
    pool: &'a BufferPool,
}

impl<'a> std::ops::Deref for PooledBuffer<'a> {
    type Target = Vec<u8>;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.buf.as_ref().unwrap()
    }
}

impl<'a> std::ops::DerefMut for PooledBuffer<'a> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buf.as_mut().unwrap()
    }
}

impl<'a> Drop for PooledBuffer<'a> {
    fn drop(&mut self) {
        if let Some(mut buf) = self.buf.take() {
            buf.clear();
            if let Ok(mut guard) = self.pool.pool.lock() {
                guard.push(buf);
            }
        }
    }
}

// ============================================================================
// Transport Stream Abstraction (Plaintext vs TLS)
// ============================================================================

/// Unified transport stream for plaintext TCP or TLS connections.
enum ExporterStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl AsyncRead for ExporterStream {
    #[inline]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ExporterStream {
    #[inline]
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    #[inline]
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_flush(cx),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    #[inline]
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

// ============================================================================
// Prometheus Exporter Server
// ============================================================================

/// Standalone Prometheus Exporter service.
pub struct PrometheusExporter {
    config: PrometheusExporterConfig,
    metrics: Arc<MetricsStore>,
    tls_acceptor: Option<TlsAcceptor>,
    concurrency_limiter: Arc<Semaphore>,
    expected_auth_header: Option<String>,
    template: Arc<PrometheusTemplate>,
    buffer_pool: Arc<BufferPool>,
}

/// Canonical type alias adhering to `*Plugin` background plugin naming conventions.
pub type PrometheusExporterPlugin = PrometheusExporter;

impl PrometheusExporter {
    /// Constructs and initializes the Prometheus Exporter from validated configuration.
    pub fn new(
        config: PrometheusExporterConfig,
        metrics: Arc<MetricsStore>,
    ) -> Result<Self, PluginError> {
        let concurrency_limiter = Arc::new(Semaphore::new(config.scrape.max_concurrency));

        // Precompute expected Authorization header to eliminate per-request string formatting/allocations
        let expected_auth_header = match config.auth.mode.to_ascii_lowercase().as_str() {
            "bearer" => {
                let token = config.auth.token.as_deref().unwrap_or("").trim();
                Some(format!("Bearer {token}"))
            }
            "basic" => {
                let username = config.auth.username.as_deref().unwrap_or("").trim();
                let password = config.auth.password.as_deref().unwrap_or("").trim();
                let raw = format!("{username}:{password}");
                let encoded = BASE64_STANDARD.encode(raw.as_bytes());
                Some(format!("Basic {encoded}"))
            }
            _ => None,
        };

        // Initialize TLS engine if configured
        let tls_acceptor = if config.protocol.eq_ignore_ascii_case("https")
            || !config.tls.mode.eq_ignore_ascii_case("none")
        {
            let cert_path = config.tls.cert_file.as_deref().ok_or_else(|| {
                PluginError::Config("Missing cert_file in TLS exporter config".into())
            })?;
            let key_path = config.tls.key_file.as_deref().ok_or_else(|| {
                PluginError::Config("Missing key_file in TLS exporter config".into())
            })?;

            let cert_pem = std::fs::read_to_string(cert_path).map_err(|e| {
                PluginError::Tls(format!("Failed to read cert_file '{cert_path}': {e}"))
            })?;
            let key_pem = std::fs::read_to_string(key_path).map_err(|e| {
                PluginError::Tls(format!("Failed to read key_file '{key_path}': {e}"))
            })?;

            let client_ca_pem = if config.tls.mode.eq_ignore_ascii_case("mtls") {
                let ca_path = config.tls.client_ca_file.as_deref().ok_or_else(|| {
                    PluginError::Config("Missing client_ca_file in mTLS exporter config".into())
                })?;
                Some(std::fs::read_to_string(ca_path).map_err(|e| {
                    PluginError::Tls(format!("Failed to read client_ca_file '{ca_path}': {e}"))
                })?)
            } else {
                None
            };

            let server_tls = ServerTlsConfig {
                sni: vec!["*".to_string()],
                versions: vec!["tls1.2".into(), "tls1.3".into()],
                alpn: vec!["http/1.1".into()],
                cert_pem,
                key_pem,
                client_ca_pem,
            };

            let rustls_config = ServerTlsConfig::build(&[server_tls])
                .map_err(|e| PluginError::Tls(e.to_string()))?;
            Some(TlsAcceptor::from(rustls_config))
        } else {
            None
        };

        let template = Arc::new(PrometheusTemplate::new());
        let buffer_bytes = config.scrape.max_buffer_size_kb.max(32) * 1024;
        let buffer_pool = Arc::new(BufferPool::new(config.scrape.max_concurrency, buffer_bytes));

        Ok(Self {
            config,
            metrics,
            tls_acceptor,
            concurrency_limiter,
            expected_auth_header,
            template,
            buffer_pool,
        })
    }

    /// Deserializes and constructs an instance directly from a [`velda_sync::post_sync::plugin::PluginConfig`].
    pub fn from_config(
        config: &velda_sync::post_sync::plugin::PluginConfig,
    ) -> Result<Self, PluginError> {
        let exporter_cfg =
            velda_sync::validate_prometheus_exporter_config(&config.id, &config.config).map_err(
                |e| PluginError::Config(format!("Invalid Prometheus exporter config: {e}")),
            )?;
        Self::new(exporter_cfg, velda_observability::global_metrics())
    }

    /// Spawns and drives the Exporter server accept loop until shutdown signal is received.
    pub async fn run_server(
        self: Arc<Self>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> Result<(), PluginError> {
        let addr = self
            .config
            .listen
            .parse::<std::net::SocketAddr>()
            .map_err(|e| {
                PluginError::Config(format!(
                    "Invalid listen address '{}': {e}",
                    self.config.listen
                ))
            })?;
        let std_listener = velda_core::global_socket_manager()
            .acquire_shadow_tcp(addr, "prometheus_exporter")
            .map_err(|e| PluginError::Config(format!("Failed to acquire shadow socket: {e}")))?;
        std_listener.set_nonblocking(true).map_err(|e| {
            PluginError::Config(format!(
                "Failed to set nonblocking on background socket: {e}"
            ))
        })?;
        let listener = TcpListener::from_std(std_listener).map_err(|e| {
            PluginError::Config(format!("Failed to convert to tokio TcpListener: {e}"))
        })?;

        tracing::info!(
            listen = %self.config.listen,
            path = %self.config.path,
            protocol = %self.config.protocol,
            auth_mode = %self.config.auth.mode,
            tls_mode = %self.config.tls.mode,
            "Prometheus exporter started on shadow subsystem"
        );

        loop {
            tokio::select! {
                accept_res = listener.accept() => {
                    let (tcp_stream, _peer_addr) = match accept_res {
                        Ok(res) => res,
                        Err(err) => {
                            tracing::warn!(error = %err, "Exporter accept error");
                            continue;
                        }
                    };

                    let server = Arc::clone(&self);
                    tokio::spawn(async move {
                        if let Err(err) = server.handle_connection(tcp_stream).await {
                            tracing::debug!(error = %err, "Exporter client connection ended");
                        }
                    });
                }
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        tracing::info!("Prometheus exporter received shutdown signal; stopping accept loop");
                        break;
                    }
                }
            }
        }

        velda_core::global_socket_manager().release_shadow_tcp("prometheus_exporter");
        Ok(())
    }

    /// Spawns and drives the Exporter server accept loop until shutdown signal is received.
    pub async fn run(self, shutdown_rx: watch::Receiver<bool>) -> Result<(), PluginError> {
        Arc::new(self).run_server(shutdown_rx).await
    }

    /// Handles a single incoming scrape or health check connection.
    async fn handle_connection(&self, tcp_stream: TcpStream) -> Result<(), PluginError> {
        let timeout_duration = Duration::from_millis(self.config.scrape.timeout_ms);

        tokio::time::timeout(timeout_duration, async {
            let mut stream = if let Some(tls_acceptor) = &self.tls_acceptor {
                let tls_stream = tls_acceptor
                    .accept(tcp_stream)
                    .await
                    .map_err(|e| PluginError::Tls(format!("TLS handshake failed: {e}")))?;
                ExporterStream::Tls(Box::new(tls_stream))
            } else {
                ExporterStream::Plain(tcp_stream)
            };

            self.serve_stream(&mut stream).await
        })
        .await
        .map_err(|_| PluginError::ScrapeTimeout(self.config.scrape.timeout_ms))?
    }

    /// Parses HTTP request and writes formatted HTTP/1.1 response.
    async fn serve_stream(&self, stream: &mut ExporterStream) -> Result<(), PluginError> {
        let mut buf = [0u8; 4096];
        let bytes_read = stream.read(&mut buf).await?;
        if bytes_read == 0 {
            return Ok(());
        }

        let mut headers = [httparse::EMPTY_HEADER; 32];
        let mut req = httparse::Request::new(&mut headers);

        let parse_status = req
            .parse(&buf[..bytes_read])
            .map_err(|e| PluginError::Http(format!("Failed to parse HTTP request: {e}")))?;

        if parse_status.is_partial() {
            return Ok(());
        }

        let method = req.method.unwrap_or("");
        let path = req.path.unwrap_or("");

        // 1. Extract Headers of interest (Origin, Authorization)
        let mut client_origin = None;
        let mut client_auth = None;

        for h in req.headers.iter() {
            if h.name.eq_ignore_ascii_case("origin") {
                client_origin = std::str::from_utf8(h.value).ok().map(|v| v.trim());
            } else if h.name.eq_ignore_ascii_case("authorization") {
                client_auth = std::str::from_utf8(h.value).ok().map(|v| v.trim());
            }
        }

        // 2. Validate CORS Configuration
        let mut cors_headers = String::new();
        let mut origin_allowed = false;
        if self.config.cors.enabled {
            if let Some(origin) = client_origin
                && self
                    .config
                    .cors
                    .allow_origins
                    .iter()
                    .any(|o| o == "*" || o.eq_ignore_ascii_case(origin))
            {
                origin_allowed = true;
                cors_headers.push_str(&format!(
                    "Access-Control-Allow-Origin: {origin}\r\nVary: Origin\r\n"
                ));
            }

            // Handle OPTIONS preflight
            if method.eq_ignore_ascii_case("OPTIONS") {
                let allow_methods = if self.config.cors.allow_methods.is_empty() {
                    "GET, OPTIONS".to_string()
                } else {
                    self.config.cors.allow_methods.join(", ")
                };
                let allow_headers = self.config.cors.allow_headers.join(", ");

                let response = format!(
                    "HTTP/1.1 204 No Content\r\n\
                     Connection: close\r\n\
                     {}\
                     Access-Control-Allow-Methods: {}\r\n\
                     Access-Control-Allow-Headers: {}\r\n\
                     Access-Control-Max-Age: 86400\r\n\r\n",
                    if origin_allowed { &cors_headers } else { "" },
                    allow_methods,
                    allow_headers
                );
                stream.write_all(response.as_bytes()).await?;
                stream.flush().await?;
                return Ok(());
            }
        }

        // 3. Health check probe bypasses Auth (standard k8s/monitoring convention)
        if path == "/healthz" {
            let response = b"HTTP/1.1 200 OK\r\n\
                             Content-Type: text/plain; charset=utf-8\r\n\
                             Content-Length: 2\r\n\
                             Connection: close\r\n\r\n\
                             OK";
            stream.write_all(response).await?;
            stream.flush().await?;
            return Ok(());
        }

        // 4. Validate Authentication for Metrics Endpoints
        if let Some(expected_auth) = &self.expected_auth_header {
            let is_authorized = client_auth
                .map(|auth| auth == expected_auth.as_str())
                .unwrap_or(false);

            if !is_authorized {
                let auth_scheme = if self.config.auth.mode.eq_ignore_ascii_case("bearer") {
                    "Bearer"
                } else {
                    "Basic"
                };
                let response = format!(
                    "HTTP/1.1 401 Unauthorized\r\n\
                     WWW-Authenticate: {} realm=\"velda-metrics\"\r\n\
                     Content-Type: text/plain; charset=utf-8\r\n\
                     Content-Length: 12\r\n\
                     Connection: close\r\n\r\n\
                     Unauthorized",
                    auth_scheme
                );
                stream.write_all(response.as_bytes()).await?;
                stream.flush().await?;
                return Ok(());
            }
        }

        // 5. Route to Metrics Path
        if path == self.config.path {
            // Check concurrency limit
            let _permit = match self.concurrency_limiter.try_acquire() {
                Ok(permit) => permit,
                Err(_) => {
                    let response = b"HTTP/1.1 503 Service Unavailable\r\n\
                                     Retry-After: 1\r\n\
                                     Content-Type: text/plain; charset=utf-8\r\n\
                                     Content-Length: 30\r\n\
                                     Connection: close\r\n\r\n\
                                     Max Scrape Concurrency Reached";
                    stream.write_all(response).await?;
                    stream.flush().await?;
                    return Ok(());
                }
            };

            // Acquire lock-free point-in-time snapshot
            let snapshot = self.metrics.snapshot();
            let max_buffer_bytes = self.config.scrape.max_buffer_size_kb * 1024;

            // Acquire zero-allocation pooled scratch buffer
            let mut body_buf = self.buffer_pool.acquire();

            match self
                .template
                .format_snapshot_into(&snapshot, &mut body_buf, max_buffer_bytes)
            {
                Ok(()) => {
                    let mut header_buf = [0u8; 512];
                    let header_len = format_http_200_header(
                        &mut header_buf,
                        body_buf.len(),
                        if origin_allowed { &cors_headers } else { "" },
                    );
                    stream.write_all(&header_buf[..header_len]).await?;
                    stream.write_all(&body_buf).await?;
                    stream.flush().await?;
                }
                Err(PluginError::BufferOverflow(limit_kb)) => {
                    let err_msg = format!("Metrics output exceeded {limit_kb} KB limit");
                    let response = format!(
                        "HTTP/1.1 500 Internal Server Error\r\n\
                         Content-Type: text/plain; charset=utf-8\r\n\
                         Content-Length: {}\r\n\
                         Connection: close\r\n\r\n\
                         {}",
                        err_msg.len(),
                        err_msg
                    );
                    stream.write_all(response.as_bytes()).await?;
                    stream.flush().await?;
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    let response = format!(
                        "HTTP/1.1 500 Internal Server Error\r\n\
                         Content-Type: text/plain; charset=utf-8\r\n\
                         Content-Length: {}\r\n\
                         Connection: close\r\n\r\n\
                         {}",
                        err_msg.len(),
                        err_msg
                    );
                    stream.write_all(response.as_bytes()).await?;
                    stream.flush().await?;
                }
            }
        } else {
            // Not Found
            let response = b"HTTP/1.1 404 Not Found\r\n\
                             Content-Type: text/plain; charset=utf-8\r\n\
                             Content-Length: 9\r\n\
                             Connection: close\r\n\r\n\
                             Not Found";
            stream.write_all(response).await?;
            stream.flush().await?;
        }

        Ok(())
    }
}

/// Zero-allocation HTTP 200 response header formatter.
fn format_http_200_header(out: &mut [u8; 512], content_len: usize, cors_header: &str) -> usize {
    let mut curr = 0;
    let p1 = b"HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: ";
    out[curr..curr + p1.len()].copy_from_slice(p1);
    curr += p1.len();

    let mut len_digits = [0u8; 24];
    let len_str_len = fast_write_u64_slice(&mut len_digits, content_len as u64);
    out[curr..curr + len_str_len].copy_from_slice(&len_digits[..len_str_len]);
    curr += len_str_len;

    let p2 = b"\r\nConnection: close\r\n";
    out[curr..curr + p2.len()].copy_from_slice(p2);
    curr += p2.len();

    if !cors_header.is_empty() {
        let cors_bytes = cors_header.as_bytes();
        out[curr..curr + cors_bytes.len()].copy_from_slice(cors_bytes);
        curr += cors_bytes.len();
    }

    let p3 = b"\r\n";
    out[curr..curr + p3.len()].copy_from_slice(p3);
    curr += p3.len();

    curr
}
