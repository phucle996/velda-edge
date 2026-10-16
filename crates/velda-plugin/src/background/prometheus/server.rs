//! Auxiliary Prometheus Exporter HTTP/1.1 & HTTPS Server.
//!
//! Exposes OpenMetrics / Prometheus scrape endpoints (`GET /metrics`) and liveness probes (`GET /healthz`)
//! on an auxiliary network interface strictly separated from Data Plane traffic workers.
//!
//! # Hard Architectural Boundary: Auxiliary vs Traffic Workers
//! - **Auxiliary Domain**: This server runs exclusively on the Supervisor / Auxiliary core runtime.
//! - **Zero Interference**: It never touches traffic event loops, worker pinning, or hot-path pipelines.
//! - **Non-blocking Telemetry**: Metrics are extracted via lock-free point-in-time snapshots
//!   ([`MetricsStore::snapshot`]), ensuring zero write stalls or cacheline contention for worker threads.
//!
//! # Strict Declarative Features
//! - **Transport**: Plaintext HTTP/1.1 or HTTPS with TLS 1.2/1.3 and optional mutual TLS (mTLS).
//! - **Authentication**: Precomputed token checks for Bearer or HTTP Basic authentication.
//! - **CORS**: Standards-compliant Preflight (`OPTIONS`) and Origin validation.
//! - **Scrape Protection**: Strict concurrency bounds via Semaphore and hard per-scrape timeout limits.

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

use super::formatter::format_snapshot;
use crate::error::PluginError;

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

/// Standalone Auxiliary Prometheus Exporter service.
pub struct PrometheusExporter {
    config: PrometheusExporterConfig,
    metrics: Arc<MetricsStore>,
    tls_acceptor: Option<TlsAcceptor>,
    concurrency_limiter: Arc<Semaphore>,
    expected_auth_header: Option<String>,
}

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

        Ok(Self {
            config,
            metrics,
            tls_acceptor,
            concurrency_limiter,
            expected_auth_header,
        })
    }

    /// Spawns and drives the Exporter server accept loop until shutdown signal is received.
    pub async fn run(self, mut shutdown_rx: watch::Receiver<bool>) -> Result<(), PluginError> {
        let listener = TcpListener::bind(&self.config.listen).await.map_err(|e| {
            PluginError::Config(format!(
                "Failed to bind exporter listener to '{}': {e}",
                self.config.listen
            ))
        })?;

        tracing::info!(
            listen = %self.config.listen,
            path = %self.config.path,
            protocol = %self.config.protocol,
            auth_mode = %self.config.auth.mode,
            tls_mode = %self.config.tls.mode,
            "Prometheus exporter started on auxiliary subsystem"
        );

        let shared_self = Arc::new(self);

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

                    let server = Arc::clone(&shared_self);
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

        Ok(())
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
            let response = "HTTP/1.1 200 OK\r\n\
                            Content-Type: text/plain; charset=utf-8\r\n\
                            Content-Length: 2\r\n\
                            Connection: close\r\n\r\n\
                            OK";
            stream.write_all(response.as_bytes()).await?;
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
                    let response = "HTTP/1.1 503 Service Unavailable\r\n\
                                    Retry-After: 1\r\n\
                                    Content-Type: text/plain; charset=utf-8\r\n\
                                    Content-Length: 30\r\n\
                                    Connection: close\r\n\r\n\
                                    Max Scrape Concurrency Reached";
                    stream.write_all(response.as_bytes()).await?;
                    stream.flush().await?;
                    return Ok(());
                }
            };

            // Acquire lock-free point-in-time snapshot
            let snapshot = self.metrics.snapshot();
            let max_buffer_bytes = self.config.scrape.max_buffer_size_kb * 1024;
            let mut body_buf = String::with_capacity(32 * 1024);

            match format_snapshot(&snapshot, &mut body_buf, max_buffer_bytes) {
                Ok(()) => {
                    let response_header = format!(
                        "HTTP/1.1 200 OK\r\n\
                         Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
                         Content-Length: {}\r\n\
                         Connection: close\r\n\
                         {}\r\n",
                        body_buf.len(),
                        cors_headers
                    );
                    stream.write_all(response_header.as_bytes()).await?;
                    stream.write_all(body_buf.as_bytes()).await?;
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
            let response = "HTTP/1.1 404 Not Found\r\n\
                            Content-Type: text/plain; charset=utf-8\r\n\
                            Content-Length: 9\r\n\
                            Connection: close\r\n\r\n\
                            Not Found";
            stream.write_all(response.as_bytes()).await?;
            stream.flush().await?;
        }

        Ok(())
    }
}
