//! Downstream TLS server engine and async handshake execution.
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                    UNIFIED TLS HANDSHAKE + JA4 CAPTURE ENGINE                           │
//! ├─────────────────────────────────────────────────────────────────────────────────────────┤
//! │  Incoming IO (TcpStream)                                                                │
//! │    │                                                                                    │
//! │    ▼ Wrap temporarily in Ja4CaptureIo -> tokio_rustls::LazyConfigAcceptor               │
//! │    │ (rustls handles TCP fragmentation, multi-record 0x16 reassembly & TLS alerts)      │
//! │    ▼ StartHandshake { accepted, io: capture }                                           │
//! │    ├── Unwrap raw IO cleanly (zero wrapper left on post-handshake TlsStream<IO>)        │
//! │    ├── compute_ja4_from_tls_records(captured) -> ja4: u64 (fail if ja4 == 0)          │
//! │    └── StartHandshake::from_parts(accepted, raw_io).into_stream(config)                 │
//! └─────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use rustls::ServerConfig;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::server::TlsStream;

use super::config::{ServerTlsConfig, TlsServerParams};
use super::handshake::{TlsHandshakeInfo, extract_alpn, extract_handshake_info};
use super::ja4::compute_client_hello_signals_from_tls_records;
use super::resolver::SniConfigResolver;
use crate::error::TlsError;

const JA4_INLINE_CAPTURE_CAP: usize = 2048;
const MAX_JA4_CAPTURE_BYTES: usize = 32 * 1024;

/// Result of a completed downstream TLS handshake carrying the extracted 64-bit JA4 ID and 16-bit `tls_traits`.
#[derive(Debug)]
pub struct AcceptedTlsStream<IO> {
    /// Post-handshake TLS stream over the unwrapped underlying transport `IO`.
    pub stream: TlsStream<IO>,
    /// Mandatory non-zero 64-bit JA4 fingerprint extracted from `ClientHello`.
    pub ja4: u64,
    /// Compact 16-bit non-colliding TLS ClientHello structural traits.
    pub tls_traits: u16,
}

/// Temporary `AsyncRead + AsyncWrite` interceptor recording raw TLS `ClientHello` bytes
/// read by `LazyConfigAcceptor` before unwrapping back to the plain `IO` transport.
struct Ja4CaptureIo<IO> {
    inner: IO,
    inline: [u8; JA4_INLINE_CAPTURE_CAP],
    inline_len: usize,
    overflow: Option<Vec<u8>>,
}

impl<IO> Ja4CaptureIo<IO> {
    #[inline]
    fn new(inner: IO) -> Self {
        Self {
            inner,
            inline: [0u8; JA4_INLINE_CAPTURE_CAP],
            inline_len: 0,
            overflow: None,
        }
    }

    #[inline]
    fn captured(&self) -> &[u8] {
        match self.overflow {
            Some(ref vec) => vec.as_slice(),
            None => &self.inline[..self.inline_len],
        }
    }

    #[inline]
    fn into_inner(self) -> IO {
        self.inner
    }

    fn record_bytes(&mut self, data: &[u8]) {
        if data.is_empty() {
            return;
        }
        if let Some(ref mut vec) = self.overflow {
            let rem = MAX_JA4_CAPTURE_BYTES.saturating_sub(vec.len());
            if rem > 0 {
                vec.extend_from_slice(&data[..data.len().min(rem)]);
            }
            return;
        }

        if self.inline_len + data.len() <= JA4_INLINE_CAPTURE_CAP {
            self.inline[self.inline_len..self.inline_len + data.len()].copy_from_slice(data);
            self.inline_len += data.len();
        } else if self.inline_len < MAX_JA4_CAPTURE_BYTES {
            let total_cap = (self.inline_len + data.len()).clamp(4096, MAX_JA4_CAPTURE_BYTES);
            let mut vec = Vec::with_capacity(total_cap);
            vec.extend_from_slice(&self.inline[..self.inline_len]);
            let rem = MAX_JA4_CAPTURE_BYTES.saturating_sub(vec.len());
            vec.extend_from_slice(&data[..data.len().min(rem)]);
            self.overflow = Some(vec);
        }
    }
}

impl<IO: AsyncRead + Unpin> AsyncRead for Ja4CaptureIo<IO> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let poll = Pin::new(&mut this.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &poll {
            let filled = buf.filled();
            if filled.len() > before {
                this.record_bytes(&filled[before..]);
            }
        }
        poll
    }
}

impl<IO: AsyncWrite + Unpin> AsyncWrite for Ja4CaptureIo<IO> {
    #[inline]
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    #[inline]
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    #[inline]
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }

    #[inline]
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    #[inline]
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write_vectored(cx, bufs)
    }
}

/// Downstream TLS server engine holding compiled [`ServerConfig`] state and runtime parameters.
#[derive(Clone)]
pub struct TlsServerEngine {
    config: Arc<ServerConfig>,
    sni_configs: Option<Arc<SniConfigResolver>>,
    params: TlsServerParams,
}

impl std::fmt::Debug for TlsServerEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsServerEngine")
            .field("alpn_protocols", &self.config.alpn_protocols)
            .field("has_sni_configs", &self.sni_configs.is_some())
            .field("params", &self.params)
            .finish()
    }
}

impl TlsServerEngine {
    /// Builds a new downstream TLS server engine from server configurations using hardware probe.
    pub fn new(servers: &[ServerTlsConfig]) -> Result<Self, TlsError> {
        let params = TlsServerParams::from_hardware();
        Self::new_with_params(servers, &params)
    }

    /// Builds a new downstream TLS server engine from server configurations and explicit runtime parameters.
    pub fn new_with_params(
        servers: &[ServerTlsConfig],
        params: &TlsServerParams,
    ) -> Result<Self, TlsError> {
        let config = ServerTlsConfig::build_with_params(servers, params)?;
        let sni_configs = if servers.len() > 1 {
            let mut resolver = SniConfigResolver::new(config.clone());
            for server in servers {
                let single = server.build_single(params)?;
                resolver.add_config(&server.sni, single);
            }
            Some(Arc::new(resolver))
        } else {
            None
        };

        Ok(Self {
            config,
            sni_configs,
            params: params.clone(),
        })
    }

    /// Creates an engine directly from an existing `Arc<ServerConfig>`.
    pub fn from_config(config: Arc<ServerConfig>) -> Self {
        Self::from_config_with_params(config, TlsServerParams::from_hardware())
    }

    /// Creates an engine directly from an existing `Arc<ServerConfig>` and explicit runtime parameters.
    pub fn from_config_with_params(config: Arc<ServerConfig>, params: TlsServerParams) -> Self {
        Self {
            config,
            sni_configs: None,
            params,
        }
    }

    /// Attaches an explicit `SniConfigResolver` for multi-domain ALPN and certificate isolation.
    pub fn with_sni_configs(mut self, sni_configs: Arc<SniConfigResolver>) -> Self {
        self.sni_configs = Some(sni_configs);
        self
    }

    /// Returns a reference to the active `SniConfigResolver`, if multi-domain isolation is enabled.
    pub fn sni_configs(&self) -> Option<&Arc<SniConfigResolver>> {
        self.sni_configs.as_ref()
    }

    /// Performs downstream TLS termination with mandatory JA4 capture bounded by `self.params.handshake_timeout`.
    pub async fn accept<IO>(&self, stream: IO) -> Result<AcceptedTlsStream<IO>, TlsError>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        self.accept_with_timeout(stream, self.params.handshake_timeout)
            .await
    }

    /// Performs downstream TLS termination with mandatory JA4 `ClientHello` fingerprinting
    /// in a single non-duplicating `LazyConfigAcceptor` pass bounded by an explicit timeout.
    ///
    /// Unwraps the temporary capture wrapper before driving the remaining TLS handshake,
    /// returning an un-wrapped `TlsStream<IO>` and rejecting any TLS handshake lacking a valid JA4.
    pub async fn accept_with_timeout<IO>(
        &self,
        stream: IO,
        timeout: std::time::Duration,
    ) -> Result<AcceptedTlsStream<IO>, TlsError>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        match tokio::time::timeout(timeout, async move {
            let lazy = tokio_rustls::LazyConfigAcceptor::new(
                rustls::server::Acceptor::default(),
                Ja4CaptureIo::new(stream),
            );
            let start = lazy
                .await
                .map_err(|e| TlsError::HandshakeFailed(e.to_string()))?;
            let tokio_rustls::StartHandshake { accepted, io, .. } = start;

            let config = if let Some(ref resolver) = self.sni_configs {
                resolver.resolve(accepted.client_hello().server_name())
            } else {
                self.config.clone()
            };

            let signals = compute_client_hello_signals_from_tls_records(io.captured());
            if signals.ja4 == 0 {
                return Err(TlsError::HandshakeFailed(
                    "TLS ClientHello missing mandatory JA4 fingerprint".to_string(),
                ));
            }

            let raw_stream = io.into_inner();
            let tls_stream = tokio_rustls::StartHandshake::from_parts(accepted, raw_stream)
                .into_stream(config)
                .await
                .map_err(|e| TlsError::HandshakeFailed(e.to_string()))?;

            Ok(AcceptedTlsStream {
                stream: tls_stream,
                ja4: signals.ja4,
                tls_traits: signals.tls_traits,
            })
        })
        .await
        {
            Ok(res) => res,
            Err(_) => Err(TlsError::HandshakeFailed(format!(
                "Downstream TLS handshake timed out after {:?}",
                timeout
            ))),
        }
    }

    /// Returns a reference to the active parameters.
    pub fn params(&self) -> &TlsServerParams {
        &self.params
    }

    /// Returns a reference to the compiled `ServerConfig`.
    pub fn config(&self) -> &Arc<ServerConfig> {
        &self.config
    }

    /// Compiles a QUIC [`quinn_proto::ServerConfig`] sharing this engine's TLS state,
    /// certificates, and unified session resumption cache / 0-RTT tickets.
    pub fn build_quic_config(&self) -> Result<quinn_proto::ServerConfig, TlsError> {
        crate::server::quic::build_quic_server_config(self.config.clone())
    }

    /// Extracts negotiated ALPN protocol string with zero heap allocation on the hot path.
    #[inline]
    pub fn extract_alpn<IO>(stream: &TlsStream<IO>) -> Option<std::borrow::Cow<'static, str>> {
        extract_alpn(stream)
    }

    /// Extracts connection metadata after a successful downstream TLS handshake.
    pub fn extract_handshake_info<IO>(stream: &TlsStream<IO>) -> TlsHandshakeInfo {
        extract_handshake_info(stream)
    }
}
