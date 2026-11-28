//! QUIC TLS server configuration bridge and zero-overhead JA4 ClientHello extraction.
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                     QUIC JA4 EXTRACTION VIA QUINN-PROTO CRYPTO SESSION                  │
//! ├─────────────────────────────────────────────────────────────────────────────────────────┤
//! │  quinn-proto Endpoint (decrypts Initial packet + reassembles CRYPTO stream)             │
//! │    └──► VeldaQuicSession::read_handshake(&[u8])                                         │
//! │           ├── Accumulate ClientHello header..hs_len (<= 32 KiB)                         │
//! │           ├── compute_ja4_u64(&ch, is_quic=true) -> u64 ("q13d...")                     │
//! │           └── Release capture buffer immediately                                        │
//! │  Protocol Engine (HTTP/3, gRPC-UDP) -> extract_quic_ja4(conn) -> RequestState.ja4       │
//! └─────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::any::Any;
use std::sync::Arc;

use quinn_proto::crypto::rustls::QuicServerConfig;
use quinn_proto::crypto::{
    self, ExportKeyingMaterialError, Keys, PacketKey, ServerConfig as QuinnCryptoServerConfig,
    Session as QuinnCryptoSession, UnsupportedVersion,
};
use quinn_proto::transport_parameters::TransportParameters;
use quinn_proto::{Connection, ConnectionId, ServerConfig as QuinnServerConfig, TransportError};
use rustls::ServerConfig as RustlsServerConfig;

use crate::error::TlsError;
use crate::server::ja4::{compute_client_hello_signals, tls_client_hello_handshake_len};

const MAX_QUIC_JA4_CAPTURE_BYTES: usize = 32 * 1024;

/// Handshake metadata exposed by [`VeldaQuicSession`] to protocol engines.
#[derive(Debug, Clone, Copy)]
pub struct VeldaQuicHandshakeData {
    /// Deterministic 64-bit JA4 fingerprint (`0` only before `ClientHello` arrives).
    pub ja4: u64,
    /// Compact 16-bit non-colliding TLS ClientHello structural traits.
    pub tls_traits: u16,
}

struct VeldaQuicServerConfig {
    inner: Arc<QuicServerConfig>,
}

impl QuinnCryptoServerConfig for VeldaQuicServerConfig {
    #[inline]
    fn initial_keys(
        &self,
        version: u32,
        dst_cid: &ConnectionId,
    ) -> Result<Keys, UnsupportedVersion> {
        self.inner.initial_keys(version, dst_cid)
    }

    #[inline]
    fn retry_tag(&self, version: u32, orig_dst_cid: &ConnectionId, packet: &[u8]) -> [u8; 16] {
        self.inner.retry_tag(version, orig_dst_cid, packet)
    }

    fn start_session(
        self: Arc<Self>,
        version: u32,
        params: &TransportParameters,
    ) -> Box<dyn QuinnCryptoSession> {
        let inner = self.inner.clone().start_session(version, params);
        Box::new(VeldaQuicSession {
            inner,
            ch_buf: Vec::new(),
            ja4: 0,
            tls_traits: 0,
            capture_done: false,
        })
    }
}

struct VeldaQuicSession {
    inner: Box<dyn QuinnCryptoSession>,
    ch_buf: Vec<u8>,
    ja4: u64,
    tls_traits: u16,
    capture_done: bool,
}

impl QuinnCryptoSession for VeldaQuicSession {
    #[inline]
    fn initial_keys(&self, dst_cid: &ConnectionId, side: quinn_proto::Side) -> Keys {
        self.inner.initial_keys(dst_cid, side)
    }

    fn handshake_data(&self) -> Option<Box<dyn Any>> {
        if self.ja4 != 0 || self.inner.handshake_data().is_some() {
            Some(Box::new(VeldaQuicHandshakeData {
                ja4: self.ja4,
                tls_traits: self.tls_traits,
            }))
        } else {
            None
        }
    }

    #[inline]
    fn peer_identity(&self) -> Option<Box<dyn Any>> {
        self.inner.peer_identity()
    }

    #[inline]
    fn early_crypto(&self) -> Option<(Box<dyn crypto::HeaderKey>, Box<dyn PacketKey>)> {
        self.inner.early_crypto()
    }

    #[inline]
    fn early_data_accepted(&self) -> Option<bool> {
        self.inner.early_data_accepted()
    }

    #[inline]
    fn is_handshaking(&self) -> bool {
        self.inner.is_handshaking()
    }

    fn read_handshake(&mut self, buf: &[u8]) -> Result<bool, TransportError> {
        if !self.capture_done && !buf.is_empty() {
            let remaining = MAX_QUIC_JA4_CAPTURE_BYTES.saturating_sub(self.ch_buf.len());
            if remaining == 0 {
                self.capture_done = true;
                self.ch_buf = Vec::new();
            } else {
                let take = buf.len().min(remaining);
                self.ch_buf.extend_from_slice(&buf[..take]);
                if let Some(needed) = tls_client_hello_handshake_len(&self.ch_buf) {
                    if needed > MAX_QUIC_JA4_CAPTURE_BYTES {
                        self.capture_done = true;
                        self.ch_buf = Vec::new();
                    } else if self.ch_buf.len() >= needed {
                        let sig = compute_client_hello_signals(&self.ch_buf[..needed], true);
                        self.ja4 = sig.ja4;
                        self.tls_traits = sig.tls_traits;
                        self.capture_done = true;
                        self.ch_buf = Vec::new();
                    }
                } else if self.ch_buf.len() >= 4 {
                    self.capture_done = true;
                    self.ch_buf = Vec::new();
                }
            }
        }
        self.inner.read_handshake(buf)
    }

    #[inline]
    fn transport_parameters(&self) -> Result<Option<TransportParameters>, TransportError> {
        self.inner.transport_parameters()
    }

    #[inline]
    fn write_handshake(&mut self, buf: &mut Vec<u8>) -> Option<Keys> {
        self.inner.write_handshake(buf)
    }

    #[inline]
    fn next_1rtt_keys(&mut self) -> Option<crypto::KeyPair<Box<dyn PacketKey>>> {
        self.inner.next_1rtt_keys()
    }

    #[inline]
    fn is_valid_retry(&self, orig_dst_cid: &ConnectionId, header: &[u8], payload: &[u8]) -> bool {
        self.inner.is_valid_retry(orig_dst_cid, header, payload)
    }

    #[inline]
    fn export_keying_material(
        &self,
        output: &mut [u8],
        label: &[u8],
        context: &[u8],
    ) -> Result<(), ExportKeyingMaterialError> {
        self.inner.export_keying_material(output, label, context)
    }
}

/// Extracts `(ja4, tls_traits)` captured during the QUIC TLS handshake.
///
/// Returns `(0, 0)` only if the connection has not yet received a valid `ClientHello`.
#[inline]
pub fn extract_quic_signals(conn: &Connection) -> (u64, u16) {
    conn.crypto_session()
        .handshake_data()
        .and_then(|d| {
            d.downcast_ref::<VeldaQuicHandshakeData>()
                .map(|v| (v.ja4, v.tls_traits))
        })
        .unwrap_or((0, 0))
}

/// Extracts the 64-bit JA4 fingerprint captured during the QUIC TLS handshake.
///
/// Returns `0` only if the connection has not yet received a valid `ClientHello`.
#[inline]
pub fn extract_quic_ja4(conn: &Connection) -> u64 {
    extract_quic_signals(conn).0
}

/// Compiles an active [`RustlsServerConfig`] into a QUIC [`QuinnServerConfig`]
/// wired with automatic JA4 `ClientHello` fingerprinting across all QUIC protocols.
pub fn build_quic_server_config(
    rustls_config: Arc<RustlsServerConfig>,
) -> Result<QuinnServerConfig, TlsError> {
    // RFC 9001 / rustls QUIC invariant:
    // QUIC sessions require max_early_data_size to be exactly 0 (disabled) or u32::MAX (enabled).
    // Intermediate byte thresholds configured for TCP TLS (e.g. 4096, 8192) cause
    // rustls::quic::ServerConnection::new() to panic during handshake.
    let rustls_config = if (rustls_config.max_early_data_size != 0
        && rustls_config.max_early_data_size != u32::MAX)
        || (!rustls_config.alpn_protocols.iter().any(|p| p == b"h3"))
    {
        let mut cloned = (*rustls_config).clone();
        if cloned.max_early_data_size != 0 && cloned.max_early_data_size != u32::MAX {
            cloned.max_early_data_size = u32::MAX;
        }
        if !cloned.alpn_protocols.iter().any(|p| p == b"h3") {
            cloned.alpn_protocols.push(b"h3".to_vec());
        }
        Arc::new(cloned)
    } else {
        rustls_config
    };

    let quic_crypto = QuicServerConfig::try_from(rustls_config)
        .map_err(|e| TlsError::HandshakeFailed(e.to_string()))?;
    let wrapped = Arc::new(VeldaQuicServerConfig {
        inner: Arc::new(quic_crypto),
    });
    Ok(QuinnServerConfig::with_crypto(wrapped))
}
