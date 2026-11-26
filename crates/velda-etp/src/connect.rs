//! Outbound TCP/TLS handshake, TLS certificate integer pinning (`[u64; 4]`),
//! LKG integer IP freeze (`ColdTargetAddr`), and isolated 64B-aligned `ColdConnectBlueprint`.
//!
//! ```text
//!   EtpTlsConfig::build_client_config()
//!         │
//!         ├─► insecure_skip_verify == true     ──► InsecureServerCertVerifier
//!         ├─► pinned_cert_sha256 == Some(hex)  ──► PinnedSha256ServerCertVerifier ([u64; 4])
//!         ├─► ca_cert_pem == Some(pem)         ──► custom RootCertStore
//!         └─► ca_cert_pem == None              ──► webpki_roots::TLS_SERVER_ROOTS
//!
//!   ColdConnectBlueprint::connect_stream() / EtpStream::connect_target()
//!         │
//!         ▼
//!   tokio::time::timeout(timeout)
//!         │
//!         ├─► Elapsed ──► Err(EtpError::Timeout)
//!         └─► ColdTargetAddr match:
//!               ├─ Ip { addr, .. } ──► TcpStream::connect(*addr) [integer SocketAddr]
//!               └─ Dns { host, port, port_be, cached_ip_lo, cached_ip_hi }
//!                     ├─ cached != 0 ──► convert (ip_u128, port_be) -> SocketAddr
//!                     │                  dial TcpStream::connect(cached_addr) [0 DNS!]
//!                     │                  └─ connect Err? -> re-query DNS once, update cache
//!                     └─ cached == 0 ──► lookup_host -> connect -> freeze ip_u128 atomically
//!                     │
//!                     ▼
//!               set_nodelay(true)
//!                     ├─ tls == None ──► Ok(EtpStream::Plain(TcpStream))
//!                     └─ tls == Some((cfg, sni)) ──► TlsConnector::connect(sni, tcp)
//!                                                      ──► Ok(EtpStream::Tls(...))
//!
//!   Cold Blueprint, Frozen Integer Cache, Pin & Stream Memory Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ ColdConnectBlueprint (#[repr(align(64))], isolated from EtpPageArena)      │
//!   │   id: Box<str> | target: ColdTargetAddr (ip_u128 + port_be + AtomicU64x2)  │
//!   │   tls: Option<(Arc<ClientConfig>, ServerName)> | connect_timeout: Duration │
//!   ├────────────────────────────────────────────────────────────────────────────┤
//!   │ PinnedSha256ServerCertVerifier { pinned_words: [u64; 4] }  (constant-time) │
//!   │ EtpStream (24 Bytes inside ArenaSlot<EtpStream>): Plain | Box<TlsStream>   │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::io::BufReader;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use rustls::ClientConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::error::EtpError;

// ============================================================================
// TLS Policy, Certificate Integer Pinning & Rustls Compilation
// ============================================================================

/// Transport-level security mode for an ETP connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EtpTransportSecurity {
    Plain,
    Tls,
    Mtls,
    #[serde(alias = "starttls")]
    StartTls,
}

impl EtpTransportSecurity {
    /// Canonical lowercase snake_case name matching JSON schema.
    #[inline]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Plain => "plain",
            Self::Tls => "tls",
            Self::Mtls => "mtls",
            Self::StartTls => "start_tls",
        }
    }
}

impl std::fmt::Display for EtpTransportSecurity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Supported TLS protocol version policies for outbound ETP connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum EtpTlsVersion {
    /// Negotiates TLS 1.3 or TLS 1.2 automatically (prefers TLS 1.3).
    #[default]
    #[serde(rename = "auto", alias = "all")]
    Auto,
    /// Restricts handshake strictly to TLS 1.2.
    #[serde(rename = "1.2", alias = "tls1.2", alias = "tls12")]
    Tls12,
    /// Restricts handshake strictly to TLS 1.3.
    #[serde(rename = "1.3", alias = "tls1.3", alias = "tls13")]
    Tls13,
}

/// Outbound TLS / mTLS configuration for an External Third-Party endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct EtpTlsConfig {
    /// Optional custom CA certificate bundle in PEM format.
    /// When `None`, `webpki_roots::TLS_SERVER_ROOTS` is used.
    #[serde(default)]
    pub ca_cert_pem: Option<String>,

    /// Optional client leaf certificate chain in PEM format for mTLS.
    #[serde(default)]
    pub client_cert_pem: Option<String>,

    /// Optional client private key in PEM format for mTLS.
    #[serde(default)]
    pub client_key_pem: Option<String>,

    /// Optional explicit SNI server name override.
    #[serde(default)]
    pub server_name: Option<String>,

    /// Disables server certificate chain and hostname verification when true.
    #[serde(default)]
    pub insecure_skip_verify: bool,

    /// TLS protocol version policy (`auto`, `1.2`, or `1.3`).
    #[serde(default)]
    pub tls_version: EtpTlsVersion,

    /// Optional SHA-256 certificate pin (`"sha256:<64 hex>"` or `"<64 hex>"`,
    /// optional `:` / `-` separators) verified in constant time as `[u64; 4]`.
    #[serde(default)]
    pub pinned_cert_sha256: Option<String>,
}

#[derive(Debug, Clone, Copy)]
struct InsecureServerCertVerifier;

impl rustls::client::danger::ServerCertVerifier for InsecureServerCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[derive(Debug, Clone, Copy)]
struct PinnedSha256ServerCertVerifier {
    pinned_words: [u64; 4],
}

impl rustls::client::danger::ServerCertVerifier for PinnedSha256ServerCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        let actual = sha256_u64x4(end_entity.as_ref());
        let diff = (actual[0] ^ self.pinned_words[0])
            | (actual[1] ^ self.pinned_words[1])
            | (actual[2] ^ self.pinned_words[2])
            | (actual[3] ^ self.pinned_words[3]);
        if diff != 0 {
            return Err(rustls::Error::General(
                "Server certificate SHA-256 integer pin mismatch".to_string(),
            ));
        }
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &rustls::crypto::aws_lc_rs::default_provider().signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::aws_lc_rs::default_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// FIPS 180-4 SHA-256 over arbitrary DER bytes, packed into `[u64; 4]` little-endian words,
/// using a fixed 128-byte stack tail buffer with zero heap allocation.
pub fn sha256_u64x4(data: &[u8]) -> [u64; 4] {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];

    #[inline(always)]
    fn compress_block(h: &mut [u32; 8], block: &[u8; 64]) {
        let mut w = [0u32; 64];
        let (words, _) = block.as_chunks::<4>();
        for (i, word) in words.iter().enumerate() {
            w[i] = u32::from_be_bytes(*word);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = *h;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);

            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }

    let (full_blocks, rem) = data.as_chunks::<64>();
    for block in full_blocks {
        compress_block(&mut h, block);
    }

    let mut tail = [0u8; 128];
    tail[..rem.len()].copy_from_slice(rem);
    tail[rem.len()] = 0x80;
    let tail_blocks = if rem.len() < 56 { 1 } else { 2 };
    let bit_len = (data.len() as u64).wrapping_mul(8);
    let len_offset = tail_blocks * 64 - 8;
    tail[len_offset..len_offset + 8].copy_from_slice(&bit_len.to_be_bytes());

    let (blocks, _) = tail[..tail_blocks * 64].as_chunks::<64>();
    for block in blocks {
        compress_block(&mut h, block);
    }

    let mut digest = [0u8; 32];
    for (i, word) in h.iter().enumerate() {
        digest[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    [
        u64::from_le_bytes(digest[0..8].try_into().unwrap()),
        u64::from_le_bytes(digest[8..16].try_into().unwrap()),
        u64::from_le_bytes(digest[16..24].try_into().unwrap()),
        u64::from_le_bytes(digest[24..32].try_into().unwrap()),
    ]
}

fn parse_certs_pem(pem: &str) -> Result<Vec<CertificateDer<'static>>, EtpError> {
    let mut reader = BufReader::new(pem.as_bytes());
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| EtpError::Tls(format!("Failed to parse PEM certificates: {e}")))?;
    if certs.is_empty() {
        return Err(EtpError::Tls(
            "Certificate PEM contains no certificates".to_string(),
        ));
    }
    Ok(certs)
}

fn parse_private_key_pem(pem: &str) -> Result<PrivateKeyDer<'static>, EtpError> {
    let mut reader = BufReader::new(pem.as_bytes());
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| EtpError::Tls(format!("Failed to parse PEM private key: {e}")))?
        .ok_or_else(|| EtpError::Tls("Private key PEM contains no valid key".to_string()))
}

impl EtpTlsConfig {
    /// Parses and validates `pinned_cert_sha256` into `[u64; 4]` little-endian integer words.
    pub fn parse_pinned_sha256_u64x4(&self) -> Result<Option<[u64; 4]>, EtpError> {
        let Some(raw) = &self.pinned_cert_sha256 else {
            return Ok(None);
        };
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(EtpError::Tls(
                "pinned_cert_sha256 cannot be empty when specified".to_string(),
            ));
        }
        let hex_body = if trimmed.len() >= 7 && trimmed[..7].eq_ignore_ascii_case("sha256:") {
            trimmed[7..].trim()
        } else {
            trimmed
        };

        let mut nibbles = Vec::with_capacity(64);
        for &b in hex_body.as_bytes() {
            if b == b':' || b == b'-' {
                continue;
            }
            let nibble = match b {
                b'0'..=b'9' => b - b'0',
                b'a'..=b'f' => b - b'a' + 10,
                b'A'..=b'F' => b - b'A' + 10,
                _ => {
                    return Err(EtpError::Tls(format!(
                        "pinned_cert_sha256 contains non-hex character '{}'",
                        b as char
                    )));
                }
            };
            nibbles.push(nibble);
        }

        if nibbles.len() != 64 {
            return Err(EtpError::Tls(format!(
                "pinned_cert_sha256 must decode to 32 bytes (64 hex chars), got {} hex chars",
                nibbles.len()
            )));
        }

        let mut bytes = [0u8; 32];
        let (pairs, _) = nibbles.as_chunks::<2>();
        for (i, &[hi, lo]) in pairs.iter().enumerate() {
            bytes[i] = (hi << 4) | lo;
        }
        Ok(Some([
            u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
            u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
        ]))
    }

    /// Validates PEM certificate, pin hex, and key material without performing network I/O.
    pub fn validate(&self) -> Result<(), EtpError> {
        self.parse_pinned_sha256_u64x4()?;

        if let Some(ca_pem) = &self.ca_cert_pem {
            let trimmed = ca_pem.trim();
            if trimmed.is_empty() {
                return Err(EtpError::Tls(
                    "ca_cert_pem cannot be empty when specified".to_string(),
                ));
            }
            parse_certs_pem(trimmed)?;
        }

        match (&self.client_cert_pem, &self.client_key_pem) {
            (Some(cert_pem), Some(key_pem)) => {
                parse_certs_pem(cert_pem.trim())?;
                parse_private_key_pem(key_pem.trim())?;
            }
            (None, None) => {}
            (Some(_), None) => {
                return Err(EtpError::Tls(
                    "client_cert_pem was provided without client_key_pem".to_string(),
                ));
            }
            (None, Some(_)) => {
                return Err(EtpError::Tls(
                    "client_key_pem was provided without client_cert_pem".to_string(),
                ));
            }
        }

        if let Some(sni) = &self.server_name {
            let trimmed = sni.trim();
            if trimmed.is_empty() {
                return Err(EtpError::Tls(
                    "server_name cannot be empty when specified".to_string(),
                ));
            }
            ServerName::try_from(trimmed)
                .map_err(|e| EtpError::Tls(format!("Invalid TLS server_name '{trimmed}': {e}")))?;
        }

        Ok(())
    }

    /// Compiles this TLS configuration into a `rustls::ClientConfig` with in-memory
    /// session resumption (TLS 1.3 PSK tickets and TLS 1.2 Session IDs).
    pub fn build_client_config(&self) -> Result<Arc<ClientConfig>, EtpError> {
        self.build_client_config_with_alpn(&[])
    }

    /// Compiles this TLS configuration into a `rustls::ClientConfig` with optional
    /// ALPN protocol offers and in-memory session resumption.
    pub fn build_client_config_with_alpn(
        &self,
        alpn_protocols: &[&[u8]],
    ) -> Result<Arc<ClientConfig>, EtpError> {
        let versions: &[&'static rustls::SupportedProtocolVersion] = match self.tls_version {
            EtpTlsVersion::Auto => &[&rustls::version::TLS13, &rustls::version::TLS12],
            EtpTlsVersion::Tls12 => &[&rustls::version::TLS12],
            EtpTlsVersion::Tls13 => &[&rustls::version::TLS13],
        };

        let builder = ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_protocol_versions(versions)
        .map_err(|e| EtpError::Tls(format!("Failed to select TLS versions: {e}")))?;

        let builder = if self.insecure_skip_verify {
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(InsecureServerCertVerifier))
        } else if let Some(pinned_words) = self.parse_pinned_sha256_u64x4()? {
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(PinnedSha256ServerCertVerifier {
                    pinned_words,
                }))
        } else {
            let mut root_store = rustls::RootCertStore::empty();
            if let Some(ca_pem) = &self.ca_cert_pem {
                for cert in parse_certs_pem(ca_pem.trim())? {
                    root_store
                        .add(cert)
                        .map_err(|e| EtpError::Tls(format!("Invalid CA certificate: {e}")))?;
                }
            } else {
                root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            }
            builder.with_root_certificates(root_store)
        };

        let mut client_config = match (&self.client_cert_pem, &self.client_key_pem) {
            (Some(cert_pem), Some(key_pem)) => {
                let certs = parse_certs_pem(cert_pem.trim())?;
                let key = parse_private_key_pem(key_pem.trim())?;
                builder
                    .with_client_auth_cert(certs, key)
                    .map_err(|e| EtpError::Tls(format!("Invalid client auth key/cert: {e}")))?
            }
            (None, None) => builder.with_no_client_auth(),
            (Some(_), None) => {
                return Err(EtpError::Tls(
                    "client_cert_pem was provided without client_key_pem".to_string(),
                ));
            }
            (None, Some(_)) => {
                return Err(EtpError::Tls(
                    "client_key_pem was provided without client_cert_pem".to_string(),
                ));
            }
        };

        if !alpn_protocols.is_empty() {
            client_config.alpn_protocols = alpn_protocols.iter().map(|p| p.to_vec()).collect();
        }
        client_config.resumption = rustls::client::Resumption::in_memory_sessions(64);
        Ok(Arc::new(client_config))
    }
}

/// Compiles optional TLS settings and SNI server name for an ETP pool blueprint.
pub(crate) fn build_tls_target(
    id: &str,
    host: &str,
    tls_opt: Option<&EtpTlsConfig>,
) -> Result<Option<(Arc<ClientConfig>, ServerName<'static>)>, EtpError> {
    build_tls_target_with_alpn(id, host, tls_opt, &[])
}

/// Compiles optional TLS settings, ALPN offers, and SNI server name for an ETP pool blueprint.
pub(crate) fn build_tls_target_with_alpn(
    id: &str,
    host: &str,
    tls_opt: Option<&EtpTlsConfig>,
    alpn_protocols: &[&[u8]],
) -> Result<Option<(Arc<ClientConfig>, ServerName<'static>)>, EtpError> {
    let Some(tls_cfg) = tls_opt else {
        return Ok(None);
    };
    let client_config = tls_cfg.build_client_config_with_alpn(alpn_protocols)?;
    let server_name_raw = tls_cfg
        .server_name
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(host);
    let server_name = ServerName::try_from(server_name_raw.to_string()).map_err(|e| {
        EtpError::Tls(format!(
            "ETP '{id}' invalid TLS server name '{server_name_raw}': {e}"
        ))
    })?;
    Ok(Some((client_config, server_name)))
}

// ============================================================================
// Integer-First Target Resolution & LKG Integer IP Freeze
// ============================================================================

/// Pre-encoded network target avoiding string IP parsing and repeat DNS lookups on
/// connect and reconnect paths.
#[derive(Debug)]
pub enum ColdTargetAddr {
    /// Pre-encoded integer IPv4/IPv6 target via `velda_core::lut::ip`.
    Ip {
        ip_u128: u128,
        port_be: u16,
        addr: SocketAddr,
    },
    /// Domain target that freezes its resolved `IpAddr` into lock-free 128-bit integer
    /// registers (`cached_ip_lo`, `cached_ip_hi`) guarded by a SeqLock (`cached_ip_seq`).
    Dns {
        host: Box<str>,
        port: u16,
        port_be: u16,
        cached_ip_lo: AtomicU64,
        cached_ip_hi: AtomicU64,
        cached_ip_seq: AtomicU64,
    },
}

impl Clone for ColdTargetAddr {
    fn clone(&self) -> Self {
        match self {
            Self::Ip {
                ip_u128,
                port_be,
                addr,
            } => Self::Ip {
                ip_u128: *ip_u128,
                port_be: *port_be,
                addr: *addr,
            },
            Self::Dns {
                host,
                port,
                port_be,
                ..
            } => {
                let ip = self.cached_ip_u128();
                Self::Dns {
                    host: host.clone(),
                    port: *port,
                    port_be: *port_be,
                    cached_ip_lo: AtomicU64::new(ip as u64),
                    cached_ip_hi: AtomicU64::new((ip >> 64) as u64),
                    cached_ip_seq: AtomicU64::new(0),
                }
            }
        }
    }
}

impl PartialEq for ColdTargetAddr {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Ip {
                    ip_u128: a_ip,
                    port_be: a_p,
                    addr: a_addr,
                },
                Self::Ip {
                    ip_u128: b_ip,
                    port_be: b_p,
                    addr: b_addr,
                },
            ) => a_ip == b_ip && a_p == b_p && a_addr == b_addr,
            (
                Self::Dns {
                    host: a_h,
                    port: a_p,
                    port_be: a_be,
                    ..
                },
                Self::Dns {
                    host: b_h,
                    port: b_p,
                    port_be: b_be,
                    ..
                },
            ) => a_h == b_h && a_p == b_p && a_be == b_be,
            _ => false,
        }
    }
}

impl Eq for ColdTargetAddr {}

#[inline(always)]
pub(crate) fn ip_u128_to_socket_addr(ip_u128: u128, port_be: u16) -> SocketAddr {
    if velda_core::lut::ip::is_v4_u128(ip_u128) {
        SocketAddr::V4(velda_core::lut::ip::v4itoaddr(ip_u128, port_be))
    } else {
        SocketAddr::V6(velda_core::lut::ip::v6itoaddr(ip_u128, port_be))
    }
}

impl ColdTargetAddr {
    /// Parses `host` and `port` into an integer [`ColdTargetAddr::Ip`] via
    /// `velda_core::lut::ip` when `host` is an IPv4 or IPv6 literal, or constructs
    /// [`ColdTargetAddr::Dns`] with zeroed LKG integer IP freeze registers.
    pub fn parse(host: &str, port: u16) -> Self {
        let trimmed = host.trim();
        let clean_host = trimmed
            .strip_prefix('[')
            .and_then(|s| s.strip_suffix(']'))
            .unwrap_or(trimmed);
        let port_be = port.to_be();

        if let Some(ip_u128) = velda_core::lut::ip::parse_ip_u128(clean_host.as_bytes()) {
            let addr = ip_u128_to_socket_addr(ip_u128, port_be);
            Self::Ip {
                ip_u128,
                port_be,
                addr,
            }
        } else {
            Self::Dns {
                host: clean_host.into(),
                port,
                port_be,
                cached_ip_lo: AtomicU64::new(0),
                cached_ip_hi: AtomicU64::new(0),
                cached_ip_seq: AtomicU64::new(0),
            }
        }
    }

    /// Returns the active 128-bit integer IP (`0` if an unresolved `Dns` target has not
    /// completed its first lookup yet) using a torn-read-free SeqLock over `lo`/`hi`.
    #[inline]
    pub fn cached_ip_u128(&self) -> u128 {
        match self {
            Self::Ip { ip_u128, .. } => *ip_u128,
            Self::Dns {
                cached_ip_lo,
                cached_ip_hi,
                cached_ip_seq,
                ..
            } => loop {
                let seq1 = cached_ip_seq.load(Ordering::Acquire);
                if (seq1 & 1) != 0 {
                    std::hint::spin_loop();
                    continue;
                }
                let lo = cached_ip_lo.load(Ordering::Acquire);
                let hi = cached_ip_hi.load(Ordering::Acquire);
                std::sync::atomic::fence(Ordering::Acquire);
                let seq2 = cached_ip_seq.load(Ordering::Relaxed);
                if seq1 == seq2 {
                    return ((hi as u128) << 64) | (lo as u128);
                }
            },
        }
    }

    #[inline]
    pub(crate) fn freeze_ip(&self, ip: std::net::IpAddr) {
        if let Self::Dns {
            cached_ip_lo,
            cached_ip_hi,
            cached_ip_seq,
            ..
        } = self
        {
            let ip_u128 = velda_core::lut::ip::encode_ip_u128(ip);
            let mut seq = cached_ip_seq.load(Ordering::Relaxed);
            loop {
                if (seq & 1) != 0 {
                    std::hint::spin_loop();
                    seq = cached_ip_seq.load(Ordering::Relaxed);
                    continue;
                }
                match cached_ip_seq.compare_exchange_weak(
                    seq,
                    seq.wrapping_add(1),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(observed) => seq = observed,
                }
            }
            std::sync::atomic::fence(Ordering::Release);
            cached_ip_lo.store(ip_u128 as u64, Ordering::Release);
            cached_ip_hi.store((ip_u128 >> 64) as u64, Ordering::Release);
            cached_ip_seq.store(seq.wrapping_add(2), Ordering::Release);
        }
    }
}

// ============================================================================
// Cold Blueprint & Unified TCP/TLS Stream
// ============================================================================

/// Isolated cold-path connection creation state separated from hot `EtpPageArena` cache lines.
#[repr(align(64))]
#[derive(Debug, Clone)]
pub struct ColdConnectBlueprint {
    pub id: Box<str>,
    pub target: ColdTargetAddr,
    pub tls: Option<(Arc<ClientConfig>, ServerName<'static>)>,
    pub connect_timeout: Duration,
}

impl ColdConnectBlueprint {
    /// Constructs a new 64-byte aligned [`ColdConnectBlueprint`].
    pub fn new(
        id: &str,
        host: &str,
        port: u16,
        tls: Option<(Arc<ClientConfig>, ServerName<'static>)>,
        connect_timeout: Duration,
    ) -> Self {
        Self {
            id: id.into(),
            target: ColdTargetAddr::parse(host, port),
            tls,
            connect_timeout,
        }
    }

    /// Establishes a fresh [`EtpStream`] from this blueprint, tagging timeouts with `self.id`.
    pub async fn connect_stream(&self) -> Result<EtpStream, EtpError> {
        match EtpStream::connect_target(&self.target, self.tls.as_ref(), self.connect_timeout).await
        {
            Ok(stream) => Ok(stream),
            Err(EtpError::Timeout { timeout_ms, .. }) => Err(EtpError::Timeout {
                id: self.id.to_string(),
                timeout_ms,
            }),
            Err(err) => Err(err),
        }
    }

    /// Establishes a fresh raw TCP socket with `TCP_NODELAY` (for StartTLS negotiation before upgrade).
    pub async fn connect_tcp(&self) -> Result<TcpStream, EtpError> {
        let timeout_ms = u64::try_from(self.connect_timeout.as_millis()).unwrap_or(u64::MAX);
        match tokio::time::timeout(self.connect_timeout, connect_tcp_target(&self.target)).await {
            Ok(res) => res,
            Err(_) => Err(EtpError::Timeout {
                id: self.id.to_string(),
                timeout_ms,
            }),
        }
    }

    /// Upgrades an already-connected TCP socket (after protocol StartTLS negotiation) to [`EtpStream::Tls`].
    pub async fn upgrade_tls(&self, tcp: TcpStream) -> Result<EtpStream, EtpError> {
        let Some((client_config, server_name)) = &self.tls else {
            return Err(EtpError::Tls(format!(
                "ETP '{}' cannot upgrade to TLS without compiled TLS configuration",
                self.id
            )));
        };
        let timeout_ms = u64::try_from(self.connect_timeout.as_millis()).unwrap_or(u64::MAX);
        let connector = TlsConnector::from(Arc::clone(client_config));
        match tokio::time::timeout(
            self.connect_timeout,
            connector.connect(server_name.clone(), tcp),
        )
        .await
        {
            Ok(Ok(tls_stream)) => Ok(EtpStream::Tls(Box::new(tls_stream))),
            Ok(Err(e)) => Err(EtpError::Tls(format!("TLS handshake failed: {e}"))),
            Err(_) => Err(EtpError::Timeout {
                id: self.id.to_string(),
                timeout_ms,
            }),
        }
    }
}

async fn connect_tcp_target(target: &ColdTargetAddr) -> Result<TcpStream, EtpError> {
    let tcp = match target {
        ColdTargetAddr::Ip { addr, .. } => TcpStream::connect(*addr).await?,
        ColdTargetAddr::Dns {
            host,
            port,
            port_be,
            ..
        } => {
            let cached = target.cached_ip_u128();
            if cached != 0 {
                let cached_addr = ip_u128_to_socket_addr(cached, *port_be);
                if let Ok(stream) = TcpStream::connect(cached_addr).await {
                    stream
                } else {
                    connect_and_freeze_dns(target, host, *port).await?
                }
            } else {
                connect_and_freeze_dns(target, host, *port).await?
            }
        }
    };
    tcp.set_nodelay(true)?;
    Ok(tcp)
}

/// Unified TCP or TLS stream connected to an External Third-Party endpoint.
#[derive(Debug)]
pub enum EtpStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl EtpStream {
    /// Connects directly to a pre-parsed [`ColdTargetAddr`] with `TCP_NODELAY` enabled and
    /// optional TLS handshake bounded by `timeout`.
    pub async fn connect_target(
        target: &ColdTargetAddr,
        tls: Option<&(Arc<ClientConfig>, ServerName<'static>)>,
        timeout: Duration,
    ) -> Result<Self, EtpError> {
        let timeout_ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);

        let fut = async {
            let tcp = connect_tcp_target(target).await?;

            if let Some((client_config, server_name)) = tls {
                let connector = TlsConnector::from(Arc::clone(client_config));
                let tls_stream = connector
                    .connect(server_name.clone(), tcp)
                    .await
                    .map_err(|e| EtpError::Tls(format!("TLS handshake failed: {e}")))?;
                Ok(Self::Tls(Box::new(tls_stream)))
            } else {
                Ok(Self::Plain(tcp))
            }
        };

        match tokio::time::timeout(timeout, fut).await {
            Ok(res) => res,
            Err(_) => {
                let id = match target {
                    ColdTargetAddr::Ip { addr, .. } => addr.to_string(),
                    ColdTargetAddr::Dns { host, port, .. } => format!("{host}:{port}"),
                };
                Err(EtpError::Timeout { id, timeout_ms })
            }
        }
    }

    /// Connects to `(host, port)` with `TCP_NODELAY` enabled and optional TLS handshake
    /// bounded by `timeout`.
    pub async fn connect(
        host: &str,
        port: u16,
        tls: Option<&(Arc<ClientConfig>, ServerName<'static>)>,
        timeout: Duration,
    ) -> Result<Self, EtpError> {
        let target = ColdTargetAddr::parse(host, port);
        Self::connect_target(&target, tls, timeout).await
    }

    /// Configures `TCP_NODELAY` on the underlying TCP socket.
    pub fn set_nodelay(&self, nodelay: bool) -> Result<(), EtpError> {
        match self {
            Self::Plain(tcp) => tcp.set_nodelay(nodelay).map_err(EtpError::Io),
            Self::Tls(tls) => tls.get_ref().0.set_nodelay(nodelay).map_err(EtpError::Io),
        }
    }

    /// Returns `true` if this stream is TLS-encrypted.
    pub fn is_tls(&self) -> bool {
        matches!(self, Self::Tls(_))
    }

    /// Writes the entire buffer to the stream.
    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), EtpError> {
        match self {
            Self::Plain(tcp) => tcp.write_all(buf).await?,
            Self::Tls(tls) => tls.write_all(buf).await?,
        }
        Ok(())
    }

    /// Flushes any buffered outbound bytes.
    pub async fn flush(&mut self) -> Result<(), EtpError> {
        match self {
            Self::Plain(tcp) => tcp.flush().await?,
            Self::Tls(tls) => tls.flush().await?,
        }
        Ok(())
    }

    /// Reads up to `buf.len()` bytes from the stream.
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize, EtpError> {
        let n = match self {
            Self::Plain(tcp) => tcp.read(buf).await?,
            Self::Tls(tls) => tls.read(buf).await?,
        };
        Ok(n)
    }

    /// Reads the exact number of bytes required to fill `buf`.
    pub async fn read_exact(&mut self, buf: &mut [u8]) -> Result<usize, EtpError> {
        let n = match self {
            Self::Plain(tcp) => tcp.read_exact(buf).await?,
            Self::Tls(tls) => tls.read_exact(buf).await?,
        };
        Ok(n)
    }
}

impl AsyncRead for EtpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(tcp) => Pin::new(tcp).poll_read(cx, buf),
            Self::Tls(tls) => Pin::new(tls.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for EtpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(tcp) => Pin::new(tcp).poll_write(cx, buf),
            Self::Tls(tls) => Pin::new(tls.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(tcp) => Pin::new(tcp).poll_flush(cx),
            Self::Tls(tls) => Pin::new(tls.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(tcp) => Pin::new(tcp).poll_shutdown(cx),
            Self::Tls(tls) => Pin::new(tls.as_mut()).poll_shutdown(cx),
        }
    }
}

async fn connect_and_freeze_dns(
    target: &ColdTargetAddr,
    host: &str,
    port: u16,
) -> Result<TcpStream, EtpError> {
    let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host, port)).await?.collect();
    if addrs.is_empty() {
        return Err(EtpError::Io(std::io::Error::new(
            std::io::ErrorKind::AddrNotAvailable,
            format!("DNS lookup for '{host}:{port}' returned no addresses"),
        )));
    }

    let mut last_err: Option<std::io::Error> = None;
    for addr in addrs {
        match TcpStream::connect(addr).await {
            Ok(stream) => {
                target.freeze_ip(addr.ip());
                return Ok(stream);
            }
            Err(err) => {
                last_err = Some(err);
            }
        }
    }

    Err(EtpError::Io(last_err.unwrap_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::AddrNotAvailable,
            format!("Failed to connect to resolved addresses for '{host}:{port}'"),
        )
    })))
}
