//! 16-bit Non-Colliding TLS `ClientHello` Structural Bitfield Dictionary (`tls_traits`).
//!
//! ```text
//!  15..14 │ 13..12 │  11  │  10  │  9   │  8  │   7    │  6  │ 5..3 │ 2..0
//!  ext_bk │ cph_bk │ tick │ alps │ cmpc │ psk │ grease │ sni │ alpn │ ver
//! ```
//!
//! Constants only — `velda-tls` computes and populates these bits once per TLS/QUIC handshake.

/// Encoded TLS version values stored in `tls_traits` bits `0..=2`.
pub const TLS_VER_MASK: u16 = 0x0007;
pub const TLS_VER_NONE: u16 = 0;
pub const TLS_VER_LEGACY: u16 = 1;
pub const TLS_VER_12: u16 = 2;
pub const TLS_VER_13: u16 = 3;

/// Encoded ALPN token values stored in `tls_traits` bits `3..=5`.
pub const TLS_ALPN_SHIFT: u16 = 3;
pub const TLS_ALPN_MASK: u16 = 0x0007 << TLS_ALPN_SHIFT;
pub const TLS_ALPN_NONE: u16 = 0;
pub const TLS_ALPN_H1: u16 = 1;
pub const TLS_ALPN_H2: u16 = 2;
pub const TLS_ALPN_H3: u16 = 3;
pub const TLS_ALPN_OTHER: u16 = 4;

/// Boolean TLS structural flags stored in `tls_traits` bits `6..=11`.
pub const TLS_FLAG_HAS_SNI: u16 = 1 << 6;
pub const TLS_FLAG_HAS_GREASE: u16 = 1 << 7;
pub const TLS_FLAG_HAS_PSK: u16 = 1 << 8;
pub const TLS_FLAG_HAS_CERT_COMPRESS: u16 = 1 << 9;
pub const TLS_FLAG_HAS_ALPS: u16 = 1 << 10;
pub const TLS_FLAG_HAS_SESSION_TICKET: u16 = 1 << 11;

/// Cipher suite & extension count buckets stored in `tls_traits` bits `12..=15`.
pub const TLS_CIPHER_BUCKET_SHIFT: u16 = 12;
pub const TLS_CIPHER_BUCKET_MASK: u16 = 0x0003 << TLS_CIPHER_BUCKET_SHIFT;
pub const TLS_EXT_BUCKET_SHIFT: u16 = 14;
pub const TLS_EXT_BUCKET_MASK: u16 = 0x0003 << TLS_EXT_BUCKET_SHIFT;
