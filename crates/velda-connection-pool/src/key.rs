//! Compact 24-byte connection identity keys with out-of-line TLS string interning.
//!
//! ```text
//! CompactPoolKey (24 bytes, Copy, align=8 -> 170 keys / 4KB page)
//! ┌─────────────────┬─────────────────┬─────────┬───────┬───────┬────────────┐
//! │ ip_hi (8B)      │ ip_lo (8B)      │ port_be │ proto │ alpn  │ sni_id     │
//! │                 │                 │  (2B)   │ (1B)  │ (1B)  │   (4B)     │
//! └─────────────────┴─────────────────┴─────────┴───────┴───────┴────────────┘
//!                                                         │       └─ 0xFFFF_FFFF = None
//!                                                         └───────── 0xFF = None
//! ```
//!
//! Because Velda Edge typically performs TLS termination at the edge, most backend
//! connections are plaintext (`sni_id = 0xFFFF_FFFF`, `alpn_id = 0xFF`), incurring
//! zero pointer chasing, zero `Arc` refcounting, and zero heap allocations.

use std::fmt;
use std::sync::RwLock;

/// Sentinel indicating no ALPN token (plaintext or default protocol).
pub const ALPN_NONE: u8 = 0xFF;

/// Sentinel indicating no SNI hostname (plaintext backend connection).
pub const SNI_NONE: u32 = 0xFFFF_FFFF;

/// Canonical protocol identifiers for [`CompactPoolKey::proto_id`].
pub const PROTO_TCP: u8 = 0;
pub const PROTO_HTTP1: u8 = 1;
pub const PROTO_HTTP2: u8 = 2;
pub const PROTO_HTTP3: u8 = 3;
pub const PROTO_GRPC: u8 = 4;

/// Compact 24-byte `Copy` connection identity key (`170 keys / 4KB page`).
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactPoolKey {
    /// Upper 64 bits of `u128` backend IP (IPv4-mapped `::ffff:a.b.c.d` or IPv6).
    pub ip_hi: u64,
    /// Lower 64 bits of `u128` backend IP.
    pub ip_lo: u64,
    /// Target physical backend port in network byte order (big-endian).
    pub port_be: u16,
    /// Protocol identifier (`PROTO_TCP`, `PROTO_HTTP1`, `PROTO_HTTP2`, ...).
    pub proto_id: u8,
    /// Interned ALPN index (`0xFF` = None).
    pub alpn_id: u8,
    /// Interned SNI index (`0xFFFF_FFFF` = None).
    pub sni_id: u32,
}

const _: () = assert!(std::mem::size_of::<CompactPoolKey>() == 24);
const _: () = assert!(std::mem::align_of::<CompactPoolKey>() == 8);

impl CompactPoolKey {
    /// Creates a plaintext TCP connection key (`sni_id = SNI_NONE`, `alpn_id = ALPN_NONE`).
    #[inline]
    pub const fn tcp(target_ip: u128, target_port_be: u16) -> Self {
        Self::new(target_ip, target_port_be, PROTO_TCP, SNI_NONE, ALPN_NONE)
    }

    /// Creates an HTTP/1.1 plaintext connection key (`sni_id = SNI_NONE`, `alpn_id = ALPN_NONE`).
    #[inline]
    pub const fn http1(target_ip: u128, target_port_be: u16) -> Self {
        Self::new(target_ip, target_port_be, PROTO_HTTP1, SNI_NONE, ALPN_NONE)
    }

    /// Creates a connection key with explicit protocol and interned TLS dimensions.
    #[inline]
    pub const fn new(
        target_ip: u128,
        target_port_be: u16,
        proto_id: u8,
        sni_id: u32,
        alpn_id: u8,
    ) -> Self {
        Self {
            ip_hi: (target_ip >> 64) as u64,
            ip_lo: target_ip as u64,
            port_be: target_port_be,
            proto_id,
            alpn_id,
            sni_id,
        }
    }

    /// Reconstructs the `u128` target IP address.
    #[inline]
    pub const fn target_ip(&self) -> u128 {
        ((self.ip_hi as u128) << 64) | (self.ip_lo as u128)
    }

    /// Sets the interned SNI ID on this key.
    #[inline]
    pub const fn with_sni_id(mut self, sni_id: u32) -> Self {
        self.sni_id = sni_id;
        self
    }

    /// Sets the interned ALPN ID on this key.
    #[inline]
    pub const fn with_alpn_id(mut self, alpn_id: u8) -> Self {
        self.alpn_id = alpn_id;
        self
    }
}

impl fmt::Display for CompactPoolKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ip = self.target_ip();
        let (buf, len) = velda_core::lut::ip::format_ip_bytes(ip);
        let ip_str = std::str::from_utf8(&buf[..len]).unwrap_or("invalid-ip");
        let port = u16::from_be(self.port_be);
        let proto = match self.proto_id {
            PROTO_TCP => "tcp",
            PROTO_HTTP1 => "http1",
            PROTO_HTTP2 => "http2",
            PROTO_HTTP3 => "http3",
            PROTO_GRPC => "grpc",
            _ => "other",
        };
        if velda_core::lut::ip::is_v4_u128(ip) {
            write!(
                f,
                "{ip_str}:{port} [{proto}] (sni_id={:#x}, alpn_id={:#x})",
                self.sni_id, self.alpn_id
            )
        } else {
            write!(
                f,
                "[{ip_str}]:{port} [{proto}] (sni_id={:#x}, alpn_id={:#x})",
                self.sni_id, self.alpn_id
            )
        }
    }
}

/// Out-of-line string interning arena for backend SNI and ALPN tokens.
///
/// Populated only during config load / cold-path setup when TLS-to-backend is enabled,
/// keeping the hot-path `CompactPoolKey` completely free of `Arc<str>` or heap pointers.
#[derive(Debug, Default)]
pub struct TlsStringArena {
    snis: RwLock<Vec<Box<str>>>,
    alpns: RwLock<Vec<Box<str>>>,
}

impl TlsStringArena {
    /// Creates an empty TLS string interning arena.
    pub fn new() -> Self {
        Self::default()
    }

    /// Interns an optional SNI string, returning `SNI_NONE` (`0xFFFF_FFFF`) if `None` or empty.
    pub fn intern_sni(&self, sni: Option<&str>) -> u32 {
        let Some(s) = sni.filter(|v| !v.is_empty()) else {
            return SNI_NONE;
        };
        {
            let guard = self.snis.read().unwrap_or_else(|e| e.into_inner());
            if let Some(pos) = guard.iter().position(|item| item.as_ref() == s) {
                return pos as u32;
            }
        }
        let mut guard = self.snis.write().unwrap_or_else(|e| e.into_inner());
        if let Some(pos) = guard.iter().position(|item| item.as_ref() == s) {
            return pos as u32;
        }
        let idx = guard.len();
        assert!(
            idx < SNI_NONE as usize,
            "TlsStringArena SNI capacity exceeded"
        );
        guard.push(s.into());
        idx as u32
    }

    /// Interns an optional ALPN token, returning `ALPN_NONE` (`0xFF`) if `None` or empty.
    pub fn intern_alpn(&self, alpn: Option<&str>) -> u8 {
        let Some(s) = alpn.filter(|v| !v.is_empty()) else {
            return ALPN_NONE;
        };
        {
            let guard = self.alpns.read().unwrap_or_else(|e| e.into_inner());
            if let Some(pos) = guard.iter().position(|item| item.as_ref() == s) {
                return pos as u8;
            }
        }
        let mut guard = self.alpns.write().unwrap_or_else(|e| e.into_inner());
        if let Some(pos) = guard.iter().position(|item| item.as_ref() == s) {
            return pos as u8;
        }
        let idx = guard.len();
        assert!(
            idx < ALPN_NONE as usize,
            "TlsStringArena ALPN capacity exceeded"
        );
        guard.push(s.into());
        idx as u8
    }

    /// Resolves an interned SNI ID back to its string slice, or `None` if `SNI_NONE`.
    pub fn resolve_sni(&self, id: u32) -> Option<String> {
        if id == SNI_NONE {
            return None;
        }
        let guard = self.snis.read().unwrap_or_else(|e| e.into_inner());
        guard.get(id as usize).map(|s| s.to_string())
    }

    /// Resolves an interned ALPN ID back to its string slice, or `None` if `ALPN_NONE`.
    pub fn resolve_alpn(&self, id: u8) -> Option<String> {
        if id == ALPN_NONE {
            return None;
        }
        let guard = self.alpns.read().unwrap_or_else(|e| e.into_inner());
        guard.get(id as usize).map(|s| s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use velda_core::lut::ip::v4_to_u128;

    #[test]
    fn test_compact_key_size_and_sentinels() {
        assert_eq!(std::mem::size_of::<CompactPoolKey>(), 24);
        assert_eq!(4096 / std::mem::size_of::<CompactPoolKey>(), 170);

        let ip = v4_to_u128(0x0a000001);
        let k1 = CompactPoolKey::tcp(ip, 8080u16.to_be());
        assert_eq!(k1.target_ip(), ip);
        assert_eq!(k1.sni_id, SNI_NONE);
        assert_eq!(k1.alpn_id, ALPN_NONE);
        assert_eq!(k1.proto_id, PROTO_TCP);
    }

    #[test]
    fn test_tls_string_arena_interning() {
        let arena = TlsStringArena::new();
        assert_eq!(arena.intern_sni(None), SNI_NONE);
        assert_eq!(arena.intern_sni(Some("")), SNI_NONE);
        assert_eq!(arena.intern_alpn(None), ALPN_NONE);

        let s1 = arena.intern_sni(Some("api.velda.internal"));
        let s2 = arena.intern_sni(Some("api.velda.internal"));
        let s3 = arena.intern_sni(Some("db.velda.internal"));
        assert_eq!(s1, s2);
        assert_ne!(s1, s3);
        assert_eq!(arena.resolve_sni(s1).as_deref(), Some("api.velda.internal"));
        assert_eq!(arena.resolve_sni(SNI_NONE), None);

        let a1 = arena.intern_alpn(Some("h2"));
        let a2 = arena.intern_alpn(Some("http/1.1"));
        assert_ne!(a1, a2);
        assert_eq!(arena.resolve_alpn(a1).as_deref(), Some("h2"));
        assert_eq!(arena.resolve_alpn(ALPN_NONE), None);
    }
}
