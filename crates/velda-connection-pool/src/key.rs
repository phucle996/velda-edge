//! Connection identity and pooling keys.

use std::fmt;
use std::sync::Arc;

/// Connection reuse identity.
///
/// Connections are pooled and reused strictly when all identity dimensions match.
/// Zero-allocation on hot path via `Arc<str>` for string dimensions and pure integer `(u128, u16)`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConnectionKey {
    /// Target physical backend IP (`u128` IPv4-mapped `::ffff:a.b.c.d` or IPv6).
    pub target_ip: u128,
    /// Target physical backend port in network byte order (big-endian).
    pub target_port_be: u16,
    /// Protocol or transport identity ("tcp", "http1", "http2").
    pub protocol: Arc<str>,
    /// Server Name Indication (SNI) when TLS is used.
    pub sni: Option<Arc<str>>,
    /// Application-Layer Protocol Negotiation (ALPN) token when applicable.
    pub alpn: Option<Arc<str>>,
}

impl ConnectionKey {
    /// Creates a new connection key for a plain TCP / L4 connection.
    #[inline]
    pub fn tcp(target_ip: u128, target_port_be: u16) -> Self {
        Self {
            target_ip,
            target_port_be,
            protocol: Arc::from("tcp"),
            sni: None,
            alpn: None,
        }
    }

    /// Creates a connection key for an HTTP/1 or HTTP/2 connection.
    #[inline]
    pub fn http(
        target_ip: u128,
        target_port_be: u16,
        protocol: impl Into<Arc<str>>,
        sni: Option<Arc<str>>,
        alpn: Option<Arc<str>>,
    ) -> Self {
        Self {
            target_ip,
            target_port_be,
            protocol: protocol.into(),
            sni,
            alpn,
        }
    }

    /// Sets the SNI on this connection key.
    #[inline]
    pub fn with_sni(mut self, sni: impl Into<Arc<str>>) -> Self {
        self.sni = Some(sni.into());
        self
    }

    /// Sets the ALPN token on this connection key.
    #[inline]
    pub fn with_alpn(mut self, alpn: impl Into<Arc<str>>) -> Self {
        self.alpn = Some(alpn.into());
        self
    }
}

impl fmt::Display for ConnectionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (buf, len) = velda_core::lut::ip::format_ip_bytes(self.target_ip);
        let ip_str = std::str::from_utf8(&buf[..len]).unwrap_or("invalid-ip");
        let port = u16::from_be(self.target_port_be);
        if velda_core::lut::ip::is_v4_u128(self.target_ip) {
            write!(
                f,
                "{ip_str}:{port} [{}] (sni={:?}, alpn={:?})",
                self.protocol, self.sni, self.alpn
            )
        } else {
            write!(
                f,
                "[{ip_str}]:{port} [{}] (sni={:?}, alpn={:?})",
                self.protocol, self.sni, self.alpn
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use velda_core::lut::ip::v4_to_u128;

    #[test]
    fn test_connection_key_tcp() {
        let ip = v4_to_u128(0x7F00_0001);
        let port_be = 8080u16.to_be();
        let key = ConnectionKey::tcp(ip, port_be);
        assert_eq!(key.target_ip, ip);
        assert_eq!(key.target_port_be, port_be);
        assert_eq!(&*key.protocol, "tcp");
        assert!(key.sni.is_none());
        assert!(key.alpn.is_none());
    }

    #[test]
    fn test_connection_key_http_and_builder() {
        let ip = v4_to_u128(0x0A00_0001);
        let port_be = 443u16.to_be();
        let key = ConnectionKey::http(ip, port_be, "http2", None, None)
            .with_sni("api.velda.io")
            .with_alpn("h2");
        assert_eq!(key.target_ip, ip);
        assert_eq!(key.target_port_be, port_be);
        assert_eq!(&*key.protocol, "http2");
        assert_eq!(key.sni.as_deref(), Some("api.velda.io"));
        assert_eq!(key.alpn.as_deref(), Some("h2"));
    }

    #[test]
    fn test_connection_key_hash_and_equality() {
        let ip1 = v4_to_u128(0x0A00_0001);
        let ip2 = v4_to_u128(0x0A00_0002);
        let port_be = 8080u16.to_be();

        let k1 = ConnectionKey::tcp(ip1, port_be);
        let k1_dup = ConnectionKey::tcp(ip1, port_be);
        let k2 = ConnectionKey::tcp(ip2, port_be);
        let k3 = ConnectionKey::http(ip1, port_be, "http1", None, None);

        assert_eq!(k1, k1_dup);
        assert_ne!(k1, k2);
        assert_ne!(k1, k3);

        let mut set = HashSet::new();
        set.insert(k1.clone());
        assert!(set.contains(&k1_dup));
        assert!(!set.contains(&k2));
    }

    #[test]
    fn test_connection_key_display() {
        let ip = v4_to_u128(u32::from_be_bytes([192, 168, 1, 1]));
        let port_be = 8443u16.to_be();
        let key = ConnectionKey::http(ip, port_be, "http2", Some("secure.io".into()), None);
        let s = key.to_string();
        assert!(s.contains("192.168.1.1:8443"));
        assert!(s.contains("http2"));
        assert!(s.contains("secure.io"));
    }

    #[test]
    fn test_connection_key_ipv6() {
        let ip = 1u128;
        let port_be = 8443u16.to_be();
        let key = ConnectionKey::http(
            ip,
            port_be,
            "http3",
            Some("ipv6.example.com".into()),
            Some("h3".into()),
        );
        assert_eq!(key.target_ip, ip);
        assert_eq!(key.target_port_be, port_be);
        assert_eq!(&*key.protocol, "http3");
        assert_eq!(key.sni.as_deref(), Some("ipv6.example.com"));
        assert_eq!(key.alpn.as_deref(), Some("h3"));
    }

    #[test]
    fn test_key_sni_alpn_diff() {
        let ip = v4_to_u128(0x0A00_0001);
        let port_be = 443u16.to_be();
        let k1 = ConnectionKey::http(ip, port_be, "https", Some("api.com".into()), None);
        let k2 = ConnectionKey::http(ip, port_be, "https", Some("web.com".into()), None);
        let k3 = ConnectionKey::http(
            ip,
            port_be,
            "https",
            Some("api.com".into()),
            Some("h2".into()),
        );

        assert_ne!(k1, k2);
        assert_ne!(k1, k3);
        assert_ne!(k2, k3);
    }
}
