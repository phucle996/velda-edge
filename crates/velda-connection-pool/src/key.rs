//! Connection identity and pooling keys.

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

/// Connection reuse identity.
///
/// Connections are pooled and reused strictly when all identity dimensions match.
/// Zero-allocation on hot path via `Arc<str>` for string dimensions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConnectionKey {
    /// Target physical backend IP socket address.
    pub target_addr: SocketAddr,
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
    pub fn tcp(target_addr: SocketAddr) -> Self {
        Self {
            target_addr,
            protocol: Arc::from("tcp"),
            sni: None,
            alpn: None,
        }
    }

    /// Creates a connection key for an HTTP/1 or HTTP/2 connection.
    #[inline]
    pub fn http(
        target_addr: SocketAddr,
        protocol: impl Into<Arc<str>>,
        sni: Option<Arc<str>>,
        alpn: Option<Arc<str>>,
    ) -> Self {
        Self {
            target_addr,
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
        write!(
            f,
            "{} [{}] (sni={:?}, alpn={:?})",
            self.target_addr, self.protocol, self.sni, self.alpn
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_connection_key_tcp() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let key = ConnectionKey::tcp(addr);
        assert_eq!(key.target_addr, addr);
        assert_eq!(&*key.protocol, "tcp");
        assert!(key.sni.is_none());
        assert!(key.alpn.is_none());
    }

    #[test]
    fn test_connection_key_http_and_builder() {
        let addr: SocketAddr = "10.0.0.1:443".parse().unwrap();
        let key = ConnectionKey::http(addr, "http2", None, None)
            .with_sni("api.velda.io")
            .with_alpn("h2");
        assert_eq!(key.target_addr, addr);
        assert_eq!(&*key.protocol, "http2");
        assert_eq!(key.sni.as_deref(), Some("api.velda.io"));
        assert_eq!(key.alpn.as_deref(), Some("h2"));
    }

    #[test]
    fn test_connection_key_hash_and_equality() {
        let addr1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
        let addr2: SocketAddr = "10.0.0.2:8080".parse().unwrap();

        let k1 = ConnectionKey::tcp(addr1);
        let k1_dup = ConnectionKey::tcp(addr1);
        let k2 = ConnectionKey::tcp(addr2);
        let k3 = ConnectionKey::http(addr1, "http1", None, None);

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
        let addr: SocketAddr = "192.168.1.1:8443".parse().unwrap();
        let key = ConnectionKey::http(addr, "http2", Some("secure.io".into()), None);
        let s = key.to_string();
        assert!(s.contains("192.168.1.1:8443"));
        assert!(s.contains("http2"));
        assert!(s.contains("secure.io"));
    }

    #[test]
    fn test_connection_key_ipv6() {
        let addr: SocketAddr = "[::1]:8443".parse().unwrap();
        let key = ConnectionKey::http(
            addr,
            "http3",
            Some("ipv6.example.com".into()),
            Some("h3".into()),
        );
        assert_eq!(key.target_addr, addr);
        assert_eq!(&*key.protocol, "http3");
        assert_eq!(key.sni.as_deref(), Some("ipv6.example.com"));
        assert_eq!(key.alpn.as_deref(), Some("h3"));
    }

    #[test]
    fn test_connection_key_sni_and_alpn_differentiation() {
        let addr: SocketAddr = "10.0.0.1:443".parse().unwrap();
        let k1 = ConnectionKey::http(addr, "https", Some("api.com".into()), None);
        let k2 = ConnectionKey::http(addr, "https", Some("web.com".into()), None);
        let k3 = ConnectionKey::http(addr, "https", Some("api.com".into()), Some("h2".into()));

        assert_ne!(k1, k2);
        assert_ne!(k1, k3);
        assert_ne!(k2, k3);
    }
}
