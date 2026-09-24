//! Ingress binding configuration and listener wrapper aligned with listeners.json schema.

use std::net::SocketAddr;

use super::classifier::PathKind;
use crate::connection::Connection;
use crate::error::{Result, TransportError};
use crate::tcp::config::TcpListenerConfig;
use crate::tcp::listener::TcpListener;

/// Ingress binding configuration matching the user's declared listener definition in listeners.json.
///
/// Strictly mirrors the user configuration without hardcoded gateway defaults:
/// - `id`: listener identifier (e.g. "http", "https", "tcp-ingress")
/// - `address`: socket address (e.g. "0.0.0.0:80")
/// - `protocol`: declared protocol ("http", "tcp", "udp")
/// - `tls`: TLS enablement and profile name
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngressBinding {
    /// Unique listener identifier declared in listeners.json.
    pub id: String,
    /// Local address to bind and listen on.
    pub addr: SocketAddr,
    /// Declared protocol string ("http", "tcp", or "udp").
    pub protocol: String,
    /// Whether TLS is enabled for this listener.
    pub tls_enabled: bool,
    /// Declared TLS profile name (e.g. "default") when TLS is enabled.
    pub tls_profile: Option<String>,
    /// Resolved path kind derived strictly from user's declared protocol and TLS state.
    pub path: PathKind,
    /// TCP socket listener options (e.g. nodelay, backlog, buffer sizes).
    pub tcp_config: TcpListenerConfig,
}

impl IngressBinding {
    /// Creates a new ingress binding matching the user's explicit configuration.
    pub fn new(
        id: impl Into<String>,
        addr: SocketAddr,
        protocol: impl Into<String>,
        tls_enabled: bool,
        tls_profile: Option<String>,
    ) -> Result<Self> {
        let id = id.into();
        let protocol = protocol.into();
        let proto_lower = protocol.to_ascii_lowercase();

        let path = match proto_lower.as_str() {
            "http" => {
                if tls_enabled {
                    PathKind::Tls
                } else {
                    PathKind::Http
                }
            }
            "tcp" => {
                if tls_enabled {
                    PathKind::Tls
                } else {
                    PathKind::L4Direct
                }
            }
            "udp" => PathKind::L4Direct,
            other => {
                return Err(TransportError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("unsupported listener protocol '{other}' for listener '{id}'"),
                )));
            }
        };

        Ok(Self {
            id,
            addr,
            protocol,
            tls_enabled,
            tls_profile,
            path,
            tcp_config: TcpListenerConfig::default(),
        })
    }

    /// Returns whether this binding is for UDP transport.
    #[inline]
    pub fn is_udp(&self) -> bool {
        self.protocol.eq_ignore_ascii_case("udp")
    }

    /// Returns whether this binding is for TCP-based transport (HTTP, HTTPS, or raw TCP).
    #[inline]
    pub fn is_tcp(&self) -> bool {
        !self.is_udp()
    }

    /// Configures the TCP listener socket parameters.
    pub fn with_tcp_config(mut self, config: TcpListenerConfig) -> Self {
        self.tcp_config = config;
        self
    }
}

/// Ingress listener managing client connection arrival according to user configuration.
#[derive(Debug)]
pub struct IngressListener {
    listener: TcpListener,
    binding: IngressBinding,
}

impl IngressListener {
    /// Binds an ingress listener according to the specified user binding configuration.
    pub fn bind(binding: IngressBinding) -> Result<Self> {
        let listener = TcpListener::bind(binding.addr, binding.tcp_config.clone())?;
        Ok(Self { listener, binding })
    }

    /// Returns the local bound address of this ingress listener.
    #[inline]
    pub const fn local_addr(&self) -> SocketAddr {
        self.listener.local_addr()
    }

    /// Returns a reference to the binding configuration.
    #[inline]
    pub const fn binding(&self) -> &IngressBinding {
        &self.binding
    }

    /// Returns the unique listener identifier.
    #[inline]
    pub fn id(&self) -> &str {
        &self.binding.id
    }

    /// Returns the resolved path kind for connections on this listener.
    #[inline]
    pub const fn path(&self) -> PathKind {
        self.binding.path
    }

    /// Accepts an incoming connection and returns its strictly declared traffic path.
    pub async fn accept(&self) -> Result<(Connection, PathKind)> {
        let conn = self.listener.accept().await?;
        Ok((conn, self.binding.path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ingress_binding_from_user_json_schema() {
        // Matches listeners.json "http"
        let http_binding =
            IngressBinding::new("http", "0.0.0.0:80".parse().unwrap(), "http", false, None)
                .unwrap();
        assert_eq!(http_binding.path, PathKind::Http);
        assert_eq!(http_binding.tls_profile, None);

        // Matches listeners.json "https"
        let https_binding = IngressBinding::new(
            "https",
            "0.0.0.0:443".parse().unwrap(),
            "http",
            true,
            Some("default".into()),
        )
        .unwrap();
        assert_eq!(https_binding.path, PathKind::Tls);
        assert_eq!(https_binding.tls_profile.as_deref(), Some("default"));

        // Matches listeners.json "tcp-ingress"
        let tcp_binding = IngressBinding::new(
            "tcp-ingress",
            "0.0.0.0:9000".parse().unwrap(),
            "tcp",
            false,
            None,
        )
        .unwrap();
        assert_eq!(tcp_binding.path, PathKind::L4Direct);
    }

    #[test]
    fn test_unsupported_protocol_fails() {
        let result = IngressBinding::new(
            "bad",
            "0.0.0.0:80".parse().unwrap(),
            "unsupported_proto",
            false,
            None,
        );
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_ingress_listener_binds_ephemeral_socket() {
        let binding =
            IngressBinding::new("active", "127.0.0.1:0".parse().unwrap(), "tcp", false, None)
                .unwrap();

        let listener = IngressListener::bind(binding).unwrap();
        assert_ne!(listener.local_addr().port(), 0);
        assert_eq!(listener.id(), "active");
        assert_eq!(listener.path(), PathKind::L4Direct);
    }
}
