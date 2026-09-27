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
    ) -> Result<Self> {
        let id = id.into();
        let protocol = protocol.into();
        let proto_lower = protocol.to_ascii_lowercase();

        let path = match proto_lower.as_str() {
            "tcp" => PathKind::L4Direct,
            "udp" => PathKind::L4Direct,
            "http" => PathKind::Http,
            "http1" => PathKind::Http1,
            "http2" => PathKind::Http2,
            "http3" => PathKind::Http3,
            "quic" => PathKind::Quic,
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
            path,
            tcp_config: TcpListenerConfig::default(),
        })
    }

    /// Returns whether this binding is for UDP-based transport (raw UDP or HTTP/3 / QUIC).
    #[inline]
    pub fn is_udp(&self) -> bool {
        matches!(
            self.protocol.to_ascii_lowercase().as_str(),
            "udp" | "http3" | "quic"
        )
    }

    /// Returns whether this binding is for TCP-based transport (HTTP/1, HTTP/2, TLS, or raw TCP).
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

    /// Spawns an ingress accept loop on the provided `JoinSet`.
    pub fn spawn_accept_loop<L4H, L7H, FutL4, FutL7>(
        ingress: std::sync::Arc<Self>,
        tasks: &mut tokio::task::JoinSet<()>,
        shutdown: tokio::sync::watch::Receiver<bool>,
        l4_fn: L4H,
        l7_fn: L7H,
    ) where
        L4H: Fn(Connection) -> FutL4 + Send + Sync + Clone + 'static,
        FutL4: std::future::Future<Output = ()> + Send + 'static,
        L7H: Fn(crate::forwarding::l7::TcpL7Handoff) -> FutL7 + Send + Sync + Clone + 'static,
        FutL7: std::future::Future<Output = ()> + Send + 'static,
    {
        tasks.spawn(async move {
            let mut shutdown = shutdown;
            let local_addr = ingress.local_addr();
            tracing::info!(
                listener_id = %ingress.id(),
                listen_addr = %local_addr,
                path = %ingress.path(),
                "TCP ingress loop running"
            );

            loop {
                if *shutdown.borrow() {
                    break;
                }

                tokio::select! {
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            tracing::info!(
                                listener_id = %ingress.id(),
                                listen_addr = %local_addr,
                                "TCP ingress loop shutting down"
                            );
                            break;
                        }
                    }
                    res = ingress.accept() => {
                        match res {
                            Ok((conn, path)) => {
                                tracing::debug!(
                                    listener_id = %ingress.id(),
                                    conn_id = %conn.id(),
                                    path = %path,
                                    peer = %conn.peer(),
                                    "Ingress accepted connection"
                                );
                                if path != PathKind::L4Direct || ingress.binding().tls_enabled {
                                    let handoff = crate::forwarding::l7::TcpL7Handoff::new(
                                        conn,
                                        path,
                                        ingress.binding().id.clone(),
                                    );
                                    tokio::spawn(l7_fn(handoff));
                                } else {
                                    tokio::spawn(l4_fn(conn));
                                }
                            }
                            Err(err) => {
                                tracing::error!(
                                    listener_id = %ingress.id(),
                                    listen_addr = %local_addr,
                                    error = %err,
                                    "TCP accept error"
                                );
                            }
                        }
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ingress_binding_from_user_json_schema() {
        // Matches listeners.json "http"
        let http_binding =
            IngressBinding::new("http", "0.0.0.0:80".parse().unwrap(), "http", false).unwrap();
        assert_eq!(http_binding.path, PathKind::Http);
        assert!(!http_binding.tls_enabled);

        // Matches listeners.json "https"
        let https_binding =
            IngressBinding::new("https", "0.0.0.0:443".parse().unwrap(), "http", true).unwrap();
        assert_eq!(https_binding.path, PathKind::Http);
        assert!(https_binding.tls_enabled);

        // Matches listeners.json "tcp-ingress"
        let tcp_binding =
            IngressBinding::new("tcp-ingress", "0.0.0.0:9000".parse().unwrap(), "tcp", false)
                .unwrap();
        assert_eq!(tcp_binding.path, PathKind::L4Direct);
        assert!(tcp_binding.is_tcp());
        assert!(!tcp_binding.is_udp());

        // UDP L4 binding
        let udp_binding =
            IngressBinding::new("udp-ingress", "0.0.0.0:53".parse().unwrap(), "udp", false)
                .unwrap();
        assert_eq!(udp_binding.path, PathKind::L4Direct);
        assert!(udp_binding.is_udp());
        assert!(!udp_binding.is_tcp());

        // HTTP/1 binding
        let h1_binding = IngressBinding::new(
            "h1-ingress",
            "0.0.0.0:8080".parse().unwrap(),
            "http1",
            false,
        )
        .unwrap();
        assert_eq!(h1_binding.path, PathKind::Http1);
        assert!(h1_binding.is_tcp());

        // HTTP/2 binding (cleartext H2C)
        let h2_binding = IngressBinding::new(
            "h2-ingress",
            "0.0.0.0:8082".parse().unwrap(),
            "http2",
            false,
        )
        .unwrap();
        assert_eq!(h2_binding.path, PathKind::Http2);
        assert!(h2_binding.is_tcp());

        // HTTP/3 binding (over UDP)
        let h3_binding =
            IngressBinding::new("h3-ingress", "0.0.0.0:8443".parse().unwrap(), "http3", true)
                .unwrap();
        assert_eq!(h3_binding.path, PathKind::Http3);
        assert!(h3_binding.is_udp());
        assert!(!h3_binding.is_tcp());

        // QUIC binding
        let quic_binding = IngressBinding::new(
            "quic-ingress",
            "0.0.0.0:4433".parse().unwrap(),
            "quic",
            true,
        )
        .unwrap();
        assert_eq!(quic_binding.path, PathKind::Quic);
        assert!(quic_binding.is_udp());
    }

    #[test]
    fn test_unsupported_protocol_fails() {
        let result = IngressBinding::new(
            "bad",
            "0.0.0.0:80".parse().unwrap(),
            "unsupported_proto",
            false,
        );
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_ingress_listener_binds_ephemeral_socket() {
        let binding =
            IngressBinding::new("active", "127.0.0.1:0".parse().unwrap(), "tcp", false).unwrap();

        let listener = IngressListener::bind(binding).unwrap();
        assert_ne!(listener.local_addr().port(), 0);
        assert_eq!(listener.id(), "active");
        assert_eq!(listener.path(), PathKind::L4Direct);
    }
}
