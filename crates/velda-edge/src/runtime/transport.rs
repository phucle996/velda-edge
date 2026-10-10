//! Transport binding compiler.
//!
//! Converts declarative [`ListenerConfig`] into `velda-transport` [`IngressBinding`]s
//! ready for `TrafficEngine` reconciliation.

use std::net::SocketAddr;

use velda_sync::post_sync::listener::ListenerConfig;
use velda_transport::{IngressBinding, TcpBinding, TcpListenerConfig, UdpBinding, UdpSocketConfig};

use crate::error::EdgeError;

/// Converts a list of listener configurations into transport-ready [`IngressBinding`]s
/// applying host/tier-tuned TCP and UDP socket configurations.
pub(super) fn active_bindings_with_configs(
    listeners: &[ListenerConfig],
    tcp_config: Option<&TcpListenerConfig>,
    udp_config: Option<&UdpSocketConfig>,
) -> Result<Vec<IngressBinding>, EdgeError> {
    let mut bindings = Vec::with_capacity(listeners.len());
    for cfg in listeners {
        let binding = listener_to_binding(cfg, tcp_config, udp_config)?;
        bindings.push(binding);
    }
    Ok(bindings)
}

/// Converts a declarative `ListenerConfig` into a `velda-transport` `IngressBinding`.
fn listener_to_binding(
    config: &ListenerConfig,
    tcp_config: Option<&TcpListenerConfig>,
    udp_config: Option<&UdpSocketConfig>,
) -> Result<IngressBinding, EdgeError> {
    if config.id.trim().is_empty() {
        return Err(EdgeError::InvalidConfig {
            detail: "listener ID cannot be empty".to_string(),
        });
    }

    let addr: SocketAddr = config
        .address
        .parse()
        .map_err(|e| EdgeError::InvalidAddress {
            id: config.id.clone(),
            addr: config.address.clone(),
            reason: format!("{e}"),
        })?;

    if config.transport.protocol.eq_ignore_ascii_case("tcp") {
        let mut tcp = TcpBinding::new(&config.id, addr, config.tls.enabled);
        if let Some(cfg) = tcp_config {
            tcp.config = cfg.clone();
        }
        Ok(IngressBinding::Tcp(tcp))
    } else if config.transport.protocol.eq_ignore_ascii_case("udp") {
        let mut udp = UdpBinding::new(&config.id, addr, config.tls.enabled);
        if let Some(cfg) = udp_config {
            udp.config = cfg.clone();
        }
        Ok(IngressBinding::Udp(udp))
    } else {
        Err(EdgeError::InvalidConfig {
            detail: format!(
                "unsupported transport protocol '{}' for listener '{}' (must be 'tcp' or 'udp')",
                config.transport.protocol, config.id
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use velda_sync::post_sync::listener::{
        ListenerApplicationConfig, ListenerTlsConfig, ListenerTransportConfig,
    };

    #[test]
    fn test_listener_to_binding_mapping() {
        // Raw TCP -> TcpBinding
        let raw_tcp = ListenerConfig {
            id: "tcp-raw".into(),
            address: "127.0.0.1:9000".into(),
            transport: ListenerTransportConfig {
                protocol: "tcp".into(),
            },
            application: ListenerApplicationConfig {
                protocol: "raw".into(),
                version: None,
                streaming: velda_sync::StreamingMode::DISABLED,
            },
            tls: ListenerTlsConfig::default(),
            http1: None,
            http2: None,
            grpc: None,
            http3: None,
            raw: None,
        };
        let binding = listener_to_binding(&raw_tcp, None, None).unwrap();
        assert!(binding.is_tcp());
        assert_eq!(binding.id(), "tcp-raw");

        // HTTP/1.1 over TCP -> TcpBinding
        let http1 = ListenerConfig {
            id: "http".into(),
            address: "127.0.0.1:80".into(),
            transport: ListenerTransportConfig {
                protocol: "tcp".into(),
            },
            application: ListenerApplicationConfig {
                protocol: "http".into(),
                version: Some("1.1".into()),
                streaming: velda_sync::StreamingMode::DISABLED,
            },
            tls: ListenerTlsConfig::default(),
            http1: None,
            http2: None,
            grpc: None,
            http3: None,
            raw: None,
        };
        let binding = listener_to_binding(&http1, None, None).unwrap();
        assert!(binding.is_tcp());
        assert_eq!(binding.id(), "http");

        // HTTP/2 over TCP -> TcpBinding
        let http2 = ListenerConfig {
            id: "http2".into(),
            address: "127.0.0.1:8080".into(),
            transport: ListenerTransportConfig {
                protocol: "tcp".into(),
            },
            application: ListenerApplicationConfig {
                protocol: "http".into(),
                version: Some("2".into()),
                streaming: velda_sync::StreamingMode::DISABLED,
            },
            tls: ListenerTlsConfig::default(),
            http1: None,
            http2: None,
            grpc: None,
            http3: None,
            raw: None,
        };
        let binding = listener_to_binding(&http2, None, None).unwrap();
        assert!(binding.is_tcp());

        // HTTP over UDP -> UdpBinding
        let http3 = ListenerConfig {
            id: "http3".into(),
            address: "127.0.0.1:443".into(),
            transport: ListenerTransportConfig {
                protocol: "udp".into(),
            },
            application: ListenerApplicationConfig {
                protocol: "http".into(),
                version: Some("3".into()),
                streaming: velda_sync::StreamingMode::DISABLED,
            },
            tls: ListenerTlsConfig { enabled: true },
            http1: None,
            http2: None,
            grpc: None,
            http3: None,
            raw: None,
        };
        let binding = listener_to_binding(&http3, None, None).unwrap();
        assert!(binding.is_udp());
        assert_eq!(binding.id(), "http3");
        assert!(binding.tls_enabled());
    }
}
