//! Transport binding compiler.
//!
//! Converts declarative [`ListenerConfig`] into `velda-transport` [`IngressBinding`]s
//! ready for `TrafficEngine` reconciliation.

use std::net::SocketAddr;

use velda_sync::post_sync::listener::ListenerConfig;
use velda_transport::IngressBinding;

use crate::error::EdgeError;

/// Converts a list of listener configurations into transport-ready [`IngressBinding`]s.
pub(super) fn active_bindings(
    listeners: &[ListenerConfig],
) -> Result<Vec<IngressBinding>, EdgeError> {
    let mut bindings = Vec::with_capacity(listeners.len());
    for cfg in listeners {
        bindings.push(listener_to_binding(cfg)?);
    }
    Ok(bindings)
}

/// Converts a declarative `ListenerConfig` into a `velda-transport` `IngressBinding`.
fn listener_to_binding(config: &ListenerConfig) -> Result<IngressBinding, EdgeError> {
    let addr: SocketAddr = config
        .address
        .parse()
        .map_err(|e| EdgeError::InvalidAddress {
            id: config.id.clone(),
            addr: config.address.clone(),
            reason: format!("{e}"),
        })?;

    let binding = IngressBinding::from_protocols(
        &config.id,
        addr,
        &config.transport.protocol,
        &config.application.protocol,
        config.tls.enabled,
    )?;

    Ok(binding)
}

#[cfg(test)]
mod tests {
    use super::*;
    use velda_sync::post_sync::listener::{
        ListenerApplicationConfig, ListenerLimitsConfig, ListenerTlsConfig, ListenerTransportConfig,
    };
    use velda_transport::PathKind;

    #[test]
    fn test_listener_to_binding_mapping() {
        // Raw TCP -> L4Direct
        let raw_tcp = ListenerConfig {
            id: "tcp-raw".into(),
            address: "127.0.0.1:9000".into(),
            transport: ListenerTransportConfig {
                protocol: "tcp".into(),
            },
            application: ListenerApplicationConfig {
                protocol: "raw".into(),
                version: None,
            },
            tls: ListenerTlsConfig::default(),
            limits: ListenerLimitsConfig::default(),
        };
        let binding = listener_to_binding(&raw_tcp).unwrap();
        assert_eq!(binding.protocol, "tcp");
        assert_eq!(binding.path, PathKind::L4Direct);

        // HTTP/1.1 over TCP -> L7Handoff
        let http1 = ListenerConfig {
            id: "http".into(),
            address: "127.0.0.1:80".into(),
            transport: ListenerTransportConfig {
                protocol: "tcp".into(),
            },
            application: ListenerApplicationConfig {
                protocol: "http".into(),
                version: Some("1.1".into()),
            },
            tls: ListenerTlsConfig::default(),
            limits: ListenerLimitsConfig::default(),
        };
        let binding = listener_to_binding(&http1).unwrap();
        assert_eq!(binding.protocol, "tcp");
        assert_eq!(binding.path, PathKind::L7Handoff);

        // HTTP/2 over TCP -> L7Handoff
        let http2 = ListenerConfig {
            id: "http2".into(),
            address: "127.0.0.1:8080".into(),
            transport: ListenerTransportConfig {
                protocol: "tcp".into(),
            },
            application: ListenerApplicationConfig {
                protocol: "http".into(),
                version: Some("2".into()),
            },
            tls: ListenerTlsConfig::default(),
            limits: ListenerLimitsConfig::default(),
        };
        let binding = listener_to_binding(&http2).unwrap();
        assert_eq!(binding.protocol, "tcp");
        assert_eq!(binding.path, PathKind::L7Handoff);

        // HTTP over UDP -> L7Handoff
        let http3 = ListenerConfig {
            id: "http3".into(),
            address: "127.0.0.1:443".into(),
            transport: ListenerTransportConfig {
                protocol: "udp".into(),
            },
            application: ListenerApplicationConfig {
                protocol: "http".into(),
                version: Some("3".into()),
            },
            tls: ListenerTlsConfig { enabled: true },
            limits: ListenerLimitsConfig::default(),
        };
        let binding = listener_to_binding(&http3).unwrap();
        assert_eq!(binding.protocol, "udp");
        assert_eq!(binding.path, PathKind::L7Handoff);
        assert!(binding.tls_enabled);
    }
}
