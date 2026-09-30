//! Composer compilation from listener configurations.
//!
//! Compiles `ListenerConfig` declarations into a pre-built [`Composer`] instance
//! with typed [`CompiledListenerComposition`] entries ready for O(1) lookup
//! on the request serving hot path.

use velda_composer::{ApplicationProtocol, CompiledListenerComposition, Composer};
use velda_sync::post_sync::listener::ListenerConfig;

use crate::error::EdgeError;

/// Compiles a list of listener configurations into a [`Composer`] instance.
///
/// Only L7 listeners (application.protocol != "raw") produce composition entries.
/// L4 direct listeners are handled entirely by `velda-transport` and never reach Composer.
pub(crate) fn build_composer(listeners: &[ListenerConfig]) -> Result<Composer, EdgeError> {
    let mut compositions = Vec::new();

    for listener in listeners {
        // L4 raw listeners are dispatched directly by transport — skip
        if listener.application.protocol.eq_ignore_ascii_case("raw") {
            continue;
        }

        let protocol = ApplicationProtocol::from_str_proto(&listener.application.protocol)
            .ok_or_else(|| EdgeError::InvalidConfig {
                detail: format!(
                    "listener '{}': unsupported application protocol '{}'; must be 'http1', 'http2', 'http3', or 'grpc'",
                    listener.id, listener.application.protocol
                ),
            })?;

        let limits = listener.limits.to_ingress_limits();

        compositions.push(CompiledListenerComposition::new(
            &listener.id,
            protocol,
            listener.tls.enabled,
            limits,
        ));
    }

    Ok(Composer::with_listeners(compositions))
}

#[cfg(test)]
mod tests {
    use super::*;
    use velda_sync::post_sync::listener::{
        ListenerApplicationConfig, ListenerLimitsConfig, ListenerTlsConfig, ListenerTransportConfig,
    };

    #[test]
    fn test_build_composer_from_listeners() {
        let listeners = vec![
            // HTTP/1.1 cleartext → should produce composition
            ListenerConfig {
                id: "http".into(),
                address: "0.0.0.0:80".into(),
                transport: ListenerTransportConfig {
                    protocol: "tcp".into(),
                },
                application: ListenerApplicationConfig {
                    protocol: "http1".into(),
                    version: None,
                },
                tls: ListenerTlsConfig::default(),
                limits: ListenerLimitsConfig::default(),
            },
            // HTTP/2 with TLS → should produce composition
            ListenerConfig {
                id: "https".into(),
                address: "0.0.0.0:443".into(),
                transport: ListenerTransportConfig {
                    protocol: "tcp".into(),
                },
                application: ListenerApplicationConfig {
                    protocol: "http2".into(),
                    version: None,
                },
                tls: ListenerTlsConfig { enabled: true },
                limits: ListenerLimitsConfig::default(),
            },
            // Raw TCP → should be skipped (L4 direct)
            ListenerConfig {
                id: "tcp-raw".into(),
                address: "0.0.0.0:9000".into(),
                transport: ListenerTransportConfig {
                    protocol: "tcp".into(),
                },
                application: ListenerApplicationConfig {
                    protocol: "raw".into(),
                    version: None,
                },
                tls: ListenerTlsConfig::default(),
                limits: ListenerLimitsConfig::default(),
            },
        ];

        let composer = build_composer(&listeners).unwrap();

        // HTTP/1.1 cleartext listener registered
        let http = composer.get_listener("http").unwrap();
        assert_eq!(http.protocol, ApplicationProtocol::Http1);
        assert!(!http.tls_enabled);

        // HTTP/2 TLS listener registered
        let https = composer.get_listener("https").unwrap();
        assert_eq!(https.protocol, ApplicationProtocol::Http2);
        assert!(https.tls_enabled);

        // Raw TCP listener NOT registered (L4 direct)
        assert!(composer.get_listener("tcp-raw").is_none());
    }

    #[test]
    fn test_build_composer_empty_listeners() {
        let composer = build_composer(&[]).unwrap();
        assert!(composer.get_listener("anything").is_none());
    }

    #[test]
    fn test_build_composer_unsupported_protocol_fails() {
        let listeners = vec![ListenerConfig {
            id: "bad".into(),
            address: "0.0.0.0:80".into(),
            transport: ListenerTransportConfig {
                protocol: "tcp".into(),
            },
            application: ListenerApplicationConfig {
                protocol: "grpc-unknown".into(),
                version: None,
            },
            tls: ListenerTlsConfig::default(),
            limits: ListenerLimitsConfig::default(),
        }];

        let result = build_composer(&listeners);
        assert!(result.is_err());
    }
}
