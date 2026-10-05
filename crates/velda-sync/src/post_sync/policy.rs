//! Cross-domain configuration policies and semantic integrity validation.
//!
//! Enforces global invariants that cross individual domain boundaries
//! (`routes` <-> `listeners` <-> `upstreams`).

use crate::SyncError;
use crate::post_sync::{listener, route, upstream};

/// Validates cross-domain streaming capability constraints.
///
/// ### Mathematical Invariant:
/// $$\text{Upstream.streaming} \subseteq \text{Listener.streaming}$$
///
/// Listener streaming flags declare **allowance/capability**,
/// while Upstream streaming flags declare **requirement**.
///
/// - `listener.streaming.server = false` $\implies$ Server streaming is strictly forbidden.
/// - `listener.streaming.client = false` $\implies$ Client streaming is strictly forbidden.
/// - `listener.streaming.server = true` $\implies$ Server streaming is allowed (upstream may be `false` or `true`).
/// - `listener.streaming.client = true` $\implies$ Client streaming is allowed (upstream may be `false` or `true`).
///
/// Therefore, any upstream reached from a listener must satisfy:
/// - `upstream.streaming.client => listener.streaming.client`
/// - `upstream.streaming.server => listener.streaming.server`
pub fn validate_streaming_policy(
    listeners: &[listener::ListenerConfig],
    routes: &[route::RouteConfig],
    upstreams: &[upstream::UpstreamConfig],
) -> Result<(), SyncError> {
    for route in routes {
        let listener = match listeners.iter().find(|l| l.id == route.listener) {
            Some(l) => l,
            None => continue,
        };
        let upstream = match upstreams.iter().find(|u| u.id == route.upstream) {
            Some(u) => u,
            None => continue,
        };

        let listener_streaming = listener.application.streaming;
        let upstream_streaming = upstream.protocol.streaming;

        if upstream_streaming.client && !listener_streaming.client {
            return Err(SyncError::Validation {
                domain: "streaming".into(),
                reason: format!(
                    "Route '{}': Upstream '{}' requires client streaming, but Listener '{}' has client streaming disabled",
                    route.id, upstream.id, listener.id
                ),
            });
        }

        if upstream_streaming.server && !listener_streaming.server {
            return Err(SyncError::Validation {
                domain: "streaming".into(),
                reason: format!(
                    "Route '{}': Upstream '{}' requires server streaming, but Listener '{}' has server streaming disabled",
                    route.id, upstream.id, listener.id
                ),
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StreamingMode;
    use crate::post_sync::listener::{
        ListenerApplicationConfig, ListenerConfig, ListenerTlsConfig, ListenerTransportConfig,
    };
    use crate::post_sync::route::{RouteConfig, RouteMatch, RouteTimeouts};
    use crate::post_sync::upstream::{
        EndpointConfig, LoadBalancerConfig, UpstreamConfig, UpstreamProtocolConfig,
        UpstreamTimeouts,
    };

    fn make_listener(id: &str, streaming: StreamingMode) -> ListenerConfig {
        ListenerConfig {
            id: id.into(),
            address: "127.0.0.1:80".into(),
            transport: ListenerTransportConfig {
                protocol: "tcp".into(),
            },
            application: ListenerApplicationConfig {
                protocol: "http1".into(),
                version: None,
                streaming,
            },
            tls: ListenerTlsConfig { enabled: false },
            http1: None,
            http2: None,
            grpc: None,
            http3: None,
            raw: None,
        }
    }

    fn make_upstream(id: &str, streaming: StreamingMode) -> UpstreamConfig {
        UpstreamConfig {
            id: id.into(),
            mode: "endpoints".into(),
            protocol: UpstreamProtocolConfig {
                transport: "tcp".into(),
                application: "http1".into(),
                streaming,
            },
            target: None,
            resolver: None,
            endpoints: vec![EndpointConfig {
                address: "127.0.0.1:8080".into(),
                weight: 100,
            }],
            load_balancer: LoadBalancerConfig {
                algorithm: "round_robin".into(),
            },
            timeouts: UpstreamTimeouts {
                connect_ms: 100,
                idle_ms: 1000,
                request_ms: None,
            },
            health_check: None,
            tls: None,
            pool: None,
        }
    }

    fn make_route(id: &str, listener: &str, upstream: &str) -> RouteConfig {
        RouteConfig {
            id: id.into(),
            kind: "l7".into(),
            listener: listener.into(),
            match_rule: RouteMatch::default(),
            timeouts: RouteTimeouts::default(),
            upstream: upstream.into(),
            plugins: vec![],
        }
    }

    #[test]
    fn test_listener_allows_all_upstream_is_buffered() {
        let listeners = vec![make_listener("l1", StreamingMode::DUPLEX)];
        let upstreams = vec![make_upstream("u1", StreamingMode::DISABLED)];
        let routes = vec![make_route("r1", "l1", "u1")];

        assert!(validate_streaming_policy(&listeners, &routes, &upstreams).is_ok());
    }

    #[test]
    fn test_listener_allows_all_upstream_is_server_streaming() {
        let listeners = vec![make_listener("l1", StreamingMode::DUPLEX)];
        let upstreams = vec![make_upstream("u1", StreamingMode::SERVER)];
        let routes = vec![make_route("r1", "l1", "u1")];

        assert!(validate_streaming_policy(&listeners, &routes, &upstreams).is_ok());
    }

    #[test]
    fn test_listener_forbids_streaming_upstream_requires_server() {
        let listeners = vec![make_listener("l1", StreamingMode::DISABLED)];
        let upstreams = vec![make_upstream("u1", StreamingMode::SERVER)];
        let routes = vec![make_route("r1", "l1", "u1")];

        let err = validate_streaming_policy(&listeners, &routes, &upstreams).unwrap_err();
        assert!(matches!(err, SyncError::Validation { .. }));
        assert!(err.to_string().contains("requires server streaming"));
    }

    #[test]
    fn test_listener_forbids_streaming_upstream_requires_client() {
        let listeners = vec![make_listener("l1", StreamingMode::DISABLED)];
        let upstreams = vec![make_upstream("u1", StreamingMode::CLIENT)];
        let routes = vec![make_route("r1", "l1", "u1")];

        let err = validate_streaming_policy(&listeners, &routes, &upstreams).unwrap_err();
        assert!(matches!(err, SyncError::Validation { .. }));
        assert!(err.to_string().contains("requires client streaming"));
    }
}
