//! Router compilation from route and upstream configurations.

use std::net::SocketAddr;

use velda_core::{RouteId, TransportProtocol, UpstreamId};
use velda_router::{GrpcRoute, Http1Route, Http2Route, Http3Route, L4Route, Router, RouterBuilder};
use velda_sync::post_sync::listener::ListenerConfig;
use velda_sync::post_sync::route::RouteConfig;
use velda_sync::post_sync::upstream::UpstreamConfig;

use crate::error::EdgeError;

fn hash_id_to_u32(s: &str) -> u32 {
    let mut h: u32 = 0x811c9dc5;
    for b in s.as_bytes() {
        h ^= *b as u32;
        h = h.wrapping_mul(0x01000193);
    }
    h
}

fn resolve_target_endpoints(upstream_id: &str, upstreams: &[UpstreamConfig]) -> Vec<SocketAddr> {
    let mut target_endpoints = Vec::new();
    if let Some(up) = upstreams.iter().find(|u| u.id == upstream_id) {
        for ep in &up.endpoints {
            if let Ok(addr) = ep.address.parse::<SocketAddr>() {
                target_endpoints.push(addr);
            } else {
                tracing::warn!(
                    upstream = %up.id,
                    endpoint = %ep.address,
                    "Unable to parse endpoint address as SocketAddr"
                );
            }
        }

        if target_endpoints.is_empty()
            && let Some(ref target) = up.target
            && let Ok(ip) = target.host.parse::<std::net::IpAddr>()
        {
            target_endpoints.push(SocketAddr::new(ip, target.port));
        }
    }
    target_endpoints
}

/// Compiles declarative route, upstream, and listener configurations into a [`Router`] instance.
pub fn build_router(
    routes: &[RouteConfig],
    upstreams: &[UpstreamConfig],
    listeners: &[ListenerConfig],
) -> Result<Router, EdgeError> {
    let mut builder = RouterBuilder::new();

    for route in routes {
        let target_endpoints = resolve_target_endpoints(&route.upstream, upstreams);

        if route.kind.eq_ignore_ascii_case("l4") {
            let protocol_str = route.match_rule.protocol.as_deref().unwrap_or("tcp");

            let protocol = match protocol_str.to_ascii_lowercase().as_str() {
                "tcp" => TransportProtocol::Tcp,
                "udp" => TransportProtocol::Udp,
                other => {
                    return Err(EdgeError::InvalidConfig {
                        detail: format!(
                            "route '{}': unsupported L4 transport protocol '{}'",
                            route.id, other
                        ),
                    });
                }
            };

            let route_id = RouteId::new(hash_id_to_u32(&route.id));
            let upstream_id = UpstreamId::new(hash_id_to_u32(&route.upstream));

            // Validate cross-protocol compatibility between L4 route and targeted upstream
            if let Some(up) = upstreams.iter().find(|u| u.id == route.upstream) {
                let up_app = up.protocol.application.to_ascii_lowercase();
                let up_trans = up.protocol.transport.to_ascii_lowercase();
                if up_app != "raw" || up_trans != protocol_str {
                    return Err(EdgeError::InvalidConfig {
                        detail: format!(
                            "Cross-protocol violation: L4 {} route '{}' targets upstream '{}' with incompatible protocol '{}/{}'",
                            protocol_str, route.id, up.id, up_app, up_trans
                        ),
                    });
                }
            }

            let udp_idle_timeout = if protocol == TransportProtocol::Udp {
                if route.timeouts.downstream_idle_ms == Some(0)
                    || route.plugins.iter().any(|p| {
                        p.eq_ignore_ascii_case("unidirectional")
                            || p.eq_ignore_ascii_case("fire_and_forget")
                    })
                {
                    None
                } else {
                    let ms = route.timeouts.downstream_idle_ms.unwrap_or(30_000);
                    Some(std::time::Duration::from_millis(ms))
                }
            } else {
                None
            };

            let l4_route = L4Route::new(
                route_id,
                &route.listener,
                protocol,
                upstream_id,
                &route.upstream,
            )
            .with_target_endpoints(target_endpoints)
            .with_udp_idle_timeout(udp_idle_timeout)
            .with_plugins(route.plugins.clone());

            builder = builder.add_l4_route(l4_route);
        } else if route.kind.eq_ignore_ascii_case("l7") {
            let route_id = RouteId::new(hash_id_to_u32(&route.id));
            let upstream_id = UpstreamId::new(hash_id_to_u32(&route.upstream));

            let protocol_str = match route.match_rule.protocol.as_deref() {
                Some("grpc") => "grpc",
                Some("http1") | Some("http/1.1") => "http1",
                Some("http2") | Some("h2") => "http2",
                Some("http3") | Some("h3") => "http3",
                Some("http") | None => {
                    let listener = listeners
                        .iter()
                        .find(|l| l.id == route.listener)
                        .ok_or_else(|| EdgeError::InvalidConfig {
                            detail: format!(
                                "route '{}': refers to unknown listener '{}'",
                                route.id, route.listener
                            ),
                        })?;
                    match listener.application.protocol.to_ascii_lowercase().as_str() {
                        "http1" => "http1",
                        "http2" => "http2",
                        "http3" => "http3",
                        "grpc" => "grpc",
                        other => {
                            return Err(EdgeError::InvalidConfig {
                                detail: format!(
                                    "route '{}': listener '{}' has non-L7 protocol '{}'",
                                    route.id, route.listener, other
                                ),
                            });
                        }
                    }
                }
                Some(other) => {
                    return Err(EdgeError::InvalidConfig {
                        detail: format!("route '{}': unsupported protocol '{}'", route.id, other),
                    });
                }
            };

            // Validate cross-protocol compatibility between L7 route and targeted upstream
            if let Some(up) = upstreams.iter().find(|u| u.id == route.upstream) {
                let up_app = up.protocol.application.to_ascii_lowercase();
                if up_app != protocol_str {
                    return Err(EdgeError::InvalidConfig {
                        detail: format!(
                            "Cross-protocol violation: L7 {} route '{}' targets upstream '{}' with incompatible protocol '{}'",
                            protocol_str, route.id, up.id, up_app
                        ),
                    });
                }
            }

            match protocol_str {
                "grpc" => {
                    let service = route
                        .match_rule
                        .path
                        .as_deref()
                        .or(route.match_rule.path_prefix.as_deref())
                        .unwrap_or("*");

                    let mut grpc_route = GrpcRoute::new(
                        route_id,
                        &route.listener,
                        service,
                        upstream_id,
                        &route.upstream,
                    )
                    .with_target_endpoints(target_endpoints)
                    .with_plugins(route.plugins.clone());

                    if let Some(ref auth) = route.match_rule.host {
                        grpc_route = grpc_route.with_authority(auth.clone());
                    }

                    builder = builder.add_grpc_route(grpc_route);
                }
                "http1" => {
                    let mut http1_route = if let Some(ref exact) = route.match_rule.path {
                        Http1Route::new_exact(
                            route_id,
                            &route.listener,
                            exact.clone(),
                            upstream_id,
                            &route.upstream,
                        )
                    } else if let Some(ref prefix) = route.match_rule.path_prefix {
                        Http1Route::new(
                            route_id,
                            &route.listener,
                            prefix.clone(),
                            upstream_id,
                            &route.upstream,
                        )
                    } else {
                        Http1Route::new(
                            route_id,
                            &route.listener,
                            "/",
                            upstream_id,
                            &route.upstream,
                        )
                    }
                    .with_target_endpoints(target_endpoints)
                    .with_plugins(route.plugins.clone());

                    if let Some(ref host) = route.match_rule.host {
                        http1_route = http1_route.with_host(host.clone());
                    }

                    builder = builder.add_http1_route(http1_route);
                }
                "http2" => {
                    let mut http2_route = if let Some(ref exact) = route.match_rule.path {
                        Http2Route::new_exact(
                            route_id,
                            &route.listener,
                            exact.clone(),
                            upstream_id,
                            &route.upstream,
                        )
                    } else if let Some(ref prefix) = route.match_rule.path_prefix {
                        Http2Route::new(
                            route_id,
                            &route.listener,
                            prefix.clone(),
                            upstream_id,
                            &route.upstream,
                        )
                    } else {
                        Http2Route::new(
                            route_id,
                            &route.listener,
                            "/",
                            upstream_id,
                            &route.upstream,
                        )
                    }
                    .with_target_endpoints(target_endpoints)
                    .with_plugins(route.plugins.clone());

                    if let Some(ref host) = route.match_rule.host {
                        http2_route = http2_route.with_host(host.clone());
                    }

                    builder = builder.add_http2_route(http2_route);
                }
                "http3" => {
                    let mut http3_route = if let Some(ref exact) = route.match_rule.path {
                        Http3Route::new_exact(
                            route_id,
                            &route.listener,
                            exact.clone(),
                            upstream_id,
                            &route.upstream,
                        )
                    } else if let Some(ref prefix) = route.match_rule.path_prefix {
                        Http3Route::new(
                            route_id,
                            &route.listener,
                            prefix.clone(),
                            upstream_id,
                            &route.upstream,
                        )
                    } else {
                        Http3Route::new(
                            route_id,
                            &route.listener,
                            "/",
                            upstream_id,
                            &route.upstream,
                        )
                    }
                    .with_target_endpoints(target_endpoints)
                    .with_plugins(route.plugins.clone());

                    if let Some(ref host) = route.match_rule.host {
                        http3_route = http3_route.with_host(host.clone());
                    }

                    builder = builder.add_http3_route(http3_route);
                }
                _ => unreachable!(),
            }
        }
    }

    builder.build().map_err(|e| EdgeError::InvalidConfig {
        detail: e.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use velda_sync::post_sync::listener::{
        ListenerApplicationConfig, ListenerConfig, ListenerTlsConfig, ListenerTransportConfig,
    };
    use velda_sync::post_sync::route::{RouteMatch, RouteTimeouts};
    use velda_sync::post_sync::upstream::{
        EndpointConfig, LoadBalancerConfig, UpstreamProtocolConfig, UpstreamTimeouts,
    };

    #[test]
    fn test_build_router_from_routes() {
        let listeners = vec![
            ListenerConfig {
                id: "postgres-in".into(),
                address: "127.0.0.1:5432".into(),
                transport: ListenerTransportConfig {
                    protocol: "tcp".into(),
                },
                application: ListenerApplicationConfig {
                    protocol: "raw".into(),
                    version: None,
                    streaming: velda_sync::StreamingMode::DISABLED,
                },
                tls: ListenerTlsConfig { enabled: false },
                http1: None,
                http2: None,
                grpc: None,
                http3: None,
                raw: None,
            },
            ListenerConfig {
                id: "dns-in".into(),
                address: "127.0.0.1:53".into(),
                transport: ListenerTransportConfig {
                    protocol: "udp".into(),
                },
                application: ListenerApplicationConfig {
                    protocol: "raw".into(),
                    version: None,
                    streaming: velda_sync::StreamingMode::DISABLED,
                },
                tls: ListenerTlsConfig { enabled: false },
                http1: None,
                http2: None,
                grpc: None,
                http3: None,
                raw: None,
            },
            ListenerConfig {
                id: "http-in".into(),
                address: "127.0.0.1:80".into(),
                transport: ListenerTransportConfig {
                    protocol: "tcp".into(),
                },
                application: ListenerApplicationConfig {
                    protocol: "http1".into(),
                    version: Some("1.1".into()),
                    streaming: velda_sync::StreamingMode::DISABLED,
                },
                tls: ListenerTlsConfig { enabled: false },
                http1: None,
                http2: None,
                grpc: None,
                http3: None,
                raw: None,
            },
        ];

        let routes = vec![
            RouteConfig {
                id: "postgres-route".into(),
                kind: "l4".into(),
                listener: "postgres-in".into(),
                match_rule: RouteMatch {
                    protocol: Some("tcp".into()),
                    ..Default::default()
                },
                timeouts: RouteTimeouts::default(),
                upstream: "postgres-backend".into(),
                plugins: vec!["rate-limit".into()],
            },
            RouteConfig {
                id: "dns-route".into(),
                kind: "l4".into(),
                listener: "dns-in".into(),
                match_rule: RouteMatch {
                    protocol: Some("udp".into()),
                    ..Default::default()
                },
                timeouts: RouteTimeouts::default(),
                upstream: "dns-backend".into(),
                plugins: vec![],
            },
            RouteConfig {
                id: "http-route".into(),
                kind: "l7".into(),
                listener: "http-in".into(),
                match_rule: RouteMatch {
                    path_prefix: Some("/api".into()),
                    ..Default::default()
                },
                timeouts: RouteTimeouts::default(),
                upstream: "http-backend".into(),
                plugins: vec![],
            },
        ];

        let upstreams = vec![
            UpstreamConfig {
                id: "postgres-backend".into(),
                mode: "endpoints".into(),
                protocol: UpstreamProtocolConfig {
                    transport: "tcp".into(),
                    application: "raw".into(),
                    streaming: velda_sync::StreamingMode::DISABLED,
                },
                target: None,
                resolver: None,
                endpoints: vec![EndpointConfig {
                    address: "127.0.0.1:5432".into(),
                    weight: 100,
                }],
                load_balancer: LoadBalancerConfig {
                    algorithm: "round_robin".into(),
                },
                timeouts: UpstreamTimeouts {
                    connect_ms: 500,
                    idle_ms: 10000,
                    request_ms: None,
                },
                health_check: None,
                tls: None,
            },
            UpstreamConfig {
                id: "dns-backend".into(),
                mode: "endpoints".into(),
                protocol: UpstreamProtocolConfig {
                    transport: "udp".into(),
                    application: "raw".into(),
                    streaming: velda_sync::StreamingMode::DISABLED,
                },
                target: None,
                resolver: None,
                endpoints: vec![EndpointConfig {
                    address: "127.0.0.1:53".into(),
                    weight: 100,
                }],
                load_balancer: LoadBalancerConfig {
                    algorithm: "round_robin".into(),
                },
                timeouts: UpstreamTimeouts {
                    connect_ms: 500,
                    idle_ms: 10000,
                    request_ms: None,
                },
                health_check: None,
                tls: None,
            },
        ];

        let router = build_router(&routes, &upstreams, &listeners).unwrap();
        let tcp = router
            .route_l4("postgres-in", TransportProtocol::Tcp)
            .unwrap();
        assert_eq!(tcp.listener_id, "postgres-in");
        assert_eq!(tcp.protocol, TransportProtocol::Tcp);
        assert_eq!(tcp.upstream_name, "postgres-backend");
        assert_eq!(tcp.plugins, vec!["rate-limit"]);
        assert_eq!(
            tcp.select_target().unwrap(),
            "127.0.0.1:5432".parse::<SocketAddr>().unwrap()
        );

        let udp = router.route_l4("dns-in", TransportProtocol::Udp).unwrap();
        assert_eq!(udp.listener_id, "dns-in");
        assert_eq!(udp.protocol, TransportProtocol::Udp);
        assert_eq!(udp.upstream_name, "dns-backend");
        assert_eq!(
            udp.select_target().unwrap(),
            "127.0.0.1:53".parse::<SocketAddr>().unwrap()
        );

        // Verify L7 HTTP/1.1 route compilation
        let http_req = velda_router::Http1RouteRequest::new("/api/v1/users");
        let http_matched = router.route_http1("http-in", &http_req).unwrap();
        assert_eq!(http_matched.upstream_name, "http-backend");

        // Verify protocol isolation: HTTP/1 route is NOT in HTTP/2 router
        let h2_req = velda_router::Http2RouteRequest::new("/api/v1/users");
        assert!(router.route_http2("http-in", &h2_req).is_none());

        assert!(router.route_l4("unknown", TransportProtocol::Tcp).is_none());
    }
}
