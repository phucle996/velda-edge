//! Router compilation from route and upstream configurations.

use std::net::SocketAddr;

use velda_core::{RouteId, TransportProtocol, UpstreamId};
use velda_router::{GrpcRoute, HttpRoute, L4Route, Router, RouterBuilder};
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

/// Compiles a list of declarative [`RouteConfig`] and [`UpstreamConfig`] declarations into a [`Router`] instance.
pub(crate) fn build_router(
    routes: &[RouteConfig],
    upstreams: &[UpstreamConfig],
) -> Result<Router, EdgeError> {
    let mut builder = RouterBuilder::new();

    for route in routes {
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

            let mut target_endpoints = Vec::new();
            if let Some(up) = upstreams.iter().find(|u| u.id == route.upstream) {
                for ep in &up.endpoints {
                    if let Ok(addr) = ep.address.parse::<SocketAddr>() {
                        target_endpoints.push(addr);
                    } else {
                        tracing::warn!(
                            route = %route.id,
                            upstream = %up.id,
                            endpoint = %ep.address,
                            "Unable to parse endpoint address as SocketAddr in L4 route"
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

            let protocol_str = route.match_rule.protocol.as_deref().unwrap_or("http");

            if protocol_str.eq_ignore_ascii_case("grpc") {
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
                .with_plugins(route.plugins.clone());

                if let Some(ref auth) = route.match_rule.host {
                    grpc_route = grpc_route.with_authority(auth.clone());
                }

                builder = builder.add_grpc_route(grpc_route);
            } else {
                let mut http_route = if let Some(ref exact) = route.match_rule.path {
                    HttpRoute::new_exact(
                        route_id,
                        &route.listener,
                        exact.clone(),
                        upstream_id,
                        &route.upstream,
                    )
                } else if let Some(ref prefix) = route.match_rule.path_prefix {
                    HttpRoute::new(
                        route_id,
                        &route.listener,
                        prefix.clone(),
                        upstream_id,
                        &route.upstream,
                    )
                } else {
                    // Host-only route without path restriction matches any path under that host
                    HttpRoute::new(route_id, &route.listener, "/", upstream_id, &route.upstream)
                }
                .with_plugins(route.plugins.clone());

                if let Some(ref host) = route.match_rule.host {
                    http_route = http_route.with_host(host.clone());
                }

                builder = builder.add_http_route(http_route);
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
    use velda_sync::post_sync::route::{RouteMatch, RouteTimeouts};
    use velda_sync::post_sync::upstream::{
        EndpointConfig, LoadBalancerConfig, UpstreamProtocolConfig, UpstreamTimeouts,
    };

    #[test]
    fn test_build_router_from_routes() {
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
            // L7 route should be skipped in L4 router builder for now
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
                    version: None,
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
                    version: None,
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

        let router = build_router(&routes, &upstreams).unwrap();
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

        // Verify L7 HTTP route compilation
        let http_req = velda_router::HttpRouteRequest::new("/api/v1/users");
        let http_matched = router.route_http("http-in", &http_req).unwrap();
        assert_eq!(http_matched.upstream_name, "http-backend");

        assert!(router.route_l4("unknown", TransportProtocol::Tcp).is_none());
    }
}
