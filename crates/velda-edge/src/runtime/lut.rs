//! Dense Integer Look-Up Table (LUT) compiler and catalog publisher (`velda_edge::runtime::lut`).
//!
//! ```text
//!                          BOOTSTRAP & CONFIG RELOAD
//!                                     │
//!                     ┌───────────────┴───────────────┐
//!                     ▼                               ▼
//!             routes: &[RouteConfig]       upstreams: &[UpstreamConfig]
//!                     │                               │
//!         ┌───────────┴───────────┐                   │
//!         ▼                       ▼                   ▼
//!  route_to_upstream      RouteCatalog          UpstreamCatalog
//!   Box<[UpstreamId]>    (GLOBAL ArcSwap)      (GLOBAL ArcSwap)
//! ```

use velda_core::UpstreamId;
use velda_sync::post_sync::route::RouteConfig;
use velda_sync::post_sync::upstream::UpstreamConfig;

/// Compiles `route_to_upstream` mapping and publishes 4KB page-aligned route/upstream byte catalogs.
pub fn build_runtime_luts(
    routes: &[RouteConfig],
    upstreams: &[UpstreamConfig],
) -> Box<[UpstreamId]> {
    let route_catalog = velda_router::RouteCatalog::build(routes.iter().map(|r| r.id.as_str()));
    let upstream_catalog =
        velda_upstream::UpstreamCatalog::build(upstreams.iter().map(|u| u.id.as_str()));

    velda_router::set_global_route_catalog(route_catalog);
    velda_upstream::set_global_upstream_catalog(upstream_catalog);

    routes
        .iter()
        .map(|r| {
            let up_idx = upstreams
                .iter()
                .position(|u| u.id == r.upstream)
                .map(|i| i as u16)
                .unwrap_or(velda_core::UNASSIGNED_UPSTREAM_ID);
            UpstreamId(up_idx)
        })
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

#[cfg(test)]
mod tests {
    use super::*;
    use velda_sync::post_sync::route::{RouteConfig, RouteMatch, RouteTimeouts};
    use velda_sync::post_sync::upstream::{
        EndpointConfig, LoadBalancerConfig, UpstreamConfig, UpstreamProtocolConfig,
        UpstreamTimeouts as SyncTimeouts,
    };

    #[test]
    fn build_luts() {
        let make_upstream = |id: &str, app: &str, addr: &str, weight: u32| UpstreamConfig {
            id: id.into(),
            mode: "static".into(),
            protocol: UpstreamProtocolConfig {
                transport: "tcp".into(),
                application: app.into(),
                streaming: velda_core::StreamingMode::disabled(),
            },
            target: None,
            resolver: None,
            endpoints: vec![EndpointConfig {
                address: addr.into(),
                weight,
            }],
            load_balancer: LoadBalancerConfig {
                algorithm: "round_robin".into(),
            },
            timeouts: SyncTimeouts {
                connect_ms: 1000,
                idle_ms: 5000,
                request_ms: Some(3000),
            },
            health_check: None,
            tls: None,
            pool: None,
            buffer: None,
        };

        let upstreams = vec![
            make_upstream("srv-orders", "http1", "127.0.0.1:8081", 1),
            make_upstream("srv-payments", "http2", "127.0.0.1:8082", 2),
        ];

        let routes = vec![
            RouteConfig {
                id: "route-orders".into(),
                kind: "l7".into(),
                listener: "lsn-http".into(),
                match_rule: RouteMatch {
                    path_prefix: Some("/api/orders".into()),
                    ..Default::default()
                },
                timeouts: RouteTimeouts::default(),
                upstream: "srv-orders".into(),
                session_affinity: None,
                plugins: vec![],
            },
            RouteConfig {
                id: "route-payments".into(),
                kind: "l7".into(),
                listener: "lsn-http".into(),
                match_rule: RouteMatch {
                    path_prefix: Some("/api/payments".into()),
                    ..Default::default()
                },
                timeouts: RouteTimeouts::default(),
                upstream: "srv-payments".into(),
                session_affinity: None,
                plugins: vec![],
            },
        ];

        let route_to_upstream = build_runtime_luts(&routes, &upstreams);

        assert_eq!(route_to_upstream.len(), 2);
        assert_eq!(route_to_upstream[0], UpstreamId(0));
        assert_eq!(route_to_upstream[1], UpstreamId(1));

        velda_router::with_route_bytes(0, |b| assert_eq!(b, b"route-orders"));
        velda_router::with_route_bytes(1, |b| assert_eq!(b, b"route-payments"));
        velda_router::with_route_bytes(velda_core::UNMATCHED_ROUTE_ID, |b| {
            assert_eq!(b, b"unmatched")
        });
        velda_upstream::with_upstream_bytes(0, |b| assert_eq!(b, b"srv-orders"));
        velda_upstream::with_upstream_bytes(1, |b| assert_eq!(b, b"srv-payments"));
        velda_upstream::with_upstream_bytes(velda_core::UNASSIGNED_UPSTREAM_ID, |b| {
            assert_eq!(b, b"unmatched")
        });
    }
}
