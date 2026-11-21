//! Integration tests for Upstream traffic, lifecycle, discovery, and health failover.

use std::time::Duration;

use velda_core::Endpoint;
use velda_discovery::Discovery;
use velda_lb::RoundRobin;
use velda_upstream::{Upstream, UpstreamTimeouts};

#[tokio::test]
async fn test_upstream_explicit_discovery_and_round_robin() {
    let ep1 = (
        velda_core::lut::ip::v4_to_u128(0x0A00_0001),
        8080u16.to_be(),
    );
    let ep2 = (
        velda_core::lut::ip::v4_to_u128(0x0A00_0002),
        8080u16.to_be(),
    );
    let ep3 = (
        velda_core::lut::ip::v4_to_u128(0x0A00_0003),
        8080u16.to_be(),
    );

    let endpoints = vec![
        Endpoint::from_v4([10, 0, 0, 1], 8080, 1),
        Endpoint::from_v4([10, 0, 0, 2], 8080, 1),
        Endpoint::from_v4([10, 0, 0, 3], 8080, 1),
    ];

    let discovery = Discovery::new_explicit(endpoints);
    let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(500), Duration::from_secs(30));
    let upstream = Upstream::new(
        "explicit-svc",
        "tcp",
        discovery,
        RoundRobin::new(),
        timeouts,
    );

    // Select 3 times: should round-robin e1, e2, e3
    let s1 = upstream.select_endpoint().unwrap();
    let s2 = upstream.select_endpoint().unwrap();
    let s3 = upstream.select_endpoint().unwrap();

    assert_eq!(s1, ep1);
    assert_eq!(s2, ep2);
    assert_eq!(s3, ep3);
}

#[tokio::test]
async fn test_upstream_passive_health_and_failover() {
    let ep1 = (
        velda_core::lut::ip::v4_to_u128(0x0A00_0001),
        8080u16.to_be(),
    );
    let ep2 = (
        velda_core::lut::ip::v4_to_u128(0x0A00_0002),
        8080u16.to_be(),
    );

    let discovery = Discovery::new_explicit(vec![
        Endpoint::from_v4([10, 0, 0, 1], 8080, 1),
        Endpoint::from_v4([10, 0, 0, 2], 8080, 1),
    ]);

    let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(500), Duration::from_secs(30));
    let upstream = Upstream::new(
        "failover-svc",
        "tcp",
        discovery,
        RoundRobin::new(),
        timeouts,
    )
    .with_health_config(velda_upstream::HealthConfig::passive_only(
        1,
        Duration::from_secs(10),
    ));

    // Execute with failure on ep1, should failover to ep2
    let res = upstream
        .execute(|ep| async move {
            if ep == ep1 {
                Err("simulated connection failure")
            } else {
                Ok(ep)
            }
        })
        .await
        .unwrap();

    assert_eq!(res, ep2);
    assert!(!upstream.health().is_healthy(&ep1));
    assert!(upstream.health().is_healthy(&ep2));
}

#[tokio::test]
async fn test_upstream_prune_retired_endpoints() {
    let ep2 = (
        velda_core::lut::ip::v4_to_u128(0x7F00_0001),
        8081u16.to_be(),
    );
    let discovery = Discovery::new_explicit(vec![
        Endpoint::from_v4([127, 0, 0, 1], 8080, 1),
        Endpoint::from_v4([127, 0, 0, 1], 8081, 1),
    ]);

    let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(500), Duration::from_secs(30));
    let health_config = velda_upstream::HealthConfig::passive_only(1, Duration::from_secs(10));
    let upstream = Upstream::new(
        "prune-svc",
        "tcp",
        discovery.clone(),
        RoundRobin::new(),
        timeouts,
    )
    .with_health_config(health_config);

    // Record health failure for ep2
    upstream.health().record_failure(&ep2);
    assert!(upstream.health().has_record(&ep2));

    // Update discovery removing ep2
    discovery.update_endpoints(vec![Endpoint::from_v4([127, 0, 0, 1], 8080, 1)], 2);
    upstream.prune_retired_endpoints();
    assert!(!upstream.health().has_record(&ep2));
}
