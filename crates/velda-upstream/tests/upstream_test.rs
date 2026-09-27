//! Integration tests for Upstream traffic, lifecycle, pooling, protocol isolation, and health failover.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use velda_upstream::{
    AcquireTarget, Discovery, Endpoint, MockConnector, Upstream, UpstreamTimeouts,
};

#[tokio::test]
async fn test_upstream_explicit_discovery_and_round_robin() {
    let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
    let ep2: SocketAddr = "10.0.0.2:8080".parse().unwrap();
    let ep3: SocketAddr = "10.0.0.3:8080".parse().unwrap();

    let endpoints = vec![
        Endpoint::new("e1", ep1, 1),
        Endpoint::new("e2", ep2, 1),
        Endpoint::new("e3", ep3, 1),
    ];

    let connector = Arc::new(MockConnector::new());
    let discovery = Discovery::new_explicit(endpoints);

    let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(500), Duration::from_secs(30));
    let upstream = Upstream::with_connector("explicit-svc", "tcp", discovery, timeouts, connector);

    // Acquire 3 times: should round-robin e1, e2, e3
    let l1 = upstream.acquire_protocol("tcp").await.unwrap();
    assert_eq!(l1.endpoint(), ep1);

    let l2 = upstream.acquire_protocol("tcp").await.unwrap();
    assert_eq!(l2.endpoint(), ep2);

    let l3 = upstream.acquire_protocol("tcp").await.unwrap();
    assert_eq!(l3.endpoint(), ep3);

    // Release all reusable
    l1.release(true);
    l2.release(true);
    l3.release(true);

    assert_eq!(upstream.pool_stats().misses, 3);
    assert_eq!(upstream.pool_stats().releases, 3);

    // Next 3 acquires should hit the pool!
    let l4 = upstream.acquire_protocol("tcp").await.unwrap();
    assert_eq!(l4.endpoint(), ep1);

    let l5 = upstream.acquire_protocol("tcp").await.unwrap();
    assert_eq!(l5.endpoint(), ep2);

    let l6 = upstream.acquire_protocol("tcp").await.unwrap();
    assert_eq!(l6.endpoint(), ep3);

    assert_eq!(upstream.pool_stats().hits, 3);
}

#[tokio::test]
async fn test_upstream_passive_health_and_failover() {
    let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
    let ep2: SocketAddr = "10.0.0.2:8080".parse().unwrap();

    let connector = Arc::new(MockConnector::new());
    let discovery = Discovery::new_explicit(vec![
        Endpoint::new("e1", ep1, 1),
        Endpoint::new("e2", ep2, 1),
    ]);

    let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(500), Duration::from_secs(30));
    let upstream = Upstream::with_connector(
        "failover-svc",
        "tcp",
        discovery,
        timeouts,
        Arc::clone(&connector),
    );

    // Mark ep1 as failing in network connector
    connector.set_failing(ep1);

    // Acquire should attempt ep1, fail, and succeed with ep2!
    let l = upstream.acquire_protocol("tcp").await.unwrap();
    assert_eq!(l.endpoint(), ep2);
    l.release(true);
}

#[tokio::test]
async fn test_upstream_lease_drop_guard_returns_to_pool() {
    let ep1: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let discovery = Discovery::new_explicit(vec![Endpoint::new("e1", ep1, 1)]);
    let connector = Arc::new(MockConnector::new());

    let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(500), Duration::from_secs(30));
    let upstream = Upstream::with_connector("drop-svc", "tcp", discovery, timeouts, connector);

    // Scope block to simulate caller forgetting to release lease
    {
        let lease = upstream.acquire().await.unwrap();
        assert_eq!(lease.endpoint(), ep1);
        assert_eq!(upstream.pool_stats().misses, 1);
        // lease dropped here
    }

    // Drop guard safely returned healthy connection back to the pool
    assert_eq!(upstream.pool_stats().releases, 1);

    // Subsequent acquire hits the pool
    let lease2 = upstream.acquire().await.unwrap();
    assert_eq!(lease2.endpoint(), ep1);
    assert_eq!(upstream.pool_stats().hits, 1);
    lease2.release(true);
}

#[tokio::test]
async fn test_upstream_protocol_isolation_in_pool() {
    let ep1: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let discovery = Discovery::new_explicit(vec![Endpoint::new("e1", ep1, 1)]);
    let connector = Arc::new(MockConnector::new());

    let timeouts = UpstreamTimeouts::tcp(Duration::from_millis(500), Duration::from_secs(30));
    let upstream = Upstream::with_connector("proto-svc", "http1", discovery, timeouts, connector);

    // 1. Acquire with default "http1" -> Miss
    let l1 = upstream.acquire().await.unwrap();
    assert_eq!(upstream.pool_stats().misses, 1);
    l1.release(true);

    // 2. Acquire with target override "http2" on same endpoint -> Must be Miss (protocol isolated!)
    let target_h2 = AcquireTarget::new("http2");
    let l2 = upstream
        .acquire_with_target(target_h2.clone())
        .await
        .unwrap();
    assert_eq!(upstream.pool_stats().misses, 2);
    l2.release(true);

    // 3. Re-acquire "http2" -> Hits the "http2" pooled connection
    let l3 = upstream.acquire_with_target(target_h2).await.unwrap();
    assert_eq!(upstream.pool_stats().hits, 1);
    l3.release(true);
}
