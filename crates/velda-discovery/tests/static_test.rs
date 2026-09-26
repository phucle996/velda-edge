//! Integration tests for static explicit endpoint discovery.

use std::net::SocketAddr;
use velda_discovery::{Discovery, Endpoint};

#[test]
fn test_explicit_discovery_lifecycle() {
    let ep1: SocketAddr = "10.0.0.1:8080".parse().unwrap();
    let ep2: SocketAddr = "10.0.0.2:8080".parse().unwrap();

    let endpoints = vec![
        Endpoint::new("ep-1", ep1, 10),
        Endpoint::new("ep-2", ep2, 20),
    ];

    let disc = Discovery::new_explicit(endpoints);
    let snapshot = disc.current_endpoints();

    assert_eq!(snapshot.len(), 2);
    assert_eq!(snapshot.generation(), 1);
    assert!(snapshot.contains_addr(&ep1));
    assert!(snapshot.contains_addr(&ep2));

    let found = snapshot.find_by_addr(&ep1).unwrap();
    assert_eq!(found.id.0, "ep-1");
    assert_eq!(found.weight, 10);
}

mod common;

use common::MockDnsTransport;

#[test]
fn test_discovery_from_mode_explicit() {
    let ep: SocketAddr = "192.168.1.1:80".parse().unwrap();
    let endpoints = vec![Endpoint::new("srv", ep, 1)];

    let disc = velda_discovery::Discovery::from_mode::<
        velda_discovery::ResolvConfServerProvider,
        MockDnsTransport,
    >(velda_discovery::DiscoveryMode::Explicit(endpoints), None)
    .unwrap();

    let snapshot = disc.current_endpoints();
    assert_eq!(snapshot.len(), 1);
    assert!(snapshot.contains_addr(&ep));
}
