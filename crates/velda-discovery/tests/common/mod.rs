//! Mock DNS transport for testing velda-discovery.

#![allow(dead_code)]

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use velda_discovery::{DiscoveryError, DnsTransport, Result};

#[derive(Default)]
pub struct MockDnsTransport {
    table: RwLock<HashMap<(SocketAddr, String), Vec<IpAddr>>>,
    query_count: AtomicUsize,
    failing_servers: RwLock<std::collections::HashSet<SocketAddr>>,
    server_delays: RwLock<HashMap<SocketAddr, Duration>>,
}

impl MockDnsTransport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_response(&self, server: SocketAddr, host: &str, ips: Vec<IpAddr>) {
        self.table
            .write()
            .unwrap()
            .insert((server, host.to_lowercase()), ips);
    }

    pub fn set_failing(&self, server: SocketAddr) {
        self.failing_servers.write().unwrap().insert(server);
    }

    pub fn set_delay(&self, server: SocketAddr, delay: Duration) {
        self.server_delays.write().unwrap().insert(server, delay);
    }

    pub fn query_count(&self) -> usize {
        self.query_count.load(Ordering::SeqCst)
    }
}

impl DnsTransport for MockDnsTransport {
    async fn query(&self, server: SocketAddr, host: &str) -> Result<Vec<IpAddr>> {
        self.query_count.fetch_add(1, Ordering::SeqCst);

        // Simulate network delay if configured
        let maybe_delay = self.server_delays.read().unwrap().get(&server).copied();
        if let Some(delay) = maybe_delay {
            tokio::time::sleep(delay).await;
        }

        if self.failing_servers.read().unwrap().contains(&server) {
            return Err(DiscoveryError::ServerUnreachable {
                address: server,
                reason: "simulated connection timeout".into(),
            });
        }

        let key = (server, host.to_lowercase());
        if let Some(ips) = self.table.read().unwrap().get(&key) {
            Ok(ips.clone())
        } else {
            Ok(Vec::new()) // NXDOMAIN / Empty
        }
    }
}
