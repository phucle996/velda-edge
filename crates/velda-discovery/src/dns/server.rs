//! Phase 2: DNS Server Connect Phase.
//!
//! Responsible for managing upstream DNS nameserver destinations, resolving their
//! target addresses (via direct IP, `/etc/hosts`, or public bootstrap lookup), and
//! validating / managing UDP/TCP socket connectivity.
//!
//! Invariant:
//! Zero hardcoded public DNS addresses. Upstream nameservers are resolved explicitly
//! from direct IP addresses, configured host+port targets, or local static hosts bootstrap.

use std::fmt;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::dns::bootstrap::HostsFileSource;
use crate::error::{DiscoveryError, Result};

/// Trait contract for providing upstream DNS nameservers.
pub trait DnsServerProvider: Send + Sync + 'static {
    /// Returns the ordered list of upstream DNS nameservers to query.
    fn servers(&self) -> Vec<SocketAddr>;
}

/// Representation of a DNS server target prior to address resolution.
///
/// Can be configured as a direct IP address, a domain/hostname with port,
/// or parsed from standard string formats (`"10.96.0.10:53"`, `"dns.google:53"`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DnsServerTarget {
    /// Direct IP socket address (zero lookup required).
    Ip(SocketAddr),
    /// Domain/hostname with port (resolves via `/etc/hosts` or public bootstrap lookup).
    Host { host: String, port: u16 },
}

impl DnsServerTarget {
    /// Constructs a target from a concrete socket address.
    pub fn from_ip(addr: SocketAddr) -> Self {
        Self::Ip(addr)
    }

    /// Constructs a target from a host and port.
    pub fn from_host_port(host: impl Into<String>, port: u16) -> Self {
        let host_str = host.into();
        if let Ok(ip) = host_str.parse::<IpAddr>() {
            Self::Ip(SocketAddr::new(ip, port))
        } else {
            Self::Host {
                host: host_str,
                port,
            }
        }
    }

    /// Resolves this target into a concrete [`SocketAddr`] for network connectivity.
    ///
    /// Resolution Order:
    /// 1. Direct IP: If already an IP or parses as an IP, returns immediately with zero I/O.
    /// 2. `/etc/hosts`: Checks the provided [`HostsFileSource`] for local static resolution.
    /// 3. Public Bootstrap: Falls back to system resolver (`ToSocketAddrs`) for public domains.
    pub fn resolve(&self, hosts: Option<&HostsFileSource>) -> Result<SocketAddr> {
        match self {
            Self::Ip(addr) => Ok(*addr),
            Self::Host { host, port } => {
                // If the host string is an IP literal, return directly
                if let Ok(ip) = host.parse::<IpAddr>() {
                    return Ok(SocketAddr::new(ip, *port));
                }

                // 1. Check local static hosts file first
                let key = host.to_lowercase();
                if let Some(hosts_source) = hosts
                    && let Some(ip) = hosts_source.lookup(&key)
                {
                    return Ok(SocketAddr::new(ip, *port));
                }

                // 2. Fallback to public domain / bootstrap system resolution
                let addr_str = format!("{}:{}", host, port);
                match addr_str.to_socket_addrs() {
                    Ok(mut iter) => {
                        if let Some(addr) = iter.next() {
                            Ok(addr)
                        } else {
                            Err(DiscoveryError::ServerTargetResolutionFailed {
                                target: addr_str,
                                reason: "bootstrap DNS resolution returned no addresses".into(),
                            })
                        }
                    }
                    Err(e) => Err(DiscoveryError::ServerTargetResolutionFailed {
                        target: addr_str,
                        reason: format!("bootstrap resolution failed: {e}"),
                    }),
                }
            }
        }
    }

    /// Resolves this target into a concrete [`SocketAddr`] asynchronously.
    ///
    /// FIX (Blocker 2 - Async Thread Blocking): Uses `tokio::net::lookup_host` for public
    /// domain resolution to prevent blocking Tokio executor worker threads.
    pub async fn resolve_async(&self, hosts: Option<&HostsFileSource>) -> Result<SocketAddr> {
        match self {
            Self::Ip(addr) => Ok(*addr),
            Self::Host { host, port } => {
                if let Ok(ip) = host.parse::<IpAddr>() {
                    return Ok(SocketAddr::new(ip, *port));
                }

                let key = host.to_lowercase();
                if let Some(hosts_source) = hosts
                    && let Some(ip) = hosts_source.lookup(&key)
                {
                    return Ok(SocketAddr::new(ip, *port));
                }

                let target_label = format!("{}:{}", host, port);
                match tokio::net::lookup_host((host.as_str(), *port)).await {
                    Ok(mut iter) => {
                        if let Some(addr) = iter.next() {
                            Ok(addr)
                        } else {
                            Err(DiscoveryError::ServerTargetResolutionFailed {
                                target: target_label,
                                reason: "async bootstrap DNS resolution returned no addresses"
                                    .into(),
                            })
                        }
                    }
                    Err(e) => Err(DiscoveryError::ServerTargetResolutionFailed {
                        target: target_label,
                        reason: format!("async bootstrap resolution failed: {e}"),
                    }),
                }
            }
        }
    }
}

impl FromStr for DnsServerTarget {
    type Err = DiscoveryError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let trimmed = s.trim();

        // 1. Direct SocketAddr check (e.g. "127.0.0.1:53" or "[::1]:53")
        if let Ok(addr) = trimmed.parse::<SocketAddr>() {
            return Ok(Self::Ip(addr));
        }

        // 2. Host:Port check (port must be explicitly specified)
        if let Some((host, port_str)) = trimmed.rsplit_once(':') {
            let port = port_str.parse::<u16>().map_err(|e| {
                DiscoveryError::ServerTargetResolutionFailed {
                    target: s.to_string(),
                    reason: format!("invalid port '{port_str}': {e}"),
                }
            })?;
            let clean_host = host.trim_matches(|c| c == '[' || c == ']');
            if clean_host.is_empty() {
                return Err(DiscoveryError::ServerTargetResolutionFailed {
                    target: s.to_string(),
                    reason: "host portion cannot be empty".into(),
                });
            }
            if let Ok(ip) = clean_host.parse::<IpAddr>() {
                return Ok(Self::Ip(SocketAddr::new(ip, port)));
            }
            return Ok(Self::Host {
                host: clean_host.to_string(),
                port,
            });
        }

        // 3. No port provided -> Strict failure. Do not fall back to any default port.
        Err(DiscoveryError::ServerTargetResolutionFailed {
            target: s.to_string(),
            reason: "port is strictly required for DNS server (expected 'host:port' or 'ip:port')"
                .into(),
        })
    }
}

impl fmt::Display for DnsServerTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ip(addr) => write!(f, "{}", addr),
            Self::Host { host, port } => write!(f, "{}:{}", host, port),
        }
    }
}

// Custom Serde serialization / deserialization to accept either JSON string or object
impl Serialize for DnsServerTarget {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Ip(addr) => serializer.serialize_str(&addr.to_string()),
            Self::Host { host, port } => {
                #[derive(Serialize)]
                struct Helper<'a> {
                    host: &'a str,
                    port: u16,
                }
                Helper { host, port: *port }.serialize(serializer)
            }
        }
    }
}

impl<'de> Deserialize<'de> for DnsServerTarget {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Helper {
            Structured { host: String, port: u16 },
            AddressObject { address: String },
            Text(String),
        }

        match Helper::deserialize(deserializer)? {
            Helper::Structured { host, port } => Ok(Self::from_host_port(host, port)),
            Helper::AddressObject { address } => {
                Self::from_str(&address).map_err(serde::de::Error::custom)
            }
            Helper::Text(s) => Self::from_str(&s).map_err(serde::de::Error::custom),
        }
    }
}

/// An active upstream DNS server entity with a confirmed network socket address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsServer {
    target: DnsServerTarget,
    address: SocketAddr,
}

impl DnsServer {
    /// Resolves a target against the provided `/etc/hosts` and creates a `DnsServer`.
    pub fn new(target: DnsServerTarget, hosts: Option<&HostsFileSource>) -> Result<Self> {
        let address = target.resolve(hosts)?;
        Ok(Self { target, address })
    }

    /// Resolves a target asynchronously against the provided `/etc/hosts` and creates a `DnsServer`.
    ///
    /// FIX (Blocker 2 - Async Thread Blocking): Non-blocking asynchronous constructor.
    pub async fn new_async(
        target: DnsServerTarget,
        hosts: Option<&HostsFileSource>,
    ) -> Result<Self> {
        let address = target.resolve_async(hosts).await?;
        Ok(Self { target, address })
    }

    /// Creates a `DnsServer` directly from a verified `SocketAddr`.
    pub fn from_ip(address: SocketAddr) -> Self {
        Self {
            target: DnsServerTarget::Ip(address),
            address,
        }
    }

    /// Creates a `DnsServer` with explicit target and resolved socket address.
    pub fn from_target_and_addr(target: DnsServerTarget, address: SocketAddr) -> Self {
        Self { target, address }
    }

    /// Returns the resolved socket address of the nameserver.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Returns the configuration target of the nameserver.
    pub fn target(&self) -> &DnsServerTarget {
        &self.target
    }

    /// Connects a UDP socket to this DNS server to verify route/network connectivity.
    pub async fn connect_udp(&self) -> Result<tokio::net::UdpSocket> {
        let bind_addr: SocketAddr = if self.address.is_ipv6() {
            "[::]:0".parse().unwrap()
        } else {
            "0.0.0.0:0".parse().unwrap()
        };

        let socket = tokio::net::UdpSocket::bind(bind_addr).await.map_err(|e| {
            DiscoveryError::ServerUnreachable {
                address: self.address,
                reason: format!("failed to bind local UDP socket: {e}"),
            }
        })?;

        socket
            .connect(self.address)
            .await
            .map_err(|e| DiscoveryError::ServerUnreachable {
                address: self.address,
                reason: format!("failed to connect to DNS server: {e}"),
            })?;

        Ok(socket)
    }
}

/// Static DNS server provider holding pre-resolved nameserver destinations.
#[derive(Debug, Clone, Default)]
pub struct StaticServerProvider {
    servers: Vec<DnsServer>,
    cached_addrs: Vec<SocketAddr>,
}

impl StaticServerProvider {
    /// Constructs a provider from a list of resolved `DnsServer` entities.
    pub fn new(servers: Vec<DnsServer>) -> Self {
        let cached_addrs = servers.iter().map(|s| s.address()).collect();
        Self {
            servers,
            cached_addrs,
        }
    }

    /// Constructs a provider from concrete socket addresses.
    pub fn from_addresses(addrs: Vec<SocketAddr>) -> Self {
        let servers = addrs.into_iter().map(DnsServer::from_ip).collect();
        Self::new(servers)
    }

    /// Constructs a provider by resolving multiple targets with an optional `/etc/hosts` table.
    pub fn from_targets(
        targets: &[DnsServerTarget],
        hosts: Option<&HostsFileSource>,
    ) -> Result<Self> {
        let mut servers = Vec::with_capacity(targets.len());
        for target in targets {
            let server = DnsServer::new(target.clone(), hosts)?;
            servers.push(server);
        }
        Ok(Self::new(servers))
    }

    /// Constructs a provider asynchronously by resolving multiple targets with an optional `/etc/hosts` table.
    ///
    /// FIX (Blocker 2 - Async Thread Blocking): Resolves targets without blocking Tokio worker threads.
    pub async fn from_targets_async(
        targets: &[DnsServerTarget],
        hosts: Option<&HostsFileSource>,
    ) -> Result<Self> {
        let mut servers = Vec::with_capacity(targets.len());
        for target in targets {
            let server = DnsServer::new_async(target.clone(), hosts).await?;
            servers.push(server);
        }
        Ok(Self::new(servers))
    }

    /// Returns references to the resolved DNS server entities.
    pub fn servers_list(&self) -> &[DnsServer] {
        &self.servers
    }
}

impl DnsServerProvider for StaticServerProvider {
    fn servers(&self) -> Vec<SocketAddr> {
        self.cached_addrs.clone()
    }
}

impl DnsServerProvider for Vec<SocketAddr> {
    fn servers(&self) -> Vec<SocketAddr> {
        self.clone()
    }
}

impl DnsServerProvider for Vec<DnsServer> {
    fn servers(&self) -> Vec<SocketAddr> {
        self.iter().map(|s| s.address()).collect()
    }
}

impl DnsServerProvider for DnsServer {
    fn servers(&self) -> Vec<SocketAddr> {
        vec![self.address()]
    }
}

impl<P: DnsServerProvider + ?Sized> DnsServerProvider for std::sync::Arc<P> {
    fn servers(&self) -> Vec<SocketAddr> {
        (**self).servers()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_target_from_ip_and_resolution() {
        let ip_addr: SocketAddr = "10.96.0.10:53".parse().unwrap();
        let target = DnsServerTarget::from_ip(ip_addr);

        // Immediate resolution without hosts file
        let resolved = target.resolve(None).unwrap();
        assert_eq!(resolved, ip_addr);
    }

    #[test]
    fn test_target_from_host_with_hosts_file() {
        let hosts_content = "192.168.10.5 coredns.internal\n";
        let hosts = HostsFileSource::from_content(hosts_content);

        let target = DnsServerTarget::from_host_port("coredns.internal", 5353);
        let resolved = target.resolve(Some(&hosts)).unwrap();
        assert_eq!(resolved, "192.168.10.5:5353".parse().unwrap());
    }

    #[test]
    fn test_target_ip_literal_in_host_field() {
        let target = DnsServerTarget::from_host_port("172.16.0.2", 53);
        let resolved = target.resolve(None).unwrap();
        assert_eq!(resolved, "172.16.0.2:53".parse().unwrap());
    }

    #[test]
    fn test_target_from_str() {
        let target1: DnsServerTarget = "10.0.0.1:53".parse().unwrap();
        assert_eq!(target1, DnsServerTarget::Ip("10.0.0.1:53".parse().unwrap()));

        // Missing port must fail strictly (no default port fallback)
        let no_port_ip = "10.0.0.1".parse::<DnsServerTarget>();
        assert!(no_port_ip.is_err());

        let no_port_domain = "dns.local".parse::<DnsServerTarget>();
        assert!(no_port_domain.is_err());

        let target3: DnsServerTarget = "dns.local:5353".parse().unwrap();
        assert_eq!(
            target3,
            DnsServerTarget::Host {
                host: "dns.local".to_string(),
                port: 5353,
            }
        );
    }

    #[test]
    fn test_serde_json_deserialization() {
        let json_obj = r#"{"host": "coredns.internal", "port": 53}"#;
        let target: DnsServerTarget = serde_json::from_str(json_obj).unwrap();
        assert_eq!(
            target,
            DnsServerTarget::Host {
                host: "coredns.internal".to_string(),
                port: 53,
            }
        );

        let json_str = r#""10.96.0.10:53""#;
        let target_ip: DnsServerTarget = serde_json::from_str(json_str).unwrap();
        assert_eq!(
            target_ip,
            DnsServerTarget::Ip("10.96.0.10:53".parse().unwrap())
        );
    }

    #[test]
    fn test_static_server_provider() {
        let hosts = HostsFileSource::from_content("10.0.1.1 dns1.local\n10.0.1.2 dns2.local");
        let targets = vec![
            DnsServerTarget::from_ip("127.0.0.1:53".parse().unwrap()),
            DnsServerTarget::from_host_port("dns1.local", 53),
        ];

        let provider = StaticServerProvider::from_targets(&targets, Some(&hosts)).unwrap();
        let servers = provider.servers();
        assert_eq!(servers.len(), 2);
        assert_eq!(servers[0], "127.0.0.1:53".parse().unwrap());
        assert_eq!(servers[1], "10.0.1.1:53".parse().unwrap());
    }

    #[tokio::test]
    async fn test_dns_server_connect_udp() {
        let dummy = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let dummy_addr = dummy.local_addr().unwrap();

        let server = DnsServer::from_ip(dummy_addr);
        let connected_socket = server.connect_udp().await.unwrap();

        assert_eq!(connected_socket.peer_addr().unwrap(), dummy_addr);
    }
}
