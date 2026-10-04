//! Phase 1: Bootstrap Phase.
//!
//! Responsible for eagerly loading local environment configuration into memory:
//! - `/etc/hosts` for zero-network resolution of cluster-local / static domains.
//! - `/etc/resolv.conf` for discovering OS-configured upstream nameservers.
//!
//! Invariant:
//! Zero hardcoded public DNS addresses. All bootstrap state originates strictly
//! from OS network files or explicit deployment manifests loaded ahead-of-time.

use std::collections::HashMap;
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::path::Path;

use crate::dns::server::DnsServerProvider;
use crate::error::{DiscoveryError, Result};

// ============================================================================
// 1. Hosts File Source (/etc/hosts)
// ============================================================================

/// Static hosts lookup source reading `/etc/hosts` or custom hosts table format.
///
/// Supports dual-stack mappings by storing all parsed IPv4 and IPv6 addresses for a domain.
#[derive(Debug, Clone, Default)]
pub struct HostsFileSource {
    hosts_table: HashMap<String, Vec<IpAddr>>,
}

impl HostsFileSource {
    /// Loads static hosts definitions from `/etc/hosts` or falls back to an empty table.
    pub fn load_system() -> Self {
        Self::from_system().unwrap_or_default()
    }

    /// Loads static hosts definitions from `/etc/hosts` if available on the system.
    pub fn from_system() -> Result<Self> {
        Self::from_file("/etc/hosts")
    }

    /// Loads static hosts definitions from a custom file path.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let path_ref = path.as_ref();
        let content =
            fs::read_to_string(path_ref).map_err(|e| DiscoveryError::HostsParsingFailed {
                path: path_ref.to_string_lossy().into_owned(),
                reason: e.to_string(),
            })?;
        Ok(Self::from_content(&content))
    }

    /// Parses hosts file content string into an in-memory lookup table.
    ///
    /// Preserves both IPv4 and IPv6 entries per hostname in order of occurrence.
    pub fn from_content(content: &str) -> Self {
        let mut table = HashMap::new();
        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let mut parts = trimmed.split_whitespace();
            if let Some(ip) = parts
                .next()
                .and_then(|ip_str| ip_str.parse::<IpAddr>().ok())
            {
                for hostname in parts {
                    let entry: &mut Vec<IpAddr> = table.entry(hostname.to_lowercase()).or_default();
                    if !entry.contains(&ip) {
                        entry.push(ip);
                    }
                }
            }
        }
        Self { hosts_table: table }
    }

    /// Creates an empty hosts lookup source.
    pub fn empty() -> Self {
        Self {
            hosts_table: HashMap::new(),
        }
    }

    /// Looks up a hostname in the local hosts table, returning all mapped addresses.
    pub fn lookup(&self, host: &str) -> Option<&[IpAddr]> {
        if host.bytes().any(|b| b.is_ascii_uppercase()) {
            self.hosts_table
                .get(&host.to_lowercase())
                .map(|v| v.as_slice())
        } else {
            self.hosts_table.get(host).map(|v| v.as_slice())
        }
    }

    /// Returns the number of static host mappings loaded.
    pub fn len(&self) -> usize {
        self.hosts_table.len()
    }

    /// Returns true if no host mappings are present.
    pub fn is_empty(&self) -> bool {
        self.hosts_table.is_empty()
    }
}

// ============================================================================
// 2. Resolv.conf Server Provider (/etc/resolv.conf)
// ============================================================================

/// Nameserver provider reading from `/etc/resolv.conf` or explicit configuration.
#[derive(Debug, Clone)]
pub struct ResolvConfServerProvider {
    nameservers: Vec<SocketAddr>,
}

impl ResolvConfServerProvider {
    /// Loads nameservers from `/etc/resolv.conf` or falls back to loopback `127.0.0.1:53`.
    pub fn load_system() -> Self {
        Self::from_system().unwrap_or_else(|_| Self::from_content("nameserver 127.0.0.1"))
    }

    /// Reads nameservers from the host system `/etc/resolv.conf`.
    pub fn from_system() -> Result<Self> {
        Self::from_file("/etc/resolv.conf")
    }

    /// Reads nameservers from a specified resolv.conf file path.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let path_ref = path.as_ref();
        let content =
            fs::read_to_string(path_ref).map_err(|e| DiscoveryError::HostsParsingFailed {
                path: path_ref.to_string_lossy().into_owned(),
                reason: e.to_string(),
            })?;
        Ok(Self::from_content(&content))
    }

    /// Parses nameserver directives from resolv.conf formatted text content.
    ///
    /// Lines format: `nameserver <IP>`
    pub fn from_content(content: &str) -> Self {
        let mut servers = Vec::new();
        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with('#') || trimmed.starts_with(';') || trimmed.is_empty() {
                continue;
            }
            let mut parts = trimmed.split_whitespace();
            let is_nameserver = parts
                .next()
                .is_some_and(|d| d.eq_ignore_ascii_case("nameserver"));
            if is_nameserver && let Some(ip) = parts.next().and_then(|s| s.parse::<IpAddr>().ok()) {
                servers.push(SocketAddr::new(ip, 53));
            }
        }
        Self {
            nameservers: servers,
        }
    }

    /// Constructs a provider with explicit nameserver socket addresses.
    pub fn with_servers(servers: Vec<SocketAddr>) -> Self {
        Self {
            nameservers: servers,
        }
    }
}

impl Default for ResolvConfServerProvider {
    fn default() -> Self {
        Self::load_system()
    }
}

impl DnsServerProvider for ResolvConfServerProvider {
    fn servers(&self) -> &[SocketAddr] {
        &self.nameservers
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hosts_file_parsing() {
        let sample = r#"
        # System loopback
        127.0.0.1   localhost localhost.localdomain
        ::1         localhost ip6-localhost
        10.0.0.42   api.internal user.service
        "#;

        let hosts = HostsFileSource::from_content(sample);
        assert_eq!(
            hosts.lookup("localhost"),
            Some(&["127.0.0.1".parse().unwrap(), "::1".parse().unwrap()][..])
        );
        assert_eq!(
            hosts.lookup("api.internal"),
            Some(&["10.0.0.42".parse().unwrap()][..])
        );
        assert_eq!(
            hosts.lookup("user.service"),
            Some(&["10.0.0.42".parse().unwrap()][..])
        );
        assert_eq!(hosts.lookup("unknown.host"), None);
    }

    #[test]
    fn test_resolv_conf_parsing() {
        let sample = r#"
        # System resolv.conf
        nameserver 10.96.0.10
        search default.svc.cluster.local svc.cluster.local
        options ndots:5
        nameserver 127.0.0.53
        "#;

        let provider = ResolvConfServerProvider::from_content(sample);
        let servers = provider.servers();
        assert_eq!(servers.len(), 2);
        assert_eq!(servers[0], "10.96.0.10:53".parse().unwrap());
        assert_eq!(servers[1], "127.0.0.53:53".parse().unwrap());
    }
}
