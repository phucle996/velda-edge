//! Node-local runtime sizing and hardware tuning profile.
//!
//! # Lifecycle & Priority Hierarchy:
//! 1. **Operator Override (File Priority)**: Checks `<runtime_dir>/runtime.json`.
//!    If present and valid, operator-specified values are strictly preserved.
//! 2. **Hardware Probe Fallback**: If the file does not exist or has missing fields,
//!    probes host CPU and RAM (cgroup limit or physical) to calculate [`CpuTier`] and [`MemoryTier`].
//! 3. **Self-Persisting Write-Back**: Serializes the resulting profile back to
//!    `<runtime_dir>/runtime.json` (best-effort, ignoring read-only filesystem errors).
//!
//! Invariant: Hot-path request serving never reads or writes this file.
//! This lifecycle runs exclusively during cold-start bootstrap.

use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use velda_core::hardware::HardwareTopology;
use velda_discovery::DnsResolverConfig;
use velda_transport::{EngineConfig, TcpListenerConfig, UdpSocketConfig};

/// Hardware information snapshot recorded in `runtime.json`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HardwareProfile {
    pub detected_ram_bytes: usize,
    pub detected_cores: usize,
    pub worker_threads: usize,
    pub cpu_tier: String,
    pub memory_tier: String,
    pub tier: String,
}

/// Discovery subsystem tuning parameters.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiscoveryRuntimeConfig {
    pub max_dns_cache_capacity: usize,
    pub max_lkg_capacity: usize,
    pub dns_max_packet_size: usize,
    pub query_timeout_ms: u64,
    pub negative_ttl_secs: u64,
    pub positive_ttl_secs: u64,
}

/// TCP-specific runtime configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TcpRuntimeConfig {
    pub backlog: u32,
    pub nodelay: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keepalive_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recv_buffer_size: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub send_buffer_size: Option<usize>,
    pub copy_buffer_size: usize,
}

/// UDP-specific runtime configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UdpRuntimeConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recv_buffer_size: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub send_buffer_size: Option<usize>,
}

/// Transport subsystem tuning parameters.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransportRuntimeConfig {
    /// Number of network I/O event loop workers.
    pub io_workers: usize,
    /// Maximum concurrent active connections allowed before L4 rate-limiting/drop to protect RAM.
    pub max_active_connections: usize,
    /// Depth of the administrative reconciliation command channel.
    pub reconcile_channel_capacity: usize,
    /// TCP socket options and buffer sizing.
    pub tcp: TcpRuntimeConfig,
    /// UDP socket options and buffer sizing.
    pub udp: UdpRuntimeConfig,
}

/// Downstream TLS termination tuning parameters recorded in `runtime.json`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TlsRuntimeConfig {
    /// In-memory TLS session resumption cache capacity.
    pub session_cache_capacity: usize,
    /// Maximum size in bytes of TLS 1.3 0-RTT early data accepted from clients.
    pub max_early_data_size: u32,
    /// Number of single-use TLS 1.3 session tickets issued per handshake.
    pub send_tls13_tickets: usize,
    /// Maximum downstream TLS handshake timeout in seconds.
    pub handshake_timeout_secs: u64,
}

impl TlsRuntimeConfig {
    /// Converts this runtime configuration to a strongly-typed [`velda_tls::TlsServerParams`].
    pub fn to_tls_server_params(&self) -> velda_tls::TlsServerParams {
        velda_tls::TlsServerParams {
            session_cache_capacity: self.session_cache_capacity,
            max_early_data_size: self.max_early_data_size,
            send_tls13_tickets: self.send_tls13_tickets,
            handshake_timeout: std::time::Duration::from_secs(self.handshake_timeout_secs),
        }
    }
}

/// Complete runtime profile for the edge node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeProfile {
    pub version: u32,
    pub hardware: HardwareProfile,
    pub discovery: DiscoveryRuntimeConfig,
    pub transport: TransportRuntimeConfig,
    pub tls: TlsRuntimeConfig,
}

impl RuntimeProfile {
    /// Generates a default runtime profile from the probed hardware topology.
    pub fn from_hardware(hardware: &HardwareTopology) -> Self {
        let cpu_tier = hardware.cpu_tier();
        let memory_tier = hardware.memory_tier();

        let dns_config = DnsResolverConfig::for_tier(memory_tier);
        let engine_config = EngineConfig::for_tiers(cpu_tier, memory_tier);
        let tls_params = velda_tls::TlsServerParams::for_tiers(cpu_tier, memory_tier);

        Self {
            version: 1,
            hardware: HardwareProfile {
                detected_ram_bytes: hardware.memory_bytes(),
                detected_cores: hardware.available_cores(),
                worker_threads: engine_config.io_workers,
                cpu_tier: cpu_tier.as_str().to_string(),
                memory_tier: memory_tier.as_str().to_string(),
                tier: memory_tier.as_str().to_string(),
            },
            discovery: DiscoveryRuntimeConfig {
                max_dns_cache_capacity: dns_config.cache_capacity,
                max_lkg_capacity: dns_config.lkg_capacity,
                dns_max_packet_size: dns_config.max_packet_size,
                query_timeout_ms: dns_config.query_timeout.as_millis() as u64,
                negative_ttl_secs: dns_config.negative_ttl.as_secs(),
                positive_ttl_secs: dns_config.positive_ttl.as_secs(),
            },
            transport: TransportRuntimeConfig {
                io_workers: engine_config.io_workers,
                max_active_connections: engine_config.max_active_connections,
                reconcile_channel_capacity: engine_config.reconcile_channel_capacity,
                tcp: TcpRuntimeConfig {
                    backlog: engine_config.tcp.backlog,
                    nodelay: engine_config.tcp.nodelay,
                    keepalive_secs: engine_config.tcp.keepalive.map(|d| d.as_secs()),
                    recv_buffer_size: engine_config.tcp.recv_buffer_size,
                    send_buffer_size: engine_config.tcp.send_buffer_size,
                    copy_buffer_size: engine_config.tcp.copy_buffer_size,
                },
                udp: UdpRuntimeConfig {
                    recv_buffer_size: engine_config.udp.recv_buffer_size,
                    send_buffer_size: engine_config.udp.send_buffer_size,
                },
            },
            tls: TlsRuntimeConfig {
                session_cache_capacity: tls_params.session_cache_capacity,
                max_early_data_size: tls_params.max_early_data_size,
                send_tls13_tickets: tls_params.send_tls13_tickets,
                handshake_timeout_secs: tls_params.handshake_timeout.as_secs(),
            },
        }
    }

    /// Converts the discovery section into a strongly-typed [`DnsResolverConfig`].
    pub fn to_dns_resolver_config(&self) -> DnsResolverConfig {
        DnsResolverConfig {
            positive_ttl: std::time::Duration::from_secs(self.discovery.positive_ttl_secs),
            hosts_ttl: std::time::Duration::from_secs(300),
            negative_ttl: std::time::Duration::from_secs(self.discovery.negative_ttl_secs),
            query_timeout: std::time::Duration::from_millis(self.discovery.query_timeout_ms),
            cache_capacity: self.discovery.max_dns_cache_capacity,
            lkg_capacity: self.discovery.max_lkg_capacity,
            max_packet_size: self.discovery.dns_max_packet_size,
        }
    }

    /// Converts the transport section into a strongly-typed [`TcpListenerConfig`].
    pub fn to_tcp_listener_config(&self) -> TcpListenerConfig {
        let mut cfg = TcpListenerConfig::new()
            .with_backlog(self.transport.tcp.backlog)
            .with_nodelay(self.transport.tcp.nodelay)
            .with_copy_buffer_size(self.transport.tcp.copy_buffer_size)
            .with_keepalive(
                self.transport
                    .tcp
                    .keepalive_secs
                    .map(std::time::Duration::from_secs),
            );
        if let Some(r) = self.transport.tcp.recv_buffer_size {
            cfg = cfg.with_recv_buffer_size(r);
        }
        if let Some(s) = self.transport.tcp.send_buffer_size {
            cfg = cfg.with_send_buffer_size(s);
        }
        cfg
    }

    /// Converts the transport section into a strongly-typed [`UdpSocketConfig`].
    pub fn to_udp_socket_config(&self) -> UdpSocketConfig {
        let mut cfg = UdpSocketConfig::new();
        if let Some(r) = self.transport.udp.recv_buffer_size {
            cfg = cfg.with_recv_buffer_size(r);
        }
        if let Some(s) = self.transport.udp.send_buffer_size {
            cfg = cfg.with_send_buffer_size(s);
        }
        cfg
    }

    /// Converts the TLS section into a strongly-typed [`velda_tls::TlsServerParams`].
    pub fn to_tls_server_params(&self) -> velda_tls::TlsServerParams {
        self.tls.to_tls_server_params()
    }

    /// Deep merges partial operator overrides into this runtime profile.
    fn merge_partial(&mut self, partial: PartialRuntimeProfile) {
        if let Some(v) = partial.version {
            self.version = v;
        }
        if let Some(hw) = partial.hardware {
            if let Some(r) = hw.detected_ram_bytes {
                self.hardware.detected_ram_bytes = r;
            }
            if let Some(c) = hw.detected_cores {
                self.hardware.detected_cores = c;
            }
            if let Some(w) = hw.worker_threads {
                self.hardware.worker_threads = w;
            }
            if let Some(ct) = hw.cpu_tier {
                self.hardware.cpu_tier = ct;
            }
            if let Some(mt) = hw.memory_tier {
                self.hardware.memory_tier = mt;
            }
            if let Some(t) = hw.tier {
                self.hardware.tier = t;
            }
        }
        if let Some(disc) = partial.discovery {
            if let Some(c) = disc.max_dns_cache_capacity {
                self.discovery.max_dns_cache_capacity = c;
            }
            if let Some(l) = disc.max_lkg_capacity {
                self.discovery.max_lkg_capacity = l;
            }
            if let Some(p) = disc.dns_max_packet_size {
                self.discovery.dns_max_packet_size = p;
            }
            if let Some(t) = disc.query_timeout_ms {
                self.discovery.query_timeout_ms = t;
            }
            if let Some(n) = disc.negative_ttl_secs {
                self.discovery.negative_ttl_secs = n;
            }
            if let Some(pos) = disc.positive_ttl_secs {
                self.discovery.positive_ttl_secs = pos;
            }
        }
        if let Some(tr) = partial.transport {
            if let Some(w) = tr.io_workers {
                self.transport.io_workers = w;
            }
            if let Some(m) = tr.max_active_connections {
                self.transport.max_active_connections = m;
            }
            if let Some(rc) = tr.reconcile_channel_capacity {
                self.transport.reconcile_channel_capacity = rc;
            }

            // Grouped TCP overrides
            if let Some(tcp) = tr.tcp {
                if let Some(b) = tcp.backlog {
                    self.transport.tcp.backlog = b;
                }
                if let Some(n) = tcp.nodelay {
                    self.transport.tcp.nodelay = n;
                }
                if let Some(k) = tcp.keepalive_secs {
                    self.transport.tcp.keepalive_secs = Some(k);
                }
                if let Some(r) = tcp.recv_buffer_size {
                    self.transport.tcp.recv_buffer_size = Some(r);
                }
                if let Some(s) = tcp.send_buffer_size {
                    self.transport.tcp.send_buffer_size = Some(s);
                }
                if let Some(c) = tcp.copy_buffer_size {
                    self.transport.tcp.copy_buffer_size = c;
                }
            }

            // Grouped UDP overrides
            if let Some(udp) = tr.udp {
                if let Some(r) = udp.recv_buffer_size {
                    self.transport.udp.recv_buffer_size = Some(r);
                }
                if let Some(s) = udp.send_buffer_size {
                    self.transport.udp.send_buffer_size = Some(s);
                }
            }
        }
        if let Some(tls) = partial.tls {
            if let Some(c) = tls.session_cache_capacity {
                self.tls.session_cache_capacity = c;
            }
            if let Some(e) = tls.max_early_data_size {
                self.tls.max_early_data_size = e;
            }
            if let Some(t) = tls.send_tls13_tickets {
                self.tls.send_tls13_tickets = t;
            }
            if let Some(h) = tls.handshake_timeout_secs {
                self.tls.handshake_timeout_secs = h;
            }
        }
    }
}

/// Helper deserializer for partial/sparse user configurations in `runtime.json`.
#[derive(Debug, Deserialize, Default)]
struct PartialHardwareProfile {
    detected_ram_bytes: Option<usize>,
    detected_cores: Option<usize>,
    worker_threads: Option<usize>,
    cpu_tier: Option<String>,
    memory_tier: Option<String>,
    tier: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct PartialDiscoveryConfig {
    max_dns_cache_capacity: Option<usize>,
    max_lkg_capacity: Option<usize>,
    dns_max_packet_size: Option<usize>,
    query_timeout_ms: Option<u64>,
    negative_ttl_secs: Option<u64>,
    positive_ttl_secs: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
struct PartialTcpConfig {
    backlog: Option<u32>,
    nodelay: Option<bool>,
    keepalive_secs: Option<u64>,
    recv_buffer_size: Option<usize>,
    send_buffer_size: Option<usize>,
    copy_buffer_size: Option<usize>,
}

#[derive(Debug, Deserialize, Default)]
struct PartialUdpConfig {
    recv_buffer_size: Option<usize>,
    send_buffer_size: Option<usize>,
}

#[derive(Debug, Deserialize, Default)]
struct PartialTransportConfig {
    io_workers: Option<usize>,
    max_active_connections: Option<usize>,
    reconcile_channel_capacity: Option<usize>,
    tcp: Option<PartialTcpConfig>,
    udp: Option<PartialUdpConfig>,
}

#[derive(Debug, Deserialize, Default)]
struct PartialTlsConfig {
    session_cache_capacity: Option<usize>,
    max_early_data_size: Option<u32>,
    send_tls13_tickets: Option<usize>,
    handshake_timeout_secs: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
struct PartialRuntimeProfile {
    version: Option<u32>,
    hardware: Option<PartialHardwareProfile>,
    discovery: Option<PartialDiscoveryConfig>,
    transport: Option<PartialTransportConfig>,
    tls: Option<PartialTlsConfig>,
}

/// Resolves the runtime profile for the edge process:
/// 1. Reads `<runtime_dir>/runtime.json` if available.
/// 2. If fields are omitted, merges them on top of probed hardware tier defaults.
/// 3. If file does not exist, uses 100% hardware tier defaults.
/// 4. Best-effort writes back the complete merged `<runtime_dir>/runtime.json`.
pub fn resolve_runtime_profile(runtime_dir: &Path, hardware: &HardwareTopology) -> RuntimeProfile {
    let profile_path = runtime_dir.join("runtime.json");
    let mut profile = RuntimeProfile::from_hardware(hardware);
    let mut loaded_from_file = false;

    if profile_path.exists()
        && let Ok(content) = fs::read_to_string(&profile_path)
    {
        match serde_json::from_str::<PartialRuntimeProfile>(&content) {
            Ok(partial) => {
                profile.merge_partial(partial);
                loaded_from_file = true;
                tracing::info!(
                    path = %profile_path.display(),
                    cpu_tier = %profile.hardware.cpu_tier,
                    memory_tier = %profile.hardware.memory_tier,
                    io_workers = profile.transport.io_workers,
                    max_conns = profile.transport.max_active_connections,
                    "Loaded runtime profile from runtime.json with operator overrides"
                );
            }
            Err(e) => {
                tracing::warn!(
                    path = %profile_path.display(),
                    error = %e,
                    "Failed to parse runtime.json; falling back to hardware-probed defaults"
                );
            }
        }
    }

    // Best-effort write-back: ignore read-only / permission errors
    if let Ok(json_str) = serde_json::to_string_pretty(&profile) {
        if let Err(e) = fs::create_dir_all(runtime_dir) {
            tracing::warn!(
                path = %runtime_dir.display(),
                error = %e,
                "Failed to create runtime dir to persist runtime.json (read-only filesystem?)"
            );
        } else if let Err(e) = fs::write(&profile_path, json_str) {
            tracing::warn!(
                path = %profile_path.display(),
                error = %e,
                "Failed to persist runtime.json (read-only filesystem?)"
            );
        } else if !loaded_from_file {
            tracing::info!(
                path = %profile_path.display(),
                cpu_tier = %profile.hardware.cpu_tier,
                memory_tier = %profile.hardware.memory_tier,
                io_workers = profile.transport.io_workers,
                max_conns = profile.transport.max_active_connections,
                "Generated baseline runtime.json from hardware probe"
            );
        }
    }

    profile
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runtime_profile_from_hardware_across_all_tiers() {
        const MB: usize = 1024 * 1024;
        const GB: usize = 1024 * MB;

        let test_cases = [
            (256 * MB, "constrained", 1_000, 500, 10_000, 1024, 0),
            (1024 * MB, "small", 10_000, 2_000, 50_000, 2048, 4096),
            (4 * GB, "medium", 50_000, 10_000, 200_000, 8192, 8192),
            (16 * GB, "large", 150_000, 30_000, 500_000, 16384, 8192),
            (48 * GB, "xlarge", 400_000, 80_000, 1_000_000, 32768, 16384),
            (
                96 * GB,
                "2xlarge",
                1_000_000,
                200_000,
                2_000_000,
                65536,
                16384,
            ),
            (
                256 * GB,
                "ultra",
                2_500_000,
                500_000,
                4_000_000,
                131072,
                32768,
            ),
        ];

        for (
            ram_bytes,
            expected_tier,
            expected_cache,
            expected_lkg,
            expected_conns,
            expected_tls_cache,
            expected_early_data,
        ) in test_cases
        {
            let hardware = HardwareTopology::with_workers_and_memory(4, ram_bytes);
            let profile = RuntimeProfile::from_hardware(&hardware);

            assert_eq!(profile.hardware.tier, expected_tier);
            assert_eq!(profile.hardware.memory_tier, expected_tier);
            assert_eq!(profile.hardware.cpu_tier, "small"); // 4 cores = small
            assert_eq!(profile.transport.io_workers, 2); // small = 2 io_workers
            assert_eq!(profile.transport.max_active_connections, expected_conns);
            assert_eq!(profile.discovery.max_dns_cache_capacity, expected_cache);
            assert_eq!(profile.discovery.max_lkg_capacity, expected_lkg);

            // TLS profile assertions
            assert_eq!(profile.tls.session_cache_capacity, expected_tls_cache);
            assert_eq!(profile.tls.max_early_data_size, expected_early_data);
            assert_eq!(profile.tls.send_tls13_tickets, 2); // small cpu = 2 tickets
            assert_eq!(profile.tls.handshake_timeout_secs, 8); // small cpu = 8s

            let tls_params = profile.to_tls_server_params();
            assert_eq!(tls_params.session_cache_capacity, expected_tls_cache);
            assert_eq!(tls_params.max_early_data_size, expected_early_data);
            assert_eq!(tls_params.send_tls13_tickets, 2);
            assert_eq!(tls_params.handshake_timeout.as_secs(), 8);

            let dns_cfg = profile.to_dns_resolver_config();
            assert_eq!(dns_cfg.cache_capacity, expected_cache);
            assert_eq!(dns_cfg.lkg_capacity, expected_lkg);

            let tcp_cfg = profile.to_tcp_listener_config();
            assert!(tcp_cfg.backlog >= 512);
            assert_eq!(tcp_cfg.copy_buffer_size, 16 * 1024); // small cpu = 16KB

            let udp_cfg = profile.to_udp_socket_config();
            assert!(udp_cfg.recv_buffer_size.is_some());
        }
    }

    #[test]
    fn test_resolve_runtime_profile_operator_override() {
        let temp_dir = tempfile::tempdir().unwrap();
        let runtime_json_path = temp_dir.path().join("runtime.json");

        // Custom operator override with explicitly tuned values
        let custom_json = r#"{
            "version": 1,
            "hardware": {
                "detected_ram_bytes": 1073741824,
                "detected_cores": 8,
                "worker_threads": 8,
                "tier": "custom"
            },
            "discovery": {
                "max_dns_cache_capacity": 42000,
                "max_lkg_capacity": 7777,
                "dns_max_packet_size": 2048,
                "query_timeout_ms": 1500,
                "negative_ttl_secs": 25,
                "positive_ttl_secs": 45
            },
            "transport": {
                "io_workers": 8
            }
        }"#;

        fs::write(&runtime_json_path, custom_json).unwrap();

        let hardware = HardwareTopology::with_workers_and_memory(2, 256 * 1024 * 1024);
        // Even though hardware is constrained (256MB), operator's runtime.json MUST win!
        let resolved = resolve_runtime_profile(temp_dir.path(), &hardware);

        assert_eq!(resolved.hardware.tier, "custom");
        assert_eq!(resolved.discovery.max_dns_cache_capacity, 42_000);
        assert_eq!(resolved.discovery.max_lkg_capacity, 7_777);
        assert_eq!(resolved.discovery.dns_max_packet_size, 2048);
        assert_eq!(resolved.transport.io_workers, 8);
    }

    #[test]
    fn test_resolve_runtime_profile_fallback_and_writeback() {
        let temp_dir = tempfile::tempdir().unwrap();
        let runtime_json_path = temp_dir.path().join("runtime.json");

        assert!(!runtime_json_path.exists());

        let hardware = HardwareTopology::with_workers_and_memory(2, 160 * 1024 * 1024 * 1024); // 160GB -> Ultra
        let resolved = resolve_runtime_profile(temp_dir.path(), &hardware);

        assert_eq!(resolved.hardware.tier, "ultra");
        assert_eq!(resolved.discovery.max_dns_cache_capacity, 2_500_000);
        assert_eq!(resolved.discovery.max_lkg_capacity, 500_000);
        assert_eq!(resolved.transport.max_active_connections, 4_000_000);

        // Verify write-back occurred
        assert!(runtime_json_path.exists());
        let content = fs::read_to_string(&runtime_json_path).unwrap();
        let parsed: RuntimeProfile = serde_json::from_str(&content).unwrap();
        assert_eq!(parsed, resolved);
    }

    #[test]
    fn test_resolve_runtime_profile_partial_override_with_tier_fallback() {
        let temp_dir = tempfile::tempdir().unwrap();
        let runtime_json_path = temp_dir.path().join("runtime.json");

        // Operator only overrides 1 single field!
        let partial_json = r#"{
            "discovery": {
                "max_dns_cache_capacity": 99999
            }
        }"#;

        fs::write(&runtime_json_path, partial_json).unwrap();

        // Hardware is Medium (4 GB RAM, 4 cores)
        let hardware = HardwareTopology::with_workers_and_memory(4, 4 * 1024 * 1024 * 1024);
        let resolved = resolve_runtime_profile(temp_dir.path(), &hardware);

        // Explicitly overridden field MUST match
        assert_eq!(resolved.discovery.max_dns_cache_capacity, 99_999);

        // All missing fields MUST cleanly fallback to Medium tier defaults!
        assert_eq!(resolved.hardware.tier, "medium");
        assert_eq!(resolved.discovery.max_lkg_capacity, 10_000);
        assert_eq!(resolved.discovery.dns_max_packet_size, 1024);
        assert_eq!(resolved.discovery.query_timeout_ms, 2000);
        assert_eq!(resolved.transport.io_workers, 2); // 4 cores = small cpu tier (2 workers)
        assert_eq!(resolved.transport.max_active_connections, 200_000);

        // Verify that the file was updated with the full merged state
        let updated_content = fs::read_to_string(&runtime_json_path).unwrap();
        let updated_profile: RuntimeProfile = serde_json::from_str(&updated_content).unwrap();
        assert_eq!(updated_profile.discovery.max_dns_cache_capacity, 99_999);
        assert_eq!(updated_profile.discovery.max_lkg_capacity, 10_000);
    }

    #[test]
    fn test_resolve_runtime_profile_grouped_transport_override() {
        let temp_dir = tempfile::tempdir().unwrap();
        let runtime_json_path = temp_dir.path().join("runtime.json");

        let json = r#"{
            "version": 1,
            "transport": {
                "io_workers": 12,
                "max_active_connections": 750000,
                "tcp": {
                    "backlog": 9999,
                    "copy_buffer_size": 49152
                },
                "udp": {
                    "recv_buffer_size": 5242880
                }
            }
        }"#;

        fs::write(&runtime_json_path, json).unwrap();
        let hardware = HardwareTopology::with_workers_and_memory(4, 4 * 1024 * 1024 * 1024);
        let resolved = resolve_runtime_profile(temp_dir.path(), &hardware);

        assert_eq!(resolved.transport.io_workers, 12);
        assert_eq!(resolved.transport.max_active_connections, 750_000);
        assert_eq!(resolved.transport.tcp.backlog, 9999);
        assert_eq!(resolved.transport.tcp.copy_buffer_size, 49_152);
        assert_eq!(resolved.transport.udp.recv_buffer_size, Some(5_242_880));
    }

    #[test]
    fn test_resolve_runtime_profile_tls_override() {
        let temp_dir = tempfile::tempdir().unwrap();
        let runtime_json_path = temp_dir.path().join("runtime.json");

        let json = r#"{
            "version": 1,
            "tls": {
                "session_cache_capacity": 50000,
                "max_early_data_size": 65536,
                "send_tls13_tickets": 7,
                "handshake_timeout_secs": 12
            }
        }"#;

        fs::write(&runtime_json_path, json).unwrap();
        let hardware = HardwareTopology::with_workers_and_memory(4, 4 * 1024 * 1024 * 1024);
        let resolved = resolve_runtime_profile(temp_dir.path(), &hardware);

        assert_eq!(resolved.tls.session_cache_capacity, 50_000);
        assert_eq!(resolved.tls.max_early_data_size, 65_536);
        assert_eq!(resolved.tls.send_tls13_tickets, 7);
        assert_eq!(resolved.tls.handshake_timeout_secs, 12);

        let params = resolved.to_tls_server_params();
        assert_eq!(params.session_cache_capacity, 50_000);
        assert_eq!(params.max_early_data_size, 65_536);
        assert_eq!(params.send_tls13_tickets, 7);
        assert_eq!(params.handshake_timeout.as_secs(), 12);
    }

    #[test]
    fn test_parse_example_runtime_json() {
        let example_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../velda-sync/example/runtime.json");
        assert!(example_path.exists(), "example/runtime.json must exist");

        let content = fs::read_to_string(&example_path).unwrap();
        let profile: RuntimeProfile = serde_json::from_str(&content)
            .expect("example/runtime.json must be valid according to RuntimeProfile schema");

        assert_eq!(profile.version, 1);
        assert_eq!(profile.hardware.tier, "medium");
        assert_eq!(profile.transport.io_workers, 4);
        assert_eq!(profile.tls.session_cache_capacity, 8192);
        assert_eq!(profile.tls.send_tls13_tickets, 4);
    }
}
