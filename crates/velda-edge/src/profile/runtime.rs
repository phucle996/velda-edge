//! Complete runtime profile entity and configuration conversions.

use serde::{Deserialize, Serialize};
use velda_core::hardware::HardwareTopology;
use velda_discovery::DnsResolverConfig;
use velda_transport::{EngineConfig, TcpListenerConfig, UdpSocketConfig};

use super::schema::*;

/// Complete runtime profile for the edge node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeProfile {
    pub version: u32,
    pub hardware: HardwareProfile,
    pub discovery: DiscoveryRuntimeConfig,
    pub transport: TransportRuntimeConfig,
    pub tls: TlsRuntimeConfig,
    #[serde(default = "velda_core::OverloadConfig::default")]
    pub overload: velda_core::OverloadConfig,
}

impl RuntimeProfile {
    /// Generates a runtime profile using explicit CPU and memory tiers,
    /// while retaining physical hardware topology details (detected RAM, detected cores, kernel support).
    pub fn for_tiers(
        hardware: &HardwareTopology,
        cpu_tier: velda_core::CpuTier,
        memory_tier: velda_core::MemoryTier,
    ) -> Self {
        let dns_config = DnsResolverConfig::for_tier(memory_tier);
        let mut engine_config = EngineConfig::for_tiers(cpu_tier, memory_tier);
        engine_config.tcp = TcpListenerConfig::for_topology(hardware);
        engine_config.udp = UdpSocketConfig::for_topology(hardware);
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
                max_negative_ttl_secs: dns_config.max_negative_ttl.as_secs(),
                query_timeout_ms: dns_config.query_timeout.as_millis() as u64,
                negative_ttl_secs: dns_config.negative_ttl.as_secs(),
                positive_ttl_secs: dns_config.positive_ttl.as_secs(),
            },
            transport: TransportRuntimeConfig {
                io_workers: engine_config.io_workers,
                max_active_connections: engine_config.max_active_connections,
                reconcile_channel_capacity: engine_config.reconcile_channel_capacity,
                cpu_pinning: true,
                tcp: TcpRuntimeConfig {
                    backlog: engine_config.tcp.backlog,
                    nodelay: engine_config.tcp.nodelay,
                    keepalive_secs: engine_config.tcp.keepalive.map(|d| d.as_secs()),
                    recv_buffer_size: engine_config.tcp.recv_buffer_size,
                    send_buffer_size: engine_config.tcp.send_buffer_size,
                    copy_buffer_size: engine_config.tcp.copy_buffer_size,
                    reuseport: engine_config.tcp.reuseport,
                    concurrency_shards: engine_config.tcp.concurrency_shards,
                    quickack: Some(engine_config.tcp.quickack),
                    defer_accept_secs: engine_config.tcp.defer_accept_secs,
                    fastopen_backlog: engine_config.tcp.fastopen_backlog,
                    busy_poll_us: engine_config.tcp.busy_poll_us,
                    incoming_cpu: Some(engine_config.tcp.incoming_cpu),
                    notsent_lowat: engine_config.tcp.notsent_lowat,
                    user_timeout_secs: engine_config.tcp.user_timeout.map(|d| d.as_secs()),
                    freebind: if engine_config.tcp.freebind {
                        Some(true)
                    } else {
                        None
                    },
                },
                udp: UdpRuntimeConfig {
                    recv_buffer_size: engine_config.udp.recv_buffer_size,
                    send_buffer_size: engine_config.udp.send_buffer_size,
                    reuseport: engine_config.udp.reuseport,
                    concurrency_shards: engine_config.udp.concurrency_shards,
                    freebind: if engine_config.udp.freebind {
                        Some(true)
                    } else {
                        None
                    },
                    gro: if engine_config.udp.gro {
                        Some(true)
                    } else {
                        None
                    },
                    rxq_ovfl: if engine_config.udp.rxq_ovfl {
                        Some(true)
                    } else {
                        None
                    },
                },
            },
            tls: TlsRuntimeConfig {
                session_cache_capacity: tls_params.session_cache_capacity,
                session_shards: tls_params.session_shards,
                max_early_data_size: tls_params.max_early_data_size,
                send_tls13_tickets: tls_params.send_tls13_tickets,
                handshake_timeout_secs: tls_params.handshake_timeout.as_secs(),
            },
            overload: velda_core::OverloadConfig::for_tier(memory_tier),
        }
    }

    /// Generates a default runtime profile from the probed hardware topology.
    pub fn from_hardware(hardware: &HardwareTopology) -> Self {
        Self::for_tiers(hardware, hardware.cpu_tier(), hardware.memory_tier())
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
            max_negative_ttl: std::time::Duration::from_secs(self.discovery.max_negative_ttl_secs),
        }
    }

    /// Converts the transport section into a strongly-typed [`TcpListenerConfig`].
    pub fn to_tcp_listener_config(&self) -> TcpListenerConfig {
        let mut cfg = TcpListenerConfig::new()
            .with_backlog(self.transport.tcp.backlog)
            .with_nodelay(self.transport.tcp.nodelay)
            .with_copy_buffer_size(self.transport.tcp.copy_buffer_size)
            .with_reuseport(self.transport.tcp.reuseport)
            .with_concurrency_shards(self.transport.tcp.concurrency_shards)
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
        if let Some(q) = self.transport.tcp.quickack {
            cfg = cfg.with_quickack(q);
        }
        if let Some(d) = self.transport.tcp.defer_accept_secs {
            cfg = cfg.with_defer_accept(Some(d));
        }
        if let Some(fb) = self.transport.tcp.fastopen_backlog {
            cfg = cfg.with_fastopen_backlog(Some(fb));
        }
        if let Some(bp) = self.transport.tcp.busy_poll_us {
            cfg = cfg.with_busy_poll(Some(bp));
        }
        if let Some(ic) = self.transport.tcp.incoming_cpu {
            cfg = cfg.with_incoming_cpu(ic);
        }
        if let Some(nl) = self.transport.tcp.notsent_lowat {
            cfg = cfg.with_notsent_lowat(Some(nl));
        }
        if let Some(ut) = self.transport.tcp.user_timeout_secs {
            cfg = cfg.with_user_timeout(Some(std::time::Duration::from_secs(ut)));
        }
        if let Some(fb) = self.transport.tcp.freebind {
            cfg = cfg.with_freebind(fb);
        }
        cfg
    }

    /// Converts the transport section into a strongly-typed [`UdpSocketConfig`].
    pub fn to_udp_socket_config(&self) -> UdpSocketConfig {
        let mut cfg = UdpSocketConfig::new()
            .with_reuseport(self.transport.udp.reuseport)
            .with_concurrency_shards(self.transport.udp.concurrency_shards);
        if let Some(r) = self.transport.udp.recv_buffer_size {
            cfg = cfg.with_recv_buffer_size(r);
        }
        if let Some(s) = self.transport.udp.send_buffer_size {
            cfg = cfg.with_send_buffer_size(s);
        }
        if let Some(fb) = self.transport.udp.freebind {
            cfg = cfg.with_freebind(fb);
        }
        if let Some(gro) = self.transport.udp.gro {
            cfg = cfg.with_gro(gro);
        }
        if let Some(rxq_ovfl) = self.transport.udp.rxq_ovfl {
            cfg = cfg.with_rxq_ovfl(rxq_ovfl);
        }
        cfg
    }

    /// Converts the TLS section into a strongly-typed [`velda_tls::TlsServerParams`].
    pub fn to_tls_server_params(&self) -> velda_tls::TlsServerParams {
        self.tls.to_tls_server_params()
    }

    /// Returns the effective memory tier, respecting operator override or fallback to probed hardware.
    pub fn memory_tier(&self) -> velda_core::MemoryTier {
        self.hardware
            .memory_tier
            .parse::<velda_core::MemoryTier>()
            .or_else(|_| self.hardware.tier.parse::<velda_core::MemoryTier>())
            .unwrap_or_else(|_| velda_core::hardware::global_hardware_topology().memory_tier())
    }

    /// Returns the effective CPU tier, respecting operator override or fallback to probed hardware.
    pub fn cpu_tier(&self) -> velda_core::CpuTier {
        self.hardware
            .cpu_tier
            .parse::<velda_core::CpuTier>()
            .unwrap_or_else(|_| velda_core::hardware::global_hardware_topology().cpu_tier())
    }
}
