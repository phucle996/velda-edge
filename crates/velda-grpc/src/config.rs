//! Configuration parameters for gRPC server and client pipelines.
//!
//! Continuously scaled by host hardware topology.
//! Zero-IO, zero JSON parsing on hot paths.

/// Tunable settings for gRPC framing, metadata limits, and stream concurrency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrpcConfig {
    /// Maximum length-prefixed message size in bytes.
    pub max_message_size: usize,
    /// Maximum metadata / header size in bytes.
    pub max_header_size: usize,
    /// Maximum number of metadata headers accepted.
    pub max_headers: usize,
    /// Connection idle timeout in milliseconds.
    pub idle_timeout_ms: u64,
    /// Maximum number of concurrent streams allowed per connection.
    pub max_concurrent_streams: u32,
    /// Keepalive PING interval in milliseconds for long-lived idle connections.
    pub keepalive_ping_interval_ms: u64,
    /// Timeout in milliseconds waiting for keepalive PING acknowledgement.
    pub keepalive_ping_timeout_ms: u64,
    /// Maximum duration allowed for an individual RPC call in milliseconds.
    pub max_call_duration_ms: u64,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self::from_hardware(4, 8 * 1024 * 1024 * 1024)
    }
}

impl GrpcConfig {
    /// Probes host hardware topology and derives the configuration.
    pub fn auto() -> Self {
        Self::for_topology(velda_core::global_hardware_topology())
    }

    /// Derives a [`GrpcConfig`] scaled deterministically from host hardware topology.
    pub fn for_topology(topo: &velda_core::HardwareTopology) -> Self {
        Self {
            max_message_size: topo.sizing.grpc_max_message_size,
            max_header_size: topo.sizing.http1_max_header_size.max(16 * 1024),
            max_headers: topo.sizing.http1_max_headers_count,
            idle_timeout_ms: 30_000,
            max_concurrent_streams: topo.sizing.h2_max_concurrent_streams,
            keepalive_ping_interval_ms: 45_000,
            keepalive_ping_timeout_ms: 10_000,
            max_call_duration_ms: 120_000,
        }
    }

    /// Derives a [`GrpcConfig`] scaled continuously from host CPU cores and RAM bytes.
    pub const fn from_hardware(cores: usize, ram_bytes: usize) -> Self {
        const KB: usize = 1024;
        const MB: usize = 1024 * KB;
        const GB: usize = 1024 * MB;

        let ram_gb = if ram_bytes / GB == 0 {
            1
        } else {
            ram_bytes / GB
        };
        let max_message_size = {
            let val = ram_gb * 4 * MB;
            if val < 4 * MB {
                4 * MB
            } else if val > 64 * MB {
                64 * MB
            } else {
                val
            }
        };

        let max_header_size = {
            let val = ram_gb * 16 * KB;
            if val < 16 * KB {
                16 * KB
            } else if val > 128 * KB {
                128 * KB
            } else {
                val
            }
        };

        let max_headers = {
            let val = ram_gb * 32;
            if val < 32 {
                32
            } else if val > 128 {
                128
            } else {
                val
            }
        };

        let max_concurrent_streams = {
            let val = (cores * 64) as u32;
            if val < 64 {
                64
            } else if val > 512 {
                512
            } else {
                val
            }
        };

        Self {
            max_message_size,
            max_header_size,
            max_headers,
            idle_timeout_ms: 30_000,
            max_concurrent_streams,
            keepalive_ping_interval_ms: 45_000,
            keepalive_ping_timeout_ms: 10_000,
            max_call_duration_ms: 120_000,
        }
    }

    /// Sets the maximum message size in bytes.
    #[inline]
    pub const fn with_max_message_size(mut self, size: usize) -> Self {
        self.max_message_size = size;
        self
    }

    /// Sets the maximum header size in bytes.
    #[inline]
    pub const fn with_max_header_size(mut self, size: usize) -> Self {
        self.max_header_size = size;
        self
    }

    /// Sets the maximum number of headers.
    #[inline]
    pub const fn with_max_headers(mut self, max: usize) -> Self {
        self.max_headers = max;
        self
    }

    /// Sets the connection idle timeout in milliseconds.
    #[inline]
    pub const fn with_idle_timeout_ms(mut self, ms: u64) -> Self {
        self.idle_timeout_ms = ms;
        self
    }

    /// Sets the maximum concurrent streams allowed.
    #[inline]
    pub const fn with_max_concurrent_streams(mut self, max: u32) -> Self {
        self.max_concurrent_streams = max;
        self
    }

    /// Sets the keepalive PING interval in milliseconds.
    #[inline]
    pub const fn with_keepalive_ping_interval_ms(mut self, ms: u64) -> Self {
        self.keepalive_ping_interval_ms = ms;
        self
    }

    /// Sets the keepalive PING timeout in milliseconds.
    #[inline]
    pub const fn with_keepalive_ping_timeout_ms(mut self, ms: u64) -> Self {
        self.keepalive_ping_timeout_ms = ms;
        self
    }

    /// Sets the maximum RPC call duration in milliseconds.
    #[inline]
    pub const fn with_max_call_duration_ms(mut self, ms: u64) -> Self {
        self.max_call_duration_ms = ms;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auto_returns_valid_config() {
        let config = GrpcConfig::auto();
        assert!(config.max_message_size >= 4 * 1024 * 1024);
        assert!(config.max_header_size >= 16 * 1024);
    }

    #[test]
    fn test_for_topology() {
        let topo = velda_core::HardwareTopology::with_workers_and_memory(4, 8 * 1024 * 1024 * 1024);
        let config = GrpcConfig::for_topology(&topo);
        assert_eq!(config.max_message_size, topo.sizing.grpc_max_message_size);
        assert_eq!(
            config.max_concurrent_streams,
            topo.sizing.h2_max_concurrent_streams
        );
    }
}
