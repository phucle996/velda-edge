//! HTTP/1.1 protocol engine configuration and limits, scaled by hardware topology.
//!
//! Follows the `for_tier(MemoryTier)` pattern. Each subsystem owns its own tier-driven
//! sizing and safety limits — `velda-core::hardware` only probes the host, it does not
//! dictate subsystem parameters.

/// Complete configuration and limits for the HTTP/1.1 protocol engine.
///
/// Controls security limits (body, header, header count, inactivity timeout)
/// and per-connection memory allocation on downstream and upstream paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Http1Config {
    /// Maximum allowed request body size in bytes.
    ///
    /// Note: Enforced strictly for `Buffered` strategy. Streaming strategies
    /// (`ServerStream`, `ClientStream`, `Duplex`) stream continuously with constant memory footprint.
    pub max_body_size: usize,
    /// Maximum allowed size of the raw header block in bytes.
    pub max_header_size: usize,
    /// Maximum number of header fields permitted per request.
    pub max_headers: usize,
    /// Maximum idle inactivity timeout in milliseconds between incoming/outgoing data chunks.
    pub idle_timeout_ms: u64,
    /// Maximum number of requests allowed per TCP keep-alive connection before closing.
    pub max_keepalive_requests: u32,
    /// Maximum duration (in milliseconds) allowed to read request line and headers (mitigates Slowloris).
    pub header_read_timeout_ms: u64,
    /// Initial read/write buffer capacity per downstream connection (bytes).
    pub initial_buffer_capacity: usize,
    /// Capacity threshold above which idle buffers are shrunk back to `initial_buffer_capacity`.
    pub shrink_threshold: usize,
    /// Read buffer size for client upload bodies (equivalent to Nginx's `client_body_buffer_size`).
    /// Determines the capacity reserved for reading payload chunks from the downstream socket into RAM.
    pub client_body_buffer_size: usize,
}

impl Default for Http1Config {
    fn default() -> Self {
        Self::auto()
    }
}

#[inline]
const fn clamp_usize(val: usize, min: usize, max: usize) -> usize {
    if val < min {
        min
    } else if val > max {
        max
    } else {
        val
    }
}

impl Http1Config {
    /// Creates configuration sized appropriately from hardware sizing parameters.
    pub const fn from_sizing(sizing: &velda_core::HardwareSizing) -> Self {
        Self {
            max_body_size: clamp_usize(
                sizing.http1_max_chunk_size * 100,
                10 * 1024 * 1024,
                100 * 1024 * 1024,
            ),
            max_header_size: sizing.http1_max_header_size,
            max_headers: sizing.http1_max_headers_count,
            idle_timeout_ms: 30_000,
            max_keepalive_requests: 10_000_000,
            header_read_timeout_ms: 10_000,
            initial_buffer_capacity: clamp_usize(
                sizing.http1_max_buffer_size / 8,
                2 * 1024,
                16 * 1024,
            ),
            shrink_threshold: clamp_usize(sizing.http1_max_buffer_size / 2, 8 * 1024, 64 * 1024),
            client_body_buffer_size: sizing.http1_max_buffer_size,
        }
    }

    /// Derives an [`Http1Config`] scaled continuously from host CPU cores and RAM bytes.
    pub const fn from_hardware(cores: usize, ram_bytes: usize) -> Self {
        let sizing = velda_core::HardwareSizing::from_hardware(cores, ram_bytes);
        Self::from_sizing(&sizing)
    }

    /// Creates configuration sized appropriately from hardware topology.
    pub fn for_topology(topo: &velda_core::HardwareTopology) -> Self {
        Self::from_sizing(&topo.sizing)
    }

    /// Auto-probes the host hardware topology and returns the appropriate config.
    pub fn auto() -> Self {
        Self::for_topology(velda_core::global_hardware_topology())
    }

    /// Sets the maximum request body size in bytes for buffered requests.
    ///
    /// *Note:* Enforced strictly for `Buffered` strategy; streaming strategies stream continuously.
    #[inline]
    pub const fn with_max_body_size(mut self, size: usize) -> Self {
        self.max_body_size = size;
        self
    }

    /// Sets the maximum header block size in bytes.
    #[inline]
    pub const fn with_max_header_size(mut self, size: usize) -> Self {
        self.max_header_size = size;
        self
    }

    /// Sets the maximum number of headers permitted.
    #[inline]
    pub const fn with_max_headers(mut self, count: usize) -> Self {
        self.max_headers = count;
        self
    }

    /// Sets the idle timeout in milliseconds.
    #[inline]
    pub const fn with_idle_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.idle_timeout_ms = timeout_ms;
        self
    }

    /// Sets the maximum keep-alive requests per connection.
    #[inline]
    pub const fn with_max_keepalive_requests(mut self, n: u32) -> Self {
        self.max_keepalive_requests = n;
        self
    }

    /// Sets the maximum duration allowed to read headers (mitigating Slowloris).
    #[inline]
    pub const fn with_header_read_timeout_ms(mut self, ms: u64) -> Self {
        self.header_read_timeout_ms = ms;
        self
    }

    /// Sets the initial buffer capacity.
    #[inline]
    pub const fn with_initial_buffer_capacity(mut self, capacity: usize) -> Self {
        self.initial_buffer_capacity = capacity;
        self
    }

    /// Sets the buffer shrink threshold.
    #[inline]
    pub const fn with_shrink_threshold(mut self, threshold: usize) -> Self {
        self.shrink_threshold = threshold;
        self
    }

    /// Sets the client body buffer window capacity in bytes.
    #[inline]
    pub const fn with_client_body_buffer_size(mut self, size: usize) -> Self {
        self.client_body_buffer_size = size;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http1_config_for_topology() {
        let topo = velda_core::HardwareTopology::with_workers_and_memory(4, 4 * 1024 * 1024 * 1024);
        let cfg = Http1Config::for_topology(&topo);
        assert!(cfg.max_body_size >= 10 * 1024 * 1024);
        assert!(cfg.max_header_size >= 8 * 1024);
        assert!(cfg.max_headers >= 64);
        assert!(cfg.initial_buffer_capacity >= 2 * 1024);
    }

    #[test]
    fn test_auto_returns_valid_config() {
        let cfg = Http1Config::auto();
        assert!(cfg.initial_buffer_capacity >= 2048);
        assert!(cfg.max_body_size >= 2 * 1024 * 1024);
    }
}
