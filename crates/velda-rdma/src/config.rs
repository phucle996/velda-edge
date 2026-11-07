//! # RDMA Subsystem & Queue Pair Hardware Configuration
//!
//! ## Architectural Invariants & Parameter Tuning
//!
//! High-performance kernel-bypass networking requires careful coordination between hardware HCA
//! parameters, operating system resource limits (`RLIMIT_MEMLOCK`), and runtime serving topology.
//!
//! ### Core Configuration Dimensions:
//! 1. **Topology & Physical Addressing (`device_name`, `port`, `gid_index`)**:
//!    - Specifies the physical Mellanox ConnectX or InfiniBand HCA interface in `/sys/class/infiniband`.
//!    - RoCEv2 encapsulation relies on the correct GID index to map packets to the corresponding IPv4/IPv6
//!      VLAN or overlay interface on the host network adapter.
//! 2. **Ring Buffer Depths (`max_send_wr`, `max_recv_wr`, `cq_size`)**:
//!    - Controls hardware Work Queue and Completion Queue capacities. Sized appropriately to absorb
//!      inbound edge traffic bursts without queue overflow while preserving hardware L2/L3 cache locality.
//! 3. **Latency vs. CPU Trade-off (`poll_spin_count`)**:
//!    - Sets the threshold for `AdaptiveCqPoller`. Setting this to `0` enables pure interrupt-driven
//!      Tokio `AsyncFd` polling (0% CPU when idle). Values between `50` and `2000` provide sub-microsecond
//!      completion harvesting during active bursts.
//! 4. **Reliability & Flow Control (`initial_credits`, `rnr_retry`)**:
//!    - Configures credit allowances for `CreditFlowControl` to eliminate hardware Receiver Not Ready (RNR)
//!      events. Sets the hardware HCA RNR retry limit (`0–7`, where `7` represents infinite hardware retries).

use velda_core::StreamingMode;

/// Baseline credit allotment for edge VM / low-memory deployments (128 credits).
pub const BASELINE_RDMA_CREDITS: u32 = 128;
/// Recommended standard credit allotment for 100Gbps Datacenter line-rate (256 credits).
pub const DEFAULT_RDMA_CREDITS: u32 = 256;
/// High-throughput scale-up tier for bulk / tensor streaming (512 credits).
pub const HIGH_THROUGHPUT_RDMA_CREDITS: u32 = 512;
/// Peak scale-up tier for ultra-high throughput / large offload pipelines (1024 credits).
pub const PEAK_RDMA_CREDITS: u32 = 1024;
/// Default channel slot size (64 KB).
pub const DEFAULT_RDMA_SLOT_SIZE: usize = 65536;

/// Configuration parameters for RDMA subsystem and endpoints.
#[derive(Debug, Clone)]
pub struct RdmaConfig {
    /// Preferred RDMA device name (e.g., "mlx5_0"). If `None`, probes sysfs for the first active device.
    pub device_name: Option<String>,

    /// Physical HCA port number (typically 1).
    pub port: u8,

    /// GID (Global Identifier) index for RoCEv2 / InfiniBand addressing (typically 0 or 1).
    pub gid_index: u8,

    /// Maximum outstanding Send Work Requests in the Send Queue.
    pub max_send_wr: u32,

    /// Maximum outstanding Receive Work Requests in the Receive Queue.
    pub max_recv_wr: u32,

    /// Maximum Scatter-Gather Elements (SGE) per Work Request.
    pub max_sge: u32,

    /// Completion Queue depth (number of CQEs).
    pub cq_size: u32,

    /// Initial flow control credits allocated to remote peer.
    pub initial_credits: u32,

    /// Number of busy-poll iterations before falling back to event-driven sleep in adaptive polling.
    pub poll_spin_count: u32,

    /// Hardware RNR (Receiver Not Ready) retry counter (0-7, 7 = infinite retry).
    pub rnr_retry: u8,

    /// Connection and operation timeout in milliseconds.
    pub timeout_ms: u64,

    /// Force using pure-Rust in-memory software mock channel (bypasses hardware verbs).
    pub force_mock: bool,
}

impl Default for RdmaConfig {
    fn default() -> Self {
        Self {
            device_name: None,
            port: 1,
            gid_index: 0,
            max_send_wr: DEFAULT_RDMA_CREDITS,
            max_recv_wr: DEFAULT_RDMA_CREDITS,
            max_sge: 4,
            cq_size: DEFAULT_RDMA_CREDITS * 2,
            initial_credits: DEFAULT_RDMA_CREDITS,
            poll_spin_count: 32,
            rnr_retry: 7,
            timeout_ms: 5000,
            force_mock: false,
        }
    }
}

impl RdmaConfig {
    /// Creates a mock configuration suitable for unit testing and local development without RDMA hardware.
    pub fn mock() -> Self {
        Self {
            force_mock: true,
            ..Default::default()
        }
    }

    /// Calculates optimal credit and ring buffer sizes tailored to the pipeline strategy.
    ///
    /// - `Buffered`: Scaled directly from `max_body_size` (`ceil(max_body / slot_size)`),
    ///   allowing the entire body to be transferred in a single flight (zero flow-control stops).
    /// - `ClientStream`: Scaled from client streaming buffer budget (e.g. 2MB - 4MB).
    /// - `ServerStream` / `Duplex`: Unbounded long-lived streams follow threshold tiers
    ///   (Baseline 128, Default 256, Peak 512/1024).
    pub fn for_streaming(mode: StreamingMode, max_body_size: Option<usize>) -> Self {
        let mut cfg = Self::default();
        if !mode.is_streaming_enabled() {
            // Buffered Mode: Size credits to embrace the entire max_body_size without stops
            let body = max_body_size.unwrap_or(256 * 1024);
            let needed_slots = body.div_ceil(DEFAULT_RDMA_SLOT_SIZE).max(8) as u32;
            cfg.initial_credits = needed_slots.min(DEFAULT_RDMA_CREDITS);
            cfg.max_send_wr = cfg.initial_credits;
            cfg.max_recv_wr = cfg.initial_credits;
            cfg.cq_size = cfg.initial_credits * 2;
        } else if mode.is_client_streaming() && !mode.is_server_streaming() {
            // Client Streaming: Size credits to match client upload buffer budget
            let budget = max_body_size.unwrap_or(2 * 1024 * 1024);
            let needed_slots = budget.div_ceil(DEFAULT_RDMA_SLOT_SIZE).max(16) as u32;
            cfg.initial_credits = needed_slots.clamp(BASELINE_RDMA_CREDITS, DEFAULT_RDMA_CREDITS);
            cfg.max_send_wr = cfg.initial_credits;
            cfg.max_recv_wr = cfg.initial_credits;
            cfg.cq_size = cfg.initial_credits * 2;
        } else {
            // Server Streaming or Duplex: Unbounded streaming using standard datacenter tier
            cfg.initial_credits = DEFAULT_RDMA_CREDITS;
            cfg.max_send_wr = DEFAULT_RDMA_CREDITS;
            cfg.max_recv_wr = DEFAULT_RDMA_CREDITS;
            cfg.cq_size = DEFAULT_RDMA_CREDITS * 2;
        }
        cfg
    }

    /// Explicit baseline tier (128 credits) for resource-constrained edge environments.
    pub fn baseline() -> Self {
        Self {
            initial_credits: BASELINE_RDMA_CREDITS,
            max_send_wr: BASELINE_RDMA_CREDITS,
            max_recv_wr: BASELINE_RDMA_CREDITS,
            cq_size: BASELINE_RDMA_CREDITS * 2,
            ..Default::default()
        }
    }

    /// High-throughput tier (512 credits) for heavy streaming pipelines.
    pub fn high_throughput() -> Self {
        Self {
            initial_credits: HIGH_THROUGHPUT_RDMA_CREDITS,
            max_send_wr: HIGH_THROUGHPUT_RDMA_CREDITS,
            max_recv_wr: HIGH_THROUGHPUT_RDMA_CREDITS,
            cq_size: HIGH_THROUGHPUT_RDMA_CREDITS * 2,
            ..Default::default()
        }
    }

    /// Peak scale-up tier (1024 credits) for bulk replication and AI tensor transfer.
    pub fn peak() -> Self {
        Self {
            initial_credits: PEAK_RDMA_CREDITS,
            max_send_wr: PEAK_RDMA_CREDITS,
            max_recv_wr: PEAK_RDMA_CREDITS,
            cq_size: PEAK_RDMA_CREDITS * 2,
            ..Default::default()
        }
    }
}
