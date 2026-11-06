//! Configuration for RDMA device, queue pairs, and transport pipes.

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
            max_send_wr: 256,
            max_recv_wr: 256,
            max_sge: 4,
            cq_size: 512,
            initial_credits: 64,
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
}
