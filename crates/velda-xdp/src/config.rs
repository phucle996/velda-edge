//! # XDP & eBPF Subsystem Configuration
//!
//! ## Dual Orthogonal Configuration Dimensions
//!
//! Edge listeners declaratively configure XDP along two independent axes:
//!
//! ### Dimension 1: Operational Engine Kind (`XdpEngineKind`)
//! 1. `PassFilter` (NIC-Level Filtering):
//!    - Executes an XDP filter at the driver level to drop malicious traffic (SYN floods, UDP amplification) at line rate.
//!    - Clean packets return `XDP_PASS` to traverse the standard Linux network stack.
//! 2. `Sockmap` (In-Kernel Stream Splicing):
//!    - Connects downstream and upstream sockets within kernel memory via `BPF_MAP_TYPE_SOCKMAP`.
//!    - Retains the kernel TCP state machine while bypassing socket buffer copying.
//! 3. `AfXdp` (Full Kernel Bypass):
//!    - Redirects raw Ethernet frames into user-space UMEM slabs via `XDP_REDIRECT`.
//!    - The userspace network engine manages flow control and packet reassembly directly.
//!
//! ### Dimension 2: Hardware Attachment Mode (`XdpAttachMode`)
//! 1. `Skb` (Generic Mode):
//!    - Hooks into the generic Linux network layer after core packet reception.
//!    - Executes universally on all Linux interfaces, including loopback `lo`, container `veth`, and virtual machines.
//! 2. `Drv` (Native Driver Mode):
//!    - Executes directly in the driver NAPI polling loop with zero-copy DMA into UMEM.
//! 3. `Hw` (Hardware Offload Mode):
//!    - Loads eBPF bytecode directly into the processing ASIC of a SmartNIC (e.g. Netronome Agilio).

/// Canonical operational mechanisms for XDP listeners.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum XdpEngineKind {
    /// Line-rate L4 packet filtering and DDoS mitigation at the NIC, passing clean traffic to the OS.
    PassFilter,
    /// In-kernel stream redirection between sockets via BPF sockmap/sk_msg.
    Sockmap,
    /// Full kernel bypass via AF_XDP and zero-copy UMEM DMA.
    #[default]
    AfXdp,
}

impl XdpEngineKind {
    /// Canonical string identifier.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PassFilter => "pass_filter",
            Self::Sockmap => "sockmap",
            Self::AfXdp => "af_xdp",
        }
    }
}

/// Network interface driver and hardware attachment mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum XdpAttachMode {
    /// Generic SKB mode running universally across all interfaces, VMs, and loopback.
    #[default]
    Skb,
    /// Native driver mode executing directly in the NIC driver with zero-copy DMA.
    Drv,
    /// Hardware offload mode executing on SmartNIC processors.
    Hw,
}

impl XdpAttachMode {
    /// Canonical string identifier.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Skb => "skb",
            Self::Drv => "drv",
            Self::Hw => "hw",
        }
    }

    /// Returns `true` if this mode requires native driver cooperation.
    pub const fn is_native(self) -> bool {
        matches!(self, Self::Drv | Self::Hw)
    }
}

/// Standard UMEM frame size matching a 4KB MMU page.
pub const DEFAULT_UMEM_FRAME_SIZE: usize = 4096;
/// 256 bytes reserved at frame head for TX prepends and tunnel encapsulation without memmove.
pub const DEFAULT_XDP_HEADROOM: usize = 256;
/// 1792 bytes allocated at page tail for L7 token storage without heap allocations.
pub const DEFAULT_XDP_HEADER_WORKSPACE: usize = 1792;
/// 2048 frames (8MB total memory): Sized to remain well within container `RLIMIT_MEMLOCK`
/// boundaries while absorbing 2000 in-flight packets per core during burst traffic.
pub const DEFAULT_UMEM_FRAME_COUNT: u32 = 2048;
/// Rx ring descriptor capacity (must be a power of two).
pub const DEFAULT_RX_RING_SIZE: u32 = 1024;
/// Tx ring descriptor capacity (must be a power of two).
pub const DEFAULT_TX_RING_SIZE: u32 = 1024;
/// Fill ring descriptor capacity.
pub const DEFAULT_FILL_RING_SIZE: u32 = 2048;
/// Completion ring descriptor capacity.
pub const DEFAULT_COMPLETION_RING_SIZE: u32 = 2048;

/// Configuration parameters for an XDP listener or worker channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XdpConfig {
    /// Target network interface name (e.g. "eth0", "lo").
    pub interface: String,
    /// Hardware RX/TX queue index to bind (affined to worker core ID).
    pub queue_id: u32,
    /// Operational engine kind.
    pub engine_kind: XdpEngineKind,
    /// Hardware attachment mode.
    pub attach_mode: XdpAttachMode,
    /// UMEM frame slot size (4096 bytes recommended).
    pub frame_size: usize,
    /// Total UMEM frame slot count.
    pub frame_count: u32,
    /// Headroom bytes reserved at frame start.
    pub headroom: usize,
    /// Header workspace bytes reserved at frame tail.
    pub header_workspace: usize,
    /// Rx ring depth.
    pub rx_ring_size: u32,
    /// Tx ring depth.
    pub tx_ring_size: u32,
    /// Fill ring depth.
    pub fill_ring_size: u32,
    /// Completion ring depth.
    pub comp_ring_size: u32,
    /// Requests zero-copy driver DMA (`XDP_ZEROCOPY`).
    pub zero_copy: bool,
    /// Suppresses redundant wake-up syscalls via `XDP_USE_NEED_WAKEUP`.
    pub need_wakeup: bool,
    /// Maximum busy-polling duration (microseconds) before sleeping on events.
    pub busy_poll_budget: u32,
}

impl Default for XdpConfig {
    fn default() -> Self {
        Self {
            interface: "lo".to_string(),
            queue_id: 0,
            engine_kind: XdpEngineKind::AfXdp,
            attach_mode: XdpAttachMode::Skb,
            frame_size: DEFAULT_UMEM_FRAME_SIZE,
            frame_count: DEFAULT_UMEM_FRAME_COUNT,
            headroom: DEFAULT_XDP_HEADROOM,
            header_workspace: DEFAULT_XDP_HEADER_WORKSPACE,
            rx_ring_size: DEFAULT_RX_RING_SIZE,
            tx_ring_size: DEFAULT_TX_RING_SIZE,
            fill_ring_size: DEFAULT_FILL_RING_SIZE,
            comp_ring_size: DEFAULT_COMPLETION_RING_SIZE,
            zero_copy: false,
            need_wakeup: true,
            busy_poll_budget: 0,
        }
    }
}

impl XdpConfig {
    /// Creates a configuration targeting the specified interface and queue ID.
    pub fn new(interface: impl Into<String>, queue_id: u32) -> Self {
        Self {
            interface: interface.into(),
            queue_id,
            ..Default::default()
        }
    }

    /// Sets the operational engine kind.
    pub fn with_engine_kind(mut self, kind: XdpEngineKind) -> Self {
        self.engine_kind = kind;
        self
    }

    /// Sets the hardware attachment mode (Skb, Drv, Hw).
    pub fn with_attach_mode(mut self, mode: XdpAttachMode) -> Self {
        self.attach_mode = mode;
        if mode == XdpAttachMode::Drv {
            self.zero_copy = true;
        }
        self
    }

    /// Sets the zero-copy DMA flag.
    pub fn with_zero_copy(mut self, zero_copy: bool) -> Self {
        self.zero_copy = zero_copy;
        self
    }

    /// Sets the UMEM frame count (rounded up to the next power of two).
    pub fn with_frame_count(mut self, count: u32) -> Self {
        self.frame_count = count.next_power_of_two();
        self
    }
}
