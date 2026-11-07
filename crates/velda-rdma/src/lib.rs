//! # Velda RDMA — Kernel-Bypass Transport Engine
//!
//! Provides an ultra-low latency, zero-copy, kernel-bypass remote direct memory access
//! (RoCEv2 & InfiniBand) transport engine for the Velda Edge Data Plane.
//!
//! ### Hardware Kernel-Bypass Mechanism
//! 1. **Direct Memory Access (DMA)**: Data is transferred directly between host user-space RAM
//!    and backend memory by the RDMA Host Channel Adapter (HCA / rNIC) over RoCEv2 or InfiniBand fabric.
//!    Completely bypasses the Linux kernel network stack: zero syscalls, zero kernel socket buffers (`sk_buff`),
//!    and zero CPU data copy overhead (`memcpy`).
//! 2. **Protection Domain & Memory Registration**: Pre-allocated memory slabs (`PayloadSlab`) are pinned
//!    into physical RAM (`ibv_reg_mr`) and mapped into the NIC's onboard Memory Translation Table (MTT)
//!    with hardware access keys (`lkey`/`rkey`), enabling wire-speed DMA validation.
//! 3. **Queue Pair & Credit-Based Flow Control**: Bidirectional communication executes over Reliable Connected
//!    Queue Pairs (RC QP). Atomic credit tracking strictly prevents Receiver Not Ready (RNR) hardware drops.
//! 4. **Adaptive Hybrid Polling**: Direct fast-path busy polling during active traffic bursts
//!    coupled with slow-path hardware MSI-X interrupt sleep (`tokio::io::unix::AsyncFd` via `ibv_comp_channel`)
//!    during idle windows, eliminating idle CPU burn.
//! 5. **Decoupled Send & Receive Flows ("Minh bạch và tách biệt 2 luồng")**: Transparent separation of
//!    Send (`RdmaSender`, `HardwareSender`, `RdmaWriteHalf`) and Receive (`RdmaReceiver`, `HardwareReceiver`, `RdmaReadHalf`)
//!    flows with zero mutex contention and independent half-split capabilities (`into_split`).
//!
//! ### Subsystem Boundary (`AGENTS.md` Section 2.8)
//! - `velda-upstream`: Owns connection lifecycle, sharded pool state, and endpoint selection ("WHEN & WHO").
//! - `velda-rdma`: Owns verbs, queue pairs, memory registration, and wire protocol mechanics ("HOW").
//!
//! ### Deterministic Thread Topology (`AGENTS.md` Section 2.9)
//! - **Frontline Traffic Worker (`velda-worker-{id}`)**: Exclusively serves hot-path traffic by leasing
//!   pre-warmed Queue Pairs from its core-local lane shard without cross-thread coordination. Never spawns threads or blocks on the hot path.
//! - **Shadow Logistics Worker (`velda-shadow-{id}`)**: Autonomous background companion thread pinned to the
//!   same CPU core running under `SCHED_IDLE`, exclusively responsible for Out-of-Band (OOB) handshakes,
//!   hardware QP initialization, and replenishing warm connections into the lane shard.

pub mod channel;
pub mod config;
pub mod connector;
pub mod error;
pub mod ffi;
pub mod mock;
pub mod pool;
pub mod verbs;

pub use channel::{
    DEFAULT_CHANNEL_SLOT_SIZE, HardwareChannel, RDMA_CREDIT_MASK, RDMA_FLAG_EOF, RdmaChannel,
    RdmaReceiver, RdmaSender, RdmaStream,
};
pub use config::{
    BASELINE_RDMA_CREDITS, DEFAULT_RDMA_CREDITS, DEFAULT_RDMA_SLOT_SIZE,
    HIGH_THROUGHPUT_RDMA_CREDITS, PEAK_RDMA_CREDITS, RdmaConfig,
};
pub use connector::{RdmaConnector, RdmaListener};
pub use error::RdmaError;
pub use mock::{MockChannel, MockMessage};
pub use pool::{RdmaSlabArena, RdmaSlabClass, RdmaSlabConfig, RdmaSlabLease};
pub use verbs::{
    CompletionQueue, MemoryRegion, ProtectionDomain, QpAddress, QueuePair, RdmaContext, RdmaDevice,
};
