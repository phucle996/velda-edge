//! # RDMA Transport Channels, Stream Adapters, and Flow Control
//!
//! Subsystems:
//! - `hardware`: Real HCA RoCEv2/InfiniBand queue pair channels with decoupled send/recv handles.
//! - `channel`: Decoupled `RdmaSender`, `RdmaReceiver`, and unified `RdmaChannel` abstractions.
//! - `flow_control`: Lock-free atomic credit-based flow control preventing hardware RNR drops.
//! - `poller`: Adaptive spin-poll / event-driven hybrid Completion Queue harvesting.
//! - `stream`: Tokio `AsyncRead` and `AsyncWrite` stream adapter with decoupled split halves.
//! - `wr`: Work Request builders for Send, Receive, and RDMA Write operations.

pub mod flow_control;
pub mod hardware;
pub mod poller;
pub mod stream;
pub mod unified;
pub mod wr;

pub use flow_control::CreditFlowControl;
pub use hardware::{
    DEFAULT_CHANNEL_SLOT_SIZE, HardwareChannel, HardwareChannelShared, HardwareReceiver,
    HardwareSender, RDMA_CREDIT_MASK, RDMA_FLAG_EOF, WR_ID_SEND,
};
pub use poller::AdaptiveCqPoller;
pub use stream::{DEFAULT_RDMA_STREAM_MAX_CHUNK, RdmaReadHalf, RdmaStream, RdmaWriteHalf};
pub use unified::{RdmaChannel, RdmaReceiver, RdmaSender};
