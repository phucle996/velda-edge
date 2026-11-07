//! # Decoupled Read & Write Channels for XDP
//!
//! Provides transparent separation of Inbound/Receive flows ([`XdpReceiver`], [`XdpReadHalf`])
//! and Outbound/Send flows ([`XdpSender`], [`XdpWriteHalf`]) with zero mutex contention
//! and independent half-split capabilities ([`XdpStream::into_split`]), symmetric to `velda-rdma`.

pub mod receiver;
pub mod sender;
pub mod stream;

pub use receiver::{XdpReadHalf, XdpReceiver};
pub use sender::{XdpSender, XdpWriteHalf};
pub use stream::XdpStream;
