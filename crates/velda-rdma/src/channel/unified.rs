//! # Decoupled RDMA Channel, Sender, and Receiver Abstractions
//!
//! Provides the unified `RdmaChannel`, `RdmaSender`, and `RdmaReceiver` representations
//! abstracting over hardware Verbs or pure-Rust mock transports.

use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;

use crate::channel::hardware::{HardwareChannel, HardwareReceiver, HardwareSender};
use crate::channel::stream::RdmaStream;
use crate::error::RdmaError;
use crate::mock::{MockChannel, MockReceiver, MockSender};

/// Decoupled RDMA sender handle, abstracting over hardware or mock transport.
#[derive(Clone)]
pub enum RdmaSender {
    /// Hardware-backed RDMA sender.
    Hardware(Arc<HardwareSender>),
    /// In-memory mock sender.
    Mock(MockSender),
}

impl RdmaSender {
    /// Sends a payload over the RDMA channel.
    pub async fn send(&self, data: &[u8]) -> Result<(), RdmaError> {
        match self {
            Self::Hardware(hw) => hw.send(data).await,
            Self::Mock(mock) => mock.send(Bytes::copy_from_slice(data), None).await,
        }
    }

    /// Sends registered memory directly over the RDMA channel.
    pub async fn send_registered(
        &self,
        addr: u64,
        length: u32,
        lkey: u32,
    ) -> Result<(), RdmaError> {
        match self {
            Self::Hardware(hw) => hw.send_registered(addr, length, lkey).await,
            Self::Mock(mock) => mock.send_registered(addr, length, lkey).await,
        }
    }

    /// Sends directly from an `RdmaSlabLease` zero-copy buffer.
    pub async fn send_lease(
        &self,
        lease: &crate::pool::RdmaSlabLease,
        length: usize,
    ) -> Result<(), RdmaError> {
        match self {
            Self::Hardware(hw) => hw.send_lease(lease, length).await,
            Self::Mock(mock) => mock.send_lease(lease, length).await,
        }
    }

    /// Performs a One-Sided RDMA Write.
    pub async fn rdma_write(
        &self,
        remote_addr: u64,
        rkey: u32,
        data: &[u8],
    ) -> Result<(), RdmaError> {
        match self {
            Self::Hardware(hw) => hw.rdma_write(remote_addr, rkey, data).await,
            Self::Mock(mock) => {
                mock.rdma_write(remote_addr, rkey, Bytes::copy_from_slice(data), None)
                    .await
            }
        }
    }

    /// Sends an explicit EOF shutdown frame to peer (Fixes Scenario 5).
    pub async fn shutdown(&self) -> Result<(), RdmaError> {
        match self {
            Self::Hardware(hw) => hw.shutdown().await,
            Self::Mock(mock) => mock.shutdown().await,
        }
    }

    /// Checks whether the sender's underlying channel is healthy.
    #[inline(always)]
    pub fn is_healthy(&self) -> bool {
        match self {
            Self::Hardware(hw) => hw.is_healthy(),
            Self::Mock(mock) => mock.is_healthy(),
        }
    }

    /// Marks the underlying channel as broken.
    #[inline(always)]
    pub fn mark_broken(&self) {
        match self {
            Self::Hardware(hw) => hw.mark_broken(),
            Self::Mock(mock) => mock.mark_broken(),
        }
    }
}

/// Decoupled RDMA receiver handle, abstracting over hardware or mock transport.
#[derive(Clone)]
pub enum RdmaReceiver {
    /// Hardware-backed RDMA receiver.
    Hardware(Arc<HardwareReceiver>),
    /// In-memory mock receiver.
    Mock(Arc<MockReceiver>),
}

impl RdmaReceiver {
    /// Receives a payload from the RDMA channel.
    pub async fn recv(&self, timeout: Duration) -> Result<Bytes, RdmaError> {
        match self {
            Self::Hardware(hw) => hw.recv(timeout).await,
            Self::Mock(mock) => {
                let msg = mock.recv(timeout).await?;
                Ok(msg.payload)
            }
        }
    }

    /// Checks whether the receiver's underlying channel is healthy.
    #[inline(always)]
    pub fn is_healthy(&self) -> bool {
        match self {
            Self::Hardware(hw) => hw.is_healthy(),
            Self::Mock(mock) => mock.is_healthy(),
        }
    }

    /// Marks the underlying channel as broken.
    #[inline(always)]
    pub fn mark_broken(&self) {
        match self {
            Self::Hardware(hw) => hw.mark_broken(),
            Self::Mock(mock) => mock.mark_broken(),
        }
    }
}

/// Unified RDMA Channel holding decoupled sender and receiver flows.
#[derive(Clone)]
pub struct RdmaChannel {
    pub sender: RdmaSender,
    pub receiver: RdmaReceiver,
}

impl RdmaChannel {
    /// Creates a new `RdmaChannel` from sender and receiver halves.
    pub fn new(sender: RdmaSender, receiver: RdmaReceiver) -> Self {
        Self { sender, receiver }
    }

    /// Creates an `RdmaChannel` wrapping an active hardware channel.
    pub fn from_hardware(hw: HardwareChannel) -> Self {
        let (s, r) = hw.into_split();
        Self {
            sender: RdmaSender::Hardware(s),
            receiver: RdmaReceiver::Hardware(r),
        }
    }

    /// Creates an `RdmaChannel` wrapping an in-memory mock channel.
    pub fn from_mock(mock: MockChannel) -> Self {
        let (s, r) = mock.into_split();
        Self {
            sender: RdmaSender::Mock(s),
            receiver: RdmaReceiver::Mock(r),
        }
    }

    /// Splits the channel into independent sender and receiver handles.
    pub fn split(&self) -> (RdmaSender, RdmaReceiver) {
        (self.sender.clone(), self.receiver.clone())
    }

    /// Consumes the channel, yielding independent sender and receiver handles.
    pub fn into_split(self) -> (RdmaSender, RdmaReceiver) {
        (self.sender, self.receiver)
    }

    /// Converts this channel into a standard Tokio streaming adapter.
    pub fn into_stream(self) -> RdmaStream {
        RdmaStream::new(self)
    }

    /// Checks whether the channel is healthy.
    #[inline(always)]
    pub fn is_healthy(&self) -> bool {
        self.sender.is_healthy() && self.receiver.is_healthy()
    }

    /// Marks the channel as broken.
    #[inline(always)]
    pub fn mark_broken(&self) {
        self.sender.mark_broken();
        self.receiver.mark_broken();
    }
}
