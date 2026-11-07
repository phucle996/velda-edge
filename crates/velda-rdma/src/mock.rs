//! # Pure-Rust Software Mock for RDMA Queue Pairs and Streams
//!
//! ## Architectural Invariants & Test Portability
//!
//! Production edge environments running Velda Edge utilize Mellanox ConnectX or compatible RoCEv2/InfiniBand
//! Host Channel Adapters (HCAs) driven by `libibverbs.so`. However, local developer workstations, standard
//! cloud continuous integration (CI) runners, and containerized test harnesses rarely have access to physical
//! RDMA hardware or kernel InfiniBand modules (`ib_uverbs`, `rdma_rxe`).
//!
//! To ensure deterministic quality verification (`cargo test --workspace`) without hardware dependencies:
//! - `MockChannel` provides an in-memory simulation of a connected RDMA Queue Pair.
//! - Uses bounded Tokio `mpsc` channels to simulate Send and Receive Work Request ring buffers.
//! - Simulates immediate data payloads (`imm_data`), work request IDs (`wr_id`), and atomic credit accounting.
//! - Accurately replicates hardware flow-control backpressure and credit starvation errors.
//! - Explicitly decouples the Send path (`MockSender`) and Receive path (`MockReceiver`), eliminating
//!   head-of-line blocking between concurrent transmissions and receptions.

use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex as AsyncMutex, mpsc};

use crate::error::RdmaError;
use crate::verbs::qp::QpAddress;

/// Mock Work Request ID for standard Send operations.
const MOCK_WR_ID_SEND: u64 = 1;
/// Mock Work Request ID for One-Sided RDMA Write operations.
const MOCK_WR_ID_WRITE: u64 = 2;
/// Mock baseline QP numbers.
const MOCK_BASE_QPN_1: u32 = 1001;
const MOCK_BASE_QPN_2: u32 = 1002;
/// Mock baseline packet sequence numbers.
const MOCK_BASE_PSN_1: u32 = 100;
const MOCK_BASE_PSN_2: u32 = 200;

/// Simulated RDMA message transmitted over memory channels.
#[derive(Debug, Clone)]
pub struct MockMessage {
    pub wr_id: u64,
    pub payload: Bytes,
    pub imm_data: Option<u32>,
    pub remote_addr: Option<u64>,
    pub rkey: Option<u32>,
}

/// Simulated RDMA send flow handle.
#[derive(Clone)]
pub struct MockSender {
    tx: mpsc::Sender<MockMessage>,
    pub send_credits: Arc<AtomicU32>,
    pub(crate) healthy: Arc<AtomicBool>,
}

impl MockSender {
    /// Checks whether the mock channel is healthy.
    #[inline(always)]
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    /// Marks the mock channel as broken.
    #[inline(always)]
    pub fn mark_broken(&self) {
        self.healthy.store(false, Ordering::Release);
    }

    /// Simulates posting an RDMA Send Work Request.
    pub async fn send(&self, payload: Bytes, imm_data: Option<u32>) -> Result<(), RdmaError> {
        let mut current = self.send_credits.load(Ordering::Acquire);
        loop {
            if current == 0 {
                return Err(RdmaError::InsufficientCredits {
                    available: 0,
                    required: 1,
                });
            }
            match self.send_credits.compare_exchange_weak(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }

        let msg = MockMessage {
            wr_id: MOCK_WR_ID_SEND,
            payload,
            imm_data,
            remote_addr: None,
            rkey: None,
        };

        self.tx
            .send(msg)
            .await
            .map_err(|_| RdmaError::Disconnected("Mock peer disconnected".into()))
    }

    /// Sends an explicit EOF shutdown frame to mock receiver (Fixes Scenario 5).
    pub async fn shutdown(&self) -> Result<(), RdmaError> {
        let msg = MockMessage {
            wr_id: MOCK_WR_ID_SEND,
            payload: Bytes::new(),
            imm_data: Some(crate::channel::hardware::RDMA_FLAG_EOF),
            remote_addr: None,
            rkey: None,
        };
        let _ = self.tx.send(msg).await;
        Ok(())
    }

    /// Simulates posting an RDMA Send Work Request referencing a pre-registered memory address.
    pub async fn send_registered(
        &self,
        addr: u64,
        length: u32,
        _lkey: u32,
    ) -> Result<(), RdmaError> {
        let slice = unsafe { std::slice::from_raw_parts(addr as *const u8, length as usize) };
        self.send(Bytes::copy_from_slice(slice), None).await
    }

    /// Simulates posting an RDMA Send Work Request directly from an `RdmaSlabLease`.
    pub async fn send_lease(
        &self,
        lease: &crate::pool::RdmaSlabLease,
        length: usize,
    ) -> Result<(), RdmaError> {
        if length > lease.capacity() {
            return Err(RdmaError::BufferTooSmall {
                capacity: lease.capacity(),
                needed: length,
            });
        }
        let payload = Bytes::copy_from_slice(&lease[..length]);
        self.send(payload, None).await
    }

    /// Simulates a One-Sided RDMA Write.
    pub async fn rdma_write(
        &self,
        remote_addr: u64,
        rkey: u32,
        payload: Bytes,
        imm_data: Option<u32>,
    ) -> Result<(), RdmaError> {
        let msg = MockMessage {
            wr_id: MOCK_WR_ID_WRITE,
            payload,
            imm_data,
            remote_addr: Some(remote_addr),
            rkey: Some(rkey),
        };

        self.tx
            .send(msg)
            .await
            .map_err(|_| RdmaError::Disconnected("Mock peer disconnected".into()))
    }
}

/// Simulated RDMA receive flow handle.
pub struct MockReceiver {
    rx: AsyncMutex<mpsc::Receiver<MockMessage>>,
    remote_credits: Arc<AtomicU32>,
    pub(crate) healthy: Arc<AtomicBool>,
}

impl MockReceiver {
    /// Checks whether the mock channel is healthy.
    #[inline(always)]
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    /// Marks the mock channel as broken.
    #[inline(always)]
    pub fn mark_broken(&self) {
        self.healthy.store(false, Ordering::Release);
    }

    /// Simulates receiving an RDMA Send Work Request.
    pub async fn recv(&self, timeout: Duration) -> Result<MockMessage, RdmaError> {
        let mut rx = self.rx.lock().await;
        match tokio::time::timeout(timeout, rx.recv()).await {
            Ok(Some(msg)) => {
                // Grant send credit back to peer
                self.remote_credits.fetch_add(1, Ordering::Release);
                Ok(msg)
            }
            Ok(None) => Err(RdmaError::Disconnected("Mock peer closed channel".into())),
            Err(_) => Err(RdmaError::Timeout("Mock recv timed out".into())),
        }
    }
}

/// In-memory mock representing a connected RDMA Queue Pair.
#[derive(Clone)]
pub struct MockChannel {
    pub sender: MockSender,
    pub receiver: Arc<MockReceiver>,
    /// Local Queue Pair address projection.
    pub qp_address: QpAddress,
    /// Remote peer Queue Pair address projection.
    pub remote_address: QpAddress,
}

impl MockChannel {
    /// Creates a bidirectional pair of simulated RDMA channels connected to each other.
    pub fn pair(capacity: usize) -> (Self, Self) {
        let (tx1, rx1) = mpsc::channel(capacity);
        let (tx2, rx2) = mpsc::channel(capacity);

        let credits1 = Arc::new(AtomicU32::new(capacity as u32));
        let credits2 = Arc::new(AtomicU32::new(capacity as u32));

        let healthy1 = Arc::new(AtomicBool::new(true));
        let healthy2 = Arc::new(AtomicBool::new(true));

        let addr1 = QpAddress {
            qpn: MOCK_BASE_QPN_1,
            psn: MOCK_BASE_PSN_1,
            lid: 1,
            gid: crate::ffi::ibv_gid { raw: [0; 16] },
        };

        let addr2 = QpAddress {
            qpn: MOCK_BASE_QPN_2,
            psn: MOCK_BASE_PSN_2,
            lid: 2,
            gid: crate::ffi::ibv_gid { raw: [1; 16] },
        };

        let channel1 = Self {
            sender: MockSender {
                tx: tx1,
                send_credits: credits1.clone(),
                healthy: healthy1.clone(),
            },
            receiver: Arc::new(MockReceiver {
                rx: AsyncMutex::new(rx2),
                remote_credits: credits2.clone(),
                healthy: healthy1,
            }),
            qp_address: addr1,
            remote_address: addr2,
        };

        let channel2 = Self {
            sender: MockSender {
                tx: tx2,
                send_credits: credits2,
                healthy: healthy2.clone(),
            },
            receiver: Arc::new(MockReceiver {
                rx: AsyncMutex::new(rx1),
                remote_credits: credits1,
                healthy: healthy2,
            }),
            qp_address: addr2,
            remote_address: addr1,
        };

        (channel1, channel2)
    }

    /// Splits the channel into decoupled sender and receiver handles.
    pub fn split(&self) -> (MockSender, Arc<MockReceiver>) {
        (self.sender.clone(), self.receiver.clone())
    }

    /// Consumes the channel, yielding decoupled sender and receiver handles.
    pub fn into_split(self) -> (MockSender, Arc<MockReceiver>) {
        (self.sender, self.receiver)
    }

    /// Checks whether the mock channel is healthy.
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
