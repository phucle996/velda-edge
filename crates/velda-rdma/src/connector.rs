//! # Asynchronous RDMA Connector and Out-of-Band (OOB) QP Handshake
//!
//! ## Architectural Invariants & Subsystem Boundaries (`AGENTS.md` Sec 2.8)
//!
//! In Velda Edge's architecture:
//! - **Upstream Subsystem (`velda-upstream`)**: Owns "WHEN & WHO" — endpoint target selection, load
//!   balancing, passive health monitoring, connection pooling, and lane sharding. It never invokes
//!   raw socket or Verbs syscalls directly.
//! - **RDMA Connector (`RdmaConnector`)**: Owns "HOW" — physical hardware device discovery, Queue Pair
//!   instantiation, Out-of-Band (OOB) bootstrap handshake, and hardware state transitions (`INIT -> RTR -> RTS`).
//!
//! ## The Chicken-and-Egg Bootstrap Mechanism
//! In RDMA Reliable Connected (RC) mode, two HCAs cannot exchange data or state until both local and remote
//! Queue Pairs have transitioned into the Ready to Send (`RTS`) state. However, configuring the hardware QP
//! into `RTR` and `RTS` requires the remote peer's Queue Pair Number (`qpn`), Packet Sequence Number (`psn`),
//! Local Identifier (`lid`), and RoCEv2 Global Identifier (`gid`).
//!
//! Because an uninitialized RDMA link cannot transmit these parameters, `RdmaConnector` performs an Out-of-Band
//! (OOB) TCP handshake:
//! 1. **OOB TCP Connection**: Connects to the upstream target's TCP management port (or dedicated listener).
//! 2. **Framing & Address Exchange**: Transmits the magic header `b"VRDM"` followed by the 28-byte binary
//!    `QpAddress` structure in network byte order.
//! 3. **Hardware State Transition**: Reads the peer's `QpAddress` from the stream, programs the local QP
//!    hardware into `RTR` with the peer's attributes, and finally transitions to `RTS`.
//! 4. **Handoff**: The OOB TCP stream is cleanly closed or detached; the active, verified `RdmaChannel` is
//!    returned to the upstream pool for hot-path leasing.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex as AsyncMutex;

use crate::channel::{HardwareChannel, RdmaChannel, RdmaStream};
use crate::config::RdmaConfig;
use crate::error::RdmaError;
use crate::ffi::ibv_gid;
use crate::mock::MockChannel;
use crate::verbs::cq::CompletionQueue;
use crate::verbs::device::RdmaDevice;
use crate::verbs::pd::ProtectionDomain;
use crate::verbs::qp::{QpAddress, QueuePair};

const OOB_MAGIC: &[u8; 4] = b"VRDM";

/// Narrow connector interface for establishing RDMA transport channels to upstream backends.
pub struct RdmaConnector;

impl RdmaConnector {
    /// Connects to the upstream target, establishes an RDMA Queue Pair, and returns a ready `RdmaChannel`.
    pub async fn connect(
        target: SocketAddr,
        config: &RdmaConfig,
    ) -> Result<RdmaChannel, RdmaError> {
        // Fallback to pure-Rust software mock if requested or if no physical devices exist
        if config.force_mock {
            let (client_mock, _server_mock) = MockChannel::pair(config.max_send_wr as usize);
            return Ok(RdmaChannel::Mock(Arc::new(AsyncMutex::new(client_mock))));
        }

        let devices = match RdmaDevice::list_all() {
            Ok(devs) if !devs.is_empty() => devs,
            _ => {
                tracing::debug!("No physical RDMA devices detected, falling back to mock channel");
                let (client_mock, _server_mock) = MockChannel::pair(config.max_send_wr as usize);
                return Ok(RdmaChannel::Mock(Arc::new(AsyncMutex::new(client_mock))));
            }
        };

        // Select device
        let device = match &config.device_name {
            Some(name) => devices
                .into_iter()
                .find(|d| d.name() == name)
                .ok_or_else(|| RdmaError::DeviceNotFound(name.clone()))?,
            None => devices.into_iter().next().unwrap(),
        };

        let context = device.open()?;
        let pd = ProtectionDomain::new(context.clone())?;
        let cq = CompletionQueue::new(context.clone(), config.cq_size, true)?;

        let qp = QueuePair::new(
            pd.clone(),
            cq.clone(),
            config.max_send_wr,
            config.max_recv_wr,
            config.max_sge,
        )?;

        // Step 1: Transition local QP into INIT state (configures HCA port number and access flags).
        qp.to_init(config.port)?;

        // Step 2: Query local HCA port attributes (LID, RoCEv2 GID) and generate local 24-bit PSN.
        let port_attr = context.query_port(config.port)?;
        let local_gid = context
            .query_gid(config.port, config.gid_index as i32)
            .unwrap_or(ibv_gid { raw: [0; 16] });

        let local_psn = rand_psn();
        let local_addr = QpAddress {
            qpn: qp.qp_num(),
            psn: local_psn,
            lid: port_attr.lid,
            gid: local_gid,
        };

        // Step 3: Out-of-Band (OOB) TCP handshake to exchange binary QpAddress with remote peer.
        // Resolves the chicken-and-egg bootstrap since RDMA cannot communicate prior to RTS.
        let remote_addr = exchange_oob(
            target,
            &local_addr,
            Duration::from_millis(config.timeout_ms),
        )
        .await?;

        // Step 4: Transition local QP from INIT -> RTR (Ready to Receive) with remote peer's attributes.
        qp.to_rtr(&remote_addr, config.port, config.gid_index, None)?;

        // Step 5: Transition local QP from RTR -> RTS (Ready to Send) enabling outbound transmissions.
        qp.to_rts(local_psn, 14, 7, config.rnr_retry)?;

        // Step 6: Construct HardwareChannel, pin contiguous buffer slots, and pre-post initial receive buffers.
        let hw_channel = HardwareChannel::new(qp, cq, pd, config)?;
        Ok(RdmaChannel::Hardware(Arc::new(hw_channel)))
    }

    /// Helper to connect and immediately return an `RdmaStream` compatible with `AsyncRead + AsyncWrite`.
    pub async fn connect_stream(
        target: SocketAddr,
        config: &RdmaConfig,
    ) -> Result<RdmaStream, RdmaError> {
        let channel = Self::connect(target, config).await?;
        Ok(RdmaStream::new(channel))
    }
}

/// Generates a random 24-bit Packet Sequence Number (PSN).
fn rand_psn() -> u32 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    ((now ^ (now >> 24)) & 0x00FF_FFFF) as u32
}

/// Exchanges QpAddress over a short-lived out-of-band TCP socket.
async fn exchange_oob(
    target: SocketAddr,
    local: &QpAddress,
    timeout: Duration,
) -> Result<QpAddress, RdmaError> {
    let handshake_fut = async {
        let mut stream = TcpStream::connect(target).await?;

        // Format 32-byte bootstrap frame:
        // [0..4]: Magic b"VRDM"
        // [4..8]: QPN (be)
        // [8..12]: PSN (be)
        // [12..14]: LID (be)
        // [14..16]: reserved
        // [16..32]: GID (16 bytes)
        let mut tx_buf = [0u8; 32];
        tx_buf[0..4].copy_from_slice(OOB_MAGIC);
        tx_buf[4..8].copy_from_slice(&local.qpn.to_be_bytes());
        tx_buf[8..12].copy_from_slice(&local.psn.to_be_bytes());
        tx_buf[12..14].copy_from_slice(&local.lid.to_be_bytes());
        tx_buf[16..32].copy_from_slice(&local.gid.raw);

        stream.write_all(&tx_buf).await?;
        stream.flush().await?;

        let mut rx_buf = [0u8; 32];
        stream.read_exact(&mut rx_buf).await?;

        if &rx_buf[0..4] != OOB_MAGIC {
            return Err(RdmaError::Disconnected(
                "Invalid OOB handshake magic header from remote peer".into(),
            ));
        }

        let remote_qpn = u32::from_be_bytes(rx_buf[4..8].try_into().unwrap());
        let remote_psn = u32::from_be_bytes(rx_buf[8..12].try_into().unwrap());
        let remote_lid = u16::from_be_bytes(rx_buf[12..14].try_into().unwrap());
        let mut remote_gid = ibv_gid { raw: [0; 16] };
        remote_gid.raw.copy_from_slice(&rx_buf[16..32]);

        Ok(QpAddress {
            qpn: remote_qpn,
            psn: remote_psn,
            lid: remote_lid,
            gid: remote_gid,
        })
    };

    match tokio::time::timeout(timeout, handshake_fut).await {
        Ok(res) => res,
        Err(_) => Err(RdmaError::Timeout("OOB handshake timed out".into())),
    }
}
