//! Non-Blocking UDP Transport for StatsD / DogStatsD Telemetry.
//!
//! Manages the client UDP socket acquired through [`velda_core::global_socket_manager()`],
//! executes kernel fast-path `connect()`, and transmits MTU-bounded metric datagrams.
//!
//! # Architecture & Kernel Fast-Path
//! Calling `connect()` on a UDP socket associates it with the remote peer address in the kernel.
//! Subsequent writes bypass per-packet routing table lookups and ARP cache validation,
//! reducing transmission overhead to a single direct `send()` syscall.

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;

use crate::error::PluginError;

/// Active UDP datagram transport channel.
pub struct StatsdTransport {
    socket: Arc<UdpSocket>,
    destination: SocketAddr,
    plugin_id: String,
}

impl StatsdTransport {
    /// Connects to target StatsD daemon endpoint using a socket allocated by Core SocketManager.
    pub async fn connect(
        endpoint: &str,
        plugin_id: impl Into<String>,
    ) -> Result<Self, PluginError> {
        let plugin_id = plugin_id.into();

        // 1. Resolve destination address
        let destination: SocketAddr = match endpoint.parse() {
            Ok(addr) => addr,
            Err(_) => {
                let addrs: Vec<SocketAddr> = tokio::net::lookup_host(endpoint)
                    .await
                    .map_err(|e| {
                        PluginError::Config(format!("Failed to resolve endpoint '{endpoint}': {e}"))
                    })?
                    .collect();
                *addrs.first().ok_or_else(|| {
                    PluginError::Config(format!(
                        "DNS resolution returned 0 addresses for '{endpoint}'"
                    ))
                })?
            }
        };

        // 2. Select bind domain based on destination family (IPv4 vs IPv6 ephemeral port 0)
        let bind_addr: SocketAddr = if destination.is_ipv6() {
            "[::]:0".parse().unwrap()
        } else {
            "0.0.0.0:0".parse().unwrap()
        };

        // 3. Acquire UDP socket through Core SocketManager (Single Source of Truth)
        let std_socket = velda_core::global_socket_manager()
            .acquire_shadow_udp(bind_addr, &plugin_id)
            .map_err(|e| {
                PluginError::Config(format!("Failed to acquire shadow UDP socket: {e}"))
            })?;

        std_socket.set_nonblocking(true).map_err(|e| {
            PluginError::Config(format!("Failed to set nonblocking on UDP socket: {e}"))
        })?;

        let socket = UdpSocket::from_std(std_socket).map_err(|e| {
            PluginError::Config(format!(
                "Failed to convert std UdpSocket to tokio UdpSocket: {e}"
            ))
        })?;

        // 4. Kernel fast-path connect: binds destination address in kernel socket table
        socket.connect(destination).await?;

        Ok(Self {
            socket: Arc::new(socket),
            destination,
            plugin_id,
        })
    }

    /// Transmits a pre-formatted, MTU-safe datagram to the connected StatsD endpoint.
    #[inline(always)]
    pub async fn send_packet(&self, packet: &[u8]) -> Result<usize, std::io::Error> {
        self.socket.send(packet).await
    }

    /// Transmits multiple MTU-safe datagrams in a single vectorized `sendmmsg` syscall.
    ///
    /// On Linux, this invokes `libc::sendmmsg` to batch all datagrams into a single
    /// kernel transition (Ring 3 -> Ring 0), bypassing per-packet syscall overhead.
    /// On non-Linux platforms, falls back to a sequential transmission loop.
    pub async fn send_batch(&self, packets: &[impl AsRef<[u8]>]) -> Result<usize, std::io::Error> {
        if packets.is_empty() {
            return Ok(0);
        }

        #[cfg(target_os = "linux")]
        {
            self.send_batch_linux(packets).await
        }

        #[cfg(not(target_os = "linux"))]
        {
            let mut sent = 0;
            for packet in packets {
                self.send_packet(packet.as_ref()).await?;
                sent += 1;
            }
            Ok(sent)
        }
    }

    #[cfg(target_os = "linux")]
    async fn send_batch_linux(
        &self,
        packets: &[impl AsRef<[u8]>],
    ) -> Result<usize, std::io::Error> {
        use std::os::fd::AsRawFd;

        let fd = self.socket.as_raw_fd();
        let total = packets.len();
        let mut total_sent = 0;

        while total_sent < total {
            self.socket.writable().await?;

            let chunk = &packets[total_sent..];
            match try_sendmmsg_chunk(fd, chunk) {
                Ok(0) => {
                    tokio::task::yield_now().await;
                }
                Ok(sent) => {
                    total_sent += sent;
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    tokio::task::yield_now().await;
                }
                Err(err) => return Err(err),
            }
        }

        Ok(total_sent)
    }

    /// Returns the resolved destination socket address.
    #[inline(always)]
    pub fn destination(&self) -> SocketAddr {
        self.destination
    }

    /// Releases socket registration from the Core SocketManager.
    pub fn release(&self) {
        velda_core::global_socket_manager().release_shadow_udp(&self.plugin_id);
    }
}

#[cfg(target_os = "linux")]
#[inline]
fn try_sendmmsg_chunk(
    fd: std::os::fd::RawFd,
    chunk: &[impl AsRef<[u8]>],
) -> Result<usize, std::io::Error> {
    let chunk_len = chunk.len().min(64);
    let mut iovecs: [libc::iovec; 64] = unsafe { std::mem::zeroed() };
    for (i, p) in chunk[..chunk_len].iter().enumerate() {
        let slice = p.as_ref();
        iovecs[i].iov_base = slice.as_ptr() as *mut libc::c_void;
        iovecs[i].iov_len = slice.len();
    }

    let mut msgs: [libc::mmsghdr; 64] = unsafe { std::mem::zeroed() };
    for i in 0..chunk_len {
        // Connected UDP socket has destination cached in kernel control block -> msg_name is null
        msgs[i].msg_hdr.msg_name = std::ptr::null_mut();
        msgs[i].msg_hdr.msg_namelen = 0;
        msgs[i].msg_hdr.msg_iov = &mut iovecs[i] as *mut libc::iovec;
        msgs[i].msg_hdr.msg_iovlen = 1;
        msgs[i].msg_hdr.msg_control = std::ptr::null_mut();
        msgs[i].msg_hdr.msg_controllen = 0;
        msgs[i].msg_hdr.msg_flags = 0;
        msgs[i].msg_len = 0;
    }

    let res = unsafe {
        libc::sendmmsg(
            fd,
            msgs.as_mut_ptr(),
            chunk_len as libc::c_uint,
            libc::MSG_DONTWAIT,
        )
    };

    if res >= 0 {
        Ok(res as usize)
    } else {
        Err(std::io::Error::last_os_error())
    }
}

impl Drop for StatsdTransport {
    fn drop(&mut self) {
        self.release();
    }
}
