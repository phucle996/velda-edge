//! Direct raw integer socket connection helpers (`(ip, port_be, is_v6)` / `&Endpoint`).
//!
//! Populates `libc::sockaddr_in` / `libc::sockaddr_in6` directly from `Endpoint`
//! without round-tripping through `std::net::SocketAddr`.

use std::io;
use std::net::SocketAddr;
use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd};
use std::sync::OnceLock;
use std::time::Duration;

use tokio::net::{TcpStream, UdpSocket};
use velda_core::Endpoint;
use velda_core::hardware::{HardwareTopology, global_hardware_topology};
use velda_core::lut::ip::{is_v4_u128, v4_to_u128, v4itoaddr, v6itoaddr};

/// Pre-computed outbound socket tuning cached once from [`HardwareTopology`] (`OnceLock`).
#[derive(Debug, Clone, Copy)]
pub struct OutboundSocketTuning {
    pub tcp_fastopen_connect: bool,
    pub ip_bind_address_no_port: bool,
    pub tcp_syncnt: u32,
    pub tcp_notsent_lowat: Option<u32>,
    pub tcp_user_timeout_ms: Option<u32>,
    pub tcp_keepalive_tuning: bool,
    pub tcp_quickack: bool,
    pub bbr: bool,
    pub busy_poll_us: Option<u32>,
    pub prefer_busy_poll: bool,
    pub tx_rehash: bool,
    pub rto_min_us: Option<u32>,
    pub delack_max_us: Option<u32>,
    pub tcp_buf_bytes: Option<usize>,
    pub udp_gro: bool,
    pub udp_gso: bool,
    pub udp_rxq_ovfl: bool,
    pub udp_buf_bytes: usize,
}

impl OutboundSocketTuning {
    /// Derives outbound TCP/UDP kernel tuning parameters from probed [`HardwareTopology`].
    pub fn from_topology(topo: &HardwareTopology) -> Self {
        let ram_gb = topo.memory_bytes / (1024 * 1024 * 1024);
        let k = &topo.kernel;
        let multi_core = topo.available_cores >= 8;

        let tcp_notsent_lowat = if k.supports_tcp_notsent_lowat() {
            Some(((ram_gb * 16 * 1024).clamp(16 * 1024, 128 * 1024)) as u32)
        } else {
            None
        };

        let raw_udp_buf = (ram_gb * 2 * 1024 * 1024).clamp(1024 * 1024, 32 * 1024 * 1024);
        let udp_buf_bytes = match (k.rmem_max, k.wmem_max) {
            (Some(r), Some(w)) => raw_udp_buf.min(r.min(w) as usize).max(65536),
            (Some(r), None) => raw_udp_buf.min(r as usize).max(65536),
            (None, Some(w)) => raw_udp_buf.min(w as usize).max(65536),
            (None, None) => raw_udp_buf,
        };

        let tcp_buf_bytes = match (k.rmem_max, k.wmem_max) {
            (Some(r), Some(w)) => {
                let target = (ram_gb * 256 * 1024).clamp(128 * 1024, 4 * 1024 * 1024);
                Some(target.min(r.min(w) as usize))
            }
            _ => None,
        };

        Self {
            tcp_fastopen_connect: k.supports_tcp_fastopen_connect(),
            ip_bind_address_no_port: k.supports_ip_bind_address_no_port(),
            tcp_syncnt: if k.supports_tcp_syncnt() { 3 } else { 0 },
            tcp_notsent_lowat,
            tcp_user_timeout_ms: if k.supports_tcp_user_timeout() {
                Some(30_000)
            } else {
                None
            },
            tcp_keepalive_tuning: k.supports_tcp_keepalive_tuning(),
            tcp_quickack: k.supports_quickack(),
            bbr: k.supports_bbr(),
            busy_poll_us: if k.supports_busy_poll() && multi_core {
                Some(50)
            } else {
                None
            },
            prefer_busy_poll: k.supports_so_prefer_busy_poll() && multi_core,
            tx_rehash: k.supports_so_txrehash(),
            rto_min_us: if k.supports_tcp_microsecond_rto() {
                Some(5_000)
            } else {
                None
            },
            delack_max_us: if k.supports_tcp_microsecond_rto() {
                Some(2_000)
            } else {
                None
            },
            tcp_buf_bytes,
            udp_gro: k.supports_udp_gro(),
            udp_gso: k.supports_udp_segment(),
            udp_rxq_ovfl: k.supports_so_rxq_ovfl(),
            udp_buf_bytes,
        }
    }
}

static OUTBOUND_TUNING: OnceLock<OutboundSocketTuning> = OnceLock::new();

/// Returns the cached process-wide outbound socket tuning derived from [`global_hardware_topology`].
#[inline]
pub fn global_outbound_tuning() -> &'static OutboundSocketTuning {
    OUTBOUND_TUNING.get_or_init(|| OutboundSocketTuning::from_topology(global_hardware_topology()))
}

/// Converts a `SocketAddr` into raw `(ip: u128, port_be: u16)` integers.
#[inline(always)]
pub const fn socket_addr_to_raw(addr: SocketAddr) -> (u128, u16) {
    match addr {
        SocketAddr::V4(v4) => (v4_to_u128(v4.ip().to_bits()), v4.port().to_be()),
        SocketAddr::V6(v6) => (v6.ip().to_bits(), v6.port().to_be()),
    }
}

/// Translates raw `(ip: u128, port_be: u16)` into `SocketAddr` on stack/registers at OS boundaries.
#[inline(always)]
pub const fn raw_to_socket_addr(ip: u128, port_be: u16) -> SocketAddr {
    if is_v4_u128(ip) {
        SocketAddr::V4(v4itoaddr(ip, port_be))
    } else {
        SocketAddr::V6(v6itoaddr(ip, port_be))
    }
}

/// Extracts raw `(ip: u128, port_be: u16)` directly from a `libc::sockaddr_storage`.
#[inline(always)]
pub fn sockaddr_storage_to_raw(sa: &libc::sockaddr_storage) -> (u128, u16) {
    match sa.ss_family as libc::c_int {
        libc::AF_INET => {
            let sin = unsafe { &*(sa as *const _ as *const libc::sockaddr_in) };
            (v4_to_u128(u32::from_be(sin.sin_addr.s_addr)), sin.sin_port)
        }
        libc::AF_INET6 => {
            let sin6 = unsafe { &*(sa as *const _ as *const libc::sockaddr_in6) };
            (u128::from_be_bytes(sin6.sin6_addr.s6_addr), sin6.sin6_port)
        }
        _ => (0, 0),
    }
}

/// Formats `(ip: u128, port_be: u16)` into a human-readable string using `velda_core::lut::ip`.
#[inline]
pub fn format_raw_addr(ip: u128, port_be: u16) -> String {
    let (buf, len) = velda_core::lut::ip::format_ip_bytes(ip);
    let ip_str = std::str::from_utf8(&buf[..len]).unwrap_or("invalid-ip");
    let port = u16::from_be(port_be);
    if is_v4_u128(ip) {
        format!("{ip_str}:{port}")
    } else {
        format!("[{ip_str}]:{port}")
    }
}

/// Builds a `libc::sockaddr_storage` and its `socklen_t` directly from `(ip: u128, port_be: u16)`.
#[inline]
pub fn raw_to_sockaddr_storage_u128(
    ip: u128,
    port_be: u16,
) -> (libc::sockaddr_storage, libc::socklen_t) {
    let is_v6 = if is_v4_u128(ip) { 0 } else { 1 };
    raw_to_sockaddr_storage(ip, port_be, is_v6)
}

/// Builds a `libc::sockaddr_storage` and its `socklen_t` directly from `(ip, port_be, is_v6)`.
///
/// `port_be` is already in network byte order (big-endian).
#[inline]
pub fn raw_to_sockaddr_storage(
    ip: u128,
    port_be: u16,
    is_v6: u8,
) -> (libc::sockaddr_storage, libc::socklen_t) {
    // SAFETY: `sockaddr_storage` is a C POD struct; zero-initialization is valid.
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    if is_v6 == 0 {
        let sin = unsafe { &mut *(&mut storage as *mut _ as *mut libc::sockaddr_in) };
        sin.sin_family = libc::AF_INET as libc::sa_family_t;
        sin.sin_port = port_be;
        sin.sin_addr = libc::in_addr {
            s_addr: ((ip & 0xFFFF_FFFF) as u32).to_be(),
        };
        (
            storage,
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
        )
    } else {
        let sin6 = unsafe { &mut *(&mut storage as *mut _ as *mut libc::sockaddr_in6) };
        sin6.sin6_family = libc::AF_INET6 as libc::sa_family_t;
        sin6.sin6_port = port_be;
        sin6.sin6_addr = libc::in6_addr {
            s6_addr: ip.to_be_bytes(),
        };
        (
            storage,
            std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t,
        )
    }
}

/// Builds a `socket2::SockAddr` directly from `(ip, port_be, is_v6)`.
#[inline]
pub fn raw_to_socket2_addr(ip: u128, port_be: u16, is_v6: u8) -> socket2::SockAddr {
    let (storage, len) = raw_to_sockaddr_storage(ip, port_be, is_v6);
    // SAFETY: `raw_to_sockaddr_storage` initializes `storage` with valid `AF_INET` or `AF_INET6`
    // family and matching `len`.
    unsafe {
        socket2::SockAddr::try_init(|addr_ptr, len_ptr| {
            std::ptr::copy_nonoverlapping(
                &storage as *const _ as *const u8,
                addr_ptr as *mut u8,
                len as usize,
            );
            *len_ptr = len;
            Ok(())
        })
        .expect("raw_to_sockaddr_storage always produces a valid SockAddr")
        .1
    }
}

#[cfg(target_os = "linux")]
fn apply_tcp_pre_connect(fd: libc::c_int, is_v6: u8, t: &OutboundSocketTuning) {
    unsafe {
        if is_v6 == 0 && t.ip_bind_address_no_port {
            let val: libc::c_int = 1;
            let _ = libc::setsockopt(
                fd,
                libc::IPPROTO_IP,
                libc::IP_BIND_ADDRESS_NO_PORT,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
        if t.tcp_fastopen_connect {
            let val: libc::c_int = 1;
            let _ = libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_FASTOPEN_CONNECT,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
        if t.tcp_syncnt > 0 {
            let val = t.tcp_syncnt as libc::c_int;
            let _ = libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_SYNCNT,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
        if let Some(buf) = t.tcp_buf_bytes {
            let val = buf as libc::c_int;
            let _ = libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
            let _ = libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
        if let Some(us) = t.busy_poll_us {
            let val = us as libc::c_int;
            let _ = libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_BUSY_POLL,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
        if t.prefer_busy_poll {
            const SO_PREFER_BUSY_POLL: libc::c_int = 69;
            const SO_BUSY_POLL_BUDGET: libc::c_int = 70;
            let val: libc::c_int = 1;
            let _ = libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                SO_PREFER_BUSY_POLL,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
            let budget: libc::c_int = 64;
            let _ = libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                SO_BUSY_POLL_BUDGET,
                &budget as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
        if t.tx_rehash {
            const SO_TXREHASH: libc::c_int = 74;
            let val: libc::c_int = 1;
            let _ = libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                SO_TXREHASH,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
}

#[cfg(target_os = "linux")]
fn apply_tcp_post_connect(fd: libc::c_int, t: &OutboundSocketTuning) {
    unsafe {
        let one: libc::c_int = 1;
        let _ = libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            &one as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
        if t.bbr {
            let bbr = b"bbr";
            let _ = libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_CONGESTION,
                bbr.as_ptr() as *const libc::c_void,
                bbr.len() as libc::socklen_t,
            );
        }
        if let Some(lowat) = t.tcp_notsent_lowat {
            let val = lowat as libc::c_int;
            let _ = libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_NOTSENT_LOWAT,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
        if t.tcp_quickack {
            let _ = libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_QUICKACK,
                &one as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
        if let Some(timeout_ms) = t.tcp_user_timeout_ms {
            let val = timeout_ms as libc::c_int;
            let _ = libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_USER_TIMEOUT,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
        if t.tcp_keepalive_tuning {
            let _ = libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_KEEPALIVE,
                &one as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
            let idle: libc::c_int = 60;
            let intvl: libc::c_int = 10;
            let cnt: libc::c_int = 3;
            let _ = libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_KEEPIDLE,
                &idle as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
            let _ = libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_KEEPINTVL,
                &intvl as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
            let _ = libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_KEEPCNT,
                &cnt as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
        if let Some(rto_us) = t.rto_min_us {
            const TCP_RTO_MIN_US: libc::c_int = 45;
            let val = rto_us as libc::c_int;
            let _ = libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                TCP_RTO_MIN_US,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
        if let Some(delack_us) = t.delack_max_us {
            const TCP_DELACK_MAX_US: libc::c_int = 46;
            let val = delack_us as libc::c_int;
            let _ = libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                TCP_DELACK_MAX_US,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
}

#[cfg(target_os = "linux")]
fn apply_udp_tuning(fd: libc::c_int, t: &OutboundSocketTuning) {
    unsafe {
        let buf = t.udp_buf_bytes as libc::c_int;
        let _ = libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &buf as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
        let _ = libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_SNDBUF,
            &buf as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
        let one: libc::c_int = 1;
        if t.udp_rxq_ovfl {
            let _ = libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_RXQ_OVFL,
                &one as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
        if t.udp_gro {
            const UDP_GRO: libc::c_int = 104;
            let _ = libc::setsockopt(
                fd,
                libc::IPPROTO_UDP,
                UDP_GRO,
                &one as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
        if t.udp_gso {
            const UDP_SEGMENT: libc::c_int = 103;
            let seg: libc::c_int = 1200;
            let _ = libc::setsockopt(
                fd,
                libc::IPPROTO_UDP,
                UDP_SEGMENT,
                &seg as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
        if let Some(us) = t.busy_poll_us {
            let val = us as libc::c_int;
            let _ = libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_BUSY_POLL,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
        if t.prefer_busy_poll {
            const SO_PREFER_BUSY_POLL: libc::c_int = 69;
            const SO_BUSY_POLL_BUDGET: libc::c_int = 70;
            let _ = libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                SO_PREFER_BUSY_POLL,
                &one as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
            let budget: libc::c_int = 64;
            let _ = libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                SO_BUSY_POLL_BUDGET,
                &budget as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
}

/// Connects a non-blocking, kernel-tuned `tokio::net::TcpStream` directly from `(ip: u128, port_be: u16)`.
#[inline]
pub async fn connect_tcp_u128(ip: u128, port_be: u16, timeout: Duration) -> io::Result<TcpStream> {
    let is_v6 = if is_v4_u128(ip) { 0 } else { 1 };
    connect_tcp_raw(ip, port_be, is_v6, timeout).await
}

/// Connects a non-blocking, kernel-tuned `tokio::net::TcpStream` directly from raw `(ip, port_be, is_v6)`.
pub async fn connect_tcp_raw(
    ip: u128,
    port_be: u16,
    is_v6: u8,
    timeout: Duration,
) -> io::Result<TcpStream> {
    let domain = if is_v6 == 0 {
        socket2::Domain::IPV4
    } else {
        socket2::Domain::IPV6
    };
    let socket = socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;
    socket.set_nonblocking(true)?;

    let tuning = global_outbound_tuning();
    #[cfg(target_os = "linux")]
    apply_tcp_pre_connect(socket.as_raw_fd(), is_v6, tuning);

    let (storage, len) = raw_to_sockaddr_storage(ip, port_be, is_v6);
    let ret = unsafe {
        libc::connect(
            socket.as_raw_fd(),
            &storage as *const _ as *const libc::sockaddr,
            len,
        )
    };

    if ret == -1 {
        let err = io::Error::last_os_error();
        if err.raw_os_error() != Some(libc::EINPROGRESS) {
            return Err(err);
        }
    }

    let std_stream = unsafe { std::net::TcpStream::from_raw_fd(socket.into_raw_fd()) };
    let stream = TcpStream::from_std(std_stream)?;

    if ret == -1 {
        tokio::time::timeout(timeout, stream.writable())
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("TCP raw connect timeout ({timeout:?})"),
                )
            })??;

        if let Some(so_err) = stream.take_error()? {
            return Err(so_err);
        }
    }

    #[cfg(target_os = "linux")]
    apply_tcp_post_connect(stream.as_raw_fd(), tuning);

    Ok(stream)
}

/// Connects a non-blocking `tokio::net::TcpStream` directly from an `&Endpoint`.
#[inline]
pub async fn connect_tcp_endpoint(endpoint: &Endpoint, timeout: Duration) -> io::Result<TcpStream> {
    connect_tcp_raw(endpoint.ip, endpoint.port_be, endpoint.is_v6, timeout).await
}

/// Connects a non-blocking, kernel-tuned `tokio::net::UdpSocket` directly from `(ip: u128, port_be: u16)`.
#[inline]
pub fn connect_udp_u128(ip: u128, port_be: u16) -> io::Result<UdpSocket> {
    let is_v6 = if is_v4_u128(ip) { 0 } else { 1 };
    connect_udp_raw(ip, port_be, is_v6)
}

/// Connects a non-blocking, kernel-tuned `tokio::net::UdpSocket` directly from raw `(ip, port_be, is_v6)`.
pub fn connect_udp_raw(ip: u128, port_be: u16, is_v6: u8) -> io::Result<UdpSocket> {
    let domain = if is_v6 == 0 {
        socket2::Domain::IPV4
    } else {
        socket2::Domain::IPV6
    };
    let socket = socket2::Socket::new(domain, socket2::Type::DGRAM, Some(socket2::Protocol::UDP))?;
    socket.set_nonblocking(true)?;

    let tuning = global_outbound_tuning();
    #[cfg(target_os = "linux")]
    apply_udp_tuning(socket.as_raw_fd(), tuning);

    let (storage, len) = raw_to_sockaddr_storage(ip, port_be, is_v6);
    let ret = unsafe {
        libc::connect(
            socket.as_raw_fd(),
            &storage as *const _ as *const libc::sockaddr,
            len,
        )
    };
    if ret == -1 {
        return Err(io::Error::last_os_error());
    }

    let std_sock = unsafe { std::net::UdpSocket::from_raw_fd(socket.into_raw_fd()) };
    UdpSocket::from_std(std_sock)
}

/// Connects a non-blocking `tokio::net::UdpSocket` directly from an `&Endpoint`.
#[inline]
pub fn connect_udp_endpoint(endpoint: &Endpoint) -> io::Result<UdpSocket> {
    connect_udp_raw(endpoint.ip, endpoint.port_be, endpoint.is_v6)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn test_raw_to_sockaddr_storage_v4_and_v6() {
        let ep4 = Endpoint::from_v4([10, 20, 30, 40], 8080, 1);
        let addr4 = raw_to_socket2_addr(ep4.ip, ep4.port_be, ep4.is_v6);
        assert_eq!(
            addr4.as_socket().unwrap(),
            SocketAddr::V4(v4itoaddr(ep4.ip, ep4.port_be))
        );

        let ep6 = Endpoint::from_v6(
            [0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
            9090,
            1,
        );
        let addr6 = raw_to_socket2_addr(ep6.ip, ep6.port_be, ep6.is_v6);
        assert_eq!(
            addr6.as_socket().unwrap(),
            SocketAddr::V6(v6itoaddr(ep6.ip, ep6.port_be))
        );
    }

    #[tokio::test]
    async fn test_connect_tcp_endpoint_v4() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (ip, port_be) = socket_addr_to_raw(addr);
        let ep = Endpoint::from_raw(ip, port_be, 1);

        let accept_task = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4];
            sock.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"ping");
            sock.write_all(b"pong").await.unwrap();
        });

        let mut client = connect_tcp_endpoint(&ep, Duration::from_secs(2))
            .await
            .unwrap();
        client.write_all(b"ping").await.unwrap();
        let mut resp = [0u8; 4];
        client.read_exact(&mut resp).await.unwrap();
        assert_eq!(&resp, b"pong");

        accept_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_connect_udp_endpoint_v4() {
        let server = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = server.local_addr().unwrap();
        let (ip, port_be) = socket_addr_to_raw(addr);
        let ep = Endpoint::from_raw(ip, port_be, 1);

        let client = connect_udp_endpoint(&ep).unwrap();
        client.send(b"hello").await.unwrap();

        let mut buf = [0u8; 16];
        let (n, _) = server.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"hello");
    }
}
