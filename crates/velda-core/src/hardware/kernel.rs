//! Kernel Topology, OS Release & Capability Probe (`velda_core::hardware::kernel`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                    KERNEL CAPABILITY PROBE (uname(2) + /proc/sys/net/core)              │
//! ├─────────────────────────────────────────────────────────────────────────────────────────┤
//! │  KernelProfile                                                                          │
//! │    ├── version: KernelVersion { major, minor, patch }                                   │
//! │    ├── rmem_max: Option<u64>   (/proc/sys/net/core/rmem_max)                            │
//! │    ├── wmem_max: Option<u64>   (/proc/sys/net/core/wmem_max)                            │
//! │    └── supports_*() -> bool    (const version capability predicates)                    │
//! └─────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::fmt;

/// Structured Linux kernel release version (`major.minor.patch`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct KernelVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl KernelVersion {
    /// Creates a new kernel version representation.
    #[inline]
    pub const fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    /// Evaluates if this version meets or exceeds the required `(major, minor)` boundary.
    ///
    /// Uses flat comparison to optimize branch prediction and eliminate nested if-else ladders.
    #[inline]
    pub const fn is_at_least(&self, req_major: u32, req_minor: u32) -> bool {
        if self.major > req_major {
            true
        } else if self.major == req_major {
            self.minor >= req_minor
        } else {
            false
        }
    }
}

impl fmt::Display for KernelVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// Discovered kernel release and socket capability profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KernelProfile {
    /// Detected kernel release version.
    pub version: KernelVersion,
    /// Maximum OS socket receive buffer size in bytes (`net.core.rmem_max`).
    pub rmem_max: Option<u64>,
    /// Maximum OS socket send buffer size in bytes (`net.core.wmem_max`).
    pub wmem_max: Option<u64>,
}

impl KernelProfile {
    /// Creates a kernel profile from a detected [`KernelVersion`].
    #[inline]
    pub const fn new(version: KernelVersion) -> Self {
        Self {
            version,
            rmem_max: None,
            wmem_max: None,
        }
    }

    /// Attaches probed socket buffer sizes (`net.core.rmem_max`, `net.core.wmem_max`).
    #[inline]
    pub const fn with_socket_buffers(
        mut self,
        rmem_max: Option<u64>,
        wmem_max: Option<u64>,
    ) -> Self {
        self.rmem_max = rmem_max;
        self.wmem_max = wmem_max;
        self
    }

    // =========================================================================
    // Kernel Capability Queries (Ordered by minimum required Linux version)
    // =========================================================================

    // --- Linux 2.4 ---

    /// Evaluates if the kernel supports `TCP_DEFER_ACCEPT` (Linux >= 2.4).
    ///
    /// Postpones accept wakeups until the client actually sends initial payload bytes,
    /// eliminating useless thread wakeups on empty TCP handshakes.
    #[inline]
    pub const fn supports_defer_accept(&self) -> bool {
        self.version.is_at_least(2, 4)
    }

    /// Evaluates if the kernel supports `IP_FREEBIND` (Linux >= 2.4).
    ///
    /// Allows sockets to bind to non-local IP addresses (e.g. Floating VIPs, Anycast),
    /// enabling zero-failure cold boots and live failovers before the network interface claims the IP.
    #[inline]
    pub const fn supports_ip_freebind(&self) -> bool {
        self.version.is_at_least(2, 4)
    }

    /// Evaluates if the kernel supports `TCP_QUICKACK` (Linux >= 2.4).
    ///
    /// Immediately acknowledges incoming data to prevent 40ms delayed-ACK deadlocks
    /// with peers employing Nagle's algorithm.
    #[inline]
    pub const fn supports_quickack(&self) -> bool {
        self.version.is_at_least(2, 4)
    }

    /// Evaluates if the kernel supports `TCP_SYNCNT` (Linux >= 2.4).
    ///
    /// Limits SYN retransmission retries to fail fast and trigger failover
    /// rather than stalling workers for 60-120 seconds on partitioned backends.
    #[inline]
    pub const fn supports_tcp_syncnt(&self) -> bool {
        self.version.is_at_least(2, 4)
    }

    /// Evaluates if the kernel supports fine-grained TCP keepalive tuning (Linux >= 2.4).
    ///
    /// Enables configuring `TCP_KEEPINTVL` and `TCP_KEEPCNT` alongside `TCP_KEEPIDLE`.
    #[inline]
    pub const fn supports_tcp_keepalive_tuning(&self) -> bool {
        self.version.is_at_least(2, 4)
    }

    // --- Linux 2.6 ---

    /// Evaluates if the kernel supports `TCP_USER_TIMEOUT` (Linux >= 2.6.37).
    ///
    /// Allows setting an explicit deadline for unacknowledged transmitted data (RFC 5482),
    /// aborting hung connections when cloud NAT gateways or firewalls silently drop packets.
    #[inline]
    pub const fn supports_tcp_user_timeout(&self) -> bool {
        self.version.is_at_least(2, 6)
    }

    /// Evaluates if the kernel supports `SO_RXQ_OVFL` (Linux >= 2.6.33).
    ///
    /// Enables reading socket queue overflow dropped datagram counters from ancillary cmsg headers.
    #[inline]
    pub const fn supports_so_rxq_ovfl(&self) -> bool {
        self.version.is_at_least(2, 6)
    }

    // --- Linux 3.x ---

    /// Evaluates if the kernel supports server-side `TCP_FASTOPEN` (Linux >= 3.7).
    ///
    /// Linux 3.7 introduced the `TCP_FASTOPEN` listener option, enabling the kernel to generate
    /// and validate Fast Open cookies for incoming SYN packets.
    #[inline]
    pub const fn supports_tcp_fastopen_server(&self) -> bool {
        self.version.is_at_least(3, 7)
    }

    /// Evaluates if the kernel supports low-latency `SO_BUSY_POLL` (Linux >= 3.11).
    ///
    /// Linux 3.11 introduced socket-level busy polling, allowing high-throughput workers
    /// to poll device driver receive rings directly and bypass epoll context-switch latency spikes.
    #[inline]
    pub const fn supports_busy_poll(&self) -> bool {
        self.version.is_at_least(3, 11)
    }

    /// Evaluates if the kernel supports `TCP_NOTSENT_LOWAT` (Linux >= 3.12).
    ///
    /// Linux 3.12 added `TCP_NOTSENT_LOWAT` to combat bufferbloat by capping unsent bytes
    /// in the write queue, enabling responsive multiplexed stream prioritization (HTTP/2, gRPC).
    #[inline]
    pub const fn supports_tcp_notsent_lowat(&self) -> bool {
        self.version.is_at_least(3, 12)
    }

    /// Evaluates if the kernel supports `SO_INCOMING_CPU` (Linux >= 3.19).
    ///
    /// Enables inspecting or steering socket processing to the exact CPU core
    /// that received the network packet, optimizing L1/L2 cache locality.
    #[inline]
    pub const fn supports_incoming_cpu(&self) -> bool {
        self.version.is_at_least(3, 19)
    }

    // --- Linux 4.x ---

    /// Evaluates if the kernel supports `IP_BIND_ADDRESS_NO_PORT` (Linux >= 4.2).
    ///
    /// Delays ephemeral source port selection until `connect(2)` to eliminate 64k outbound port exhaustion.
    #[inline]
    pub const fn supports_ip_bind_address_no_port(&self) -> bool {
        self.version.is_at_least(4, 2)
    }

    /// Evaluates if the kernel supports classic BPF steering on `SO_REUSEPORT` (Linux >= 4.5).
    ///
    /// Enables deterministic mapping from NIC interrupt receiving CPU directly to the pinned worker.
    #[inline]
    pub const fn supports_so_attach_reuseport_cbpf(&self) -> bool {
        self.version.is_at_least(4, 5)
    }

    /// Evaluates if the kernel supports BBR congestion control (Linux >= 4.9).
    #[inline]
    pub const fn supports_bbr(&self) -> bool {
        self.version.is_at_least(4, 9)
    }

    /// Evaluates if the kernel supports `TCP_FASTOPEN_CONNECT` (Linux >= 4.11).
    ///
    /// Linux 4.11 introduced `TCP_FASTOPEN_CONNECT`, allowing transparent client Fast Open
    /// via standard non-blocking `connect(2)` rather than requiring complex `sendto(2)` with `MSG_FASTOPEN`.
    #[inline]
    pub const fn supports_tcp_fastopen_connect(&self) -> bool {
        self.version.is_at_least(4, 11)
    }

    /// Evaluates if the kernel supports UDP Generic Segmentation Offload (`UDP_SEGMENT`, Linux >= 4.18).
    ///
    /// Transmits up to 64KB buffers in a single system call, offloading datagram segmentation to kernel or NIC hardware.
    #[inline]
    pub const fn supports_udp_segment(&self) -> bool {
        self.version.is_at_least(4, 18)
    }

    // --- Linux 5.x ---

    /// Evaluates if the kernel supports Generic Receive Offload for UDP (`UDP_GRO`, Linux >= 5.0).
    ///
    /// Batches multiple incoming QUIC datagrams into up to 64KB buffers in a single system call.
    #[inline]
    pub const fn supports_udp_gro(&self) -> bool {
        self.version.is_at_least(5, 0)
    }

    /// Evaluates if the kernel supports IRQ-masked NAPI busy polling (`SO_PREFER_BUSY_POLL`, Linux >= 5.11).
    #[inline]
    pub const fn supports_so_prefer_busy_poll(&self) -> bool {
        self.version.is_at_least(5, 11)
    }

    /// Evaluates if the kernel supports automatic ECMP path rehashing upon loss/RTO (`SO_TXREHASH`, Linux >= 5.18).
    #[inline]
    pub const fn supports_so_txrehash(&self) -> bool {
        self.version.is_at_least(5, 18)
    }

    // --- Linux 6.x ---

    /// Evaluates if the kernel supports microsecond-level RTO and delayed ACK tuning
    /// (`TCP_RTO_MIN_US`, `TCP_DELACK_MAX_US`, `TCP_RTO_MAX_MS`, Linux >= 6.9).
    #[inline]
    pub const fn supports_tcp_microsecond_rto(&self) -> bool {
        self.version.is_at_least(6, 9)
    }

    /// Detects if the process is running inside a containerized environment (Docker, Podman, K8s).
    ///
    /// Container runtimes enforce default seccomp profiles that block specific socket syscalls
    /// (`EPERM` / `ENOPROTOOPT`). Knowing this allows socket acceleration paths to downgrade
    /// to trace-level warnings rather than assuming a broken host configuration.
    pub fn is_containerized(&self) -> bool {
        #[cfg(target_os = "linux")]
        {
            std::path::Path::new("/.dockerenv").exists()
                || std::path::Path::new("/run/.containerenv").exists()
                || std::fs::read_to_string("/proc/1/cgroup")
                    .map(|s| {
                        s.contains("docker") || s.contains("kubepods") || s.contains("containerd")
                    })
                    .unwrap_or(false)
        }
        #[cfg(not(target_os = "linux"))]
        {
            false
        }
    }
}

/// Classification of Ingress CPU Core steering generations.
///
/// Uses single-pass byte splitting without regex or dynamic heap allocations.
#[inline]
pub fn parse_version_bytes(bytes: &[u8]) -> KernelVersion {
    let mut split = bytes.split(|&b| b == b'.' || b == b'-');
    let major = split.next().map(parse_ascii_digits).unwrap_or(0);
    let minor = split.next().map(parse_ascii_digits).unwrap_or(0);
    let patch = split.next().map(parse_ascii_digits).unwrap_or(0);
    KernelVersion::new(major, minor, patch)
}

#[inline]
fn parse_ascii_digits(slice: &[u8]) -> u32 {
    let mut acc = 0u32;
    for &b in slice {
        if b.is_ascii_digit() {
            acc = acc.saturating_mul(10).saturating_add((b - b'0') as u32);
        } else {
            break;
        }
    }
    acc
}

#[cfg(target_os = "linux")]
#[cfg(target_os = "linux")]
#[inline]
fn read_sysctl_u64(path: &str) -> Option<u64> {
    std::fs::read_to_string(path)
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
}

/// Probes host OS kernel version and socket buffer capabilities.
#[cfg(target_os = "linux")]
pub fn probe_kernel() -> KernelProfile {
    let rmem_max = read_sysctl_u64("/proc/sys/net/core/rmem_max");
    let wmem_max = read_sysctl_u64("/proc/sys/net/core/wmem_max");

    // 1. Inspect kernel release string via uname(2)
    let mut uts = std::mem::MaybeUninit::<libc::utsname>::uninit();
    let res = unsafe { libc::uname(uts.as_mut_ptr()) };
    if res != 0 {
        return KernelProfile::new(KernelVersion::default())
            .with_socket_buffers(rmem_max, wmem_max);
    }
    let uts = unsafe { uts.assume_init() };

    let mut buf = [0u8; 64];
    let mut len = 0;
    while len < 63 && uts.release[len] != 0 {
        buf[len] = uts.release[len] as u8;
        len += 1;
    }

    let version = parse_version_bytes(&buf[..len]);
    KernelProfile::new(version).with_socket_buffers(rmem_max, wmem_max)
}

/// Fallback probe for non-Linux host environments.
#[cfg(not(target_os = "linux"))]
pub fn probe_kernel() -> KernelProfile {
    KernelProfile::new(KernelVersion::new(0, 0, 0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_kernel_version_formats() {
        let v1 = parse_version_bytes(b"6.8.0-45-generic");
        assert_eq!(v1, KernelVersion::new(6, 8, 0));

        let v2 = parse_version_bytes(b"5.15.12-arch1-1");
        assert_eq!(v2, KernelVersion::new(5, 15, 12));

        let v3 = parse_version_bytes(b"5.19.0");
        assert_eq!(v3, KernelVersion::new(5, 19, 0));

        let v4 = parse_version_bytes(b"invalid-kernel");
        assert_eq!(v4, KernelVersion::new(0, 0, 0));
    }

    #[test]
    fn test_kernel_version_is_at_least() {
        let v_old = KernelVersion::new(5, 15, 0);
        assert!(!v_old.is_at_least(5, 19));
        assert!(v_old.is_at_least(5, 10));

        let v_exact = KernelVersion::new(5, 19, 0);
        assert!(v_exact.is_at_least(5, 19));

        let v_new = KernelVersion::new(6, 8, 0);
        assert!(v_new.is_at_least(5, 19));
        assert!(v_new.is_at_least(6, 0));
        assert!(!v_new.is_at_least(6, 9));
    }

    #[test]
    fn test_probe_kernel_returns_valid_profile() {
        let profile = probe_kernel();
        // On Linux, version major should be at least 2
        #[cfg(target_os = "linux")]
        {
            assert!(profile.version.major >= 2);
        }
        #[cfg(not(target_os = "linux"))]
        {
            assert_eq!(profile.version, KernelVersion::new(0, 0, 0));
        }
    }
}
