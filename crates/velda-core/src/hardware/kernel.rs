//! Kernel topology, OS release, and I/O acceleration probe.
//!
//! Owns host Linux kernel version detection via `uname(2)`, capability qualification,
//! and hardware acceleration tier classification.

use std::fmt;

/// Canonical I/O acceleration tiers for edge networking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AccelerationTier {
    /// Non-Linux or degraded environment.
    ///
    /// Uses standard user-space buffers and cross-platform Tokio reactor.
    Degraded,

    /// Standard Linux Production Path (Linux < 5.19 or io_uring restricted by Seccomp).
    ///
    /// Employs `SO_REUSEPORT` multi-listener sharding, CPU core affinity,
    /// and zero-copy `splice(2)` for L4 TCP proxying.
    Standard,

    /// ACCELERATION FAST-PATH: Linux Kernel >= 5.19 with unlocked `io_uring` ring buffers.
    ///
    /// # Performance Advantage:
    /// Replaces readiness-based `epoll` syscall loops with lockless Submission/Completion
    /// ring buffers in kernel-shared memory, eliminating user/kernel context switches.
    ///
    /// # Conditions Required:
    /// 1. Target OS is Linux.
    /// 2. Kernel release version is >= 5.19 (enables `IORING_ACCEPT_MULTISHOT` & stable multi-worker networking rings).
    /// 3. `io_uring_setup` syscall dry-run succeeds without `EPERM`/`ENOSYS` (unrestricted by K8s Seccomp / sysctl).
    IoUringFastPath,
}

impl AccelerationTier {
    /// Returns the canonical string representation of this acceleration tier.
    #[inline]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Degraded => "degraded",
            Self::Standard => "standard",
            Self::IoUringFastPath => "io_uring_fast_path",
        }
    }

    /// Returns `true` if this tier is an accelerated fast-path.
    #[inline]
    pub const fn is_accelerated(&self) -> bool {
        matches!(self, Self::IoUringFastPath)
    }
}

impl fmt::Display for AccelerationTier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

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

/// Discovered kernel release and I/O acceleration profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KernelProfile {
    /// Detected kernel release version.
    pub version: KernelVersion,
    /// Classified hardware/kernel I/O acceleration tier.
    pub acceleration: AccelerationTier,
    /// Human-readable diagnostic description of how the acceleration tier was selected.
    pub reason: &'static str,
}

impl KernelProfile {
    /// Creates a kernel profile from explicit components.
    #[inline]
    pub const fn new(
        version: KernelVersion,
        acceleration: AccelerationTier,
        reason: &'static str,
    ) -> Self {
        Self {
            version,
            acceleration,
            reason,
        }
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

    // --- Linux 5.x ---

    /// Evaluates if the kernel supports Generic Receive Offload for UDP (`UDP_GRO`, Linux >= 5.0).
    ///
    /// Batches multiple incoming QUIC datagrams into up to 64KB buffers in a single system call.
    #[inline]
    pub const fn supports_udp_gro(&self) -> bool {
        self.version.is_at_least(5, 0)
    }

    /// Evaluates if the kernel version satisfies the minimum requirement for `io_uring`
    /// multishot networking and stable ring buffers (Linux >= 5.19).
    #[inline]
    pub const fn supports_io_uring(&self) -> bool {
        self.version.is_at_least(5, 19)
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CoreSteeringTier {
    /// Baseline single-acceptor listen queue (no multi-core distribution).
    Baseline,
    /// Kernel 4-tuple hashing across SO_REUSEPORT sockets (Linux >= 3.9).
    ReusePort,
    /// Steer listener socket queues to matching CPU core (SO_INCOMING_CPU, Linux >= 3.19).
    IncomingCpu,
    /// Direct BPF/eBPF steering from NIC softirq CPU to pinned worker (SO_ATTACH_REUSEPORT_CBPF, Linux >= 4.5).
    BpfCpuSteering,
}

/// Classification of Upstream outbound ephemeral port allocation generations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OutboundPortScalingTier {
    /// Standard early ephemeral port allocation at bind time (Linux < 4.2).
    Standard,
    /// Delayed port allocation until connect() time (IP_BIND_ADDRESS_NO_PORT, Linux >= 4.2).
    BindAddressNoPort,
}

/// Classification of UDP / HTTP/3 datagram receive offload generations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum UdpOffloadTier {
    /// Standard per-datagram recv_from syscalls.
    Standard,
    /// Hardware/kernel drop monitoring via SO_RXQ_OVFL (Linux >= 2.6.33).
    QueueMonitored,
    /// Generic Receive Offload batching up to 64KB per syscall via UDP_GRO (Linux >= 5.0).
    GenericReceiveOffload,
}

/// Classification of multiplexed write pacing generations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MultiplexPacingTier {
    /// Default OS send buffer.
    DefaultBuffer,
    /// Unsent queue threshold pacing via TCP_NOTSENT_LOWAT (Linux >= 3.12).
    NotsentLowat,
}

/// Classification of zombie connection teardown generations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DeadPeerTeardownTier {
    /// Standard TCP keepalive probes (Linux >= 2.4).
    KeepaliveProbes,
    /// Explicit deadline enforcement via TCP_USER_TIMEOUT (Linux >= 2.6.37).
    UserTimeout,
}

/// Evolutionary acceleration ladder resolving the highest supported kernel acceleration path
/// for each specific optimization goal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KernelAccelerationLadder {
    /// Ingress connection steering and core affinity generation.
    pub core_steering: CoreSteeringTier,
    /// Outbound ephemeral port scaling generation.
    pub outbound_port_scaling: OutboundPortScalingTier,
    /// UDP receive batching and offload generation.
    pub udp_offload: UdpOffloadTier,
    /// HTTP/2 and multiplexed stream write pacing generation.
    pub multiplex_pacing: MultiplexPacingTier,
    /// Silent peer disconnection and blackhole teardown generation.
    pub dead_peer_teardown: DeadPeerTeardownTier,
}

impl KernelAccelerationLadder {
    /// Resolves the highest evolution tier for each acceleration goal based on the detected kernel profile.
    pub fn from_kernel(kernel: &KernelProfile) -> Self {
        let core_steering = if kernel.supports_so_attach_reuseport_cbpf() {
            CoreSteeringTier::BpfCpuSteering
        } else if kernel.supports_incoming_cpu() {
            CoreSteeringTier::IncomingCpu
        } else if kernel.version.is_at_least(3, 9) {
            CoreSteeringTier::ReusePort
        } else {
            CoreSteeringTier::Baseline
        };

        let outbound_port_scaling = if kernel.supports_ip_bind_address_no_port() {
            OutboundPortScalingTier::BindAddressNoPort
        } else {
            OutboundPortScalingTier::Standard
        };

        let udp_offload = if kernel.supports_udp_gro() {
            UdpOffloadTier::GenericReceiveOffload
        } else if kernel.supports_so_rxq_ovfl() {
            UdpOffloadTier::QueueMonitored
        } else {
            UdpOffloadTier::Standard
        };

        let multiplex_pacing = if kernel.supports_tcp_notsent_lowat() {
            MultiplexPacingTier::NotsentLowat
        } else {
            MultiplexPacingTier::DefaultBuffer
        };

        let dead_peer_teardown = if kernel.supports_tcp_user_timeout() {
            DeadPeerTeardownTier::UserTimeout
        } else {
            DeadPeerTeardownTier::KeepaliveProbes
        };

        Self {
            core_steering,
            outbound_port_scaling,
            udp_offload,
            multiplex_pacing,
            dead_peer_teardown,
        }
    }
}

/// Parses raw kernel release bytes (e.g. b"6.8.0-45-generic") into a structured [`KernelVersion`].
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IoUringProbeResult {
    Available,
    BlockedBySeccomp,
    Disabled,
    Unsupported,
}

#[cfg(target_os = "linux")]
fn probe_io_uring_capability() -> IoUringProbeResult {
    // Syscall 425: __NR_io_uring_setup on Linux x86_64, aarch64, arm, riscv64.
    const SYS_IO_URING_SETUP: libc::c_long = 425;

    // Zeroed buffer of 128 bytes representing struct io_uring_params (120 bytes in kernel).
    let mut params = [0u8; 128];
    let ret = unsafe {
        libc::syscall(
            SYS_IO_URING_SETUP,
            1 as libc::c_uint, // entries = 1 (minimal ring entry)
            params.as_mut_ptr(),
        )
    };

    if ret >= 0 {
        // Success: Dry-run succeeded. Immediately close temporary ring fd to prevent leak.
        unsafe {
            libc::close(ret as libc::c_int);
        }
        IoUringProbeResult::Available
    } else {
        let err = std::io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::EPERM) => IoUringProbeResult::BlockedBySeccomp,
            Some(libc::EACCES) => IoUringProbeResult::Disabled,
            Some(libc::ENOSYS) => IoUringProbeResult::Unsupported,
            _ => IoUringProbeResult::Unsupported,
        }
    }
}

/// Probes host OS kernel version and hardware I/O acceleration capabilities.
///
/// Executes a 2-stage capability handshake:
/// 1. Version Gatekeeper: Checks if Linux kernel release is >= 5.19.
/// 2. Capability Dry-Run: Tests `io_uring_setup` syscall to verify Seccomp/cgroup unblocking.
#[cfg(target_os = "linux")]
pub fn probe_kernel() -> KernelProfile {
    // 1. Inspect kernel release string via uname(2)
    let mut uts = std::mem::MaybeUninit::<libc::utsname>::uninit();
    let res = unsafe { libc::uname(uts.as_mut_ptr()) };
    if res != 0 {
        return KernelProfile::new(
            KernelVersion::default(),
            AccelerationTier::Standard,
            "uname_probe_failed_fallback_epoll",
        );
    }
    let uts = unsafe { uts.assume_init() };

    let mut buf = [0u8; 64];
    let mut len = 0;
    while len < 63 && uts.release[len] != 0 {
        buf[len] = uts.release[len] as u8;
        len += 1;
    }

    let version = parse_version_bytes(&buf[..len]);

    // Stage 1: Version Gatekeeper
    // ACCELERATION PATH requires Linux >= 5.19 for multi-shot accept and stable network rings.
    if !version.is_at_least(5, 19) {
        return KernelProfile::new(
            version,
            AccelerationTier::Standard,
            "kernel_below_5_19_standard_epoll",
        );
    }

    // Stage 2: Capability Dry-Run (Probe io_uring_setup to verify Seccomp & permissions)
    // Avoids branch loops by matching directly on the probe result enum.
    match probe_io_uring_capability() {
        // ACCELERATION FAST-PATH: Linux Kernel >= 5.19 with unlocked io_uring ring buffers.
        // Condition: Kernel >= 5.19 AND io_uring_setup dry-run returned success (unrestricted).
        IoUringProbeResult::Available => KernelProfile::new(
            version,
            AccelerationTier::IoUringFastPath,
            "kernel_5_19_plus_io_uring_unlocked",
        ),
        IoUringProbeResult::BlockedBySeccomp => KernelProfile::new(
            version,
            AccelerationTier::Standard,
            "io_uring_blocked_by_seccomp_fallback_epoll",
        ),
        IoUringProbeResult::Disabled => KernelProfile::new(
            version,
            AccelerationTier::Standard,
            "io_uring_disabled_by_sysctl_fallback_epoll",
        ),
        IoUringProbeResult::Unsupported => KernelProfile::new(
            version,
            AccelerationTier::Standard,
            "io_uring_unsupported_fallback_epoll",
        ),
    }
}

/// Fallback probe for non-Linux host environments.
#[cfg(not(target_os = "linux"))]
pub fn probe_kernel() -> KernelProfile {
    KernelProfile::new(
        KernelVersion::new(0, 0, 0),
        AccelerationTier::Degraded,
        "non_linux_host",
    )
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
            assert!(
                profile.acceleration == AccelerationTier::Standard
                    || profile.acceleration == AccelerationTier::IoUringFastPath
            );
        }
        #[cfg(not(target_os = "linux"))]
        {
            assert_eq!(profile.acceleration, AccelerationTier::Degraded);
        }
    }
}
