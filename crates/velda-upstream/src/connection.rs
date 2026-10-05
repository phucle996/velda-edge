//! Upstream raw socket acceleration path and kernel tuning.

/// Pre-compiled Linux socket acceleration path for backend connections.
///
/// Pre-computed at bootstrap to keep connection establishment on a branchless hot path
/// without repeated hardware or kernel capability checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SocketAccelerationPath {
    /// Disables Nagle's algorithm (`TCP_NODELAY`) to eliminate 40ms delayed-ACK buffering.
    pub nodelay: bool,
    /// Sends ClientHello directly in SYN packet (`TCP_FASTOPEN_CONNECT`), shaving 1 full RTT
    /// off TLS handshakes. Only enabled for TLS where replay protection is cryptographic.
    pub fastopen: bool,
    /// Caps unsent bytes in the socket write queue (`TCP_NOTSENT_LOWAT`) to prevent bufferbloat
    /// and keep epoll notification responsive to user-space backpressure.
    pub notsent_lowat: Option<u32>,
    /// Aborts stuck TCP streams (`TCP_USER_TIMEOUT`) when cloud NAT gateways or firewalls silently
    /// drop packets, replacing the default 15-minute OS retransmit timeout with upstream deadline.
    pub user_timeout: Option<std::time::Duration>,
    /// Periodically probes idle connections (`SO_KEEPALIVE` + `TCP_KEEPIDLE`) to keep stateful
    /// middlebox / NAT table entries warm and detect silent peer reboots.
    pub keepalive: Option<std::time::Duration>,
    /// Low-latency socket polling in kernel space (`SO_BUSY_POLL`) to bypass epoll sleep/wake
    /// context-switch latency spikes on high-core server tiers.
    pub busy_poll_us: Option<u32>,
    /// Bounds SYN retransmission attempts (`TCP_SYNCNT`) to fail fast and trigger upstream
    /// failover within 3-7s rather than stalling worker tasks for 60-127s on partitioned backends.
    pub syncnt: Option<u32>,
    /// Immediately acknowledges incoming upstream response packets (`TCP_QUICKACK`) to prevent
    /// 40ms delayed-ACK stalls on backend responses.
    pub quickack: bool,
    /// Attempts BBR congestion control (`TCP_CONGESTION`) for high-throughput, low-bufferbloat egress.
    pub bbr: bool,
}

impl Default for SocketAccelerationPath {
    fn default() -> Self {
        Self {
            nodelay: true,
            fastopen: false,
            notsent_lowat: None,
            user_timeout: None,
            keepalive: None,
            busy_poll_us: None,
            syncnt: None,
            quickack: true,
            bbr: false,
        }
    }
}

impl SocketAccelerationPath {
    /// Pre-compiles socket acceleration path based on host hardware/kernel topology,
    /// upstream timeouts, and whether TLS encryption is active.
    ///
    /// Fast Open is only enabled for TLS endpoints because raw HTTP requests (like POST) are not
    /// idempotent and could suffer from TCP retransmission replays.
    pub fn for_topology(
        topo: &velda_core::HardwareTopology,
        timeouts: &crate::upstream::UpstreamTimeouts,
        is_tls: bool,
    ) -> Self {
        let fastopen = is_tls && topo.kernel.supports_tcp_fastopen_connect();

        let notsent_lowat = if topo.kernel.supports_tcp_notsent_lowat() {
            Some(notsent_lowat_for_mem_tier(topo.memory_tier()))
        } else {
            None
        };

        let user_timeout = if topo.kernel.supports_tcp_user_timeout() {
            let candidate = timeouts.connect.saturating_mul(3);
            let cap = timeouts.idle.min(std::time::Duration::from_secs(30));
            Some(candidate.max(cap).max(std::time::Duration::from_secs(10)))
        } else {
            None
        };

        let keepalive = Some(timeouts.idle / 2);

        // Limit SYN retries to 3 (gives ~7s before failing fast)
        // instead of OS default 6 retries (which hangs for up to 127s).
        let syncnt = if topo.kernel.supports_tcp_syncnt() {
            Some(3)
        } else {
            None
        };

        let quickack = topo.kernel.supports_quickack();
        let bbr = topo.kernel.supports_bbr();

        // Enable SO_BUSY_POLL only on large multi-core hardware tiers where dedicated core polling
        // yields sub-millisecond tail latency wins without starving small-tier CPU budgets.
        let busy_poll_us = if topo.kernel.supports_busy_poll()
            && matches!(
                topo.cpu_tier(),
                velda_core::CpuTier::Large
                    | velda_core::CpuTier::XLarge
                    | velda_core::CpuTier::TwoXLarge
                    | velda_core::CpuTier::Ultra
            ) {
            Some(50)
        } else {
            None
        };

        Self {
            nodelay: true,
            fastopen,
            notsent_lowat,
            user_timeout,
            keepalive,
            busy_poll_us,
            syncnt,
            quickack,
            bbr,
        }
    }

    /// Applies pre-connect socket acceleration options (e.g. `TCP_FASTOPEN_CONNECT`, `TCP_SYNCNT`).
    ///
    /// Unprivileged containers (Docker default seccomp, K8s unprivileged pods) often block
    /// Fast Open with `EPERM` or `ENOPROTOOPT`. Logging at trace level and proceeding ensures
    /// traffic still flows without hard connection failures.
    pub fn apply_pre_connect(&self, fd: std::os::unix::io::RawFd) {
        #[cfg(target_os = "linux")]
        {
            if self.fastopen {
                let val: libc::c_int = 1;
                unsafe {
                    let ret = libc::setsockopt(
                        fd,
                        libc::IPPROTO_TCP,
                        libc::TCP_FASTOPEN_CONNECT,
                        &val as *const _ as *const libc::c_void,
                        std::mem::size_of_val(&val) as libc::socklen_t,
                    );
                    if ret != 0 {
                        tracing::trace!(
                            errno = std::io::Error::last_os_error().raw_os_error(),
                            "TCP_FASTOPEN_CONNECT not supported by kernel or denied in container; skipping"
                        );
                    }
                }
            }

            if let Some(syncnt) = self.syncnt {
                let val = syncnt as libc::c_int;
                unsafe {
                    let ret = libc::setsockopt(
                        fd,
                        libc::IPPROTO_TCP,
                        libc::TCP_SYNCNT,
                        &val as *const _ as *const libc::c_void,
                        std::mem::size_of_val(&val) as libc::socklen_t,
                    );
                    if ret != 0 {
                        tracing::trace!(error = %std::io::Error::last_os_error(), "TCP_SYNCNT skipped");
                    }
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        let _ = fd;
    }

    /// Applies post-connect socket acceleration options (`TCP_NODELAY`, `TCP_NOTSENT_LOWAT`,
    /// `TCP_USER_TIMEOUT`, `SO_KEEPALIVE`, `TCP_KEEPIDLE`, `TCP_KEEPINTVL`, `TCP_KEEPCNT`,
    /// `TCP_QUICKACK`, `TCP_CONGESTION`, `SO_BUSY_POLL`).
    ///
    /// Invoked immediately after connection handshake. Any option rejected by host seccomp
    /// or legacy kernel is skipped at trace level rather than dropping an established backend stream.
    pub fn apply_post_connect(&self, fd: std::os::unix::io::RawFd) {
        #[cfg(target_os = "linux")]
        unsafe {
            if let Some(lowat) = self.notsent_lowat {
                let val = lowat as libc::c_uint;
                let ret = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_NOTSENT_LOWAT,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(error = %std::io::Error::last_os_error(), "TCP_NOTSENT_LOWAT skipped");
                }
            }

            if let Some(user_timeout) = self.user_timeout {
                let val = user_timeout.as_millis() as libc::c_uint;
                let ret = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_USER_TIMEOUT,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(error = %std::io::Error::last_os_error(), "TCP_USER_TIMEOUT skipped");
                }
            }

            if let Some(keepalive) = self.keepalive {
                let val: libc::c_int = 1;
                let _ = libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_KEEPALIVE,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );

                let idle_secs = keepalive.as_secs().max(1) as libc::c_int;
                let _ = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_KEEPIDLE,
                    &idle_secs as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&idle_secs) as libc::socklen_t,
                );

                // Configure aggressive probe interval (min 3s, max 10s)
                let intvl_secs = (idle_secs / 3).clamp(3, 10);
                let _ = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_KEEPINTVL,
                    &intvl_secs as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&intvl_secs) as libc::socklen_t,
                );

                // Max 3 failed probes before dropping connection
                let cnt: libc::c_int = 3;
                let _ = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_KEEPCNT,
                    &cnt as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&cnt) as libc::socklen_t,
                );
            }

            if self.quickack {
                let val: libc::c_int = 1;
                let _ = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_QUICKACK,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
            }

            if self.bbr {
                let bbr_name = b"bbr\0";
                let _ = libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_CONGESTION,
                    bbr_name.as_ptr() as *const libc::c_void,
                    bbr_name.len() as libc::socklen_t,
                );
            }

            if let Some(busy_poll) = self.busy_poll_us {
                let val = busy_poll as libc::c_int;
                let ret = libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_BUSY_POLL,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&val) as libc::socklen_t,
                );
                if ret != 0 {
                    tracing::trace!(error = %std::io::Error::last_os_error(), "SO_BUSY_POLL skipped (unprivileged container)");
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        let _ = fd;
    }
}

/// Maps a [`velda_core::MemoryTier`] to a `TCP_NOTSENT_LOWAT` byte threshold.
///
/// Thresholds are scaled to memory tier with a 16KB minimum:
/// - Setting the threshold too low (< 16KB) starves NIC TCP Segmentation Offload (TSO) and
///   causes epoll to wake on almost every single packet, degrading CPU cache locality.
/// - Setting it too high (> 128KB) causes bufferbloat in the kernel socket buffer and defeats
///   multiplexed stream prioritization.
pub const fn notsent_lowat_for_mem_tier(tier: velda_core::MemoryTier) -> u32 {
    use velda_core::MemoryTier;
    const KB: u32 = 1024;
    match tier {
        MemoryTier::Constrained | MemoryTier::Small => 16 * KB,
        MemoryTier::Medium => 32 * KB,
        MemoryTier::Large | MemoryTier::XLarge => 64 * KB,
        MemoryTier::TwoXLarge | MemoryTier::Ultra => 128 * KB,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_notsent_lowat_scaling_and_floor() {
        use velda_core::MemoryTier;
        assert_eq!(
            notsent_lowat_for_mem_tier(MemoryTier::Constrained),
            16 * 1024
        );
        assert_eq!(notsent_lowat_for_mem_tier(MemoryTier::Medium), 32 * 1024);
        assert_eq!(notsent_lowat_for_mem_tier(MemoryTier::Large), 64 * 1024);
        assert_eq!(notsent_lowat_for_mem_tier(MemoryTier::Ultra), 128 * 1024);
    }

    #[test]
    fn test_socket_acceleration_path_for_topology_resolution() {
        let topo = velda_core::global_hardware_topology();
        let timeouts = crate::upstream::UpstreamTimeouts::http(
            std::time::Duration::from_millis(500),
            std::time::Duration::from_secs(30),
            std::time::Duration::from_secs(5),
        );

        // Plain TCP (non-TLS)
        let accel_plain = SocketAccelerationPath::for_topology(topo, &timeouts, false);
        assert!(accel_plain.nodelay);
        assert!(!accel_plain.fastopen, "Plain TCP must not enable fastopen");

        // TLS
        let accel_tls = SocketAccelerationPath::for_topology(topo, &timeouts, true);
        assert!(accel_tls.nodelay);
        #[cfg(target_os = "linux")]
        {
            if topo.kernel.supports_tcp_fastopen_connect() {
                assert!(
                    accel_tls.fastopen,
                    "TLS on Linux >= 4.11 should enable fastopen"
                );
            }
            if topo.kernel.supports_tcp_notsent_lowat() {
                assert!(accel_tls.notsent_lowat.is_some());
            }
        }
    }
}
