//! Upstream raw socket acceleration path and kernel tuning.

/// Socket acceleration path pre-compiled during bootstrap for backend connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SocketAccelerationPath {
    /// Disable Nagle's algorithm (`TCP_NODELAY`). Defaults to `true`.
    pub nodelay: bool,
    /// Enable TCP Fast Open Connect (`TCP_FASTOPEN_CONNECT`).
    pub fastopen: bool,
    /// Sndbuf low-watermark threshold (`TCP_NOTSENT_LOWAT`) in bytes.
    pub notsent_lowat: Option<u32>,
    /// Maximum time in milliseconds that transmitted data may remain unacknowledged (`TCP_USER_TIMEOUT`).
    pub user_timeout: Option<std::time::Duration>,
    /// TCP Keep-Alive interval (`SO_KEEPALIVE` + `TCP_KEEPIDLE`).
    pub keepalive: Option<std::time::Duration>,
}

impl Default for SocketAccelerationPath {
    fn default() -> Self {
        Self {
            nodelay: true,
            fastopen: false,
            notsent_lowat: None,
            user_timeout: None,
            keepalive: None,
        }
    }
}

impl SocketAccelerationPath {
    /// Pre-compiles socket acceleration path based on host hardware/kernel topology,
    /// upstream timeouts, and whether TLS encryption is active.
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

        Self {
            nodelay: true,
            fastopen,
            notsent_lowat,
            user_timeout,
            keepalive,
        }
    }
}

/// Extension trait for pre-compiling socket acceleration path from hardware topology and timeouts.
pub trait SocketAccelerationPathExt {
    /// Pre-compiles socket acceleration path based on host hardware/kernel topology,
    /// upstream timeouts, and whether TLS encryption is active.
    fn for_topology(
        topo: &velda_core::HardwareTopology,
        timeouts: &crate::upstream::UpstreamTimeouts,
        is_tls: bool,
    ) -> Self;
}

impl SocketAccelerationPathExt for SocketAccelerationPath {
    #[inline]
    fn for_topology(
        topo: &velda_core::HardwareTopology,
        timeouts: &crate::upstream::UpstreamTimeouts,
        is_tls: bool,
    ) -> Self {
        Self::for_topology(topo, timeouts, is_tls)
    }
}

/// Maps a [`velda_core::MemoryTier`] to a `TCP_NOTSENT_LOWAT` byte threshold.
///
/// Upstream owns this sizing (hardware probing in `velda-core` stays policy-free).
/// Keeps a 16KB floor (~11 MTU) so NIC TSO is not starved and epoll is not woken per packet.
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

#[cfg(target_os = "linux")]
pub fn apply_tcp_fastopen(fd: std::os::unix::io::RawFd) {
    let val: libc::c_int = 1;
    unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_FASTOPEN_CONNECT,
            &val as *const _ as *const libc::c_void,
            std::mem::size_of_val(&val) as libc::socklen_t,
        );
    }
}

#[cfg(target_os = "linux")]
pub fn apply_post_connect_acceleration(
    fd: std::os::unix::io::RawFd,
    accel: &SocketAccelerationPath,
) {
    unsafe {
        if let Some(lowat) = accel.notsent_lowat {
            let val = lowat as libc::c_uint;
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_NOTSENT_LOWAT,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of_val(&val) as libc::socklen_t,
            );
        }

        if let Some(user_timeout) = accel.user_timeout {
            let val = user_timeout.as_millis() as libc::c_uint;
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_USER_TIMEOUT,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of_val(&val) as libc::socklen_t,
            );
        }

        if let Some(keepalive) = accel.keepalive {
            let val: libc::c_int = 1;
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_KEEPALIVE,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of_val(&val) as libc::socklen_t,
            );

            let idle_secs = keepalive.as_secs().max(1) as libc::c_int;
            libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_KEEPIDLE,
                &idle_secs as *const _ as *const libc::c_void,
                std::mem::size_of_val(&idle_secs) as libc::socklen_t,
            );
        }
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
