//! CPU Topology & Core Concurrency Probe (`velda_core::hardware::cpu`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                       COLD-START CPU CONCURRENCY PROBE PRIORITY                         │
//! ├─────────────────────────────────────────────────────────────────────────────────────────┤
//! │  [1] VELDA_WORKER_THREADS env   ──► [2] cgroup v2 cpu.max  ──► [3] cgroup v1 cfs_quota  │
//! │                                                                     │                   │
//! │  CpuProfile { available_cores: usize, source: &'static str } ◄── [4] OS logical CPUs   │
//! └─────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

/// Discovered CPU concurrency and execution profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CpuProfile {
    /// Number of logical CPU cores available to the process.
    pub available_cores: usize,
    /// Provenance source indicating how CPU capacity was resolved.
    pub source: &'static str,
}

impl CpuProfile {
    /// Creates a CPU profile with an explicit core count.
    #[inline]
    pub fn new(available_cores: usize, source: &'static str) -> Self {
        let cores = available_cores.max(1);
        Self {
            available_cores: cores,
            source,
        }
    }
}

/// Signature for bulk byte copy functions.
pub type CopyBulkFn = fn(src: &[u8], dst: &mut [u8]);

/// Pre-resolved static function pointer for bulk payload transfers.
pub static COPY_BULK_FN: std::sync::LazyLock<CopyBulkFn> =
    std::sync::LazyLock::new(|| copy_bulk_payload);

/// Copies bulk byte payloads using pure Rust slice copying so LLVM auto-vectorizes for the target CPU.
#[inline]
pub fn copy_bulk_payload(src: &[u8], dst: &mut [u8]) {
    let len = src.len().min(dst.len());
    dst[..len].copy_from_slice(&src[..len]);
}

/// Probes the host CPU topology, respecting container limits.
///
/// Priority Order:
/// 1. Cgroup v2 CPU bandwidth limit: `/sys/fs/cgroup/cpu.max`
/// 2. Cgroup v1 CFS quota limit: `/sys/fs/cgroup/cpu/cpu.cfs_quota_us` & `cpu.cfs_period_us`
/// 3. Host OS parallelism: `std::thread::available_parallelism()`
/// 4. Safe baseline: 1 core
pub fn probe_cpu() -> CpuProfile {
    // 1. Check cgroup v2 cpu.max: "quota period" (e.g., "200000 100000")
    if let Ok(content) = std::fs::read_to_string("/sys/fs/cgroup/cpu.max") {
        let mut parts = content.split_whitespace();
        if let (Some(quota_str), Some(period_str)) = (parts.next(), parts.next())
            && quota_str != "max"
            && let (Ok(quota), Ok(period)) =
                (quota_str.parse::<usize>(), period_str.parse::<usize>())
            && period > 0
        {
            let cores = (quota / period).max(1);
            return CpuProfile::new(cores, "cgroup_v2");
        }
    }

    // 2. Check cgroup v1 CFS quota & period
    if let (Ok(quota_content), Ok(period_content)) = (
        std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"),
        std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_period_us"),
    ) && let (Ok(quota), Ok(period)) = (
        quota_content.trim().parse::<i64>(),
        period_content.trim().parse::<i64>(),
    ) && quota > 0
        && period > 0
    {
        let cores = (quota as usize / period as usize).max(1);
        return CpuProfile::new(cores, "cgroup_v1");
    }

    // 3. Host OS available parallelism
    let host_cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    CpuProfile::new(host_cores, "host_sysinfo")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_probe_cpu() {
        let cpu = probe_cpu();
        assert!(cpu.available_cores >= 1);
        assert!(!cpu.source.is_empty());
    }

    #[test]
    fn test_copy_bulk_payload_boundaries() {
        for size in [
            0, 1, 3, 4, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 1024, 65536,
        ] {
            let src: Vec<u8> = (0..size).map(|i| (i % 255) as u8).collect();

            let mut dst_fn = vec![0u8; size];
            copy_bulk_payload(&src, &mut dst_fn);
            assert_eq!(
                src, dst_fn,
                "Mismatch in copy_bulk_payload at size {}",
                size
            );

            let mut dst_static = vec![0u8; size];
            (*COPY_BULK_FN)(&src, &mut dst_static);
            assert_eq!(src, dst_static, "Mismatch in COPY_BULK_FN at size {}", size);
        }
    }
}
