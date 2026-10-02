//! CPU topology and core concurrency probe.
//!
//! Owns host CPU core discovery, container cgroups limits (v1 & v2),
//! environment overrides, and CPU tier classification.

/// Canonical CPU resource classification tiers for edge concurrency sizing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CpuTier {
    /// Constrained Edge (1 – 2 cores, e.g., IoT gateways, embedded routers, small cgroups).
    Constrained,
    /// Small Edge (3 – 4 cores, e.g., standard micro instances).
    Small,
    /// Medium Edge (5 – 8 cores, e.g., standard 8-core workhorses).
    Medium,
    /// Large Edge (9 – 16 cores, e.g., regional edge nodes, Telco MEC).
    Large,
    /// Extra Large Edge (17 – 32 cores, e.g., heavy edge compute instances).
    XLarge,
    /// 2X-Large Edge (33 – 64 cores, e.g., high-density dual-socket edge servers).
    TwoXLarge,
    /// Ultra / Hyperscale (> 64 cores, e.g., massive bare-metal edge datacenters).
    Ultra,
}

impl CpuTier {
    /// Determines the CPU tier from available logical cores.
    #[inline]
    pub fn from_cores(cores: usize) -> Self {
        if cores <= 2 {
            Self::Constrained
        } else if cores <= 4 {
            Self::Small
        } else if cores <= 8 {
            Self::Medium
        } else if cores <= 16 {
            Self::Large
        } else if cores <= 32 {
            Self::XLarge
        } else if cores <= 64 {
            Self::TwoXLarge
        } else {
            Self::Ultra
        }
    }

    /// Returns the canonical kebab-case string representation of this tier.
    #[inline]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Constrained => "constrained",
            Self::Small => "small",
            Self::Medium => "medium",
            Self::Large => "large",
            Self::XLarge => "xlarge",
            Self::TwoXLarge => "2xlarge",
            Self::Ultra => "ultra",
        }
    }
}

/// Discovered CPU concurrency and execution profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CpuProfile {
    /// Number of logical CPU cores available to the process.
    pub available_cores: usize,
    /// Classified CPU resource tier.
    pub tier: CpuTier,
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
            tier: CpuTier::from_cores(cores),
            source,
        }
    }
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
    fn test_cpu_tier_from_cores() {
        assert_eq!(CpuTier::from_cores(1), CpuTier::Constrained);
        assert_eq!(CpuTier::from_cores(2), CpuTier::Constrained);
        assert_eq!(CpuTier::from_cores(3), CpuTier::Small);
        assert_eq!(CpuTier::from_cores(4), CpuTier::Small);
        assert_eq!(CpuTier::from_cores(6), CpuTier::Medium);
        assert_eq!(CpuTier::from_cores(8), CpuTier::Medium);
        assert_eq!(CpuTier::from_cores(12), CpuTier::Large);
        assert_eq!(CpuTier::from_cores(16), CpuTier::Large);
        assert_eq!(CpuTier::from_cores(24), CpuTier::XLarge);
        assert_eq!(CpuTier::from_cores(32), CpuTier::XLarge);
        assert_eq!(CpuTier::from_cores(48), CpuTier::TwoXLarge);
        assert_eq!(CpuTier::from_cores(64), CpuTier::TwoXLarge);
        assert_eq!(CpuTier::from_cores(96), CpuTier::Ultra);
        assert_eq!(CpuTier::from_cores(128), CpuTier::Ultra);
    }

    #[test]
    fn test_cpu_tier_as_str() {
        assert_eq!(CpuTier::Constrained.as_str(), "constrained");
        assert_eq!(CpuTier::Small.as_str(), "small");
        assert_eq!(CpuTier::Medium.as_str(), "medium");
        assert_eq!(CpuTier::Large.as_str(), "large");
        assert_eq!(CpuTier::XLarge.as_str(), "xlarge");
        assert_eq!(CpuTier::TwoXLarge.as_str(), "2xlarge");
        assert_eq!(CpuTier::Ultra.as_str(), "ultra");
    }

    #[test]
    fn test_probe_cpu_returns_valid_profile() {
        let cpu = probe_cpu();
        assert!(cpu.available_cores >= 1);
        assert!(!cpu.source.is_empty());
    }
}
