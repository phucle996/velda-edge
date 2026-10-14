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

impl std::str::FromStr for CpuTier {
    type Err = super::ParseTierError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "constrained" => Ok(Self::Constrained),
            "small" => Ok(Self::Small),
            "medium" => Ok(Self::Medium),
            "large" => Ok(Self::Large),
            "xlarge" => Ok(Self::XLarge),
            "2xlarge" | "twoxlarge" => Ok(Self::TwoXLarge),
            "ultra" => Ok(Self::Ultra),
            other => Err(super::ParseTierError(format!(
                "unknown cpu tier: '{other}'"
            ))),
        }
    }
}

/// Hardware SIMD & vector instruction capabilities detected at runtime via CPUID.
///
/// Enables portable binaries to dynamically activate AVX-512 or AVX2 hardware acceleration
/// without risking SIGILL crashes on older cloud VPS nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CpuVectorCapabilities {
    /// Intel/AMD AVX2 (Advanced Vector Extensions 2: 256-bit integer/float SIMD, FMA, BMI1/BMI2).
    pub avx2: bool,
    /// Intel Xeon / AMD EPYC Zen 4 AVX-512 Foundation (512-bit vector processing).
    pub avx512f: bool,
    /// ARM64 NEON Vector Extensions (128-bit SIMD on AWS Graviton / Apple Silicon / Ampere).
    pub neon: bool,
}

impl CpuVectorCapabilities {
    /// Probes CPUID hardware capabilities at runtime with zero risk of SIGILL.
    pub fn detect() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            Self {
                avx2: std::is_x86_feature_detected!("avx2"),
                avx512f: std::is_x86_feature_detected!("avx512f"),
                neon: false,
            }
        }
        #[cfg(target_arch = "aarch64")]
        {
            Self {
                avx2: false,
                avx512f: false,
                neon: std::arch::is_aarch64_feature_detected!("neon"),
            }
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            Self {
                avx2: false,
                avx512f: false,
                neon: false,
            }
        }
    }

    /// Returns a human-readable summary of detected SIMD acceleration for startup logging.
    pub const fn summary(&self) -> &'static str {
        if self.avx512f {
            "AVX-512 (512-bit Vector Engine)"
        } else if self.avx2 {
            "AVX2 + FMA (256-bit Vector Engine)"
        } else if self.neon {
            "ARM NEON (128-bit Vector Engine)"
        } else {
            "Standard Scalar Baseline"
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
    /// Hardware SIMD & vector instruction capabilities detected at runtime via CPUID.
    pub vector_capabilities: CpuVectorCapabilities,
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
            vector_capabilities: CpuVectorCapabilities::detect(),
        }
    }
}

/// Copies bulk byte payloads using runtime dynamic dispatch to the widest vector registers available on host CPU.
///
/// # Hardware Rationale
/// Under large payloads (such as 1MB HTTP/3 responses), scalar byte moves consume significant L1 cache
/// and CPU execution ports. On CPUs with AVX-512 (e.g. Intel Xeon Ice Lake / Sapphire Rapids or AMD EPYC Zen 4),
/// this function transfers 64 bytes per cycle using 512-bit ZMM registers. On AVX2 systems, it transfers
/// 32 bytes per cycle. On older systems, it transparently falls back to optimized libc memcpy.
#[inline]
pub fn copy_bulk_payload(src: &[u8], dst: &mut [u8]) {
    let len = src.len().min(dst.len());
    if len == 0 {
        return;
    }

    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx512f") && len >= 64 {
            unsafe {
                copy_bulk_avx512(&src[..len], &mut dst[..len]);
                return;
            }
        }
        if std::is_x86_feature_detected!("avx2") && len >= 32 {
            unsafe {
                copy_bulk_avx2(&src[..len], &mut dst[..len]);
                return;
            }
        }
    }

    dst[..len].copy_from_slice(&src[..len]);
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn copy_bulk_avx512(src: &[u8], dst: &mut [u8]) {
    use std::arch::x86_64::*;
    let mut i = 0;
    while i + 64 <= src.len() {
        unsafe {
            let chunk = _mm512_loadu_si512(src.as_ptr().add(i) as *const _);
            _mm512_storeu_si512(dst.as_mut_ptr().add(i) as *mut _, chunk);
        }
        i += 64;
    }
    if i < src.len() {
        dst[i..].copy_from_slice(&src[i..]);
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn copy_bulk_avx2(src: &[u8], dst: &mut [u8]) {
    use std::arch::x86_64::*;
    let mut i = 0;
    while i + 32 <= src.len() {
        unsafe {
            let chunk = _mm256_loadu_si256(src.as_ptr().add(i) as *const _);
            _mm256_storeu_si256(dst.as_mut_ptr().add(i) as *mut _, chunk);
        }
        i += 32;
    }
    if i < src.len() {
        dst[i..].copy_from_slice(&src[i..]);
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
    fn test_cpu_tier_str_roundtrip() {
        let tiers = [
            CpuTier::Constrained,
            CpuTier::Small,
            CpuTier::Medium,
            CpuTier::Large,
            CpuTier::XLarge,
            CpuTier::TwoXLarge,
            CpuTier::Ultra,
        ];
        for tier in tiers {
            assert_eq!(tier.as_str().parse::<CpuTier>().unwrap(), tier);
        }
        assert_eq!("twoxlarge".parse::<CpuTier>().unwrap(), CpuTier::TwoXLarge);
        assert!("invalid".parse::<CpuTier>().is_err());
    }

    #[test]
    fn test_probe_cpu() {
        let cpu = probe_cpu();
        assert!(cpu.available_cores >= 1);
        assert!(!cpu.source.is_empty());
        assert!(!cpu.vector_capabilities.summary().is_empty());
    }

    #[test]
    fn test_copy_bulk_payload() {
        // Test varying boundary sizes: 0B, 15B, 32B, 64B, 128B, 1024B, 65536B
        for size in [0, 7, 31, 32, 63, 64, 127, 128, 1024, 65536] {
            let src: Vec<u8> = (0..size).map(|i| (i % 255) as u8).collect();
            let mut dst = vec![0u8; size];
            copy_bulk_payload(&src, &mut dst);
            assert_eq!(src, dst, "Mismatch at payload size {}", size);
        }
    }
}
