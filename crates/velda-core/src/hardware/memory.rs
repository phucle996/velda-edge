//! Memory topology and RAM capacity probe.
//!
//! Owns host memory size discovery, container cgroups limits (v1 & v2),
//! environment overrides, and memory tier classification.

/// Canonical memory resource classification tiers for edge buffer and cache sizing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MemoryTier {
    /// Constrained Edge (< 512 MB RAM, e.g., IoT gateways, embedded routers, small cgroups).
    Constrained,
    /// Small Edge (512 MB – 2 GB RAM, e.g., lightweight edge instances).
    Small,
    /// Medium Edge (2 GB – 8 GB RAM, e.g., standard edge nodes).
    Medium,
    /// Large Edge (8 GB – 32 GB RAM, e.g., regional edge nodes, Telco MEC).
    Large,
    /// Extra Large Edge (32 GB – 64 GB RAM, e.g., heavy edge compute instances).
    XLarge,
    /// 2X-Large Edge (64 GB – 128 GB RAM, e.g., high-density edge servers).
    TwoXLarge,
    /// Ultra / Hyperscale (> 128 GB RAM, e.g., massive bare-metal edge datacenters).
    Ultra,
}

impl MemoryTier {
    /// Determines the memory tier from available memory bytes.
    #[inline]
    pub fn from_bytes(bytes: usize) -> Self {
        const MB: usize = 1024 * 1024;
        const GB: usize = 1024 * MB;
        if bytes < 512 * MB {
            Self::Constrained
        } else if bytes <= 2 * GB {
            Self::Small
        } else if bytes <= 8 * GB {
            Self::Medium
        } else if bytes <= 32 * GB {
            Self::Large
        } else if bytes <= 64 * GB {
            Self::XLarge
        } else if bytes <= 128 * GB {
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

impl std::str::FromStr for MemoryTier {
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
                "unknown memory tier: '{other}'"
            ))),
        }
    }
}

/// Discovered memory capacity and allocation profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryProfile {
    /// Total detected or allowed memory in bytes.
    pub total_bytes: usize,
    /// Classified memory resource tier.
    pub tier: MemoryTier,
    /// Provenance source indicating how memory limit was resolved.
    pub source: &'static str,
}

impl MemoryProfile {
    /// Creates a memory profile with explicit bytes and provenance source.
    #[inline]
    pub fn new(total_bytes: usize, source: &'static str) -> Self {
        Self {
            total_bytes,
            tier: MemoryTier::from_bytes(total_bytes),
            source,
        }
    }
}

/// Probes the host memory capacity, respecting container limits.
///
/// Priority Order:
/// 1. Cgroup v2 memory limit: `/sys/fs/cgroup/memory.max`
/// 2. Cgroup v1 memory limit: `/sys/fs/cgroup/memory/memory.limit_in_bytes`
/// 3. Host OS `/proc/meminfo` MemTotal
/// 4. Safe baseline: 1 GB
pub fn probe_memory() -> MemoryProfile {
    // 1. Check cgroup v2 memory limit: /sys/fs/cgroup/memory.max
    if let Ok(content) = std::fs::read_to_string("/sys/fs/cgroup/memory.max") {
        let trimmed = content.trim();
        if trimmed != "max"
            && let Ok(bytes) = trimmed.parse::<usize>()
            && bytes > 0
        {
            return MemoryProfile::new(bytes, "cgroup_v2");
        }
    }

    // 3. Check cgroup v1 memory limit: /sys/fs/cgroup/memory/memory.limit_in_bytes
    if let Ok(content) = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes") {
        let trimmed = content.trim();
        if let Ok(bytes) = trimmed.parse::<usize>()
            && bytes > 0
            && bytes < 0x7FFF_FFFF_FFFF_0000
        {
            return MemoryProfile::new(bytes, "cgroup_v1");
        }
    }

    // 4. Fallback to /proc/meminfo MemTotal (Linux bare-metal)
    if let Ok(content) = std::fs::read_to_string("/proc/meminfo") {
        for line in content.lines() {
            if line.starts_with("MemTotal:") {
                let mut parts = line.split_whitespace();
                let _ = parts.next();
                if let Some(kb_str) = parts.next()
                    && let Ok(kb) = kb_str.parse::<usize>()
                {
                    return MemoryProfile::new(kb.saturating_mul(1024), "proc_meminfo");
                }
            }
        }
    }

    // 5. Default safe baseline: 1 GB
    MemoryProfile::new(1024 * 1024 * 1024, "default_baseline")
}

#[cfg(test)]
mod tests {
    use super::*;

    const MB: usize = 1024 * 1024;
    const GB: usize = 1024 * MB;

    #[test]
    fn test_memory_tier_from_bytes() {
        assert_eq!(MemoryTier::from_bytes(256 * MB), MemoryTier::Constrained);
        assert_eq!(MemoryTier::from_bytes(1024 * MB), MemoryTier::Small);
        assert_eq!(MemoryTier::from_bytes(4 * GB), MemoryTier::Medium);
        assert_eq!(MemoryTier::from_bytes(16 * GB), MemoryTier::Large);
        assert_eq!(MemoryTier::from_bytes(48 * GB), MemoryTier::XLarge);
        assert_eq!(MemoryTier::from_bytes(96 * GB), MemoryTier::TwoXLarge);
        assert_eq!(MemoryTier::from_bytes(256 * GB), MemoryTier::Ultra);
    }

    #[test]
    fn test_memory_tier_str_roundtrip() {
        let tiers = [
            MemoryTier::Constrained,
            MemoryTier::Small,
            MemoryTier::Medium,
            MemoryTier::Large,
            MemoryTier::XLarge,
            MemoryTier::TwoXLarge,
            MemoryTier::Ultra,
        ];
        for tier in tiers {
            assert_eq!(tier.as_str().parse::<MemoryTier>().unwrap(), tier);
        }
        assert_eq!(
            "twoxlarge".parse::<MemoryTier>().unwrap(),
            MemoryTier::TwoXLarge
        );
        assert!("invalid".parse::<MemoryTier>().is_err());
    }

    #[test]
    fn test_probe_memory() {
        let mem = probe_memory();
        assert!(mem.total_bytes >= 1024 * 1024);
        assert!(!mem.source.is_empty());
    }
}
