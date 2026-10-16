//! Memory topology and RAM capacity probe.
//!
//! Owns host memory size discovery, container cgroups limits (v1 & v2),
//! environment overrides, and memory profile detection.

/// Discovered memory capacity and allocation profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryProfile {
    /// Total detected or allowed memory in bytes.
    pub total_bytes: usize,
    /// Provenance source indicating how memory limit was resolved.
    pub source: &'static str,
}

impl MemoryProfile {
    /// Creates a memory profile with explicit bytes and provenance source.
    #[inline]
    pub fn new(total_bytes: usize, source: &'static str) -> Self {
        Self {
            total_bytes,
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
    // 1. Check process-specific cgroup v2 path from /proc/self/cgroup
    if let Ok(cgroup_content) = std::fs::read_to_string("/proc/self/cgroup") {
        for line in cgroup_content.lines() {
            let parts: Vec<&str> = line.split(':').collect();
            if parts.len() == 3 {
                let sub = parts[2].trim_start_matches('/');
                if !sub.is_empty() {
                    let path = format!("/sys/fs/cgroup/{sub}/memory.max");
                    if let Ok(content) = std::fs::read_to_string(&path) {
                        let trimmed = content.trim();
                        if trimmed != "max"
                            && let Ok(bytes) = trimmed.parse::<usize>()
                            && bytes > 0
                        {
                            return MemoryProfile::new(bytes, "cgroup_v2");
                        }
                    }
                }
            }
        }
    }

    // 2. Check root cgroup v2 memory limit: /sys/fs/cgroup/memory.max
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

    #[test]
    fn test_probe_memory() {
        let mem = probe_memory();
        assert!(mem.total_bytes >= 1024 * 1024);
        assert!(!mem.source.is_empty());
    }
}
