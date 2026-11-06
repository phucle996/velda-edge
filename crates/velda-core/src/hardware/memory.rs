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
    /// Detected RAM technology (e.g. "DDR4", "DDR5", or "DRAM").
    pub ram_type: &'static str,
    /// Detected memory speed in MT/s or MHz (0 if unreadable/virtualized).
    pub speed_mhz: u16,
}

impl MemoryProfile {
    /// Creates a memory profile with explicit bytes and provenance source.
    #[inline]
    pub fn new(total_bytes: usize, source: &'static str) -> Self {
        Self {
            total_bytes,
            source,
            ram_type: "DRAM",
            speed_mhz: 0,
        }
    }

    /// Formats a clean summary string for the startup banner.
    /// E.g. "32.0 GB DDR4-3200" or "30.5 GB DRAM".
    pub fn format_summary(&self) -> String {
        let ram_gb = self.total_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        if self.speed_mhz > 0 && self.ram_type != "DRAM" {
            format!("{:.1} GB {}-{}", ram_gb, self.ram_type, self.speed_mhz)
        } else if self.ram_type != "DRAM" {
            format!("{:.1} GB {}", ram_gb, self.ram_type)
        } else {
            format!("{:.1} GB DRAM", ram_gb)
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
    let (total_bytes, source) = probe_memory_capacity();
    let (ram_type, speed_mhz) = probe_dmi_ram();
    MemoryProfile {
        total_bytes,
        source,
        ram_type,
        speed_mhz,
    }
}

fn probe_memory_capacity() -> (usize, &'static str) {
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
                            return (bytes, "cgroup_v2");
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
            return (bytes, "cgroup_v2");
        }
    }

    // 3. Check cgroup v1 memory limit: /sys/fs/cgroup/memory/memory.limit_in_bytes
    if let Ok(content) = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes") {
        let trimmed = content.trim();
        if let Ok(bytes) = trimmed.parse::<usize>()
            && bytes > 0
            && bytes < 0x7FFF_FFFF_FFFF_0000
        {
            return (bytes, "cgroup_v1");
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
                    return (kb.saturating_mul(1024), "proc_meminfo");
                }
            }
        }
    }

    // 5. Default safe baseline: 1 GB
    (1024 * 1024 * 1024, "default_baseline")
}

/// Probes RAM technology (DDR4/DDR5) and speed from Linux sysfs / DMI tables.
pub fn probe_dmi_ram() -> (&'static str, u16) {
    if let Ok(data) = std::fs::read("/sys/firmware/dmi/tables/DMI") {
        let (t, s) = parse_dmi_table(&data);
        if t != "DRAM" {
            return (t, s);
        }
    }
    ("DRAM", 0)
}

/// Parses raw SMBIOS/DMI table binary data to detect Type 17 (Memory Device) records.
pub fn parse_dmi_table(data: &[u8]) -> (&'static str, u16) {
    let mut offset = 0;
    while offset + 4 <= data.len() {
        let rec_type = data[offset];
        let rec_len = data[offset + 1] as usize;
        if rec_len < 4 || offset + rec_len > data.len() {
            break;
        }

        // SMBIOS Type 17 = Memory Device
        if rec_type == 17 && rec_len >= 23 {
            let mem_type_byte = data[offset + 18];
            let speed = u16::from_le_bytes([data[offset + 21], data[offset + 22]]);

            let ram_type = match mem_type_byte {
                0x1A => "DDR4",
                0x22 => "DDR5",
                0x18 => "DDR3",
                0x1F => "LPDDR4",
                0x23 => "LPDDR5",
                _ => "DRAM",
            };

            if ram_type != "DRAM" {
                return (ram_type, speed);
            }
        }

        // Jump over formatted section
        offset += rec_len;
        // Jump over unformatted null-terminated string section (ends with \0\0)
        while offset + 1 < data.len() {
            if data[offset] == 0 && data[offset + 1] == 0 {
                offset += 2;
                break;
            }
            offset += 1;
        }
    }

    ("DRAM", 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_probe_memory() {
        let mem = probe_memory();
        assert!(mem.total_bytes >= 1024 * 1024);
        assert!(!mem.source.is_empty());
        assert!(!mem.format_summary().is_empty());
    }

    #[test]
    fn test_parse_dmi_table_ddr4_3200() {
        // Construct mock SMBIOS Type 17 record:
        // offset 0: type 17
        // offset 1: length 24
        // offset 2-3: handle 0x0001
        // offset 18: 0x1A (DDR4)
        // offset 21-22: 3200 in little endian (0x0C80 -> [0x80, 0x0C])
        let mut dmi = vec![0u8; 24];
        dmi[0] = 17; // Type 17
        dmi[1] = 24; // Length 24
        dmi[18] = 0x1A; // DDR4
        dmi[21] = 0x80;
        dmi[22] = 0x0C; // 3200 MHz
        dmi.push(0); // string terminator
        dmi.push(0); // record terminator

        let (ram_type, speed) = parse_dmi_table(&dmi);
        assert_eq!(ram_type, "DDR4");
        assert_eq!(speed, 3200);

        let mut prof = MemoryProfile::new(32 * 1024 * 1024 * 1024, "test");
        prof.ram_type = ram_type;
        prof.speed_mhz = speed;
        assert_eq!(prof.format_summary(), "32.0 GB DDR4-3200");
    }
}
