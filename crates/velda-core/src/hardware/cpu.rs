//! CPU topology and core concurrency probe.
//!
//! Owns host CPU core discovery, container cgroups limits (v1 & v2),
//! environment overrides, and CPU profile detection.

/// Canonical hardware register width representation.
///
/// Strictly vendor-neutral (represented by bit-width rather than vendor extensions).
/// Capped at 256 bits maximum to prevent thermal throttling and CPU core downclocking
/// induced by wider (e.g. 512-bit) vector units on Intel Xeon and AMD EPYC platforms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u16)]
pub enum RegisterWidth {
    /// 32-bit scalar general-purpose registers (or 32-bit SWAR emulation).
    Bits32 = 32,
    /// 64-bit scalar general-purpose registers (standard 64-bit ALU).
    Bits64 = 64,
    /// 128-bit vector registers (e.g. SSE2 / NEON).
    Bits128 = 128,
    /// 256-bit vector registers (e.g. AVX2 / 256-bit SIMD).
    Bits256 = 256,
}

/// Hardware register width capabilities detected at runtime via CPUID.
///
/// # Architectural Invariant
/// - Strictly capped at 256-bit max. AVX-512 is deliberately excluded and banned to prevent
///   frequency drops (downclocking) that degrade frontline worker threads.
/// - Vendor-agnostic: workloads query required bit-widths (`Bits32`, `Bits64`, `Bits128`, `Bits256`)
///   rather than vendor instruction names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CpuRegisterCapabilities {
    /// 32-bit arithmetic supported.
    pub has_32: bool,
    /// 64-bit arithmetic supported.
    pub has_64: bool,
    /// 128-bit vector registers supported.
    pub has_128: bool,
    /// 256-bit vector registers supported (hardware ceiling).
    pub has_256: bool,
    /// Maximum supported register width on host.
    pub max_width: RegisterWidth,
}

impl CpuRegisterCapabilities {
    /// Probes hardware register widths at runtime with zero risk of SIGILL.
    pub fn detect() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            let has_32 = true;
            let has_64 = true;
            let has_128 = true; // SSE2 is mandatory baseline on x86_64
            let has_256 = std::is_x86_feature_detected!("avx2");
            let max_width = if has_256 {
                RegisterWidth::Bits256
            } else {
                RegisterWidth::Bits128
            };
            Self {
                has_32,
                has_64,
                has_128,
                has_256,
                max_width,
            }
        }
        #[cfg(target_arch = "aarch64")]
        {
            let has_32 = true;
            let has_64 = true;
            let has_128 = std::arch::is_aarch64_feature_detected!("neon");
            let has_256 = false; // NEON is 128-bit
            let max_width = if has_128 {
                RegisterWidth::Bits128
            } else {
                RegisterWidth::Bits64
            };
            Self {
                has_32,
                has_64,
                has_128,
                has_256,
                max_width,
            }
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            let has_32 = true;
            let has_64 = cfg!(target_pointer_width = "64");
            let has_128 = false;
            let has_256 = false;
            let max_width = if has_64 {
                RegisterWidth::Bits64
            } else {
                RegisterWidth::Bits32
            };
            Self {
                has_32,
                has_64,
                has_128,
                has_256,
                max_width,
            }
        }
    }

    /// Selects the optimal hardware register tier for a workload's declared ceiling.
    ///
    /// Workloads do not use fallback branches; instead they declare their target register width.
    /// The selector returns the exact matching register width supported by the host up to that ceiling:
    /// - Workloads targeting 64-bit use `Bits64` (or split into `Bits32` on 32-bit hardware).
    /// - Workloads targeting 256-bit use `Bits256` if available, otherwise `Bits128`, `Bits64`, or `Bits32`.
    #[inline]
    pub fn select_for_workload(&self, target_max: RegisterWidth) -> RegisterWidth {
        match target_max {
            RegisterWidth::Bits256 => {
                if self.has_256 {
                    RegisterWidth::Bits256
                } else if self.has_128 {
                    RegisterWidth::Bits128
                } else if self.has_64 {
                    RegisterWidth::Bits64
                } else {
                    RegisterWidth::Bits32
                }
            }
            RegisterWidth::Bits128 => {
                if self.has_128 {
                    RegisterWidth::Bits128
                } else if self.has_64 {
                    RegisterWidth::Bits64
                } else {
                    RegisterWidth::Bits32
                }
            }
            RegisterWidth::Bits64 => {
                if self.has_64 {
                    RegisterWidth::Bits64
                } else {
                    RegisterWidth::Bits32
                }
            }
            RegisterWidth::Bits32 => RegisterWidth::Bits32,
        }
    }

    /// Adapts the register width to the workload element count (e.g. number of 64-bit quadwords)
    /// to avoid wasteful wide-register setup for narrow workloads:
    /// - `num_quadwords >= 4` -> targets `Bits256` (AVX2-grade)
    /// - `2 <= num_quadwords < 4` -> targets `Bits128` (SSE2 / NEON)
    /// - `num_quadwords == 1` -> targets `Bits64` (Standard scalar ALU)
    /// - `num_quadwords == 0` -> `Bits32` / minimal tier.
    #[inline]
    pub fn select_for_workload_size(&self, num_quadwords: usize) -> RegisterWidth {
        let target = if num_quadwords >= 4 {
            RegisterWidth::Bits256
        } else if num_quadwords >= 2 {
            RegisterWidth::Bits128
        } else if num_quadwords == 1 {
            RegisterWidth::Bits64
        } else {
            RegisterWidth::Bits32
        };
        self.select_for_workload(target)
    }

    /// Adapts the register width for bulk byte transfers according to byte buffer length.
    ///
    /// Buffers below 32 bytes adapt to 128-bit or 64-bit to eliminate 256-bit vector setup overhead:
    /// - `byte_len >= 32` -> targets `Bits256`
    /// - `16 <= byte_len < 32` -> targets `Bits128`
    /// - `8 <= byte_len < 16` -> targets `Bits64`
    /// - `byte_len < 8` -> `Bits32`
    #[inline]
    pub fn select_for_byte_len(&self, byte_len: usize) -> RegisterWidth {
        let target = if byte_len >= 32 {
            RegisterWidth::Bits256
        } else if byte_len >= 16 {
            RegisterWidth::Bits128
        } else if byte_len >= 8 {
            RegisterWidth::Bits64
        } else {
            RegisterWidth::Bits32
        };
        self.select_for_workload(target)
    }

    /// Checks if a specific register width is natively supported by the host.
    #[inline]
    pub const fn is_supported(&self, width: RegisterWidth) -> bool {
        match width {
            RegisterWidth::Bits32 => self.has_32,
            RegisterWidth::Bits64 => self.has_64,
            RegisterWidth::Bits128 => self.has_128,
            RegisterWidth::Bits256 => self.has_256,
        }
    }

    /// Returns a human-readable summary of detected register acceleration for startup logging.
    pub const fn summary(&self) -> &'static str {
        match self.max_width {
            RegisterWidth::Bits256 => "256-bit Vector Register Engine",
            RegisterWidth::Bits128 => "128-bit Vector Register Engine",
            RegisterWidth::Bits64 => "64-bit Scalar Register Engine",
            RegisterWidth::Bits32 => "32-bit Register Engine",
        }
    }
}

/// Discovered CPU concurrency and execution profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CpuProfile {
    /// Number of logical CPU cores available to the process (respects container cgroups limits).
    pub available_cores: usize,
    /// Physical CPU cores per socket on host (e.g. 6 in a 6C/12T CPU; 0 if unknown).
    pub physical_cores: u16,
    /// Logical sibling threads per socket on host (e.g. 12 in a 6C/12T CPU; 0 if unknown).
    pub logical_cores: u16,
    /// Canonical CPU brand/model name (e.g. "11th Gen Intel Core i5-11400H @ 2.70GHz").
    pub model_name: &'static str,
    /// Provenance source indicating how CPU capacity was resolved.
    pub source: &'static str,
    /// Hardware register width capabilities detected at runtime via CPUID.
    pub register_capabilities: CpuRegisterCapabilities,
}

impl CpuProfile {
    /// Creates a CPU profile with an explicit core count and default generic model.
    #[inline]
    pub fn new(available_cores: usize, source: &'static str) -> Self {
        let cores = available_cores.max(1);
        Self {
            available_cores: cores,
            physical_cores: cores as u16,
            logical_cores: cores as u16,
            model_name: "Generic CPU",
            source,
            register_capabilities: CpuRegisterCapabilities::detect(),
        }
    }

    /// Formats a clean descriptive string for banner and telemetry.
    /// E.g. "11th Gen Intel Core i5-11400H @ 2.70GHz (6C/12T)" or "12 Cores".
    pub fn format_model_and_topology(&self) -> String {
        if self.physical_cores > 0 && self.logical_cores > 0 && self.model_name != "Generic CPU" {
            format!(
                "{} ({}C/{}T)",
                self.model_name, self.physical_cores, self.logical_cores
            )
        } else {
            format!("{} Cores", self.available_cores)
        }
    }
}

/// Signature for register-width-specific bulk byte copy functions.
pub type CopyBulkFn = fn(src: &[u8], dst: &mut [u8]);

/// Pre-resolved static function pointer for bulk payload transfers.
///
/// Bound once at boot via `select_for_workload(RegisterWidth::Bits256)` with zero hot-path branching.
pub static COPY_BULK_FN: std::sync::LazyLock<CopyBulkFn> = std::sync::LazyLock::new(|| {
    let caps = CpuRegisterCapabilities::detect();
    match caps.select_for_workload(RegisterWidth::Bits256) {
        RegisterWidth::Bits256 => copy_bulk_256,
        RegisterWidth::Bits128 => copy_bulk_128,
        RegisterWidth::Bits64 => copy_bulk_64,
        RegisterWidth::Bits32 => copy_bulk_32,
    }
});

/// Copies bulk byte payloads using workload-adaptive hardware register acceleration (up to 256-bit max).
///
/// # Hardware Invariant
/// - Capped strictly at 256 bits (`Bits256`). AVX-512 is never executed to avoid core downclocking.
/// - Workload-Adaptive: Short payloads (< 32 bytes) adapt directly to 128-bit or 64-bit scalar
///   execution to eliminate wasteful 256-bit vector register setup and YMM context switching.
#[inline]
pub fn copy_bulk_payload(src: &[u8], dst: &mut [u8]) {
    let len = src.len().min(dst.len());
    if len == 0 {
        return;
    }
    if len < 16 {
        copy_bulk_64(&src[..len], &mut dst[..len]);
    } else if len < 32 {
        copy_bulk_128(&src[..len], &mut dst[..len]);
    } else {
        (*COPY_BULK_FN)(&src[..len], &mut dst[..len]);
    }
}

/// Bulk byte copy using 256-bit vector registers (32 bytes per iteration).
#[inline]
pub fn copy_bulk_256(src: &[u8], dst: &mut [u8]) {
    let len = src.len().min(dst.len());
    #[cfg(target_arch = "x86_64")]
    {
        unsafe {
            copy_bulk_256_x86_impl(&src[..len], &mut dst[..len]);
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        copy_bulk_128(&src[..len], &mut dst[..len]);
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn copy_bulk_256_x86_impl(src: &[u8], dst: &mut [u8]) {
    use std::arch::x86_64::*;
    let len = src.len();
    let vec_len = len & !31;
    let mut i = 0;
    while i < vec_len {
        unsafe {
            let chunk = _mm256_loadu_si256(src.as_ptr().add(i) as *const __m256i);
            _mm256_storeu_si256(dst.as_mut_ptr().add(i) as *mut __m256i, chunk);
        }
        i += 32;
    }
    if vec_len < len {
        dst[vec_len..len].copy_from_slice(&src[vec_len..len]);
    }
}

/// Bulk byte copy using 128-bit vector registers (16 bytes per iteration).
#[inline]
pub fn copy_bulk_128(src: &[u8], dst: &mut [u8]) {
    let len = src.len().min(dst.len());
    #[cfg(target_arch = "x86_64")]
    {
        unsafe {
            copy_bulk_128_x86_impl(&src[..len], &mut dst[..len]);
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        unsafe {
            copy_bulk_128_neon_impl(&src[..len], &mut dst[..len]);
        }
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        copy_bulk_64(&src[..len], &mut dst[..len]);
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn copy_bulk_128_x86_impl(src: &[u8], dst: &mut [u8]) {
    use std::arch::x86_64::*;
    let len = src.len();
    let vec_len = len & !15;
    let mut i = 0;
    while i < vec_len {
        unsafe {
            let chunk = _mm_loadu_si128(src.as_ptr().add(i) as *const __m128i);
            _mm_storeu_si128(dst.as_mut_ptr().add(i) as *mut __m128i, chunk);
        }
        i += 16;
    }
    if vec_len < len {
        dst[vec_len..len].copy_from_slice(&src[vec_len..len]);
    }
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn copy_bulk_128_neon_impl(src: &[u8], dst: &mut [u8]) {
    use std::arch::aarch64::*;
    let len = src.len();
    let vec_len = len & !15;
    let mut i = 0;
    while i < vec_len {
        unsafe {
            let chunk = vld1q_u8(src.as_ptr().add(i));
            vst1q_u8(dst.as_mut_ptr().add(i), chunk);
        }
        i += 16;
    }
    if vec_len < len {
        dst[vec_len..len].copy_from_slice(&src[vec_len..len]);
    }
}

/// Bulk byte copy using 64-bit scalar registers (8 bytes per iteration).
#[inline]
pub fn copy_bulk_64(src: &[u8], dst: &mut [u8]) {
    let len = src.len().min(dst.len());
    let word_len = len & !7;
    let mut i = 0;
    while i < word_len {
        unsafe {
            let w = std::ptr::read_unaligned(src.as_ptr().add(i) as *const u64);
            std::ptr::write_unaligned(dst.as_mut_ptr().add(i) as *mut u64, w);
        }
        i += 8;
    }
    while i < len {
        dst[i] = src[i];
        i += 1;
    }
}

/// Bulk byte copy using 32-bit scalar registers (4 bytes per iteration).
#[inline]
pub fn copy_bulk_32(src: &[u8], dst: &mut [u8]) {
    let len = src.len().min(dst.len());
    let word_len = len & !3;
    let mut i = 0;
    while i < word_len {
        unsafe {
            let w = std::ptr::read_unaligned(src.as_ptr().add(i) as *const u32);
            std::ptr::write_unaligned(dst.as_mut_ptr().add(i) as *mut u32, w);
        }
        i += 4;
    }
    while i < len {
        dst[i] = src[i];
        i += 1;
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
    let (cores, source) = if let Ok(content) = std::fs::read_to_string("/sys/fs/cgroup/cpu.max") {
        let mut parts = content.split_whitespace();
        if let (Some(quota_str), Some(period_str)) = (parts.next(), parts.next())
            && quota_str != "max"
            && let (Ok(quota), Ok(period)) =
                (quota_str.parse::<usize>(), period_str.parse::<usize>())
            && period > 0
        {
            ((quota / period).max(1), "cgroup_v2")
        } else {
            probe_cgroup_v1_or_host()
        }
    } else {
        probe_cgroup_v1_or_host()
    };

    let (model_name, physical_cores, logical_cores) = probe_cpuinfo_details();
    let mut profile = CpuProfile::new(cores, source);
    profile.model_name = model_name;
    if physical_cores > 0 {
        profile.physical_cores = physical_cores;
    }
    if logical_cores > 0 {
        profile.logical_cores = logical_cores;
    }
    profile
}

fn probe_cgroup_v1_or_host() -> (usize, &'static str) {
    if let (Ok(quota_content), Ok(period_content)) = (
        std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"),
        std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_period_us"),
    ) && let (Ok(quota), Ok(period)) = (
        quota_content.trim().parse::<i64>(),
        period_content.trim().parse::<i64>(),
    ) && quota > 0
        && period > 0
    {
        ((quota as usize / period as usize).max(1), "cgroup_v1")
    } else {
        let host_cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        (host_cores, "host_sysinfo")
    }
}

/// Parses `/proc/cpuinfo` content to extract CPU model name, physical cores, and logical threads.
pub fn parse_cpuinfo_content(content: &str) -> (&'static str, u16, u16) {
    let mut model_name: Option<String> = None;
    let mut physical_cores: u16 = 0;
    let mut siblings: u16 = 0;
    let mut processor_count: u16 = 0;

    for line in content.lines() {
        let trimmed = line.trim();
        if let Some((key, val)) = trimmed.split_once(':') {
            let k = key.trim();
            let v = val.trim();
            if (k == "model name" || k == "Model" || k == "Processor" || k == "Hardware")
                && model_name.is_none()
                && !v.is_empty()
            {
                let cleaned = v
                    .replace("(R)", "")
                    .replace("(TM)", "")
                    .replace("(r)", "")
                    .replace("(tm)", "");
                let normalized: Vec<&str> = cleaned.split_whitespace().collect();
                model_name = Some(normalized.join(" "));
            } else if k == "cpu cores" && physical_cores == 0 {
                physical_cores = v.parse::<u16>().unwrap_or(0);
            } else if k == "siblings" && siblings == 0 {
                siblings = v.parse::<u16>().unwrap_or(0);
            } else if k == "processor" {
                processor_count = processor_count.saturating_add(1);
            }
        }
    }

    if siblings == 0 && processor_count > 0 {
        siblings = processor_count;
    }
    if physical_cores == 0 && siblings > 0 {
        physical_cores = siblings;
    }

    let static_model = match model_name {
        Some(name) if !name.is_empty() => Box::leak(name.into_boxed_str()) as &'static str,
        _ => "Generic CPU",
    };

    (static_model, physical_cores, siblings)
}

fn probe_cpuinfo_details() -> (&'static str, u16, u16) {
    if let Ok(content) = std::fs::read_to_string("/proc/cpuinfo") {
        parse_cpuinfo_content(&content)
    } else {
        ("Generic CPU", 0, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_probe_cpu() {
        let cpu = probe_cpu();
        assert!(cpu.available_cores >= 1);
        assert!(!cpu.source.is_empty());
        assert!(!cpu.register_capabilities.summary().is_empty());
        assert!(!cpu.format_model_and_topology().is_empty());
    }

    #[test]
    fn test_parse_cpuinfo_content() {
        let sample = "\
processor\t: 0
model name\t: 11th Gen Intel(R) Core(TM) i5-11400H @ 2.70GHz
siblings\t: 12
cpu cores\t: 6
";
        let (model, phys, sib) = parse_cpuinfo_content(sample);
        assert_eq!(model, "11th Gen Intel Core i5-11400H @ 2.70GHz");
        assert_eq!(phys, 6);
        assert_eq!(sib, 12);

        let mut prof = CpuProfile::new(12, "test");
        prof.model_name = model;
        prof.physical_cores = phys;
        prof.logical_cores = sib;
        assert_eq!(
            prof.format_model_and_topology(),
            "11th Gen Intel Core i5-11400H @ 2.70GHz (6C/12T)"
        );
    }

    #[test]
    fn test_register_width_selection() {
        let caps = CpuRegisterCapabilities::detect();
        assert!(caps.has_32);
        assert!(caps.has_64);

        // Workload requesting Bits64 gets exactly Bits64
        let w64 = caps.select_for_workload(RegisterWidth::Bits64);
        assert_eq!(w64, RegisterWidth::Bits64);

        // Workload requesting Bits32 gets exactly Bits32
        let w32 = caps.select_for_workload(RegisterWidth::Bits32);
        assert_eq!(w32, RegisterWidth::Bits32);

        // Workload requesting Bits256 gets maximum supported up to Bits256
        let w256 = caps.select_for_workload(RegisterWidth::Bits256);
        assert!(w256 <= RegisterWidth::Bits256);
        assert!(w256 >= RegisterWidth::Bits128); // On x86_64, at least 128-bit

        // Workload size adaptation test:
        // 4+ elements adapts to 256-bit (or max supported)
        assert_eq!(caps.select_for_workload_size(4), w256);
        assert_eq!(caps.select_for_workload_size(20), w256);
        // 2..3 elements adapts to 128-bit
        assert_eq!(
            caps.select_for_workload_size(2),
            caps.select_for_workload(RegisterWidth::Bits128)
        );
        assert_eq!(
            caps.select_for_workload_size(3),
            caps.select_for_workload(RegisterWidth::Bits128)
        );
        // 1 element adapts directly to 64-bit scalar
        assert_eq!(caps.select_for_workload_size(1), RegisterWidth::Bits64);

        // Byte buffer length adaptation test:
        // 32+ bytes targets 256-bit
        assert_eq!(caps.select_for_byte_len(32), w256);
        assert_eq!(caps.select_for_byte_len(128), w256);
        // 16..31 bytes targets 128-bit
        assert_eq!(
            caps.select_for_byte_len(16),
            caps.select_for_workload(RegisterWidth::Bits128)
        );
        assert_eq!(
            caps.select_for_byte_len(31),
            caps.select_for_workload(RegisterWidth::Bits128)
        );
        // 8..15 bytes targets 64-bit
        assert_eq!(caps.select_for_byte_len(8), RegisterWidth::Bits64);
        assert_eq!(caps.select_for_byte_len(15), RegisterWidth::Bits64);
    }

    #[test]
    fn test_copy_bulk_payload_all_tiers() {
        // Test varying boundary sizes across all register tiers (256, 128, 64, 32)
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

            let mut dst_256 = vec![0u8; size];
            copy_bulk_256(&src, &mut dst_256);
            assert_eq!(src, dst_256, "Mismatch in copy_bulk_256 at size {}", size);

            let mut dst_128 = vec![0u8; size];
            copy_bulk_128(&src, &mut dst_128);
            assert_eq!(src, dst_128, "Mismatch in copy_bulk_128 at size {}", size);

            let mut dst_64 = vec![0u8; size];
            copy_bulk_64(&src, &mut dst_64);
            assert_eq!(src, dst_64, "Mismatch in copy_bulk_64 at size {}", size);

            let mut dst_32 = vec![0u8; size];
            copy_bulk_32(&src, &mut dst_32);
            assert_eq!(src, dst_32, "Mismatch in copy_bulk_32 at size {}", size);
        }
    }
}
