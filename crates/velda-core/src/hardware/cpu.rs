//! CPU topology and core concurrency probe.
//!
//! Owns host CPU core discovery, container cgroups limits (v1 & v2),
//! environment overrides, and CPU profile detection.

/// Canonical hardware register width representation.
///
/// Strictly vendor-neutral (represented by bit-width rather than vendor extensions).
/// Capped at 256 bits maximum to prevent thermal throttling and CPU core downclocking
/// Maximum register width supported by the target architecture, strictly capped at 256-bit max.
///
/// # Architectural Invariant
/// - 64-bit architecture invariant: Velda Edge exclusively targets 64-bit platforms (`x86_64` and `aarch64`).
///   Scalar operations execute on 64-bit general-purpose registers (`Bits64`).
/// - Strictly capped at 256-bit max: AVX-512 is deliberately excluded to prevent thermal downclocking
///   induced by wider (e.g. 512-bit) vector units on Intel Xeon and AMD EPYC platforms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u16)]
pub enum RegisterWidth {
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
/// - Vendor-agnostic: workloads query required bit-widths (`Bits64`, `Bits128`, `Bits256`)
///   rather than vendor instruction names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CpuRegisterCapabilities {
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
            let has_64 = true;
            let has_128 = true; // SSE2 is mandatory baseline on x86_64
            let has_256 = std::is_x86_feature_detected!("avx2");
            let max_width = if has_256 {
                RegisterWidth::Bits256
            } else {
                RegisterWidth::Bits128
            };
            Self {
                has_64,
                has_128,
                has_256,
                max_width,
            }
        }
        #[cfg(target_arch = "aarch64")]
        {
            let has_64 = true;
            let has_128 = std::arch::is_aarch64_feature_detected!("neon");
            let has_256 = false; // NEON is 128-bit
            let max_width = if has_128 {
                RegisterWidth::Bits128
            } else {
                RegisterWidth::Bits64
            };
            Self {
                has_64,
                has_128,
                has_256,
                max_width,
            }
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            let has_64 = true;
            let has_128 = false;
            let has_256 = false;
            let max_width = RegisterWidth::Bits64;
            Self {
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
    /// - Workloads targeting 64-bit use `Bits64`.
    /// - Workloads targeting 256-bit use `Bits256` if available, otherwise `Bits128` or `Bits64`.
    #[inline]
    pub fn select_for_workload(&self, target_max: RegisterWidth) -> RegisterWidth {
        match target_max {
            RegisterWidth::Bits256 => {
                if self.has_256 {
                    RegisterWidth::Bits256
                } else if self.has_128 {
                    RegisterWidth::Bits128
                } else {
                    RegisterWidth::Bits64
                }
            }
            RegisterWidth::Bits128 => {
                if self.has_128 {
                    RegisterWidth::Bits128
                } else {
                    RegisterWidth::Bits64
                }
            }
            RegisterWidth::Bits64 => RegisterWidth::Bits64,
        }
    }

    /// Adapts the register width to the workload element count (e.g. number of 64-bit quadwords)
    /// to avoid wasteful wide-register setup for narrow workloads:
    /// - `num_quadwords >= 4` -> targets `Bits256` (AVX2-grade)
    /// - `2 <= num_quadwords < 4` -> targets `Bits128` (SSE2 / NEON)
    /// - `< 2` -> targets `Bits64` (Standard scalar ALU)
    #[inline]
    pub fn select_for_workload_size(&self, num_quadwords: usize) -> RegisterWidth {
        let target = if num_quadwords >= 4 {
            RegisterWidth::Bits256
        } else if num_quadwords >= 2 {
            RegisterWidth::Bits128
        } else {
            RegisterWidth::Bits64
        };
        self.select_for_workload(target)
    }

    /// Adapts the register width for bulk byte transfers according to byte buffer length.
    ///
    /// Buffers below 32 bytes adapt to 128-bit or 64-bit to eliminate 256-bit vector setup overhead:
    /// - `byte_len >= 32` -> targets `Bits256`
    /// - `16 <= byte_len < 32` -> targets `Bits128`
    /// - `< 16` -> targets `Bits64`
    #[inline]
    pub fn select_for_byte_len(&self, byte_len: usize) -> RegisterWidth {
        let target = if byte_len >= 32 {
            RegisterWidth::Bits256
        } else if byte_len >= 16 {
            RegisterWidth::Bits128
        } else {
            RegisterWidth::Bits64
        };
        self.select_for_workload(target)
    }

    /// Checks if a specific register width is natively supported by the host.
    #[inline]
    pub const fn is_supported(&self, width: RegisterWidth) -> bool {
        match width {
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
        }
    }
}

/// Discovered CPU concurrency and execution profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CpuProfile {
    /// Number of logical CPU cores available to the process.
    pub available_cores: usize,
    /// Provenance source indicating how CPU capacity was resolved.
    pub source: &'static str,
    /// Hardware register width capabilities detected at runtime via CPUID.
    pub register_capabilities: CpuRegisterCapabilities,
}

impl CpuProfile {
    /// Creates a CPU profile with an explicit core count.
    #[inline]
    pub fn new(available_cores: usize, source: &'static str) -> Self {
        let cores = available_cores.max(1);
        Self {
            available_cores: cores,
            source,
            register_capabilities: CpuRegisterCapabilities::detect(),
        }
    }
}

/// Signature for bulk byte copy functions.
pub type CopyBulkFn = fn(src: &[u8], dst: &mut [u8]);

/// Pre-resolved static function pointer for bulk payload transfers.
pub static COPY_BULK_FN: std::sync::LazyLock<CopyBulkFn> =
    std::sync::LazyLock::new(|| copy_bulk_payload);

/// Copies bulk byte payloads using workload-adaptive hardware register acceleration (AVX2 -> SSE2/NEON -> 64-bit).
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
    #[cfg(target_arch = "x86_64")]
    {
        if len >= 32 && is_x86_feature_detected!("avx2") {
            #[target_feature(enable = "avx2")]
            unsafe fn copy_avx2(src: &[u8], dst: &mut [u8], len: usize) {
                use std::arch::x86_64::*;
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
            unsafe { copy_avx2(src, dst, len) };
            return;
        }

        if len >= 16 {
            #[target_feature(enable = "sse2")]
            unsafe fn copy_sse2(src: &[u8], dst: &mut [u8], len: usize) {
                use std::arch::x86_64::*;
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
            unsafe { copy_sse2(src, dst, len) };
            return;
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if len >= 16 {
            #[target_feature(enable = "neon")]
            unsafe fn copy_neon(src: &[u8], dst: &mut [u8], len: usize) {
                use std::arch::aarch64::*;
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
            unsafe { copy_neon(src, dst, len) };
            return;
        }
    }

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
        assert!(!cpu.register_capabilities.summary().is_empty());
    }

    #[test]
    fn test_register_width_selection() {
        let caps = CpuRegisterCapabilities::detect();
        assert!(caps.has_64);

        // Workload requesting Bits64 gets exactly Bits64
        let w64 = caps.select_for_workload(RegisterWidth::Bits64);
        assert_eq!(w64, RegisterWidth::Bits64);

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
        // < 16 bytes targets 64-bit
        assert_eq!(caps.select_for_byte_len(8), RegisterWidth::Bits64);
        assert_eq!(caps.select_for_byte_len(15), RegisterWidth::Bits64);
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
