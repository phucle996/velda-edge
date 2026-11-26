//! Multi-Target Compile-Time 64-Byte SIMD / SWAR Tag Directory Scanners (`jwt_auth::simd`).
//!
//! ```text
//!   TagLine ([u8; 64], 64B aligned)
//!     ├── x86_64 + avx2  ──► `_mm256_cmpeq_epi8` (2 x 256-bit) ──► 64-bit mask
//!     ├── x86_64 + sse2  ──► `_mm_cmpeq_epi8`    (4 x 128-bit) ──► 64-bit mask
//!     ├── aarch64        ──► NEON `vceqq_u8`     (4 x 128-bit) ──► 64-bit mask
//!     └── portable       ──► Zero-borrow 64-bit SWAR fallback  ──► 64-bit mask
//! ```

/// Number of slots per contiguous SIMD bucket group (exact 64-byte cache line of `u8` tags).
pub const GROUP_WIDTH: usize = 64;

/// 64-byte cache-line aligned SIMD tag directory (`0x00..0x40`).
#[repr(C, align(64))]
#[derive(Clone, Copy)]
pub struct TagLine(pub [u8; GROUP_WIDTH]);

/// Prefetches a 64-byte L3 bucket line (`_MM_HINT_T2` / `pldl3keep`).
#[inline(always)]
pub fn prefetch_bucket_l3(ptr: *const u8) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_prefetch(ptr as *const i8, core::arch::x86_64::_MM_HINT_T2);
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        core::arch::asm!(
            "prfm pldl3keep, [{0}]",
            in(reg) ptr,
            options(nostack, preserves_flags, readonly)
        );
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _ = ptr;
}

/// Scans a 64-byte aligned `tags` cache line for `(tag & 0x7F) == target7`, returning a 64-bit lane mask.
#[inline(always)]
pub fn match_tag7_mask64(tags: &TagLine, target7: u8) -> u64 {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use core::arch::x86_64::{
            __m256i, _mm256_and_si256, _mm256_cmpeq_epi8, _mm256_load_si256, _mm256_movemask_epi8,
            _mm256_set1_epi8,
        };
        let ptr = tags.0.as_ptr();
        let mask7 = _mm256_set1_epi8(0x7F);
        let needle = _mm256_set1_epi8(target7 as i8);
        let lo = _mm256_and_si256(_mm256_load_si256(ptr as *const __m256i), mask7);
        let hi = _mm256_and_si256(_mm256_load_si256(ptr.add(32) as *const __m256i), mask7);
        let m_lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, needle)) as u32 as u64;
        let m_hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, needle)) as u32 as u64;
        m_lo | (m_hi << 32)
    }

    #[cfg(all(
        target_arch = "x86_64",
        not(target_feature = "avx2"),
        target_feature = "sse2"
    ))]
    unsafe {
        use core::arch::x86_64::{
            __m128i, _mm_and_si128, _mm_cmpeq_epi8, _mm_load_si128, _mm_movemask_epi8,
            _mm_set1_epi8,
        };
        let ptr = tags.0.as_ptr();
        let mask7 = _mm_set1_epi8(0x7F);
        let needle = _mm_set1_epi8(target7 as i8);
        let mut out = 0u64;
        let mut chunk = 0usize;
        while chunk < 4 {
            let v = _mm_and_si128(_mm_load_si128(ptr.add(chunk * 16) as *const __m128i), mask7);
            let m = _mm_movemask_epi8(_mm_cmpeq_epi8(v, needle)) as u32 as u64;
            out |= m << (chunk * 16);
            chunk += 1;
        }
        out
    }

    #[cfg(target_arch = "aarch64")]
    unsafe {
        use core::arch::aarch64::{vandq_u8, vceqq_u8, vdupq_n_u8, vld1q_u8};
        let ptr = tags.0.as_ptr();
        let mask7 = vdupq_n_u8(0x7F);
        let needle = vdupq_n_u8(target7 & 0x7F);
        let mut out = 0u64;
        let mut chunk = 0usize;
        while chunk < 4 {
            let v = vandq_u8(vld1q_u8(ptr.add(chunk * 16)), mask7);
            out |= neon_movemask_u8(vceqq_u8(v, needle)) << (chunk * 16);
            chunk += 1;
        }
        out
    }

    #[cfg(not(any(
        all(
            target_arch = "x86_64",
            any(target_feature = "avx2", target_feature = "sse2")
        ),
        target_arch = "aarch64"
    )))]
    {
        match_tag7_mask64_swar(&tags.0, target7)
    }
}

/// Scans a 64-byte aligned `tags` cache line for `tag == 0x00` (empty slots), returning a 64-bit lane mask.
#[inline(always)]
pub fn empty_slot_mask64(tags: &TagLine) -> u64 {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use core::arch::x86_64::{
            __m256i, _mm256_cmpeq_epi8, _mm256_load_si256, _mm256_movemask_epi8,
            _mm256_setzero_si256,
        };
        let ptr = tags.0.as_ptr();
        let zero = _mm256_setzero_si256();
        let lo = _mm256_load_si256(ptr as *const __m256i);
        let hi = _mm256_load_si256(ptr.add(32) as *const __m256i);
        let m_lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, zero)) as u32 as u64;
        let m_hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, zero)) as u32 as u64;
        m_lo | (m_hi << 32)
    }

    #[cfg(all(
        target_arch = "x86_64",
        not(target_feature = "avx2"),
        target_feature = "sse2"
    ))]
    unsafe {
        use core::arch::x86_64::{
            __m128i, _mm_cmpeq_epi8, _mm_load_si128, _mm_movemask_epi8, _mm_setzero_si128,
        };
        let ptr = tags.0.as_ptr();
        let zero = _mm_setzero_si128();
        let mut out = 0u64;
        let mut chunk = 0usize;
        while chunk < 4 {
            let v = _mm_load_si128(ptr.add(chunk * 16) as *const __m128i);
            let m = _mm_movemask_epi8(_mm_cmpeq_epi8(v, zero)) as u32 as u64;
            out |= m << (chunk * 16);
            chunk += 1;
        }
        out
    }

    #[cfg(target_arch = "aarch64")]
    unsafe {
        use core::arch::aarch64::{vceqzq_u8, vld1q_u8};
        let ptr = tags.0.as_ptr();
        let mut out = 0u64;
        let mut chunk = 0usize;
        while chunk < 4 {
            let cmp = vceqzq_u8(vld1q_u8(ptr.add(chunk * 16)));
            out |= neon_movemask_u8(cmp) << (chunk * 16);
            chunk += 1;
        }
        out
    }

    #[cfg(not(any(
        all(
            target_arch = "x86_64",
            any(target_feature = "avx2", target_feature = "sse2")
        ),
        target_arch = "aarch64"
    )))]
    {
        empty_slot_mask64_swar(&tags.0)
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn neon_movemask_u8(cmp: core::arch::aarch64::uint8x16_t) -> u64 {
    unsafe {
        use core::arch::aarch64::{vaddv_u8, vandq_u8, vextq_u8, vreinterpretq_u8_u64};
        const BIT_WEIGHTS: u64 = 0x8040_2010_0804_0201;
        let weights = vreinterpretq_u8_u64(core::arch::aarch64::vdupq_n_u64(BIT_WEIGHTS));
        let masked = vandq_u8(cmp, weights);
        let lo_half = vandq_u8(
            masked,
            vreinterpretq_u8_u64(core::arch::aarch64::vcombine_u64(
                core::arch::aarch64::vcreate_u64(u64::MAX),
                core::arch::aarch64::vcreate_u64(0),
            )),
        );
        let hi_shifted = vextq_u8(masked, masked, 8);
        let hi_half = vandq_u8(
            hi_shifted,
            vreinterpretq_u8_u64(core::arch::aarch64::vcombine_u64(
                core::arch::aarch64::vcreate_u64(u64::MAX),
                core::arch::aarch64::vcreate_u64(0),
            )),
        );
        (vaddv_u8(lo_half) as u64) | ((vaddv_u8(hi_half) as u64) << 8)
    }
}

#[cfg(any(
    test,
    not(any(
        all(
            target_arch = "x86_64",
            any(target_feature = "avx2", target_feature = "sse2")
        ),
        target_arch = "aarch64"
    ))
))]
#[inline(always)]
pub(crate) fn match_tag7_mask64_swar(tags: &[u8; GROUP_WIDTH], target7: u8) -> u64 {
    const LO7: u64 = 0x7F7F_7F7F_7F7F_7F7F;
    const ONES: u64 = 0x0101_0101_0101_0101;
    const MSBS: u64 = 0x8080_8080_8080_8080;

    let needle = ONES.wrapping_mul((target7 & 0x7F) as u64);
    let mut out = 0u64;
    let mut chunk = 0usize;
    while chunk < 8 {
        let base = chunk * 8;
        let word = u64::from_le_bytes(tags[base..base + 8].try_into().unwrap());
        let x = (word & LO7) ^ needle;
        let zero_msbs = !((x | MSBS).wrapping_sub(ONES)) & MSBS;
        out |= pack_msbs_u64(zero_msbs) << base;
        chunk += 1;
    }
    out
}

#[cfg(any(
    test,
    not(any(
        all(
            target_arch = "x86_64",
            any(target_feature = "avx2", target_feature = "sse2")
        ),
        target_arch = "aarch64"
    ))
))]
#[inline(always)]
pub(crate) fn empty_slot_mask64_swar(tags: &[u8; GROUP_WIDTH]) -> u64 {
    const LO7: u64 = 0x7F7F_7F7F_7F7F_7F7F;
    const ONES: u64 = 0x0101_0101_0101_0101;
    const MSBS: u64 = 0x8080_8080_8080_8080;

    let mut out = 0u64;
    let mut chunk = 0usize;
    while chunk < 8 {
        let base = chunk * 8;
        let word = u64::from_le_bytes(tags[base..base + 8].try_into().unwrap());
        let low_zero = !(((word & LO7) | MSBS).wrapping_sub(ONES)) & MSBS;
        let exact_zero = low_zero & !word;
        out |= pack_msbs_u64(exact_zero) << base;
        chunk += 1;
    }
    out
}

#[cfg(any(
    test,
    not(any(
        all(
            target_arch = "x86_64",
            any(target_feature = "avx2", target_feature = "sse2")
        ),
        target_arch = "aarch64"
    ))
))]
#[inline(always)]
const fn pack_msbs_u64(msbs: u64) -> u64 {
    ((msbs >> 7).wrapping_mul(0x0102_0408_1020_4080) >> 56) & 0xFF
}
