//! SIMD Pre-Baked Chunk Concatenation, Fixed-Width Slot Patching & Const Wire Primitives
//! (`velda_plugin::wire_baked`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                     MULTI-TARGET SIMD CHUNK CONCATENATOR & STAMPER                      │
//! ├──────────────────────┬──────────────────────────────────────────────────────────────────┤
//! │ Target Arch/Feature  │ Block Move / Overlapping Tail Store Strategy                     │
//! ├──────────────────────┼──────────────────────────────────────────────────────────────────┤
//! │ x86_64 + AVX2        │ 32B _mm256_{loadu,storeu}_si256 loop + overlapping 32B/16B tail  │
//! │ x86_64 + SSE2        │ 16B _mm_{loadu,storeu}_si128 loop + overlapping 16B tail         │
//! │ aarch64 (NEON)       │ 16B vld1q_u8 / vst1q_u8 loop + overlapping 16B tail              │
//! │ portable (SWAR)      │ 8B u64 head+tail overlapping copy + ptr::copy_nonoverlapping     │
//! └──────────────────────┴──────────────────────────────────────────────────────────────────┘
//!
//!   BakedWire<'a> over reusable &mut Vec<u8>:
//!     ├─► push_chunk(&[u8])       ──► reserve + simd_copy_nonoverlapping
//!     ├─► push_u64(n) / push_i64()──► reserve + velda_core::lut::decimal::fast_write_u64_ptr
//!     ├─► push_hex16(v)           ──► reserve 16B + velda_core::request_id::format_hex_16
//!     ├─► push_hex64(&[u8; 32])   ──► reserve 64B + 4x SIMD format_hex_16
//!     ├─► patch_bytes_at(off, s)  ──► bounds check + simd_copy_nonoverlapping in-place
//!     └─► const PG frames         ──► build_pg_query_frame / build_pg_parse_sync (.rodata)
//! ```

use std::fmt::Write as _;

use velda_core::lut::decimal::{DIGIT_PAIRS, fast_write_u64_ptr};
use velda_core::request_id::format_hex_16;

/// Copies `len` non-overlapping bytes from `src` to `dst` using compile-time selected
/// SIMD vector loads/stores and branchless overlapping tail stores for short baked chunks.
///
/// # Safety
/// `src` must be valid for reads of `len` bytes, `dst` must be valid for writes of `len` bytes,
/// and the two memory regions must not overlap.
#[inline(always)]
pub(crate) unsafe fn simd_copy_nonoverlapping(src: *const u8, dst: *mut u8, len: usize) {
    if len == 0 {
        return;
    }

    if len < 8 {
        if len >= 4 {
            unsafe {
                let head = core::ptr::read_unaligned(src as *const u32);
                let tail = core::ptr::read_unaligned(src.add(len - 4) as *const u32);
                core::ptr::write_unaligned(dst as *mut u32, head);
                core::ptr::write_unaligned(dst.add(len - 4) as *mut u32, tail);
            }
        } else if len >= 2 {
            unsafe {
                let head = core::ptr::read_unaligned(src as *const u16);
                let tail = core::ptr::read_unaligned(src.add(len - 2) as *const u16);
                core::ptr::write_unaligned(dst as *mut u16, head);
                core::ptr::write_unaligned(dst.add(len - 2) as *mut u16, tail);
            }
        } else {
            unsafe {
                *dst = *src;
            }
        }
        return;
    }

    if len <= 16 {
        unsafe {
            let head = core::ptr::read_unaligned(src as *const u64);
            let tail = core::ptr::read_unaligned(src.add(len - 8) as *const u64);
            core::ptr::write_unaligned(dst as *mut u64, head);
            core::ptr::write_unaligned(dst.add(len - 8) as *mut u64, tail);
        }
        return;
    }

    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use core::arch::x86_64::*;
        if len <= 32 {
            let head = _mm_loadu_si128(src as *const __m128i);
            let tail = _mm_loadu_si128(src.add(len - 16) as *const __m128i);
            _mm_storeu_si128(dst as *mut __m128i, head);
            _mm_storeu_si128(dst.add(len - 16) as *mut __m128i, tail);
            return;
        }
        let mut offset = 0usize;
        while offset + 32 <= len {
            let v = _mm256_loadu_si256(src.add(offset) as *const __m256i);
            _mm256_storeu_si256(dst.add(offset) as *mut __m256i, v);
            offset += 32;
        }
        if offset < len {
            let tail = _mm256_loadu_si256(src.add(len - 32) as *const __m256i);
            _mm256_storeu_si256(dst.add(len - 32) as *mut __m256i, tail);
        }
    }

    #[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
    unsafe {
        use core::arch::x86_64::*;
        if len <= 32 {
            let head = _mm_loadu_si128(src as *const __m128i);
            let tail = _mm_loadu_si128(src.add(len - 16) as *const __m128i);
            _mm_storeu_si128(dst as *mut __m128i, head);
            _mm_storeu_si128(dst.add(len - 16) as *mut __m128i, tail);
            return;
        }
        let mut offset = 0usize;
        while offset + 16 <= len {
            let v = _mm_loadu_si128(src.add(offset) as *const __m128i);
            _mm_storeu_si128(dst.add(offset) as *mut __m128i, v);
            offset += 16;
        }
        if offset < len {
            let tail = _mm_loadu_si128(src.add(len - 16) as *const __m128i);
            _mm_storeu_si128(dst.add(len - 16) as *mut __m128i, tail);
        }
    }

    #[cfg(target_arch = "aarch64")]
    unsafe {
        use core::arch::aarch64::*;
        if len <= 32 {
            let head = vld1q_u8(src);
            let tail = vld1q_u8(src.add(len - 16));
            vst1q_u8(dst, head);
            vst1q_u8(dst.add(len - 16), tail);
            return;
        }
        let mut offset = 0usize;
        while offset + 16 <= len {
            let v = vld1q_u8(src.add(offset));
            vst1q_u8(dst.add(offset), v);
            offset += 16;
        }
        if offset < len {
            let tail = vld1q_u8(src.add(len - 16));
            vst1q_u8(dst.add(len - 16), tail);
        }
    }

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    unsafe {
        core::ptr::copy_nonoverlapping(src, dst, len);
    }
}

/// Formats a 32-byte digest (SHA-256 / HMAC-SHA256) into 64 lowercase ASCII hex bytes via SIMD.
#[inline(always)]
pub(crate) fn format_hex64(digest: &[u8; 32], out: &mut [u8; 64]) {
    let w0 = u64::from_be_bytes([
        digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6], digest[7],
    ]);
    let w1 = u64::from_be_bytes([
        digest[8], digest[9], digest[10], digest[11], digest[12], digest[13], digest[14],
        digest[15],
    ]);
    let w2 = u64::from_be_bytes([
        digest[16], digest[17], digest[18], digest[19], digest[20], digest[21], digest[22],
        digest[23],
    ]);
    let w3 = u64::from_be_bytes([
        digest[24], digest[25], digest[26], digest[27], digest[28], digest[29], digest[30],
        digest[31],
    ]);
    format_hex_16(w0, (&mut out[0..16]).try_into().unwrap());
    format_hex_16(w1, (&mut out[16..32]).try_into().unwrap());
    format_hex_16(w2, (&mut out[32..48]).try_into().unwrap());
    format_hex_16(w3, (&mut out[48..64]).try_into().unwrap());
}

/// Formats UTC `date_stamp` (`YYYYMMDD`, 8B) and `amz_date` (`YYYYMMDDTHHMMSSZ`, 16B) on stack.
#[inline]
pub(crate) fn format_amz_timestamps_bytes(unix_secs: u64) -> ([u8; 8], [u8; 16]) {
    let days = (unix_secs / 86_400) as i64;
    let rem = unix_secs % 86_400;
    let hh = (rem / 3600) as usize;
    let mm = ((rem % 3600) / 60) as usize;
    let ss = (rem % 60) as usize;

    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as usize;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as usize;
    let year = (y + i64::from(m <= 2)).clamp(0, 9999) as usize;

    let y_hi = (year / 100) * 2;
    let y_lo = (year % 100) * 2;
    let m2 = m * 2;
    let d2 = d * 2;
    let h2 = hh * 2;
    let min2 = mm * 2;
    let s2 = ss * 2;

    let full = [
        DIGIT_PAIRS[y_hi],
        DIGIT_PAIRS[y_hi + 1],
        DIGIT_PAIRS[y_lo],
        DIGIT_PAIRS[y_lo + 1],
        DIGIT_PAIRS[m2],
        DIGIT_PAIRS[m2 + 1],
        DIGIT_PAIRS[d2],
        DIGIT_PAIRS[d2 + 1],
        b'T',
        DIGIT_PAIRS[h2],
        DIGIT_PAIRS[h2 + 1],
        DIGIT_PAIRS[min2],
        DIGIT_PAIRS[min2 + 1],
        DIGIT_PAIRS[s2],
        DIGIT_PAIRS[s2 + 1],
        b'Z',
    ];
    let date = [
        full[0], full[1], full[2], full[3], full[4], full[5], full[6], full[7],
    ];
    (date, full)
}

/// JSON-escapes a UTF-8 string fragment once at startup/boot for safe direct embedding inside
/// pre-baked `"..."` JSON string literals.
pub(crate) fn json_escape_bytes(raw: &str) -> Vec<u8> {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.into_bytes()
}

/// Safe, zero-allocation SIMD chunk writer over a caller-owned reusable `Vec<u8>`.
pub(crate) struct BakedWire<'a> {
    buf: &'a mut Vec<u8>,
}

impl<'a> BakedWire<'a> {
    /// Wraps an existing buffer without clearing prior contents.
    #[inline(always)]
    pub(crate) fn new(buf: &'a mut Vec<u8>) -> Self {
        Self { buf }
    }

    /// Clears `buf`, reserves `reserve_bytes`, and returns a builder ready for assembly.
    #[inline(always)]
    pub(crate) fn cleared(buf: &'a mut Vec<u8>, reserve_bytes: usize) -> Self {
        buf.clear();
        if buf.capacity() < reserve_bytes {
            buf.reserve(reserve_bytes.saturating_sub(buf.len()));
        }
        Self { buf }
    }

    /// Appends a pre-baked static or runtime byte chunk using SIMD block moves.
    #[inline(always)]
    pub(crate) fn push_chunk(&mut self, chunk: &[u8]) {
        if chunk.is_empty() {
            return;
        }
        self.buf.reserve(chunk.len());
        let old_len = self.buf.len();
        unsafe {
            simd_copy_nonoverlapping(
                chunk.as_ptr(),
                self.buf.as_mut_ptr().add(old_len),
                chunk.len(),
            );
            self.buf.set_len(old_len + chunk.len());
        }
    }

    /// Appends a single byte.
    #[inline(always)]
    pub(crate) fn push_byte(&mut self, b: u8) {
        self.buf.push(b);
    }

    /// Formats and appends a `u64` decimal using `velda_core::lut::decimal::fast_write_u64_ptr`.
    #[inline(always)]
    pub(crate) fn push_u64(&mut self, n: u64) {
        self.buf.reserve(20);
        let old_len = self.buf.len();
        unsafe {
            let written = fast_write_u64_ptr(self.buf.as_mut_ptr().add(old_len), n);
            self.buf.set_len(old_len + written);
        }
    }

    /// Formats and appends an `i64` decimal using SIMD LUT formatting.
    #[inline(always)]
    pub(crate) fn push_i64(&mut self, n: i64) {
        if n < 0 {
            self.buf.reserve(21);
            let old_len = self.buf.len();
            unsafe {
                *self.buf.as_mut_ptr().add(old_len) = b'-';
                let written =
                    fast_write_u64_ptr(self.buf.as_mut_ptr().add(old_len + 1), n.unsigned_abs());
                self.buf.set_len(old_len + 1 + written);
            }
        } else {
            self.push_u64(n as u64);
        }
    }

    /// Formats and appends a `u64` as 16 lowercase ASCII hex chars via SIMD registers.
    #[inline(always)]
    pub(crate) fn push_hex16(&mut self, val: u64) {
        self.buf.reserve(16);
        let old_len = self.buf.len();
        unsafe {
            let dst = self.buf.as_mut_ptr().add(old_len) as *mut [u8; 16];
            format_hex_16(val, &mut *dst);
            self.buf.set_len(old_len + 16);
        }
    }

    /// Formats and appends a 32-byte digest as 64 lowercase ASCII hex chars via SIMD registers.
    #[inline(always)]
    pub(crate) fn push_hex64(&mut self, digest32: &[u8; 32]) {
        self.buf.reserve(64);
        let old_len = self.buf.len();
        unsafe {
            let dst = self.buf.as_mut_ptr().add(old_len) as *mut [u8; 64];
            format_hex64(digest32, &mut *dst);
            self.buf.set_len(old_len + 64);
        }
    }

    /// Escapes and appends a runtime string inside `"..."` JSON without heap allocation.
    #[inline]
    pub(crate) fn push_json_escaped_str(&mut self, s: &str) {
        let bytes = s.as_bytes();
        if !bytes.iter().any(|&b| b == b'"' || b == b'\\' || b < 0x20) {
            self.push_chunk(bytes);
            return;
        }
        for &b in bytes {
            match b {
                b'"' => self.push_chunk(b"\\\""),
                b'\\' => self.push_chunk(b"\\\\"),
                b'\n' => self.push_chunk(b"\\n"),
                b'\r' => self.push_chunk(b"\\r"),
                b'\t' => self.push_chunk(b"\\t"),
                c if c < 0x20 => {
                    const LUT: &[u8; 16] = b"0123456789abcdef";
                    let hex = [
                        b'\\',
                        b'u',
                        b'0',
                        b'0',
                        LUT[(c >> 4) as usize],
                        LUT[(c & 0x0f) as usize],
                    ];
                    self.push_chunk(&hex);
                }
                c => self.push_byte(c),
            }
        }
    }

    /// Reserves `width` zeroed bytes and returns the start offset for [`Self::patch_bytes_at`].
    #[inline(always)]
    pub(crate) fn alloc_slot(&mut self, width: usize) -> usize {
        let off = self.buf.len();
        self.buf.resize(off + width, 0);
        off
    }

    /// Overwrites `src.len()` bytes at `offset` in-place via SIMD copy.
    #[inline(always)]
    pub(crate) fn patch_bytes_at(&mut self, offset: usize, src: &[u8]) {
        let end = offset
            .checked_add(src.len())
            .expect("BakedWire patch overflow");
        assert!(
            end <= self.buf.len(),
            "BakedWire patch_bytes_at out of bounds"
        );
        unsafe {
            simd_copy_nonoverlapping(src.as_ptr(), self.buf.as_mut_ptr().add(offset), src.len());
        }
    }

    /// Returns the assembled slice.
    #[cfg(test)]
    #[inline(always)]
    pub(crate) fn as_slice(&self) -> &[u8] {
        self.buf.as_slice()
    }

    /// Current assembled byte length.
    #[inline(always)]
    pub(crate) fn len(&self) -> usize {
        self.buf.len()
    }
}

/// Returns compile-time wire byte length of a PostgreSQL `'Q'` (Simple Query) frame.
pub(crate) const fn pg_simple_query_len(sql: &str) -> usize {
    1 + 4 + sql.len() + 1
}

/// Builds a compile-time `const` PostgreSQL `'Q'` (Simple Query) frame array.
pub(crate) const fn build_pg_simple_query<const N: usize>(sql: &str) -> [u8; N] {
    let mut out = [0u8; N];
    let sql_b = sql.as_bytes();
    let body_len = (4 + sql_b.len() + 1) as u32;
    let be = body_len.to_be_bytes();
    out[0] = b'Q';
    out[1] = be[0];
    out[2] = be[1];
    out[3] = be[2];
    out[4] = be[3];
    let mut i = 0usize;
    while i < sql_b.len() {
        out[5 + i] = sql_b[i];
        i += 1;
    }
    out[5 + sql_b.len()] = 0;
    out
}

/// Returns compile-time wire byte length of a PostgreSQL `'P'` (Parse) + `'S'` (Sync) pair.
pub(crate) const fn pg_parse_sync_len(sql: &str) -> usize {
    // 'P' + len(4) + stmt_name("\0") + sql + "\0" + param_types(2) + 'S'(1) + len(4)
    1 + (4 + 1 + sql.len() + 1 + 2) + 5
}

/// Builds a compile-time `const` PostgreSQL `'P'` (unnamed Parse) + `'S'` (Sync) wire sequence.
pub(crate) const fn build_pg_parse_sync<const N: usize>(sql: &str) -> [u8; N] {
    let mut out = [0u8; N];
    let sql_b = sql.as_bytes();
    let parse_body_len = (4 + 1 + sql_b.len() + 1 + 2) as u32;
    let be = parse_body_len.to_be_bytes();
    out[0] = b'P';
    out[1] = be[0];
    out[2] = be[1];
    out[3] = be[2];
    out[4] = be[3];
    out[5] = 0; // unnamed stmt ""\0
    let mut i = 0usize;
    while i < sql_b.len() {
        out[6 + i] = sql_b[i];
        i += 1;
    }
    let off = 6 + sql_b.len();
    out[off] = 0; // sql terminator \0
    out[off + 1] = 0; // param_type_count hi
    out[off + 2] = 0; // param_type_count lo
    out[off + 3] = b'S';
    out[off + 4] = 0;
    out[off + 5] = 0;
    out[off + 6] = 0;
    out[off + 7] = 4;
    out
}

/// Static 15-byte PostgreSQL Extended Query tail (`'E'` max_rows=0 on unnamed portal + `'S'` Sync).
pub(crate) const PG_EXECUTE_SYNC_TAIL: [u8; 15] = [
    b'E', 0, 0, 0, 9, 0, 0, 0, 0, 0, // Execute: len=9, portal=""\0, max_rows=0
    b'S', 0, 0, 0, 4, // Sync: len=4
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simd_copy_all_sizes_exact() {
        let src: Vec<u8> = (0..260u16).map(|i| (i % 251) as u8).collect();
        for len in 0..=160 {
            let mut dst = vec![0xAAu8; len + 16];
            unsafe {
                simd_copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), len);
            }
            assert_eq!(&dst[..len], &src[..len], "mismatch at len={len}");
            assert!(
                dst[len..].iter().all(|&b| b == 0xAA),
                "overwrote past end at len={len}"
            );
        }
    }

    #[test]
    fn test_format_hex64_and_amz_timestamps() {
        let mut digest = [0u8; 32];
        for (i, b) in digest.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(17).wrapping_add(5);
        }
        let mut out = [0u8; 64];
        format_hex64(&digest, &mut out);
        let expected: String = digest.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(std::str::from_utf8(&out).unwrap(), expected);

        // 2026-11-26T12:00:00Z = 1795694400
        let (date, full) = format_amz_timestamps_bytes(1_795_694_400);
        assert_eq!(&date, b"20261126");
        assert_eq!(&full, b"20261126T120000Z");
    }

    #[test]
    fn test_baked_wire_chunks_patch_and_const_pg_frames() {
        let mut buf = Vec::new();
        let mut w = BakedWire::cleared(&mut buf, 128);
        w.push_chunk(b"{\"pk\":\"");
        w.push_json_escaped_str("a\"b\\c");
        w.push_chunk(b"\",\"hash\":\"");
        let hash_slot = w.alloc_slot(16);
        w.push_chunk(b"\",\"delta\":");
        w.push_i64(-42);
        w.push_chunk(b",\"cnt\":");
        w.push_u64(987654);
        w.push_byte(b'}');

        let mut hex = [0u8; 16];
        format_hex_16(0x0123_4567_89ab_cdef, &mut hex);
        w.patch_bytes_at(hash_slot, &hex);

        assert_eq!(
            std::str::from_utf8(w.as_slice()).unwrap(),
            r#"{"pk":"a\"b\\c","hash":"0123456789abcdef","delta":-42,"cnt":987654}"#
        );

        const Q_SQL: &str = "SELECT 1";
        const Q_WIRE: [u8; pg_simple_query_len(Q_SQL)] = build_pg_simple_query(Q_SQL);
        assert_eq!(Q_WIRE[0], b'Q');
        assert_eq!(&Q_WIRE[1..5], &((4 + 8 + 1) as u32).to_be_bytes());
        assert_eq!(&Q_WIRE[5..13], b"SELECT 1");
        assert_eq!(Q_WIRE[13], 0);

        const P_WIRE: [u8; pg_parse_sync_len(Q_SQL)] = build_pg_parse_sync(Q_SQL);
        assert_eq!(P_WIRE[0], b'P');
        assert_eq!(&P_WIRE[P_WIRE.len() - 5..], &[b'S', 0, 0, 0, 4]);
    }
}
