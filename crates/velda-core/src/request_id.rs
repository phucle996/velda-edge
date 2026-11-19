//! W3C TraceContext Request ID Generator & Thread-Local Entropy Pool (`velda_core::request_id`).
//!
//! ```text
//! ┌───────────────────────────────────┬───────────────────────────────────┐
//! │ Timestamp (High 64 bits)          │ Entropy / Sequence (Low 64 bits)  │
//! │ Unix Epoch Nanoseconds (Hex 0..16)│ SplitMix64 [WorkerId ^ SeqCounter]│
//! └───────────────────────────────────┴───────────────────────────────────┘
//!
//!   Ingress Intake ──► clock::now_unix_nanos() ──┐
//!                                                ▼
//!   Thread-Local Entropy Pool ──► Pop Token ──► Assemble RequestId & HeaderValue
//! ```

use std::cell::RefCell;
use std::sync::atomic::{AtomicU16, Ordering};

use http::HeaderValue;

use crate::types::RequestId;

/// Maximum number of pre-generated entropy tokens cached in the thread-local pool.
pub const ID_POOL_CAPACITY: usize = 64;

/// Low watermark threshold. When available tokens fall below this count, a batch refill is triggered.
pub const ID_POOL_LOW_WATERMARK: usize = 16;

#[cfg(target_arch = "x86_64")]
#[inline]
fn format_hex_16_simd(val: u64, out: &mut [u8; 16]) {
    use core::arch::x86_64::*;
    let be = val.to_be_bytes();
    unsafe {
        let bytes = _mm_loadl_epi64(be.as_ptr() as *const __m128i);
        let mask_0f = _mm_set1_epi8(0x0f);
        let hi = _mm_and_si128(_mm_srli_epi16(bytes, 4), mask_0f);
        let lo = _mm_and_si128(bytes, mask_0f);
        let nibbles = _mm_unpacklo_epi8(hi, lo);
        let gt9 = _mm_cmpgt_epi8(nibbles, _mm_set1_epi8(9));
        let alpha_offset = _mm_and_si128(gt9, _mm_set1_epi8((b'a' - 10 - b'0') as i8));
        let ascii = _mm_add_epi8(
            _mm_add_epi8(nibbles, _mm_set1_epi8(b'0' as i8)),
            alpha_offset,
        );
        _mm_storeu_si128(out.as_mut_ptr() as *mut __m128i, ascii);
    }
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn format_hex_16_simd(val: u64, out: &mut [u8; 16]) {
    use core::arch::aarch64::*;
    let be = val.to_be_bytes();
    unsafe {
        let bytes = vld1_u8(be.as_ptr());
        let hi = vshr_n_u8(bytes, 4);
        let lo = vand_u8(bytes, vdup_n_u8(0x0f));
        let zipped = vzip_u8(hi, lo);
        let nibbles = vcombine_u8(zipped.0, zipped.1);
        let gt9 = vcgtq_u8(nibbles, vdupq_n_u8(9));
        let alpha_offset = vandq_u8(gt9, vdupq_n_u8(b'a' - 10 - b'0'));
        let ascii = vaddq_u8(vaddq_u8(nibbles, vdupq_n_u8(b'0')), alpha_offset);
        vst1q_u8(out.as_mut_ptr(), ascii);
    }
}

#[cfg(any(test, not(any(target_arch = "x86_64", target_arch = "aarch64"))))]
#[inline]
fn format_hex_16_swar(val: u64, out: &mut [u8; 16]) {
    #[inline(always)]
    fn swar_u32_to_hex8(x: u32) -> u64 {
        let mut v = x.to_be() as u64;
        v = ((v & 0x0000_0000_FFFF_0000) << 16) | (v & 0x0000_0000_0000_FFFF);
        v = ((v & 0x0000_FF00_0000_FF00) << 8) | (v & 0x0000_00FF_0000_00FF);
        v = ((v & 0x00F0_00F0_00F0_00F0) >> 4) | ((v & 0x000F_000F_000F_000F) << 8);
        let mask = ((v + 0x0606_0606_0606_0606) >> 4) & 0x0101_0101_0101_0101;
        v + 0x3030_3030_3030_3030 + mask * (b'a' - 10 - b'0') as u64
    }
    let hi_ascii = swar_u32_to_hex8((val >> 32) as u32);
    let lo_ascii = swar_u32_to_hex8(val as u32);
    out[0..8].copy_from_slice(&hi_ascii.to_ne_bytes());
    out[8..16].copy_from_slice(&lo_ascii.to_ne_bytes());
}

/// Encodes a 64-bit unsigned integer into 16 lowercase hex ASCII bytes.
#[inline(always)]
pub fn format_hex_16(val: u64, out: &mut [u8; 16]) {
    #[cfg(target_arch = "x86_64")]
    {
        format_hex_16_simd(val, out);
    }
    #[cfg(target_arch = "aarch64")]
    {
        format_hex_16_simd(val, out);
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        format_hex_16_swar(val, out);
    }
}

/// Formats a 128-bit RequestId (`(high_64, low_64)`) into an exact 32-character lowercase hex string.
#[inline(always)]
pub fn format_hex32_bytes(hi: u64, lo: u64) -> [u8; 32] {
    let mut out = [0u8; 32];
    format_hex_16(hi, (&mut out[0..16]).try_into().unwrap());
    format_hex_16(lo, (&mut out[16..32]).try_into().unwrap());
    out
}

/// Appends a 128-bit RequestId as exactly 32 lowercase hex characters into `buf`.
#[inline(always)]
pub fn fast_write_hex32(buf: &mut Vec<u8>, hi: u64, lo: u64) {
    let bytes = format_hex32_bytes(hi, lo);
    buf.extend_from_slice(&bytes);
}

/// Stamps a 128-bit RequestId into a pre-allocated template slice at `offset`.
#[inline(always)]
pub fn stamp_hex32_at(slice: &mut [u8], offset: usize, hi: u64, lo: u64) {
    let hex = format_hex32_bytes(hi, lo);
    debug_assert!(offset + 32 <= slice.len());
    unsafe {
        core::ptr::copy_nonoverlapping(hex.as_ptr(), slice.as_mut_ptr().add(offset), 32);
    }
}

/// 64-bit bijective entropy mixer (SplitMix64) combining worker ID and sequence counter.
#[inline(always)]
pub fn mix_entropy(worker_id: u16, seq: u64) -> u64 {
    let w = ((worker_id as u64) ^ 0x517c_c1b7_2722_0a95).rotate_left(17);
    let mut z = seq.wrapping_add(w).wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Pre-generated entropy suffix token held in the thread-local pool.
///
/// Contains entropy and its pre-formatted 16-character lowercase hex ASCII string.
/// Completely independent of time: tokens never expire or rot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntropyToken {
    pub entropy: u64,
    pub suffix_hex: [u8; 16],
}

impl EntropyToken {
    pub const ZERO: Self = Self {
        entropy: 0,
        suffix_hex: *b"0000000000000000",
    };
}

/// Thread-local pool for pre-generating entropy suffix tokens.
pub struct RequestIdPool {
    worker_id: u16,
    sequence: u64,
    count: usize,
    tokens: [EntropyToken; ID_POOL_CAPACITY],
}

impl RequestIdPool {
    /// Creates a new `RequestIdPool` bound to a specific worker thread.
    pub fn new(worker_id: u16) -> Self {
        let mut pool = Self {
            worker_id,
            sequence: 0,
            count: 0,
            tokens: [EntropyToken::ZERO; ID_POOL_CAPACITY],
        };
        pool.refill();
        pool
    }

    /// Batch refills pre-computed entropy tokens up to [`ID_POOL_CAPACITY`].
    #[inline]
    fn refill(&mut self) {
        while self.count < ID_POOL_CAPACITY {
            let seq = self.sequence;
            self.sequence = self.sequence.wrapping_add(1);
            let entropy = mix_entropy(self.worker_id, seq);
            let mut suffix_hex = [0u8; 16];
            format_hex_16(entropy, &mut suffix_hex);
            self.tokens[self.count] = EntropyToken {
                entropy,
                suffix_hex,
            };
            self.count += 1;
        }
    }

    /// Retrieves the next Request ID assembled with the given ingress timestamp in nanoseconds.
    #[inline]
    pub fn next_id(&mut self, ingress_nanos: u64) -> (RequestId, HeaderValue) {
        if self.count < ID_POOL_LOW_WATERMARK {
            self.refill();
        }

        self.count -= 1;
        let token = self.tokens[self.count];

        let raw_u128 = ((ingress_nanos as u128) << 64) | (token.entropy as u128);
        let request_id = RequestId::new(raw_u128);

        let mut hex = [0u8; 32];
        format_hex_16(ingress_nanos, (&mut hex[0..16]).try_into().unwrap());
        hex[16..32].copy_from_slice(&token.suffix_hex);

        let header_value = HeaderValue::from_bytes(&hex)
            .unwrap_or_else(|_| HeaderValue::from_static("00000000000000000000000000000000"));

        (request_id, header_value)
    }

    /// Returns the current number of cached tokens in the pool.
    #[inline(always)]
    pub fn available(&self) -> usize {
        self.count
    }
}

static NEXT_WORKER_ID: AtomicU16 = AtomicU16::new(1);

thread_local! {
    static LOCAL_POOL: RefCell<RequestIdPool> = RefCell::new(
        RequestIdPool::new(NEXT_WORKER_ID.fetch_add(1, Ordering::Relaxed))
    );
}

/// Generates the next unique W3C-compliant Request ID and its HTTP [`HeaderValue`]
/// using the supplied ingress hardware timestamp in nanoseconds.
#[inline(always)]
pub fn next_request_id(ingress_nanos: u64) -> (RequestId, HeaderValue) {
    LOCAL_POOL.with(|pool| pool.borrow_mut().next_id(ingress_nanos))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};
    use std::thread;

    #[test]
    fn hex_formatting() {
        for &val in &[0u64, 0x0123456789abcdef, 0xfedcba9876543210, u64::MAX] {
            let expected = format!("{val:016x}");
            let mut out_dispatch = [0u8; 16];
            let mut out_swar = [0u8; 16];
            format_hex_16(val, &mut out_dispatch);
            format_hex_16_swar(val, &mut out_swar);
            assert_eq!(&out_dispatch, expected.as_bytes());
            assert_eq!(&out_swar, expected.as_bytes());
        }
    }

    #[test]
    fn pool_refill() {
        let mut pool = RequestIdPool::new(42);
        assert_eq!(pool.available(), ID_POOL_CAPACITY);

        for _ in 0..50 {
            let (req_id, header_val) = pool.next_id(1_710_000_000_123_456_789);
            assert_eq!(header_val.as_bytes().len(), 32);
            assert_eq!(req_id.to_string().len(), 32);
            assert_eq!(req_id.to_string().as_bytes(), header_val.as_bytes());
        }

        assert!(pool.available() >= ID_POOL_LOW_WATERMARK);
    }

    #[test]
    fn deterministic_assembly() {
        let mut pool = RequestIdPool::new(7);
        let ingress_nanos = 0x1234_5678_9abc_def0_u64;
        let (id, header) = pool.next_id(ingress_nanos);

        assert_eq!(id.timestamp_nanos(), ingress_nanos);
        let header_str = header.to_str().unwrap();
        assert!(header_str.starts_with("123456789abcdef0"));
    }

    #[test]
    fn threaded_uniqueness() {
        const THREADS: usize = 8;
        const IDS_PER_THREAD: usize = 10_000;
        let generated_ids = Arc::new(Mutex::new(HashSet::with_capacity(THREADS * IDS_PER_THREAD)));

        let mut handles = Vec::new();
        for _ in 0..THREADS {
            let ids_clone = Arc::clone(&generated_ids);
            handles.push(thread::spawn(move || {
                let mut local_ids = Vec::with_capacity(IDS_PER_THREAD);
                for _ in 0..IDS_PER_THREAD {
                    let nanos = crate::clock::now_unix_nanos();
                    let (id, header) = next_request_id(nanos);
                    local_ids.push((id, header.to_str().unwrap().to_string()));
                }

                let mut lock = ids_clone.lock().unwrap();
                for (id, hex) in local_ids {
                    assert_eq!(id.to_string(), hex);
                    assert!(lock.insert(id), "Detected duplicate RequestId: {id}");
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let total = generated_ids.lock().unwrap().len();
        assert_eq!(total, THREADS * IDS_PER_THREAD);
    }

    #[test]
    fn test_format_hex32_bytes() {
        let hi = 0x4bf9_2f35_77b3_4da6_u64;
        let lo = 0xa3ce_929d_0e0e_4736_u64;
        let bytes = format_hex32_bytes(hi, lo);
        assert_eq!(&bytes, b"4bf92f3577b34da6a3ce929d0e0e4736");

        let mut buf = Vec::new();
        fast_write_hex32(&mut buf, hi, lo);
        assert_eq!(buf, b"4bf92f3577b34da6a3ce929d0e0e4736");

        let mut slice = [0u8; 40];
        stamp_hex32_at(&mut slice, 4, hi, lo);
        assert_eq!(&slice[4..36], b"4bf92f3577b34da6a3ce929d0e0e4736");
    }
}
