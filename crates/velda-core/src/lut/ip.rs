//! Hardware-Optimized Dual IP Lookup Tables & Formatting (`velda-core::lut::ip`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                      VELDA EDGE DUAL IP CODEBOOK (1.5 KB TOTAL L1D)                     │
//! ├───────────────────────┬───────────────┬────────────────┬───────────────┬────────────────┤
//! │ LUT Subsystem         │ Value Range   │ Variants Count │ Memory (L1D)  │ O(1) Mechanics │
//! ├───────────────────────┼───────────────┼────────────────┼───────────────┼────────────────┤
//! │ PART 1: IPv4 Octet    │ 0..=255       │ 256 entries    │ 1,024 bytes   │ Decimal ASCII  │
//! │ PART 2: IPv6 HexOctet │ 0..=255       │ 256 entries    │   512 bytes   │ Hex lowercase  │
//! ├───────────────────────┴───────────────┴────────────────┼───────────────┴────────────────┤
//! │ Total IP Codebook Footprint (IPv4 + IPv6):             │ 1,536 bytes (24 cache lines)   │
//! └────────────────────────────────────────────────────────┴────────────────────────────────┘
//!
//!  ┌─────────────────────────────────────────┬─────────────────────────────────────────┐
//!  │ PART 1: IPv4 (Decimal, Base-10)         │ PART 2: IPv6 (Hexadecimal, Base-16)     │
//!  ├─────────────────────────────────────────┼─────────────────────────────────────────┤
//!  │ • 32-bit integer (4 octets b0..b3)      │ • 128-bit integer (8 hextets w0..w7)    │
//!  │ • Format: Decimal digits + '.' separator│ • Format: Hex digits + ':' separator    │
//!  │ • Table: OCTET_TABLE (1,024 B)          │ • Table: HEX_OCTET_TABLE (512 B)        │
//!  │ • Zero Division, Direct Stamping        │ • RFC 5952 "::" Run-Length Compression  │
//!  └─────────────────────────────────────────┴─────────────────────────────────────────┘
//!
//!  IPv4 Pipeline (Host-Endian u32):
//!  [ b0 (8b) ] ──► OCTET_TABLE ──► [ e0 ] ──► "192"
//!  [ b1 (8b) ] ──► OCTET_TABLE ──► [ e1 ] ──► ".168"
//!  [ b2 (8b) ] ──► OCTET_TABLE ──► [ e2 ] ──► ".1"
//!  [ b3 (8b) ] ──► OCTET_TABLE ──► [ e3 ] ──► ".100"  ──► 7..=15 bytes
//!
//!  IPv6 Pipeline (Host-Endian u128):
//!  8 Hextets (w0..w7) ──► RFC 5952 Longest Zero Run Compression ("::")
//!                       │
//!                       ▼
//!  Each Hextet (u16) ──► HEX_OCTET_TABLE [hi, lo] ──► 1..=4 Hex Digits ──► 2..=39 bytes
//! ```

use std::net::IpAddr;

// =============================================================================
// PART 0: Lossless Integer IP Codecs & Zero-Allocation Parsers
// =============================================================================

/// Encodes an [`IpAddr`] losslessly into a 128-bit integer using RFC 4291 IPv4-mapped encoding
/// (`0x0000_0000_0000_0000_0000_FFFF_<ipv4_u32>`) so both IPv4 and IPv6 occupy a single
/// integer field without an extra version discriminator flag.
#[inline(always)]
pub const fn encode_ip_u128(ip: IpAddr) -> u128 {
    match ip {
        IpAddr::V4(v4) => v4.to_ipv6_mapped().to_bits(),
        IpAddr::V6(v6) => v6.to_bits(),
    }
}

/// Encodes an upstream backend [`IpAddr`] into a 32-bit integer (native IPv4 bits or FNV1a-32 of IPv6).
#[inline(always)]
pub const fn encode_upstream_ip_u32(ip: IpAddr) -> u32 {
    match ip {
        IpAddr::V4(v4) => v4.to_bits(),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => v4.to_bits(),
            None => {
                let octets = v6.octets();
                let mut h: u32 = 0x811c_9dc5;
                let mut i = 0;
                while i < 16 {
                    h ^= octets[i] as u32;
                    h = h.wrapping_mul(0x0100_0193);
                    i += 1;
                }
                h
            }
        },
    }
}

/// Returns `Some(ipv4_u32)` if `ip` is an RFC 4291 IPv4-mapped IPv6 integer (`::ffff:a.b.c.d`),
/// or `None` otherwise.
#[inline(always)]
pub const fn is_ipv4_mapped_u128(ip: u128) -> Option<u32> {
    if (ip >> 48) == 0 && ((ip >> 32) & 0xFFFF) == 0xFFFF {
        Some(ip as u32)
    } else {
        None
    }
}

/// Encodes a 32-bit host-endian IPv4 integer into an RFC 4291 IPv4-mapped `u128` integer.
#[inline(always)]
pub const fn encode_ipv4_u128(ipv4_bits: u32) -> u128 {
    0x0000_FFFF_0000_0000u128 | (ipv4_bits as u128)
}

/// Zero-allocation parser for dotted-decimal ASCII IPv4 bytes (`"a.b.c.d"` -> host-endian `u32`).
#[inline]
pub const fn parse_ipv4_bytes(bytes: &[u8]) -> Option<u32> {
    parse_ipv4_bytes_from(bytes, 0)
}

#[inline]
const fn parse_ipv4_bytes_from(bytes: &[u8], start: usize) -> Option<u32> {
    if start > bytes.len() {
        return None;
    }
    let len = bytes.len();
    let sub_len = len - start;
    if sub_len < 7 || sub_len > 15 {
        return None;
    }
    let mut acc: u32 = 0;
    let mut octet_idx: u8 = 0;
    let mut i = start;

    while i < len {
        let mut val: u16 = 0;
        let mut digits: u8 = 0;
        let mut first_digit: u8 = 0;

        while i < len {
            let b = bytes[i];
            if b == b'.' {
                break;
            }
            let d = b.wrapping_sub(b'0');
            if d > 9 {
                return None;
            }
            if digits == 0 {
                first_digit = d;
            } else if first_digit == 0 {
                return None;
            }
            digits += 1;
            if digits > 3 {
                return None;
            }
            val = val * 10 + d as u16;
            i += 1;
        }

        if digits == 0 || val > 255 {
            return None;
        }
        acc = (acc << 8) | (val as u32);
        octet_idx += 1;

        if i < len {
            if octet_idx == 4 {
                return None;
            }
            i += 1;
            if i == len {
                return None;
            }
        }
    }

    if octet_idx == 4 { Some(acc) } else { None }
}

/// Zero-allocation parser for ASCII IPv4 or IPv6 bytes into an RFC 4291 `u128` integer.
///
/// - IPv4 addresses are encoded as RFC 4291 IPv4-mapped `u128` (`0x0000_FFFF_<ipv4_u32>`).
/// - IPv6 addresses (including compressed `::` and embedded IPv4) are encoded as 128-bit integers.
#[inline]
pub const fn parse_ip_u128(bytes: &[u8]) -> Option<u128> {
    if let Some(v4) = parse_ipv4_bytes(bytes) {
        return Some(encode_ipv4_u128(v4));
    }
    parse_ipv6_u128(bytes)
}

/// Zero-allocation parser for ASCII IPv6 bytes into a 128-bit host-endian integer.
#[inline]
pub const fn parse_ipv6_u128(bytes: &[u8]) -> Option<u128> {
    let len = bytes.len();
    if len < 2 || len > 45 {
        return None;
    }

    let mut head = [0u16; 8];
    let mut tail = [0u16; 8];
    let mut head_len: usize = 0;
    let mut tail_len: usize = 0;
    let mut ellipsis = false;
    let mut i = 0;

    if bytes[0] == b':' {
        if bytes[1] != b':' {
            return None;
        }
        ellipsis = true;
        i = 2;
    }

    while i < len {
        let seg_start = i;
        let mut val: u16 = 0;
        let mut digits: u8 = 0;

        while i < len {
            let b = bytes[i];
            let nib = match b {
                b'0'..=b'9' => (b - b'0') as u16,
                b'a'..=b'f' => (b - b'a' + 10) as u16,
                b'A'..=b'F' => (b - b'A' + 10) as u16,
                _ => break,
            };
            digits += 1;
            if digits > 4 {
                return None;
            }
            val = (val << 4) | nib;
            i += 1;
        }

        if i < len && bytes[i] == b'.' {
            let Some(v4) = parse_ipv4_bytes_from(bytes, seg_start) else {
                return None;
            };
            let hi = (v4 >> 16) as u16;
            let lo = (v4 & 0xFFFF) as u16;
            if ellipsis {
                if head_len + tail_len + 2 >= 8 {
                    return None;
                }
                tail[tail_len] = hi;
                tail[tail_len + 1] = lo;
                tail_len += 2;
            } else {
                if head_len + 2 != 8 {
                    return None;
                }
                head[head_len] = hi;
                head[head_len + 1] = lo;
                head_len += 2;
            }
            break;
        }

        if digits == 0 {
            return None;
        }

        if ellipsis {
            if head_len + tail_len >= 7 {
                return None;
            }
            tail[tail_len] = val;
            tail_len += 1;
        } else {
            if head_len >= 8 {
                return None;
            }
            head[head_len] = val;
            head_len += 1;
        }

        if i == len {
            break;
        }
        if bytes[i] != b':' {
            return None;
        }
        i += 1;
        if i == len {
            return None;
        }
        if bytes[i] == b':' {
            if ellipsis {
                return None;
            }
            ellipsis = true;
            i += 1;
            if i == len {
                break;
            }
        }
    }

    let mut groups = [0u16; 8];
    if ellipsis {
        if head_len + tail_len >= 8 {
            return None;
        }
        let mut idx = 0;
        while idx < head_len {
            groups[idx] = head[idx];
            idx += 1;
        }
        let zeros = 8 - head_len - tail_len;
        let mut t = 0;
        while t < tail_len {
            groups[head_len + zeros + t] = tail[t];
            t += 1;
        }
    } else {
        if head_len != 8 {
            return None;
        }
        groups = head;
    }

    let mut out: u128 = 0;
    let mut k = 0;
    while k < 8 {
        out = (out << 16) | (groups[k] as u128);
        k += 1;
    }
    Some(out)
}

// =============================================================================
// PART 1: IPv4 Subsystem (Decimal Octet Table 1,024 B & Dotted Pointer Stamping)
// =============================================================================

/// Pre-computed 4-byte packed ASCII representation of an 8-bit octet (`0`..=`255`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C, align(4))]
pub struct OctetEntry {
    /// Decimal ASCII bytes (1 to 3 digits), zero-padded at tail.
    pub bytes: [u8; 3],
    /// Actual number of ASCII digits (1..=3).
    pub len: u8,
}

const fn make_octet_table() -> [OctetEntry; 256] {
    let mut table = [OctetEntry {
        bytes: [0; 3],
        len: 0,
    }; 256];
    let mut i = 0;
    while i < 256 {
        if i < 10 {
            table[i] = OctetEntry {
                bytes: [b'0' + i as u8, 0, 0],
                len: 1,
            };
        } else if i < 100 {
            table[i] = OctetEntry {
                bytes: [b'0' + (i / 10) as u8, b'0' + (i % 10) as u8, 0],
                len: 2,
            };
        } else {
            table[i] = OctetEntry {
                bytes: [
                    b'0' + (i / 100) as u8,
                    b'0' + ((i / 10) % 10) as u8,
                    b'0' + (i % 10) as u8,
                ],
                len: 3,
            };
        }
        i += 1;
    }
    table
}

/// Canonical 1 KB lookup table for all 256 IPv4 octet values.
///
/// Occupies exactly 1,024 bytes (16 cache lines) aligned to 4 bytes.
pub const OCTET_TABLE: [OctetEntry; 256] = make_octet_table();

const _: () = assert!(std::mem::size_of::<[OctetEntry; 256]>() == 1024);
const _: () = assert!(std::mem::align_of::<[OctetEntry; 256]>() == 4);

/// Returns a reference to the pre-computed [`OctetEntry`] for a given octet `0..=255`.
#[inline(always)]
pub const fn lookup_octet(octet: u8) -> &'static OctetEntry {
    &OCTET_TABLE[octet as usize]
}

/// Formats an IPv4 address (encoded as host-endian `u32`) into dotted-decimal ASCII
/// directly into `ptr`, returning the total number of bytes written (7..=15).
///
/// Uses sequential direct pointer stamping from [`OCTET_TABLE`], interleaving `.` bytes.
///
/// # Safety
/// `ptr` must point to a memory buffer with at least 15 contiguous writable bytes.
#[inline(always)]
pub unsafe fn fast_write_ipv4_ptr(mut ptr: *mut u8, ip: u32) -> usize {
    unsafe {
        let start = ptr;

        let b0 = (ip >> 24) as usize;
        let b1 = ((ip >> 16) & 0xFF) as usize;
        let b2 = ((ip >> 8) & 0xFF) as usize;
        let b3 = (ip & 0xFF) as usize;

        // Octet 0
        let e0 = OCTET_TABLE.get_unchecked(b0);
        core::ptr::copy_nonoverlapping(e0.bytes.as_ptr(), ptr, 3);
        ptr = ptr.add(e0.len as usize);

        // Dot 1 + Octet 1
        *ptr = b'.';
        let e1 = OCTET_TABLE.get_unchecked(b1);
        core::ptr::copy_nonoverlapping(e1.bytes.as_ptr(), ptr.add(1), 3);
        ptr = ptr.add(1 + e1.len as usize);

        // Dot 2 + Octet 2
        *ptr = b'.';
        let e2 = OCTET_TABLE.get_unchecked(b2);
        core::ptr::copy_nonoverlapping(e2.bytes.as_ptr(), ptr.add(1), 3);
        ptr = ptr.add(1 + e2.len as usize);

        // Dot 3 + Octet 3
        *ptr = b'.';
        let e3 = OCTET_TABLE.get_unchecked(b3);
        core::ptr::copy_nonoverlapping(e3.bytes.as_ptr(), ptr.add(1), 3);
        ptr = ptr.add(1 + e3.len as usize);

        ptr as usize - start as usize
    }
}

/// Formats an IPv4 address into standard dotted-decimal ASCII bytes in a fixed stack buffer.
///
/// Returns `([u8; 15], usize)` containing the ASCII bytes and actual length (7..=15).
#[inline(always)]
pub fn format_ipv4_bytes(ip: u32) -> ([u8; 15], usize) {
    let mut buf = [0u8; 15];
    // SAFETY: buf has 15 writable bytes.
    let len = unsafe { fast_write_ipv4_ptr(buf.as_mut_ptr(), ip) };
    (buf, len)
}

/// Appends standard dotted-decimal ASCII representation of `ip` into `buf`.
#[inline(always)]
pub fn fast_write_ipv4(buf: &mut Vec<u8>, ip: u32) {
    buf.reserve(15);
    let cur = buf.len();
    let ptr = unsafe { buf.as_mut_ptr().add(cur) };
    // SAFETY: buf has at least 15 bytes reserved past cur.
    let len = unsafe { fast_write_ipv4_ptr(ptr, ip) };
    unsafe {
        buf.set_len(cur + len);
    }
}

// =============================================================================
// PART 2: IPv6 Subsystem (Hex Octet Table 512 B & RFC 5952 Zero Compression)
// =============================================================================

const fn make_hex_octet_table() -> [[u8; 2]; 256] {
    let hex_digits = b"0123456789abcdef";
    let mut table = [[0u8; 2]; 256];
    let mut i = 0;
    while i < 256 {
        table[i] = [hex_digits[i >> 4], hex_digits[i & 0x0F]];
        i += 1;
    }
    table
}

/// Canonical 512-byte lookup table mapping each `u8` (0..=255) to 2 lowercase hex digits.
///
/// Occupies exactly 512 bytes (8 cache lines).
pub const HEX_OCTET_TABLE: [[u8; 2]; 256] = make_hex_octet_table();

const _: () = assert!(std::mem::size_of::<[[u8; 2]; 256]>() == 512);

/// Returns the 2-digit lowercase hexadecimal ASCII representation of a byte `0..=255`.
#[inline(always)]
pub const fn lookup_hex_byte(byte: u8) -> [u8; 2] {
    HEX_OCTET_TABLE[byte as usize]
}

/// Writes a 16-bit word (0..=0xFFFF) as 1 to 4 lowercase hexadecimal ASCII digits directly into `ptr`,
/// returning the number of bytes written (1..=4). Leading zeros are omitted.
///
/// # Safety
/// `ptr` must point to a buffer with at least 4 writable bytes.
#[inline(always)]
pub unsafe fn fast_write_hextet_ptr(ptr: *mut u8, w: u16) -> usize {
    unsafe {
        if w >= 0x1000 {
            let hi = HEX_OCTET_TABLE[(w >> 8) as usize];
            let lo = HEX_OCTET_TABLE[(w & 0xFF) as usize];
            *ptr = hi[0];
            *ptr.add(1) = hi[1];
            *ptr.add(2) = lo[0];
            *ptr.add(3) = lo[1];
            4
        } else if w >= 0x0100 {
            let hi = HEX_OCTET_TABLE[(w >> 8) as usize];
            let lo = HEX_OCTET_TABLE[(w & 0xFF) as usize];
            *ptr = hi[1];
            *ptr.add(1) = lo[0];
            *ptr.add(2) = lo[1];
            3
        } else if w >= 0x0010 {
            let lo = HEX_OCTET_TABLE[(w & 0xFF) as usize];
            *ptr = lo[0];
            *ptr.add(1) = lo[1];
            2
        } else {
            let lo = HEX_OCTET_TABLE[(w & 0xFF) as usize];
            *ptr = lo[1];
            1
        }
    }
}

/// Formats an IPv6 address (encoded as host-endian `u128`) into canonical RFC 5952
/// ASCII representation directly into `ptr`, returning bytes written (2..=39).
///
/// Complies strictly with RFC 5952:
/// - Leading zeros within each 16-bit hextet are suppressed.
/// - The single longest sequence of consecutive zero hextets (length >= 2) is compressed as `::`.
/// - In case of ties in longest zero runs, the first occurrence is compressed.
///
/// # Safety
/// `ptr` must point to a memory buffer with at least 39 contiguous writable bytes.
#[inline(never)]
pub unsafe fn fast_write_ipv6_ptr(mut ptr: *mut u8, ip: u128) -> usize {
    unsafe {
        let start = ptr;

        if ip == 0 {
            core::ptr::copy_nonoverlapping(b"::".as_ptr(), ptr, 2);
            return 2;
        }
        if ip == 1 {
            core::ptr::copy_nonoverlapping(b"::1".as_ptr(), ptr, 3);
            return 3;
        }

        let mut words = [0u16; 8];
        words[0] = (ip >> 112) as u16;
        words[1] = ((ip >> 96) & 0xFFFF) as u16;
        words[2] = ((ip >> 80) & 0xFFFF) as u16;
        words[3] = ((ip >> 64) & 0xFFFF) as u16;
        words[4] = ((ip >> 48) & 0xFFFF) as u16;
        words[5] = ((ip >> 32) & 0xFFFF) as u16;
        words[6] = ((ip >> 16) & 0xFFFF) as u16;
        words[7] = (ip & 0xFFFF) as u16;

        let mut best_start = 8;
        let mut best_len = 0;
        let mut cur_start = 8;
        let mut cur_len = 0;

        let mut i = 0;
        while i < 8 {
            if words[i] == 0 {
                if cur_len == 0 {
                    cur_start = i;
                }
                cur_len += 1;
            } else if cur_len > 0 {
                if cur_len > best_len {
                    best_start = cur_start;
                    best_len = cur_len;
                }
                cur_len = 0;
            }
            i += 1;
        }
        if cur_len > best_len {
            best_start = cur_start;
            best_len = cur_len;
        }

        if best_len < 2 {
            best_start = 8;
            best_len = 0;
        }

        i = 0;
        while i < 8 {
            if i == best_start {
                *ptr = b':';
                ptr = ptr.add(1);
                *ptr = b':';
                ptr = ptr.add(1);
                i += best_len;
                if i == 8 {
                    break;
                }
            } else if i > 0 && i != best_start + best_len {
                *ptr = b':';
                ptr = ptr.add(1);
            }

            let hex_len = fast_write_hextet_ptr(ptr, words[i]);
            ptr = ptr.add(hex_len);
            i += 1;
        }

        ptr as usize - start as usize
    }
}

/// Formats an IPv6 address into canonical RFC 5952 ASCII bytes in a fixed stack buffer.
///
/// Returns `([u8; 39], usize)` containing the ASCII bytes and actual length (2..=39).
#[inline(always)]
pub fn format_ipv6_bytes(ip: u128) -> ([u8; 39], usize) {
    let mut buf = [0u8; 39];
    // SAFETY: buf has 39 writable bytes.
    let len = unsafe { fast_write_ipv6_ptr(buf.as_mut_ptr(), ip) };
    (buf, len)
}

/// Appends canonical RFC 5952 ASCII representation of IPv6 `ip` into `buf`.
#[inline(always)]
pub fn fast_write_ipv6(buf: &mut Vec<u8>, ip: u128) {
    buf.reserve(39);
    let cur = buf.len();
    let ptr = unsafe { buf.as_mut_ptr().add(cur) };
    // SAFETY: buf has at least 39 bytes reserved past cur.
    let len = unsafe { fast_write_ipv6_ptr(ptr, ip) };
    unsafe {
        buf.set_len(cur + len);
    }
}

// =============================================================================
// PART 3: Unified Dispatcher (IPv4-Mapped IPv6, Loopback & Native IPv6)
// =============================================================================

/// Formats an IP address (IPv4-mapped, IPv4, or native IPv6) encoded in `u128` into standard ASCII
/// directly into `ptr`, returning total bytes written.
///
/// - If `ip` is an IPv4-mapped IPv6 address (`::ffff:a.b.c.d`), formats as dotted-decimal `"a.b.c.d"`.
/// - If `ip` is `::1` or `::`, formats as `"::1"` or `"::"`.
/// - Otherwise, formats as canonical RFC 5952 lowercase IPv6.
///
/// # Safety
/// `ptr` must point to a memory buffer with at least 45 contiguous writable bytes.
#[inline(always)]
pub unsafe fn fast_write_ip_ptr(ptr: *mut u8, ip: u128) -> usize {
    unsafe {
        if (ip >> 48) == 0 && ((ip >> 32) & 0xFFFF) == 0xFFFF {
            fast_write_ipv4_ptr(ptr, ip as u32)
        } else if ip == 1 {
            core::ptr::copy_nonoverlapping(b"::1".as_ptr(), ptr, 3);
            3
        } else if ip == 0 {
            core::ptr::copy_nonoverlapping(b"::".as_ptr(), ptr, 2);
            2
        } else {
            fast_write_ipv6_ptr(ptr, ip)
        }
    }
}

/// Formats an IP address (IPv4-mapped or IPv6) into standard ASCII bytes in a fixed stack buffer.
///
/// Returns `([u8; 45], usize)` containing the ASCII bytes and actual length (2..=39).
#[inline(always)]
pub fn format_ip_bytes(ip: u128) -> ([u8; 45], usize) {
    let mut buf = [0u8; 45];
    // SAFETY: buf has 45 writable bytes.
    let len = unsafe { fast_write_ip_ptr(buf.as_mut_ptr(), ip) };
    (buf, len)
}

/// Appends standard ASCII representation of `ip` (IPv4-mapped or IPv6) into `buf`.
#[inline(always)]
pub fn fast_write_ip(buf: &mut Vec<u8>, ip: u128) {
    buf.reserve(45);
    let cur = buf.len();
    let ptr = unsafe { buf.as_mut_ptr().add(cur) };
    // SAFETY: buf has at least 45 bytes reserved past cur.
    let len = unsafe { fast_write_ip_ptr(ptr, ip) };
    unsafe {
        buf.set_len(cur + len);
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // --- Part 1: IPv4 Tests ---

    #[test]
    fn test_ipv4_octet_table_completeness() {
        assert_eq!(std::mem::size_of::<[OctetEntry; 256]>(), 1024);
        for i in 0..=255u8 {
            let entry = lookup_octet(i);
            let s = std::str::from_utf8(&entry.bytes[..entry.len as usize]).unwrap();
            assert_eq!(s, i.to_string());
        }
    }

    #[test]
    fn test_ipv4_boundaries() {
        let cases: &[(u32, &str)] = &[
            (0x00000000, "0.0.0.0"),
            (0xFFFFFFFF, "255.255.255.255"),
            (0x7F000001, "127.0.0.1"),
            (0x0A000001, "10.0.0.1"),
            (0xC0A80101, "192.168.1.1"),
            (0xC0A80164, "192.168.1.100"),
            (0x01020304, "1.2.3.4"),
            (0x0A0B0C0D, "10.11.12.13"),
            (0x64646464, "100.100.100.100"),
        ];

        for &(ip, expected) in cases {
            let (bytes, len) = format_ipv4_bytes(ip);
            let s = std::str::from_utf8(&bytes[..len]).unwrap();
            assert_eq!(s, expected, "Failed for IP 0x{ip:08X}");

            let mut vec = Vec::new();
            fast_write_ipv4(&mut vec, ip);
            assert_eq!(std::str::from_utf8(&vec).unwrap(), expected);
        }
    }

    // --- Part 2: IPv6 Tests ---

    #[test]
    fn test_ipv6_hex_octet_table_completeness() {
        assert_eq!(std::mem::size_of::<[[u8; 2]; 256]>(), 512);
        for i in 0..=255u8 {
            let pair = lookup_hex_byte(i);
            let s = std::str::from_utf8(&pair).unwrap();
            assert_eq!(s, format!("{i:02x}"));
        }
    }

    #[test]
    fn test_ipv6_canonical_compression() {
        let cases: &[(u128, &str)] = &[
            (0, "::"),
            (1, "::1"),
            (0x2001_0DB8_0000_0000_0000_0000_0000_0001, "2001:db8::1"),
            (0xFE80_0000_0000_0000_0000_0000_0000_0001, "fe80::1"),
            (
                0x2001_0DB8_85A3_0000_0000_8A2E_0370_7334,
                "2001:db8:85a3::8a2e:370:7334",
            ),
            (
                0xFFFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF,
                "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
            ),
        ];

        for &(ip, expected) in cases {
            let (bytes, len) = format_ipv6_bytes(ip);
            let s = std::str::from_utf8(&bytes[..len]).unwrap();
            assert_eq!(s, expected, "Failed for IPv6 0x{ip:032X}");

            let mut vec = Vec::new();
            fast_write_ipv6(&mut vec, ip);
            assert_eq!(std::str::from_utf8(&vec).unwrap(), expected);
        }
    }

    // --- Part 3: Unified Dispatcher Tests ---

    #[test]
    fn test_unified_dispatcher() {
        // IPv4-mapped IPv6
        let ipv4_mapped = 0x0000_0000_0000_0000_0000_FFFF_C0A8_0164;
        let (bytes, len) = format_ip_bytes(ipv4_mapped);
        assert_eq!(std::str::from_utf8(&bytes[..len]).unwrap(), "192.168.1.100");

        // Native IPv6
        let native_v6 = 0x2001_0DB8_0000_0000_0000_0000_0000_0001;
        let (bytes, len) = format_ip_bytes(native_v6);
        assert_eq!(std::str::from_utf8(&bytes[..len]).unwrap(), "2001:db8::1");

        // Loopback & Unspecified
        let (bytes, len) = format_ip_bytes(1);
        assert_eq!(std::str::from_utf8(&bytes[..len]).unwrap(), "::1");

        let (bytes, len) = format_ip_bytes(0);
        assert_eq!(std::str::from_utf8(&bytes[..len]).unwrap(), "::");
    }

    // --- Part 0: Integer Codecs & Zero-Allocation Parsers Tests ---

    #[test]
    fn test_integer_ip_codecs_and_helpers() {
        let v4: IpAddr = "192.168.1.42".parse().unwrap();
        let v4_u128 = encode_ip_u128(v4);
        assert_eq!(v4_u128, encode_ipv4_u128(0xC0A8_012A));
        assert_eq!(is_ipv4_mapped_u128(v4_u128), Some(0xC0A8_012A));
        assert_eq!(encode_upstream_ip_u32(v4), 0xC0A8_012A);

        let v6: IpAddr = "2001:db8:85a3::8a2e:370:7334".parse().unwrap();
        let v6_u128 = encode_ip_u128(v6);
        assert_eq!(is_ipv4_mapped_u128(v6_u128), None);
        assert_ne!(encode_upstream_ip_u32(v6), 0);

        let mapped_v6: IpAddr = "::ffff:10.0.0.1".parse().unwrap();
        assert_eq!(encode_upstream_ip_u32(mapped_v6), 0x0A00_0001);
        assert_eq!(is_ipv4_mapped_u128(0), None);
        assert_eq!(is_ipv4_mapped_u128(1), None);
    }

    #[test]
    fn test_parse_ip_u128() {
        let valid_inputs = [
            "0.0.0.0",
            "127.0.0.1",
            "192.168.1.100",
            "255.255.255.255",
            "::",
            "::1",
            "fe80::",
            "fe80::1",
            "2001:db8::1",
            "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
            "1:2:3:4:5:6:7::",
            "::1:2:3:4:5:6:7",
            "::ffff:192.168.1.100",
            "::192.0.2.1",
            "64:ff9b::192.0.2.1",
            "0:0:0:0:0:ffff:192.0.2.1",
            "FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF",
        ];

        for s in valid_inputs {
            let expected: IpAddr = s.parse().unwrap();
            assert_eq!(
                parse_ip_u128(s.as_bytes()),
                Some(encode_ip_u128(expected)),
                "Mismatch for valid IP {s}"
            );
        }

        let invalid_inputs = [
            "",
            "1.2.3",
            "1.2.3.4.5",
            "1.2.3.",
            ".1.2.3",
            "1..2.3",
            "256.0.0.1",
            "01.0.0.1",
            "127.0.0.1 ",
            " 127.0.0.1",
            ":",
            "::1::2",
            ":::1",
            "2001:db8:",
            ":2001:db8",
            "12345::1",
            "1:2:3:4:5:6:7",
            "1:2:3:4:5:6:7:8:9",
            "1:2:3:4:5:6:7:8::",
            "::1:2:3:4:5:6:7:8",
            "1:2:3:4:5:6::1.2.3.4",
            "1:2:3:4:5:1.2.3.4",
            "::ffff:999.0.0.1",
            "::ffff:1.2.3.4:5",
            "not-an-ip",
        ];

        for s in invalid_inputs {
            assert_eq!(
                parse_ip_u128(s.as_bytes()),
                None,
                "Expected None for invalid IP {s:?}"
            );
        }
    }
}
