//! ClickHouse RowBinary Format Serializer (`clickhouse_log_shipper::format::row_binary`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                    CLICKHOUSE ROWBINARY SERIALIZER SPECIFICATION                        │
//! ├───────────────────┬───────────────┬────────────────┬───────────────┬────────────────────┤
//! │ Record Subsystem  │ Wire Layout   │ Encoding Type  │ Wire Size     │ Stamping Mechanics │
//! ├───────────────────┼───────────────┼────────────────┼───────────────┼────────────────────┤
//! │ ingress_time_nanos │ DateTime64(9)LE│ Binary Integer │ 8 bytes       │ write_unaligned    │
//! │ request_id        │ UInt64 LE     │ Binary Integer │ 8 bytes       │ write_unaligned    │
//! │ duration_ns       │ UInt64 LE     │ Binary Integer │ 8 bytes       │ write_unaligned    │
//! │ peer_ip           │ String        │ 1B len + ASCII │ 4..=40 bytes  │ fast_write_ip_ptr  │
//! │ upstream_ip       │ String        │ 1B len + ASCII │ 5..=16 bytes  │ fast_write_ipv4_ptr│
//! │ upstream_port/route │ Packed 2xU16  │ Binary Integer │ 4 bytes       │ Packed 32b store   │
//! │ pipe              │ String        │ 1B len + ASCII │ 11..=25 bytes │ write_fixed_str_ptr│
//! │ method            │ String        │ 1B len + ASCII │ 4..=8 bytes   │ write_fixed_str_ptr│
//! │ url               │ String        │ Varint + ASCII │ N + 1..5 bytes│ write_leb128_varint│
//! │ bytes_rx / tx     │ Packed 2xU32  │ Binary Integer │ 8 bytes       │ Packed 64b store   │
//! │ internal_status   │ String        │ 1B len + ASCII │ 4..=24 bytes  │ write_fixed_str_ptr│
//! │ user_agent_family │ String        │ 1B len + ASCII │ 5..=13 bytes  │ write_fixed_str_ptr│
//! │ user_agent_os     │ String        │ 1B len + ASCII │ 5..=8 bytes   │ write_fixed_str_ptr│
//! ├───────────────────┴───────────────┴────────────────┼───────────────┴────────────────────┤
//! │ Total Record Wire Size:                            │ ~85..=210 bytes (Zero Delimiters)  │
//! └────────────────────────────────────────────────────┴────────────────────────────────────┘
//!
//!  Memory Pipeline & Direct Pointer Stamping:
//!
//!    TelemetryBatch (L2 Prefetch _MM_HINT_T1)
//!         │
//!         ▼
//!    [ TelemetryRecord ] ──► Upfront Reservation: out.reserve(total_recs * 384)
//!         │
//!         ├─── Integers (ts, id, dur, ports) ──► core::ptr::write_unaligned (LE)
//!         ├─── IP Addrs (peer_ip, upstream)  ──► fast_write_ip_ptr (velda-core::lut::ip)
//!         ├─── Enums (pipe, meth, comp, ...) ──► write_fixed_str_ptr (1B len + Core bytes)
//!         └─── Dynamic URL (path, query)     ──► write_leb128_varint_ptr + memcpy
//!         │
//!         ▼
//!    [ Continuous RowBinary Wire Buffer ] ──► Single Terminal out.set_len(cursor)
//!
//!  RowBinary String Wire Structure:
//!  ┌───────────────┬────────────────────────────────────────────────────────┐
//!  │ 1-Byte Length │ Raw UTF-8 / ASCII Payload (Zero Null Terminator)       │
//!  │ (e.g. 0x03)   │ "GET"                                                  │
//!  └───────────────┴────────────────────────────────────────────────────────┘
//! ```
//!
//! # ClickHouse RowBinary Specification
//! - Integers: Little-endian primitive bytes (`to_le_bytes()`).
//! - Strings: Unsigned LEB128 varint byte length prefix followed by raw payload bytes (zero terminator).
//! - Zero heap allocation per record: Uses core-local pre-reserved buffers and direct pointer stamping.

use velda_core::lut::ip::{fast_write_ip_ptr, fast_write_ipv4_ptr};
use velda_observability::{TelemetryBatch, TelemetryRecord};

/// Upper bound of one RowBinary row (184 B URL + IPv6 + enum names + 2 B packed over-store).
const MAX_ROW_BYTES: usize = 512;

/// Writes unsigned LEB128 variable-length integer (ClickHouse string length prefix).
#[inline(always)]
pub fn write_leb128_varint(buf: &mut Vec<u8>, mut value: usize) {
    while value >= 0x80 {
        buf.push((value as u8 & 0x7F) | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

/// Writes unsigned LEB128 varint directly to `ptr`, returning number of bytes written.
///
/// # Safety
/// `ptr` must point to a buffer with at least 5 writable bytes.
#[inline(always)]
pub(crate) unsafe fn write_leb128_varint_ptr(mut ptr: *mut u8, mut value: usize) -> usize {
    unsafe {
        let start = ptr;
        while value >= 0x80 {
            *ptr = (value as u8 & 0x7F) | 0x80;
            ptr = ptr.add(1);
            value >>= 7;
        }
        *ptr = value as u8;
        ptr = ptr.add(1);
        ptr as usize - start as usize
    }
}

/// Writes a string slice (< 128 bytes) with ClickHouse RowBinary 1-byte LEB128 varint length prefix.
///
/// Directly writes `*ptr = s.len() as u8`, then copies payload bytes from `s`.
/// Designed for Core vocabulary enums whose ASCII representation never exceeds 127 bytes.
/// Returns total bytes written (`1 + s.len()`).
///
/// # Safety
/// `ptr` must point to a buffer with at least `1 + s.len()` writable bytes.
#[inline(always)]
pub(crate) unsafe fn write_fixed_str_ptr(ptr: *mut u8, s: &[u8]) -> usize {
    unsafe {
        *ptr = s.len() as u8;
        core::ptr::copy_nonoverlapping(s.as_ptr(), ptr.add(1), s.len());
        1 + s.len()
    }
}

/// Encodes batches of [`TelemetryRecord`] into ClickHouse RowBinary format.
///
/// Appends binary records directly into `out` buffer using direct pointer stamping
/// and Core vocabulary framing, eliminating per-field capacity checks and reallocation branching.
/// Employs `_MM_HINT_T1` prefetching to keep the record batch in L2 without polluting L1d cache.
/// Returns the number of records serialized into `out`.
#[inline(never)]
pub fn format_row_binary(batches: &[TelemetryBatch], out: &mut Vec<u8>) -> usize {
    let total_recs: usize = batches.iter().map(|b| b.len()).sum();
    out.reserve(total_recs * 256);
    let mut serialized_count = 0;

    for batch in batches {
        let recs = batch.as_ref();
        for (idx, rec) in recs.iter().enumerate() {
            #[cfg(target_arch = "x86_64")]
            if idx + 4 < recs.len() {
                unsafe {
                    use core::arch::x86_64::*;
                    let next_rec =
                        recs.get_unchecked(idx + 4) as *const TelemetryRecord as *const i8;
                    _mm_prefetch(next_rec, _MM_HINT_T1);
                    _mm_prefetch(next_rec.add(64), _MM_HINT_T1);
                    _mm_prefetch(next_rec.add(128), _MM_HINT_T1);
                    _mm_prefetch(next_rec.add(192), _MM_HINT_T1);
                }
            }

            serialized_count += 1;

            // Per-row capacity guard: worst case row is ~380 B (184 B URL + IPv6 + enum names)
            // plus the 2-byte over-store of the packed u64 at field 7..9.
            out.reserve(MAX_ROW_BYTES);

            unsafe {
                let base_ptr = out.as_mut_ptr();
                let mut ptr = base_ptr.add(out.len());

                // 1. timestamp (DateTime64(9): UInt64 LE, 8B)
                core::ptr::write_unaligned(ptr as *mut u64, rec.ingress_time_nanos.to_le());
                ptr = ptr.add(8);

                // 2. request_id (UInt64 LE, 8B)
                core::ptr::write_unaligned(ptr as *mut u64, rec.request_id.to_le());
                ptr = ptr.add(8);

                // 3. duration_ns (UInt64 LE, 8B)
                core::ptr::write_unaligned(ptr as *mut u64, rec.duration_ns.to_le());
                ptr = ptr.add(8);

                // 4. peer_ip (String: 1B varint len + ASCII IP bytes)
                let ip_len = fast_write_ip_ptr(ptr.add(1), rec.peer_ip);
                *ptr = ip_len as u8;
                ptr = ptr.add(1 + ip_len);

                // 5. upstream_ip (String: 1B varint len + ASCII IP bytes)
                if rec.upstream_ip != 0 {
                    let up_len = fast_write_ipv4_ptr(ptr.add(1), rec.upstream_ip);
                    *ptr = up_len as u8;
                    ptr = ptr.add(1 + up_len);
                } else {
                    core::ptr::copy_nonoverlapping(b"\x04none".as_ptr(), ptr, 5);
                    ptr = ptr.add(5);
                }

                // 7, 8. upstream_port (2B) + route_id (2B) = 4B packed
                let port_route = (rec.upstream_port as u32) | ((rec.route_id as u32) << 16);
                core::ptr::write_unaligned(ptr as *mut u32, port_route.to_le());
                ptr = ptr.add(4);

                // 10. pipe (Direct framing from Core vocabulary)
                ptr = ptr.add(write_fixed_str_ptr(ptr, rec.pipe.as_bytes()));

                // 11. method (Direct framing from Core vocabulary)
                ptr = ptr.add(write_fixed_str_ptr(ptr, rec.method.as_bytes()));

                // 12. url (String: varint len + raw bytes)
                let url = rec.url().as_bytes();
                let v_len = write_leb128_varint_ptr(ptr, url.len());
                ptr = ptr.add(v_len);
                core::ptr::copy_nonoverlapping(url.as_ptr(), ptr, url.len());
                ptr = ptr.add(url.len());

                // 13 & 14. bytes_rx + bytes_tx (Packed 2x UInt32 into single 64-bit store, 8B)
                let bytes_rx_tx = (rec.bytes_rx as u64) | ((rec.bytes_tx as u64) << 32);
                core::ptr::write_unaligned(ptr as *mut u64, bytes_rx_tx.to_le());
                ptr = ptr.add(8);

                // 15. internal_status (Direct framing from Core vocabulary)
                ptr = ptr.add(write_fixed_str_ptr(ptr, rec.internal_status.as_bytes()));

                // 16. user_agent_family (Direct framing from Core vocabulary)
                ptr = ptr.add(write_fixed_str_ptr(ptr, rec.ua_family.as_bytes()));

                // 18. user_agent_os (Direct framing from Core vocabulary)
                ptr = ptr.add(write_fixed_str_ptr(ptr, rec.ua_os.as_bytes()));

                out.set_len(ptr as usize - base_ptr as usize);
            }
        }
    }

    serialized_count
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use velda_core::{HttpMethod, InternalStatus, PipeKind, UaFamily, UaOs};
    use velda_observability::TelemetryRecord;

    fn make_test_record() -> TelemetryRecord {
        let mut rec = TelemetryRecord::zeroed();
        rec.ingress_time_nanos = 1_732_011_000_123_456_789;
        rec.request_id = 987654321;
        rec.duration_ns = 250_000;
        // IPv4-mapped IPv6: 192.168.1.100 -> (192<<24 | 168<<16 | 1<<8 | 100) = 0xC0A80164
        rec.peer_ip = 0x0000_0000_0000_0000_0000_FFFF_C0A8_0164;
        // Upstream IPv4: 10.0.0.1 -> 0x0A000001
        rec.upstream_ip = 0x0A000001;
        rec.upstream_port = 8080;
        rec.route_id = 42;
        rec.pipe = PipeKind::BufferedH1H1;
        rec.method = HttpMethod::Get;
        rec.internal_status = InternalStatus::Completed;
        rec.ua_family = UaFamily::Curl;
        rec.ua_os = UaOs::Linux;
        rec.bytes_rx = 128;
        rec.bytes_tx = 4096;
        rec.set_url("/api/v1/orders?id=123");
        rec
    }

    #[test]
    fn test_row_binary_serialization() {
        let rec = make_test_record();
        let batch = TelemetryBatch::from_arc(Arc::new([rec]));
        let mut out = Vec::new();

        let count = format_row_binary(&[batch], &mut out);
        assert_eq!(count, 1);
        assert!(!out.is_empty());

        // Decode fields sequentially according to RowBinary spec
        let mut offset = 0;

        // 1. timestamp: u64 LE
        let ts = u64::from_le_bytes(out[offset..offset + 8].try_into().unwrap());
        assert_eq!(ts, 1_732_011_000_123_456_789);
        offset += 8;

        // 2. request_id: u64 LE
        let req_id = u64::from_le_bytes(out[offset..offset + 8].try_into().unwrap());
        assert_eq!(req_id, 987654321);
        offset += 8;

        // 3. duration_ns: u64 LE
        let dur = u64::from_le_bytes(out[offset..offset + 8].try_into().unwrap());
        assert_eq!(dur, 250_000);
        offset += 8;

        // 4. peer_ip: varint len + string
        let ip_len = out[offset] as usize;
        offset += 1;
        let peer_ip_str = std::str::from_utf8(&out[offset..offset + ip_len]).unwrap();
        assert_eq!(peer_ip_str, "192.168.1.100");
        offset += ip_len;

        // 5. upstream_ip: varint len + string
        let up_ip_len = out[offset] as usize;
        offset += 1;
        let up_ip_str = std::str::from_utf8(&out[offset..offset + up_ip_len]).unwrap();
        assert_eq!(up_ip_str, "10.0.0.1");
        offset += up_ip_len;

        // 7. upstream_port: u16 LE
        let up_port = u16::from_le_bytes(out[offset..offset + 2].try_into().unwrap());
        assert_eq!(up_port, 8080);
        offset += 2;

        // 8. route_id: u16 LE
        let route = u16::from_le_bytes(out[offset..offset + 2].try_into().unwrap());
        assert_eq!(route, 42);
        offset += 2;

        // 10. pipe
        let pipe_len = out[offset] as usize;
        offset += 1;
        let pipe_str = std::str::from_utf8(&out[offset..offset + pipe_len]).unwrap();
        assert_eq!(pipe_str, PipeKind::BufferedH1H1.as_str());
        offset += pipe_len;

        // 11. method
        let meth_len = out[offset] as usize;
        offset += 1;
        let meth_str = std::str::from_utf8(&out[offset..offset + meth_len]).unwrap();
        assert_eq!(meth_str, HttpMethod::Get.as_str());
        offset += meth_len;

        // 12. url
        let url_len = out[offset] as usize;
        offset += 1;
        let url_str = std::str::from_utf8(&out[offset..offset + url_len]).unwrap();
        assert_eq!(url_str, "/api/v1/orders?id=123");
        offset += url_len;

        // 13 & 14. bytes_rx & bytes_tx
        let bytes_rx = u32::from_le_bytes(out[offset..offset + 4].try_into().unwrap());
        assert_eq!(bytes_rx, 128);
        offset += 4;
        let bytes_tx = u32::from_le_bytes(out[offset..offset + 4].try_into().unwrap());
        assert_eq!(bytes_tx, 4096);
        offset += 4;

        // 15. internal_status
        let comp_len = out[offset] as usize;
        offset += 1;
        let comp_str = std::str::from_utf8(&out[offset..offset + comp_len]).unwrap();
        assert_eq!(comp_str, InternalStatus::Completed.as_str());
        offset += comp_len;

        // 16. user_agent_family
        let fam_len = out[offset] as usize;
        offset += 1;
        let fam_str = std::str::from_utf8(&out[offset..offset + fam_len]).unwrap();
        assert_eq!(fam_str, UaFamily::Curl.as_str());
        offset += fam_len;

        // 18. user_agent_os
        let os_len = out[offset] as usize;
        offset += 1;
        let os_str = std::str::from_utf8(&out[offset..offset + os_len]).unwrap();
        assert_eq!(os_str, UaOs::Linux.as_str());
    }

    #[test]
    fn test_row_binary_full_serialization() {
        let mut ok_rec = make_test_record();
        ok_rec.request_id = 1;
        ok_rec.internal_status = InternalStatus::Completed;

        let mut err_rec = make_test_record();
        err_rec.request_id = 2;
        err_rec.internal_status = InternalStatus::UpstreamError;

        let batch = TelemetryBatch::from_arc(Arc::new([ok_rec, err_rec]));
        let mut out = Vec::new();

        let count = format_row_binary(std::slice::from_ref(&batch), &mut out);
        assert_eq!(count, 2);
        assert!(!out.is_empty());
    }
}
