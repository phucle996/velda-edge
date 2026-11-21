//! Kafka RecordBatch v2 Wire Formatter & Serialization (`kafka_log_shipper::format`).
//!
//! # Architecture & Kafka RecordBatch v2 Layout
//! ```text
//! ┌────────────────────────────────────────────────────────────────────────┐
//! │                    KAFKA RECORDBATCH V2 (MAGIC = 2)                   │
//! ├────────────────────────┬───────────┬───────────────────────────────────┤
//! │ Field                  │ Type      │ Description                       │
//! ├────────────────────────┼───────────┼───────────────────────────────────┤
//! │ baseOffset             │ INT64     │ First offset in batch (always 0)  │
//! │ batchLength            │ INT32     │ Byte length from epoch to end     │
//! │ partitionLeaderEpoch   │ INT32     │ Leader epoch (default -1)         │
//! │ magic                  │ INT8      │ Magic byte = 2                    │
//! │ crc                    │ UINT32    │ Castagnoli CRC32-C (from attrs)   │
//! │ attributes             │ INT16     │ Bits 0..2: Codec (0, 1=gz, 4=zstd)│
//! │ lastOffsetDelta        │ INT32     │ Record count - 1                  │
//! │ baseTimestamp          │ INT64     │ Earliest record timestamp (ms)    │
//! │ maxTimestamp           │ INT64     │ Maximum record timestamp (ms)     │
//! │ producerId             │ INT64     │ Producer ID (-1)                  │
//! │ producerEpoch          │ INT16     │ Producer epoch (-1)               │
//! │ baseSequence           │ INT32     │ Sequence number (-1)              │
//! │ recordsCount           │ INT32     │ Total records in this batch       │
//! │ records (compressed)   │ [Record]  │ Encoded records array (varints)   │
//! └────────────────────────┴───────────┴───────────────────────────────────┘
//! ```

pub mod binary;
pub mod json;
pub mod text;

use velda_observability::TelemetryRecord;

use super::compress::compress_records;
use super::config::{CompressionCodec, KafkaLogFormat};

// ── CRC32-C (Castagnoli 0x82F63B78) ──────────────────────────────────────────

/// Precomputed 256-entry lookup table for CRC32-C (Castagnoli polynomial fallback).
const CRC32C_TABLE: [u32; 256] = {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            if (crc & 1) != 0 {
                crc = (crc >> 1) ^ 0x82F63B78;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

/// Computes Castagnoli CRC32-C via 256-entry lookup table (pure software fallback).
#[inline]
pub fn crc32c_table(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &b in data {
        crc = (crc >> 8) ^ CRC32C_TABLE[((crc ^ (b as u32)) & 0xFF) as usize];
    }
    !crc
}

/// Constant stride for 3-way interleaved pipeline (256 bytes per lane -> 768 bytes per block).
#[cfg(target_arch = "x86_64")]
const CRC32C_STRIDE_K: usize = 256;

#[cfg(target_arch = "x86_64")]
const fn crc_shift_byte(mut crc: u32, zero_bytes: usize) -> u32 {
    let mut z = 0;
    while z < zero_bytes {
        crc = (crc >> 8) ^ CRC32C_TABLE[(crc & 0xFF) as usize];
        z += 1;
    }
    crc
}

/// Precomputed 4x256 linear reduction table for shifting a 32-bit CRC by `K` (256 zero bytes).
#[cfg(target_arch = "x86_64")]
const SHIFT_K_TABLE: [[u32; 256]; 4] = {
    let mut table = [[0u32; 256]; 4];
    let mut b = 0;
    while b < 4 {
        let mut v = 0;
        while v < 256 {
            let crc = (v as u32) << (b * 8);
            table[b][v] = crc_shift_byte(crc, CRC32C_STRIDE_K);
            v += 1;
        }
        b += 1;
    }
    table
};

/// Precomputed 4x256 linear reduction table for shifting a 32-bit CRC by `2*K` (512 zero bytes).
#[cfg(target_arch = "x86_64")]
const SHIFT_2K_TABLE: [[u32; 256]; 4] = {
    let mut table = [[0u32; 256]; 4];
    let mut b = 0;
    while b < 4 {
        let mut v = 0;
        while v < 256 {
            let crc = (v as u32) << (b * 8);
            table[b][v] = crc_shift_byte(crc, 2 * CRC32C_STRIDE_K);
            v += 1;
        }
        b += 1;
    }
    table
};

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn shift_k(c: u32) -> u32 {
    SHIFT_K_TABLE[0][(c & 0xFF) as usize]
        ^ SHIFT_K_TABLE[1][((c >> 8) & 0xFF) as usize]
        ^ SHIFT_K_TABLE[2][((c >> 16) & 0xFF) as usize]
        ^ SHIFT_K_TABLE[3][((c >> 24) & 0xFF) as usize]
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn shift_2k(c: u32) -> u32 {
    SHIFT_2K_TABLE[0][(c & 0xFF) as usize]
        ^ SHIFT_2K_TABLE[1][((c >> 8) & 0xFF) as usize]
        ^ SHIFT_2K_TABLE[2][((c >> 16) & 0xFF) as usize]
        ^ SHIFT_2K_TABLE[3][((c >> 24) & 0xFF) as usize]
}

/// Hardware-accelerated CRC32-C (Castagnoli) using x86_64 SSE4.2 instructions.
///
/// Implements 3-Way Instruction-Level Parallelism (ILP) interleaving:
/// - The 3-cycle serial latency of `crc32q` is hidden by running 3 independent streams
///   simultaneously on 3 separate 64-bit general-purpose registers (`crc0`, `crc1`, `crc2`).
/// - Operates with zero AVX vector instructions, preventing CPU core frequency throttling
///   or `vzeroupper` penalties on edge servers.
/// - Triples throughput from ~11 GB/s to ~30+ GB/s for medium/large batches.
///
/// # Safety
/// Caller must ensure that CPU target feature `sse4.2` is supported and enabled.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn crc32c_sse42(data: &[u8]) -> u32 {
    unsafe {
        let mut ptr = data.as_ptr();
        let mut len = data.len();
        let mut crc: u64 = !0u32 as u64;

        // 3-Way Interleaved Pipeline for blocks >= 768 bytes (3 * 256 bytes)
        const CHUNK: usize = 3 * CRC32C_STRIDE_K;
        while len >= CHUNK {
            let p0 = ptr;
            let p1 = ptr.add(CRC32C_STRIDE_K);
            let p2 = ptr.add(2 * CRC32C_STRIDE_K);

            let mut crc0 = crc;
            let mut crc1: u64 = 0;
            let mut crc2: u64 = 0;

            let mut off = 0;
            while off < CRC32C_STRIDE_K {
                let d0_0 = (p0.add(off) as *const u64).read_unaligned();
                let d1_0 = (p1.add(off) as *const u64).read_unaligned();
                let d2_0 = (p2.add(off) as *const u64).read_unaligned();
                crc0 = core::arch::x86_64::_mm_crc32_u64(crc0, d0_0);
                crc1 = core::arch::x86_64::_mm_crc32_u64(crc1, d1_0);
                crc2 = core::arch::x86_64::_mm_crc32_u64(crc2, d2_0);

                let d0_1 = (p0.add(off + 8) as *const u64).read_unaligned();
                let d1_1 = (p1.add(off + 8) as *const u64).read_unaligned();
                let d2_1 = (p2.add(off + 8) as *const u64).read_unaligned();
                crc0 = core::arch::x86_64::_mm_crc32_u64(crc0, d0_1);
                crc1 = core::arch::x86_64::_mm_crc32_u64(crc1, d1_1);
                crc2 = core::arch::x86_64::_mm_crc32_u64(crc2, d2_1);

                let d0_2 = (p0.add(off + 16) as *const u64).read_unaligned();
                let d1_2 = (p1.add(off + 16) as *const u64).read_unaligned();
                let d2_2 = (p2.add(off + 16) as *const u64).read_unaligned();
                crc0 = core::arch::x86_64::_mm_crc32_u64(crc0, d0_2);
                crc1 = core::arch::x86_64::_mm_crc32_u64(crc1, d1_2);
                crc2 = core::arch::x86_64::_mm_crc32_u64(crc2, d2_2);

                let d0_3 = (p0.add(off + 24) as *const u64).read_unaligned();
                let d1_3 = (p1.add(off + 24) as *const u64).read_unaligned();
                let d2_3 = (p2.add(off + 24) as *const u64).read_unaligned();
                crc0 = core::arch::x86_64::_mm_crc32_u64(crc0, d0_3);
                crc1 = core::arch::x86_64::_mm_crc32_u64(crc1, d1_3);
                crc2 = core::arch::x86_64::_mm_crc32_u64(crc2, d2_3);

                off += 32;
            }

            // Combine partial CRCs using precomputed constant linear tables
            crc = (shift_2k(crc0 as u32) ^ shift_k(crc1 as u32) ^ (crc2 as u32)) as u64;

            ptr = ptr.add(CHUNK);
            len -= CHUNK;
        }

        // Remainder tail / small-buffer fast path (< 768 bytes)
        while len >= 32 {
            let c0 = (ptr as *const u64).read_unaligned();
            let c1 = (ptr.add(8) as *const u64).read_unaligned();
            let c2 = (ptr.add(16) as *const u64).read_unaligned();
            let c3 = (ptr.add(24) as *const u64).read_unaligned();
            crc = core::arch::x86_64::_mm_crc32_u64(crc, c0);
            crc = core::arch::x86_64::_mm_crc32_u64(crc, c1);
            crc = core::arch::x86_64::_mm_crc32_u64(crc, c2);
            crc = core::arch::x86_64::_mm_crc32_u64(crc, c3);
            ptr = ptr.add(32);
            len -= 32;
        }

        while len >= 8 {
            let chunk = (ptr as *const u64).read_unaligned();
            crc = core::arch::x86_64::_mm_crc32_u64(crc, chunk);
            ptr = ptr.add(8);
            len -= 8;
        }

        if len >= 4 {
            let chunk = (ptr as *const u32).read_unaligned();
            crc = core::arch::x86_64::_mm_crc32_u32(crc as u32, chunk) as u64;
            ptr = ptr.add(4);
            len -= 4;
        }

        if len >= 2 {
            let chunk = (ptr as *const u16).read_unaligned();
            crc = core::arch::x86_64::_mm_crc32_u16(crc as u32, chunk) as u64;
            ptr = ptr.add(2);
            len -= 2;
        }

        if len == 1 {
            crc = core::arch::x86_64::_mm_crc32_u8(crc as u32, *ptr) as u64;
        }

        !(crc as u32)
    }
}

/// Computes Castagnoli CRC32-C checksum over the given slice using the fastest available hardware path.
#[inline]
pub fn crc32c(data: &[u8]) -> u32 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("sse4.2") {
            return unsafe { crc32c_sse42(data) };
        }
    }
    crc32c_table(data)
}

// ── Zigzag Varint Encoding ───────────────────────────────────────────────────

/// Helper returning the encoded byte length of a signed 32-bit integer when encoded as a Kafka Zigzag LEB128 varint.
#[inline(always)]
pub fn varint_len(val: i32) -> usize {
    let v = ((val << 1) ^ (val >> 31)) as u32;
    if v < 0x80 {
        1
    } else if v < 0x4000 {
        2
    } else if v < 0x20_0000 {
        3
    } else if v < 0x1000_0000 {
        4
    } else {
        5
    }
}

/// Helper returning the encoded byte length of a signed 64-bit integer when encoded as a Kafka Zigzag LEB128 varlong.
#[inline(always)]
pub fn varlong_len(val: i64) -> usize {
    let v = ((val << 1) ^ (val >> 63)) as u64;
    if v < 0x80 {
        1
    } else if v < 0x4000 {
        2
    } else if v < 0x20_0000 {
        3
    } else if v < 0x1000_0000 {
        4
    } else if v < 0x8_0000_0000 {
        5
    } else if v < 0x400_0000_0000 {
        6
    } else if v < 0x2_0000_0000_0000 {
        7
    } else if v < 0x100_0000_0000_0000 {
        8
    } else if v < 0x8000_0000_0000_0000 {
        9
    } else {
        10
    }
}

/// Writes a signed 32-bit integer as a Kafka Zigzag-encoded LEB128 varint.
#[inline(always)]
pub fn write_varint(buf: &mut Vec<u8>, val: i32) {
    let mut v = ((val << 1) ^ (val >> 31)) as u32;
    if v < 0x80 {
        buf.push(v as u8);
        return;
    }
    while v >= 0x80 {
        buf.push((v as u8 & 0x7f) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

/// Writes a signed 64-bit integer as a Kafka Zigzag-encoded LEB128 varlong.
#[inline(always)]
pub fn write_varlong(buf: &mut Vec<u8>, val: i64) {
    let mut v = ((val << 1) ^ (val >> 63)) as u64;
    if v < 0x80 {
        buf.push(v as u8);
        return;
    }
    while v >= 0x80 {
        buf.push((v as u8 & 0x7f) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

// ── RecordBatch v2 Construction ──────────────────────────────────────────────

/// Pre-baked 61-byte skeleton for Kafka RecordBatch v2 (Magic = 2).
///
/// Contains fixed invariant protocol markers:
/// - `baseOffset = 0` (offset 0..8)
/// - `partitionLeaderEpoch = -1` (offset 12..16)
/// - `magic = 2` (offset 16)
/// - `producerId = -1` (offset 43..51)
/// - `producerEpoch = -1` (offset 51..53)
/// - `baseSequence = -1` (offset 53..57)
pub const RECORDBATCH_HEADER_LEN: usize = 61;
pub const RECORDBATCH_HEADER_SKELETON: [u8; RECORDBATCH_HEADER_LEN] = [
    0, 0, 0, 0, 0, 0, 0, 0, // 0..8: baseOffset = 0
    0, 0, 0, 0, // 8..12: batchLength placeholder
    0xFF, 0xFF, 0xFF, 0xFF, // 12..16: partitionLeaderEpoch = -1
    2,    // 16: magic = 2
    0, 0, 0, 0, // 17..21: crc placeholder
    0, 0, // 21..23: attributes
    0, 0, 0, 0, // 23..27: lastOffsetDelta
    0, 0, 0, 0, 0, 0, 0, 0, // 27..35: baseTimestamp
    0, 0, 0, 0, 0, 0, 0, 0, // 35..43: maxTimestamp
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 43..51: producerId = -1
    0xFF, 0xFF, // 51..53: producerEpoch = -1
    0xFF, 0xFF, 0xFF, 0xFF, // 53..57: baseSequence = -1
    0, 0, 0, 0, // 57..61: recordsCount
];

/// Encodes an individual Kafka Record within a RecordBatch directly into `record_buf`.
///
/// Computes exact body length upfront and writes directly into the output buffer with
/// ZERO intermediate heap allocations.
#[inline]
pub fn encode_kafka_record(
    timestamp_delta: i64,
    offset_delta: i32,
    key: Option<&[u8]>,
    value: &[u8],
    record_buf: &mut Vec<u8>,
) {
    let key_part_len = match key {
        Some(k) => varint_len(k.len() as i32) + k.len(),
        None => 1, // -1 encoded as varint is 0x01
    };
    let value_part_len = varint_len(value.len() as i32) + value.len();

    // Body contains:
    // attributes (1B = 0) + timestampDelta + offsetDelta + key + value + headersCount (1B = 0)
    let body_len = 1
        + varlong_len(timestamp_delta)
        + varint_len(offset_delta)
        + key_part_len
        + value_part_len
        + 1;

    record_buf.reserve(varint_len(body_len as i32) + body_len);

    // Record framing: length varint + body directly
    write_varint(record_buf, body_len as i32);
    record_buf.push(0); // attributes
    write_varlong(record_buf, timestamp_delta);
    write_varint(record_buf, offset_delta);
    if let Some(k) = key {
        write_varint(record_buf, k.len() as i32);
        record_buf.extend_from_slice(k);
    } else {
        write_varint(record_buf, -1);
    }
    write_varint(record_buf, value.len() as i32);
    record_buf.extend_from_slice(value);
    write_varint(record_buf, 0); // headersCount
}

/// Encodes a complete Kafka RecordBatch v2 from a slice of [`TelemetryRecord`].
pub fn encode_record_batch(
    records: &[&TelemetryRecord],
    codec: CompressionCodec,
    format: KafkaLogFormat,
    record_key: Option<&[u8]>,
    out: &mut Vec<u8>,
) -> usize {
    if records.is_empty() {
        return 0;
    }

    let first_rec = records[0];
    let base_ts_ms = (first_rec.ingress_time_nanos / 1_000_000) as i64;
    let mut max_ts_ms = base_ts_ms;

    let mut raw_records = Vec::with_capacity(records.len() * 256);
    let mut val_buf = Vec::with_capacity(512);

    for (idx, rec) in records.iter().enumerate() {
        let rec_ts_ms = (rec.ingress_time_nanos / 1_000_000) as i64;
        if rec_ts_ms > max_ts_ms {
            max_ts_ms = rec_ts_ms;
        }

        val_buf.clear();
        match format {
            KafkaLogFormat::Json => json::format_single_record_json(rec, &mut val_buf),
            KafkaLogFormat::Text => text::format_single_record_text(rec, &mut val_buf),
            KafkaLogFormat::Binary => binary::format_single_record_binary(rec, &mut val_buf),
        }

        let ts_delta = rec_ts_ms - base_ts_ms;
        let offset_delta = idx as i32;

        encode_kafka_record(
            ts_delta,
            offset_delta,
            record_key,
            &val_buf,
            &mut raw_records,
        );
    }

    // Compress records array orthogonally via separate compression engine
    let compressed_records = compress_records(&raw_records, codec);

    // Pre-stamp dynamic fields directly into stack skeleton (fixed [u8; 61] - zero bounds checks)
    let mut header = RECORDBATCH_HEADER_SKELETON;
    let attrs = codec.attribute_bits();
    header[21..23].copy_from_slice(&attrs.to_be_bytes());
    let last_offset_delta = (records.len() - 1) as i32;
    header[23..27].copy_from_slice(&last_offset_delta.to_be_bytes());
    header[27..35].copy_from_slice(&base_ts_ms.to_be_bytes());
    header[35..43].copy_from_slice(&max_ts_ms.to_be_bytes());
    let count = records.len() as i32;
    header[57..61].copy_from_slice(&count.to_be_bytes());

    let start_pos = out.len();
    out.reserve(RECORDBATCH_HEADER_LEN + compressed_records.len());
    out.extend_from_slice(&header);
    out.extend_from_slice(&compressed_records);

    // Compute batchLength (from offset 12 partitionLeaderEpoch to end of batch)
    let batch_length = (out.len() - (start_pos + 12)) as i32;

    // Safety: out was extended by at least RECORDBATCH_HEADER_LEN (61 bytes), so start_pos + 21 <= out.len().
    let crc_val = unsafe {
        let crc_ptr = out.as_ptr().add(start_pos + 21);
        let crc_len = out.len() - (start_pos + 21);
        crc32c(core::slice::from_raw_parts(crc_ptr, crc_len))
    };

    // Safety: offsets 8..12 and 17..21 are strictly within the newly appended 61-byte region.
    unsafe {
        let base = out.as_mut_ptr().add(start_pos);
        (base.add(8) as *mut i32).write_unaligned(batch_length.to_be());
        (base.add(17) as *mut u32).write_unaligned(crc_val.to_be());
    }

    out.len() - start_pos
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc32c_standard_vectors() {
        assert_eq!(crc32c(b""), 0);
        // Standard Castagnoli CRC32-C test vector for "123456789" is 0xe3069283
        assert_eq!(crc32c(b"123456789"), 0xe3069283);
        assert_eq!(crc32c_table(b"123456789"), 0xe3069283);

        // Verify hardware vs table equality across diverse buffer boundaries (0..4096 bytes)
        let sample: Vec<u8> = (0..4096).map(|i| (i * 31 + 7) as u8).collect();
        for len in 0..=4096 {
            let slice = &sample[..len];
            assert_eq!(crc32c(slice), crc32c_table(slice), "Mismatch at len {len}");
        }
    }

    #[test]
    fn test_zigzag_varint() {
        let mut buf = Vec::new();

        write_varint(&mut buf, 0);
        assert_eq!(buf, vec![0x00]);
        assert_eq!(varint_len(0), 1);

        buf.clear();
        write_varint(&mut buf, -1);
        assert_eq!(buf, vec![0x01]);
        assert_eq!(varint_len(-1), 1);

        buf.clear();
        write_varint(&mut buf, 1);
        assert_eq!(buf, vec![0x02]);
        assert_eq!(varint_len(1), 1);

        buf.clear();
        write_varint(&mut buf, -2);
        assert_eq!(buf, vec![0x03]);
        assert_eq!(varint_len(-2), 1);

        // Multi-byte varint checks
        for val in [128, 1000, 65535, -32768, 1_000_000, -1_000_000] {
            buf.clear();
            write_varint(&mut buf, val);
            assert_eq!(varint_len(val), buf.len(), "Varint len mismatch for {val}");
        }

        let mut lbuf = Vec::new();
        for val in [0i64, -1, 1, 100, 1000, 1_000_000, 1_000_000_000_000] {
            lbuf.clear();
            write_varlong(&mut lbuf, val);
            assert_eq!(
                varlong_len(val),
                lbuf.len(),
                "Varlong len mismatch for {val}"
            );
        }
    }
}
