//! Direct Pointer-Stamping NDJSON Formatter & Compressor for S3 Objects.

use std::io::Write;

use flate2::Compression;
use flate2::write::GzEncoder;
use velda_core::lut::decimal::{fast_write_u16_ptr, fast_write_u64_ptr};
use velda_core::lut::ip::{fast_write_ip_ptr, fast_write_ipv4_ptr};
use velda_observability::{TelemetryBatch, TelemetryRecord};

use crate::background::log_exporter::config::S3Compression;

const MAX_ROW_BYTES: usize = 512 + 184 * 6;
const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

const JSON_PREFIX: &[u8] = b"{\"timestamp\":";
const JSON_REQ_ID: &[u8] = b",\"request_id\":";
const JSON_DURATION: &[u8] = b",\"duration_ns\":";
const JSON_PEER_IP: &[u8] = b",\"peer_ip\":\"";
const JSON_UPSTREAM_IP: &[u8] = b"\",\"upstream_ip\":\"";
const JSON_NONE: &[u8] = b"none";
const JSON_UPSTREAM_PORT: &[u8] = b"\",\"upstream_port\":";
const JSON_ROUTE_ID: &[u8] = b",\"route_id\":";
const JSON_PIPE: &[u8] = b",\"pipe\":\"";
const JSON_METHOD: &[u8] = b"\",\"method\":\"";
const JSON_URL: &[u8] = b"\",\"url\":\"";
const JSON_BYTES_RX: &[u8] = b"\",\"bytes_rx\":";
const JSON_BYTES_TX: &[u8] = b",\"bytes_tx\":";
const JSON_INTERNAL_STATUS: &[u8] = b",\"internal_status\":\"";
const JSON_UA_FAMILY: &[u8] = b"\",\"user_agent_family\":\"";
const JSON_UA_OS: &[u8] = b"\",\"user_agent_os\":\"";
const JSON_SUFFIX: &[u8] = b"\"}\n";

#[inline]
unsafe fn write_json_escaped_ptr(ptr: *mut u8, s: &[u8]) -> usize {
    let mut p = ptr;
    for &b in s {
        unsafe {
            match b {
                b'"' | b'\\' => {
                    *p = b'\\';
                    *p.add(1) = b;
                    p = p.add(2);
                }
                0x00..=0x1F => {
                    core::ptr::copy_nonoverlapping(b"\\u00".as_ptr(), p, 4);
                    *p.add(4) = HEX_DIGITS[(b >> 4) as usize];
                    *p.add(5) = HEX_DIGITS[(b & 0x0f) as usize];
                    p = p.add(6);
                }
                _ => {
                    *p = b;
                    p = p.add(1);
                }
            }
        }
    }
    p as usize - ptr as usize
}

/// Encodes telemetry batches as NDJSON directly into `out` via pointer stamping.
#[inline(never)]
pub fn format_ndjson(batches: &[TelemetryBatch], out: &mut Vec<u8>) -> usize {
    let total_recs: usize = batches.iter().map(|b| b.len()).sum();
    out.reserve(total_recs * 384);
    let mut count = 0;

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
                }
            }

            count += 1;
            out.reserve(MAX_ROW_BYTES);

            unsafe {
                let base_ptr = out.as_mut_ptr();
                let mut ptr = base_ptr.add(out.len());

                core::ptr::copy_nonoverlapping(JSON_PREFIX.as_ptr(), ptr, JSON_PREFIX.len());
                ptr = ptr.add(JSON_PREFIX.len());
                ptr = ptr.add(fast_write_u64_ptr(ptr, rec.ingress_time_nanos));

                core::ptr::copy_nonoverlapping(JSON_REQ_ID.as_ptr(), ptr, JSON_REQ_ID.len());
                ptr = ptr.add(JSON_REQ_ID.len());
                ptr = ptr.add(fast_write_u64_ptr(ptr, rec.request_id));

                core::ptr::copy_nonoverlapping(JSON_DURATION.as_ptr(), ptr, JSON_DURATION.len());
                ptr = ptr.add(JSON_DURATION.len());
                ptr = ptr.add(fast_write_u64_ptr(ptr, rec.duration_ns));

                core::ptr::copy_nonoverlapping(JSON_PEER_IP.as_ptr(), ptr, JSON_PEER_IP.len());
                ptr = ptr.add(JSON_PEER_IP.len());
                ptr = ptr.add(fast_write_ip_ptr(ptr, rec.peer_ip));

                core::ptr::copy_nonoverlapping(
                    JSON_UPSTREAM_IP.as_ptr(),
                    ptr,
                    JSON_UPSTREAM_IP.len(),
                );
                ptr = ptr.add(JSON_UPSTREAM_IP.len());
                if rec.upstream_ip != 0 {
                    ptr = ptr.add(fast_write_ipv4_ptr(ptr, rec.upstream_ip));
                } else {
                    core::ptr::copy_nonoverlapping(JSON_NONE.as_ptr(), ptr, JSON_NONE.len());
                    ptr = ptr.add(JSON_NONE.len());
                }

                core::ptr::copy_nonoverlapping(
                    JSON_UPSTREAM_PORT.as_ptr(),
                    ptr,
                    JSON_UPSTREAM_PORT.len(),
                );
                ptr = ptr.add(JSON_UPSTREAM_PORT.len());
                ptr = ptr.add(fast_write_u16_ptr(ptr, rec.upstream_port));

                core::ptr::copy_nonoverlapping(JSON_ROUTE_ID.as_ptr(), ptr, JSON_ROUTE_ID.len());
                ptr = ptr.add(JSON_ROUTE_ID.len());
                ptr = ptr.add(fast_write_u16_ptr(ptr, rec.route_id));

                core::ptr::copy_nonoverlapping(JSON_PIPE.as_ptr(), ptr, JSON_PIPE.len());
                ptr = ptr.add(JSON_PIPE.len());
                let pipe = rec.pipe.as_bytes();
                core::ptr::copy_nonoverlapping(pipe.as_ptr(), ptr, pipe.len());
                ptr = ptr.add(pipe.len());

                core::ptr::copy_nonoverlapping(JSON_METHOD.as_ptr(), ptr, JSON_METHOD.len());
                ptr = ptr.add(JSON_METHOD.len());
                let meth = rec.method.as_bytes();
                core::ptr::copy_nonoverlapping(meth.as_ptr(), ptr, meth.len());
                ptr = ptr.add(meth.len());

                core::ptr::copy_nonoverlapping(JSON_URL.as_ptr(), ptr, JSON_URL.len());
                ptr = ptr.add(JSON_URL.len());
                ptr = ptr.add(write_json_escaped_ptr(ptr, rec.url().as_bytes()));

                core::ptr::copy_nonoverlapping(JSON_BYTES_RX.as_ptr(), ptr, JSON_BYTES_RX.len());
                ptr = ptr.add(JSON_BYTES_RX.len());
                ptr = ptr.add(fast_write_u64_ptr(ptr, rec.bytes_rx as u64));

                core::ptr::copy_nonoverlapping(JSON_BYTES_TX.as_ptr(), ptr, JSON_BYTES_TX.len());
                ptr = ptr.add(JSON_BYTES_TX.len());
                ptr = ptr.add(fast_write_u64_ptr(ptr, rec.bytes_tx as u64));

                core::ptr::copy_nonoverlapping(
                    JSON_INTERNAL_STATUS.as_ptr(),
                    ptr,
                    JSON_INTERNAL_STATUS.len(),
                );
                ptr = ptr.add(JSON_INTERNAL_STATUS.len());
                let comp = rec.internal_status.as_bytes();
                core::ptr::copy_nonoverlapping(comp.as_ptr(), ptr, comp.len());
                ptr = ptr.add(comp.len());

                core::ptr::copy_nonoverlapping(JSON_UA_FAMILY.as_ptr(), ptr, JSON_UA_FAMILY.len());
                ptr = ptr.add(JSON_UA_FAMILY.len());
                let fam = rec.ua_family.as_bytes();
                core::ptr::copy_nonoverlapping(fam.as_ptr(), ptr, fam.len());
                ptr = ptr.add(fam.len());

                core::ptr::copy_nonoverlapping(JSON_UA_OS.as_ptr(), ptr, JSON_UA_OS.len());
                ptr = ptr.add(JSON_UA_OS.len());
                let os = rec.ua_os.as_bytes();
                core::ptr::copy_nonoverlapping(os.as_ptr(), ptr, os.len());
                ptr = ptr.add(os.len());

                core::ptr::copy_nonoverlapping(JSON_SUFFIX.as_ptr(), ptr, JSON_SUFFIX.len());
                ptr = ptr.add(JSON_SUFFIX.len());

                out.set_len(ptr as usize - base_ptr as usize);
            }
        }
    }

    count
}

/// Compresses `raw` according to `compression` into `out` (returns object file extension).
pub fn compress_object<'a>(
    raw: &'a [u8],
    compression: S3Compression,
    out: &'a mut Vec<u8>,
) -> std::io::Result<(&'a [u8], &'static str)> {
    match compression {
        S3Compression::Zstd => {
            out.clear();
            let compressed = zstd::bulk::compress(raw, 3)?;
            *out = compressed;
            Ok((out.as_slice(), "jsonl.zst"))
        }
        S3Compression::Gzip => {
            out.clear();
            let mut enc = GzEncoder::new(out, Compression::fast());
            enc.write_all(raw)?;
            let buf = enc.finish()?;
            Ok((buf.as_slice(), "jsonl.gz"))
        }
        S3Compression::None => Ok((raw, "jsonl")),
    }
}
