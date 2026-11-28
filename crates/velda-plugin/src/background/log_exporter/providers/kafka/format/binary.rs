//! Raw Binary Access Log Formatting (`kafka_log_shipper::format::binary`).
//!
//! Emits raw binary byte layout of [`TelemetryRecord`] with zero stringification overhead.
//!
//! # Binary Layout
//! - `0..70`: Fixed primitive header fields in little-endian binary format
//! - `70..70+url_len`: Raw UTF-8 / ASCII URL bytes (zero delimiter, length-specified)

use velda_observability::TelemetryRecord;

/// Fixed-size binary header length in bytes (excluding variable-length URL).
pub const BINARY_HEADER_LEN: usize = 70;

/// Serializes an [`TelemetryRecord`] into raw binary representation without stringification.
#[inline]
pub fn format_single_record_binary(rec: &TelemetryRecord, out: &mut Vec<u8>) {
    let url_len = rec.url_len as usize;
    let total_len = BINARY_HEADER_LEN + url_len;
    out.reserve(total_len);

    unsafe {
        let base_ptr = rec as *const TelemetryRecord as *const u8;
        let out_ptr = out.as_mut_ptr().add(out.len());
        // Copy 70 bytes of fixed fields directly
        core::ptr::copy_nonoverlapping(base_ptr, out_ptr, BINARY_HEADER_LEN);
        // Copy url_bytes[..url_len] directly
        if url_len > 0 {
            let url_src = rec.url_bytes.as_ptr();
            core::ptr::copy_nonoverlapping(url_src, out_ptr.add(BINARY_HEADER_LEN), url_len);
        }
        out.set_len(out.len() + total_len);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use velda_core::{HttpMethod, InternalStatus, PipeKind, UaFamily, UaOs};

    #[test]
    fn test_format_single_record_binary() {
        let mut rec = TelemetryRecord::zeroed();
        rec.request_id = 0x0102_0304_0506_0708;
        rec.duration_ns = 50_000;
        rec.peer_ip = 0xC0A8_0101;
        rec.ingress_time_nanos = 1_732_000_000_000_000_000;
        rec.pipe = PipeKind::BufferedH1H1;
        rec.method = HttpMethod::Get;
        rec.internal_status = InternalStatus::Completed;
        rec.ua_family = UaFamily::Curl;
        rec.ua_os = UaOs::Linux;
        rec.set_url("/healthz");

        let mut out = Vec::new();
        format_single_record_binary(&rec, &mut out);

        assert_eq!(out.len(), BINARY_HEADER_LEN + 8);
        // Verify request_id LE bytes at 0..8
        assert_eq!(&out[0..8], &rec.request_id.to_ne_bytes());
        // Verify URL string bytes at 70..78
        assert_eq!(&out[70..78], b"/healthz");
    }
}
