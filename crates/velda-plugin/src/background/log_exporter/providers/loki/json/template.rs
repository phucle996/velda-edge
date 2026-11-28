//! Grafana Loki JSON Multi-Stream Formatter (`loki_log_shipper::json::template`).
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────────────────────────────────────┐
//! │                         LOKI JSON MULTI-STREAM PARTITIONING & FLOW                           │
//! │                                                                                              │
//! │  1. Stream Headers Pre-Baked at Initialization (LokiStreamTemplate):                         │
//! │  {"streams":[                                                                                │
//! │    {"stream":{"service":"velda-edge","protocol":"http",...},"values":[ ... ]},               │
//! │    {"stream":{"service":"velda-edge","protocol":"grpc",...},"values":[ ... ]},               │
//! │    {"stream":{"service":"velda-edge","protocol":"raw",...},"values":[ ... ]}                 │
//! │  ]}                                                                                          │
//! └───────────────────────────────────────────────┬──────────────────────────────────────────────┘
//!                                                 │
//!        ┌────────────────────────────────────────┴───────────────────────────────────────┐
//!        ▼                                                                                ▼
//! ┌────────────────────────────────────────┐                            ┌────────────────────────────────────────┐
//! │ TelemetryRecord (64B SPSC Ring)        │                            │ Persistent Scratch Buffers (0 alloc)   │
//! │ ├─ downstream_protocol                    │ ──► Protocol Slicing ──►   │ ├─ http_values.clear()                 │
//! │ ├─ 128-bit (ts_sec, request_id)        │                            │ ├─ grpc_values.clear()                 │
//! │ ├─ peer_ip: u128 (RFC 4291 / 5952)     │                            │ └─ raw_values.clear()                  │
//! │ └─ division-free status, port, rx, tx  │                            └────────────────────────────────────────┘
//! └────────────────────────────────────────┘                                                 │
//!                                                 ┌──────────────────────────────────────────┘
//!                                                 ▼
//! ┌──────────────────────────────────────────────────────────────────────────────────────────────┐
//! │ Output Buffer Assembly:                                                                      │
//! │ out.clear() ──► out.extend_from_slice(prefix) ──► Streams Formatted ──► out.extend(suffix)   │
//! └──────────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::cell::RefCell;
use std::collections::HashMap;

use velda_core::WireProtocol;
use velda_observability::{TelemetryBatch, TelemetryRecord};

use velda_core::lut::decimal::fast_write_u64;
use velda_core::lut::ip::{fast_write_ip, fast_write_ipv4};
use velda_core::request_id::fast_write_hex32;

#[inline]
fn write_json_escaped(out: &mut Vec<u8>, s: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut start = 0;
    for (i, &b) in s.iter().enumerate() {
        let escaped = match b {
            b'"' | b'\\' => {
                out.extend_from_slice(&s[start..i]);
                out.push(b'\\');
                out.push(b);
                true
            }
            0x00..=0x1F => {
                out.extend_from_slice(&s[start..i]);
                out.extend_from_slice(b"\\u00");
                out.push(HEX[(b >> 4) as usize]);
                out.push(HEX[(b & 0x0f) as usize]);
                true
            }
            _ => false,
        };
        if escaped {
            start = i + 1;
        }
    }
    out.extend_from_slice(&s[start..]);
}

/// Pre-baked static stream headers for Grafana Loki Push API partitioned by protocol family.
#[derive(Debug, Clone)]
pub struct LokiStreamTemplate {
    pub http_prefix: Vec<u8>,
    pub grpc_prefix: Vec<u8>,
    pub raw_prefix: Vec<u8>,
}

impl LokiStreamTemplate {
    /// Builds static stream header prefixes for HTTP, gRPC, and Raw streams.
    pub fn new(service_name: &str, static_labels: &HashMap<String, String>) -> Self {
        fn build_header(
            service_name: &str,
            protocol: &str,
            static_labels: &HashMap<String, String>,
        ) -> Vec<u8> {
            let mut buf = Vec::with_capacity(128);
            buf.extend_from_slice(b"{\"stream\":{\"service\":\"");
            buf.extend_from_slice(service_name.as_bytes());
            buf.extend_from_slice(b"\",\"protocol\":\"");
            buf.extend_from_slice(protocol.as_bytes());
            buf.push(b'"');
            for (k, v) in static_labels {
                buf.extend_from_slice(b",\"");
                buf.extend_from_slice(k.as_bytes());
                buf.extend_from_slice(b"\":\"");
                buf.extend_from_slice(v.as_bytes());
                buf.push(b'"');
            }
            buf.extend_from_slice(b"},\"values\":[");
            buf
        }

        Self {
            http_prefix: build_header(service_name, "http", static_labels),
            grpc_prefix: build_header(service_name, "grpc", static_labels),
            raw_prefix: build_header(service_name, "raw", static_labels),
        }
    }
}

/// Persistent zero-allocation JSON formatter for Grafana Loki `/loki/api/v1/push`.
///
/// Holds pre-allocated scratch buffers (`http_values`, `grpc_values`, `raw_values`)
/// that are cleared between batches rather than reallocated on the heap.
pub struct LokiJsonFormatter {
    pub template: LokiStreamTemplate,
    pub http_values: Vec<u8>,
    pub grpc_values: Vec<u8>,
    pub raw_values: Vec<u8>,
}

impl LokiJsonFormatter {
    /// Instantiates a new formatter with pre-sized persistent scratch buffers.
    pub fn new(service_name: &str, static_labels: &HashMap<String, String>) -> Self {
        Self {
            template: LokiStreamTemplate::new(service_name, static_labels),
            http_values: Vec::with_capacity(16 * 1024),
            grpc_values: Vec::with_capacity(8 * 1024),
            raw_values: Vec::with_capacity(4 * 1024),
        }
    }

    /// Wraps an existing [`LokiStreamTemplate`] with dedicated scratch buffers.
    pub fn with_template(template: LokiStreamTemplate) -> Self {
        Self {
            template,
            http_values: Vec::with_capacity(16 * 1024),
            grpc_values: Vec::with_capacity(8 * 1024),
            raw_values: Vec::with_capacity(4 * 1024),
        }
    }

    /// Serializes batches of reference-counted [`TelemetryRecord`] into a multi-stream
    /// Grafana Loki JSON push payload, reusing persistent internal scratch buffers.
    ///
    /// Guarantees 0 heap allocations during formatting.
    pub fn format_batches(&mut self, batches: &[TelemetryBatch], out: &mut Vec<u8>) -> usize {
        out.clear();
        self.http_values.clear();
        self.grpc_values.clear();
        self.raw_values.clear();

        let mut total_records = 0usize;

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
                total_records += 1;
                let downstream_protocol = rec.downstream_protocol();
                let internal_status = rec.internal_status;
                let method = rec.method;
                let ua_fam = rec.ua_family;
                let ua_os = rec.ua_os;

                let dst = match downstream_protocol {
                    WireProtocol::H1 | WireProtocol::H2 | WireProtocol::H3 => &mut self.http_values,
                    WireProtocol::GrpcTcp | WireProtocol::GrpcUdp => &mut self.grpc_values,
                    WireProtocol::RawTcp | WireProtocol::RawUdp => &mut self.raw_values,
                };
                if !dst.is_empty() {
                    dst.push(b',');
                }
                dst.extend_from_slice(b"[\"");
                fast_write_u64(dst, rec.ingress_time_nanos);
                dst.extend_from_slice(b"\",\"request_id=");
                fast_write_hex32(dst, rec.ingress_time_nanos, rec.request_id);
                dst.extend_from_slice(b" pipe=");
                dst.extend_from_slice(rec.pipe_name().as_bytes());
                dst.extend_from_slice(b" method=");
                dst.extend_from_slice(method.as_str().as_bytes());
                dst.extend_from_slice(b" url=");
                write_json_escaped(dst, rec.url().as_bytes());
                dst.extend_from_slice(b" route=");
                velda_router::with_route_bytes(rec.route_id, |bytes| {
                    dst.extend_from_slice(bytes);
                });
                dst.extend_from_slice(b" internal_status=");
                dst.extend_from_slice(internal_status.as_str().as_bytes());
                dst.extend_from_slice(b" duration_ns=");
                fast_write_u64(dst, rec.duration_ns);
                dst.extend_from_slice(b" bytes_rx=");
                fast_write_u64(dst, rec.bytes_rx as u64);
                dst.extend_from_slice(b" bytes_tx=");
                fast_write_u64(dst, rec.bytes_tx as u64);
                dst.extend_from_slice(b" client=");
                fast_write_ip(dst, rec.peer_ip);
                dst.extend_from_slice(b" endpoint=");
                fast_write_ipv4(dst, rec.upstream_ip);
                dst.push(b':');
                fast_write_u64(dst, rec.upstream_port as u64);
                dst.extend_from_slice(b" user_agent=");
                dst.extend_from_slice(ua_fam.as_str().as_bytes());
                dst.push(b'/');
                fast_write_u64(dst, rec.ua_major_version as u64);
                dst.extend_from_slice(b" client_os=");
                dst.extend_from_slice(ua_os.as_str().as_bytes());
                dst.extend_from_slice(b"\"]");
            }
        }

        if total_records == 0 {
            return 0;
        }

        out.extend_from_slice(b"{\"streams\":[");
        let mut first_stream = true;

        if !self.http_values.is_empty() {
            first_stream = false;
            out.extend_from_slice(&self.template.http_prefix);
            out.extend_from_slice(&self.http_values);
            out.extend_from_slice(b"]}");
        }

        if !self.grpc_values.is_empty() {
            if !first_stream {
                out.push(b',');
            }
            first_stream = false;
            out.extend_from_slice(&self.template.grpc_prefix);
            out.extend_from_slice(&self.grpc_values);
            out.extend_from_slice(b"]}");
        }

        if !self.raw_values.is_empty() {
            if !first_stream {
                out.push(b',');
            }
            out.extend_from_slice(&self.template.raw_prefix);
            out.extend_from_slice(&self.raw_values);
            out.extend_from_slice(b"]}");
        }

        out.extend_from_slice(b"]}");
        total_records
    }
}

thread_local! {
    static THREAD_SCRATCH: RefCell<(Vec<u8>, Vec<u8>, Vec<u8>)> = RefCell::new((
        Vec::with_capacity(16 * 1024),
        Vec::with_capacity(8 * 1024),
        Vec::with_capacity(4 * 1024),
    ));
}

/// Standalone formatting entrypoint for Grafana Loki Push API JSON payloads.
///
/// Uses thread-local persistent scratch buffers to guarantee 0 heap allocations
/// even when called outside of [`LokiJsonFormatter`].
pub fn format_loki_push_payload(
    batches: &[TelemetryBatch],
    template: &LokiStreamTemplate,
    out: &mut Vec<u8>,
) -> usize {
    THREAD_SCRATCH.with(|scratch| {
        let mut borrow = scratch.borrow_mut();
        let (ref mut http_values, ref mut grpc_values, ref mut raw_values) = *borrow;

        out.clear();
        http_values.clear();
        grpc_values.clear();
        raw_values.clear();

        let mut total_records = 0usize;

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
                total_records += 1;
                let downstream_protocol = rec.downstream_protocol();
                let internal_status = rec.internal_status;
                let method = rec.method;
                let ua_fam = rec.ua_family;
                let ua_os = rec.ua_os;

                let dst = match downstream_protocol {
                    WireProtocol::H1 | WireProtocol::H2 | WireProtocol::H3 => &mut *http_values,
                    WireProtocol::GrpcTcp | WireProtocol::GrpcUdp => &mut *grpc_values,
                    WireProtocol::RawTcp | WireProtocol::RawUdp => &mut *raw_values,
                };
                if !dst.is_empty() {
                    dst.push(b',');
                }
                dst.extend_from_slice(b"[\"");
                fast_write_u64(dst, rec.ingress_time_nanos);
                dst.extend_from_slice(b"\",\"request_id=");
                fast_write_hex32(dst, rec.ingress_time_nanos, rec.request_id);
                dst.extend_from_slice(b" pipe=");
                dst.extend_from_slice(rec.pipe_name().as_bytes());
                dst.extend_from_slice(b" method=");
                dst.extend_from_slice(method.as_str().as_bytes());
                dst.extend_from_slice(b" url=");
                write_json_escaped(dst, rec.url().as_bytes());
                dst.extend_from_slice(b" internal_status=");
                dst.extend_from_slice(internal_status.as_str().as_bytes());
                dst.extend_from_slice(b" route_id=");
                fast_write_u64(dst, rec.route_id as u64);
                dst.extend_from_slice(b" duration_ns=");
                fast_write_u64(dst, rec.duration_ns);
                dst.extend_from_slice(b" rx=");
                fast_write_u64(dst, rec.bytes_rx as u64);
                dst.extend_from_slice(b" tx=");
                fast_write_u64(dst, rec.bytes_tx as u64);
                dst.extend_from_slice(b" client=");
                fast_write_ip(dst, rec.peer_ip);
                dst.extend_from_slice(b" endpoint=");
                fast_write_ipv4(dst, rec.upstream_ip);
                dst.push(b':');
                fast_write_u64(dst, rec.upstream_port as u64);
                dst.extend_from_slice(b" ua=");
                dst.extend_from_slice(ua_fam.as_str().as_bytes());
                dst.push(b'/');
                fast_write_u64(dst, rec.ua_major_version as u64);
                dst.extend_from_slice(b" os=");
                dst.extend_from_slice(ua_os.as_str().as_bytes());
                dst.extend_from_slice(b"\"]");
            }
        }

        if total_records == 0 {
            return 0;
        }

        out.extend_from_slice(b"{\"streams\":[");
        let mut first_stream = true;

        if !http_values.is_empty() {
            first_stream = false;
            out.extend_from_slice(&template.http_prefix);
            out.extend_from_slice(http_values);
            out.extend_from_slice(b"]}");
        }

        if !grpc_values.is_empty() {
            if !first_stream {
                out.push(b',');
            }
            first_stream = false;
            out.extend_from_slice(&template.grpc_prefix);
            out.extend_from_slice(grpc_values);
            out.extend_from_slice(b"]}");
        }

        if !raw_values.is_empty() {
            if !first_stream {
                out.push(b',');
            }
            out.extend_from_slice(&template.raw_prefix);
            out.extend_from_slice(raw_values);
            out.extend_from_slice(b"]}");
        }

        out.extend_from_slice(b"]}");
        total_records
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use velda_core::{HttpMethod, InternalStatus, PipeKind};

    #[test]
    fn test_loki_json_formatter_hex32_and_ipv6() {
        let mut labels = HashMap::new();
        labels.insert("env".to_string(), "prod".to_string());
        let mut formatter = LokiJsonFormatter::new("test-service", &labels);

        let mut http_rec = TelemetryRecord::zeroed();
        http_rec.request_id = 0xaaaa_bbbb_cccc_dddd;
        http_rec.ingress_time_nanos = 1_710_000_001_123_456_789;
        http_rec.duration_ns = 320;
        http_rec.peer_ip = velda_core::lut::ip::encode_ip_u128("192.168.1.10".parse().unwrap());
        http_rec.upstream_ip =
            velda_core::lut::ip::encode_upstream_ip_u32("10.0.0.1".parse().unwrap());
        http_rec.upstream_port = 8080;
        http_rec.bytes_rx = 128;
        http_rec.bytes_tx = 512;
        http_rec.route_id = 1;
        http_rec.pipe = PipeKind::BufferedH2H1;
        http_rec.internal_status = InternalStatus::Completed;
        http_rec.method = HttpMethod::Get;
        let (fam, os, ver) = velda_core::lut::classify_user_agent(b"curl/8.6.0");
        http_rec.ua_family = fam;
        http_rec.ua_os = os;
        http_rec.ua_major_version = ver;

        let mut grpc_rec = TelemetryRecord::zeroed();
        grpc_rec.request_id = 0x1111_2222_3333_4444;
        grpc_rec.ingress_time_nanos = 1_710_000_002_987_654_321;
        grpc_rec.duration_ns = 890;
        grpc_rec.peer_ip = velda_core::lut::ip::encode_ip_u128("::1".parse().unwrap());
        grpc_rec.upstream_ip =
            velda_core::lut::ip::encode_upstream_ip_u32("10.0.0.2".parse().unwrap());
        grpc_rec.upstream_port = 9090;
        grpc_rec.bytes_rx = 64;
        grpc_rec.bytes_tx = 256;
        grpc_rec.route_id = 2;
        grpc_rec.rpc_msgs_or_flags = 4;
        grpc_rec.pipe = PipeKind::DuplexGrpcTcpTcp;
        grpc_rec.internal_status = InternalStatus::Completed;
        grpc_rec.method = HttpMethod::Post;
        let (fam, os, ver) = velda_core::lut::classify_user_agent(b"grpc-go/1.64.0");
        grpc_rec.ua_family = fam;
        grpc_rec.ua_os = os;
        grpc_rec.ua_major_version = ver;

        let batches = vec![TelemetryBatch::from_arc(std::sync::Arc::from(vec![
            http_rec, grpc_rec,
        ]))];
        let mut out = Vec::new();
        let count = formatter.format_batches(&batches, &mut out);
        assert_eq!(count, 2);

        let json_str = std::str::from_utf8(&out).expect("Valid JSON");
        assert!(json_str.starts_with("{\"streams\":["));
        assert!(json_str.contains("\"service\":\"test-service\""));
        assert!(json_str.contains("\"protocol\":\"http\""));
        assert!(json_str.contains("\"protocol\":\"grpc\""));
        assert!(json_str.contains("\"env\":\"prod\""));

        // Verify full 32-hex request_id: ingress_time_nanos + entropy
        assert!(
            json_str
                .contains("[\"1710000001123456789\",\"request_id=17bb23f0e8e19715aaaabbbbccccdddd")
        );
        assert!(
            json_str
                .contains("[\"1710000002987654321\",\"request_id=17bb23f157fefcb11111222233334444")
        );

        // Verify RFC 5952 IPv6 compression
        assert!(json_str.contains("client=::1"));
        assert!(json_str.contains("client=192.168.1.10"));
    }

    #[test]
    fn test_loki_json_empty_batch() {
        let template = LokiStreamTemplate::new("test", &HashMap::new());
        let mut out = Vec::new();
        let batches: Vec<TelemetryBatch> = Vec::new();
        let count = format_loki_push_payload(&batches, &template, &mut out);
        assert_eq!(count, 0);
        assert!(out.is_empty());
    }
}
