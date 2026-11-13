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
//! │ AccessLogRecord (64B SPSC Ring)        │                            │ Persistent Scratch Buffers (0 alloc)   │
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
use std::sync::Arc;

use velda_observability::{
    AccessLogRecord, LogCompletionReason, LogGrpcStatus, LogHttpMethod, LogProtocol, UaFamily, UaOs,
};

use super::super::lut::{
    fast_write_hex32, fast_write_ip, fast_write_ipv4, fast_write_ts_nano, fast_write_u64,
};

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

    /// Serializes batches of reference-counted [`AccessLogRecord`] into a multi-stream
    /// Grafana Loki JSON push payload, reusing persistent internal scratch buffers.
    ///
    /// Guarantees 0 heap allocations during formatting.
    pub fn format_batches(
        &mut self,
        batches: &[Arc<[AccessLogRecord]>],
        out: &mut Vec<u8>,
    ) -> usize {
        out.clear();
        self.http_values.clear();
        self.grpc_values.clear();
        self.raw_values.clear();

        let mut total_records = 0usize;

        for batch in batches {
            for rec in batch.iter() {
                total_records += 1;
                let downstream_protocol = rec.downstream_protocol();
                let completion = LogCompletionReason::from_u8(rec.completion);

                match downstream_protocol {
                    LogProtocol::H1 | LogProtocol::H2 | LogProtocol::H3 => {
                        if !self.http_values.is_empty() {
                            self.http_values.push(b',');
                        }
                        let method = LogHttpMethod::from_u8(rec.method);
                        let ua_fam = UaFamily::from_u8(rec.ua_family);
                        let ua_os = UaOs::from_u8(rec.ua_os);

                        self.http_values.extend_from_slice(b"[\"");
                        fast_write_ts_nano(&mut self.http_values, rec.timestamp_sec);
                        self.http_values.extend_from_slice(b"\",\"request_id=");
                        fast_write_hex32(
                            &mut self.http_values,
                            rec.timestamp_sec as u64,
                            rec.request_id,
                        );
                        self.http_values.extend_from_slice(b" pipe=");
                        self.http_values
                            .extend_from_slice(rec.pipe_name().as_bytes());
                        self.http_values.extend_from_slice(b" method=");
                        self.http_values
                            .extend_from_slice(method.as_str().as_bytes());
                        self.http_values.extend_from_slice(b" url=");
                        self.http_values.extend_from_slice(rec.url().as_bytes());
                        self.http_values.extend_from_slice(b" route=");
                        velda_observability::with_route_name(rec.route_id, |name| {
                            self.http_values.extend_from_slice(name.as_bytes());
                        });
                        self.http_values.extend_from_slice(b" status=");
                        fast_write_u64(&mut self.http_values, rec.status_code as u64);
                        self.http_values.extend_from_slice(b" duration_ns=");
                        fast_write_u64(&mut self.http_values, rec.duration_ns);
                        self.http_values.extend_from_slice(b" bytes_rx=");
                        fast_write_u64(&mut self.http_values, rec.bytes_rx as u64);
                        self.http_values.extend_from_slice(b" bytes_tx=");
                        fast_write_u64(&mut self.http_values, rec.bytes_tx as u64);
                        self.http_values.extend_from_slice(b" client=");
                        fast_write_ip(&mut self.http_values, rec.peer_ip);
                        self.http_values.push(b':');
                        fast_write_u64(&mut self.http_values, rec.peer_port as u64);
                        self.http_values.extend_from_slice(b" endpoint=");
                        fast_write_ipv4(&mut self.http_values, rec.upstream_ip);
                        self.http_values.push(b':');
                        fast_write_u64(&mut self.http_values, rec.upstream_port as u64);
                        self.http_values.extend_from_slice(b" user_agent=");
                        self.http_values
                            .extend_from_slice(ua_fam.as_str().as_bytes());
                        self.http_values.push(b'/');
                        fast_write_u64(&mut self.http_values, rec.ua_major_version as u64);
                        self.http_values.extend_from_slice(b" client_os=");
                        self.http_values
                            .extend_from_slice(ua_os.as_str().as_bytes());
                        self.http_values.extend_from_slice(b" completion=");
                        self.http_values
                            .extend_from_slice(completion.as_str().as_bytes());
                        self.http_values.extend_from_slice(b"\"]");
                    }
                    LogProtocol::GrpcTcp | LogProtocol::GrpcUdp => {
                        if !self.grpc_values.is_empty() {
                            self.grpc_values.push(b',');
                        }
                        let grpc_st = LogGrpcStatus::from_u8(rec.grpc_status);
                        let ua_fam = UaFamily::from_u8(rec.ua_family);

                        self.grpc_values.extend_from_slice(b"[\"");
                        fast_write_ts_nano(&mut self.grpc_values, rec.timestamp_sec);
                        self.grpc_values.extend_from_slice(b"\",\"request_id=");
                        fast_write_hex32(
                            &mut self.grpc_values,
                            rec.timestamp_sec as u64,
                            rec.request_id,
                        );
                        self.grpc_values.extend_from_slice(b" pipe=");
                        self.grpc_values
                            .extend_from_slice(rec.pipe_name().as_bytes());
                        self.grpc_values.extend_from_slice(b" method=POST url=");
                        self.grpc_values.extend_from_slice(rec.url().as_bytes());
                        self.grpc_values.extend_from_slice(b" route=");
                        velda_observability::with_route_name(rec.route_id, |name| {
                            self.grpc_values.extend_from_slice(name.as_bytes());
                        });
                        self.grpc_values.extend_from_slice(b" grpc_status=");
                        self.grpc_values
                            .extend_from_slice(grpc_st.as_str().as_bytes());
                        self.grpc_values.extend_from_slice(b" status=");
                        fast_write_u64(&mut self.grpc_values, rec.status_code as u64);
                        self.grpc_values.extend_from_slice(b" rpc_messages=");
                        fast_write_u64(&mut self.grpc_values, rec.rpc_msgs_or_flags as u64);
                        self.grpc_values.extend_from_slice(b" duration_ns=");
                        fast_write_u64(&mut self.grpc_values, rec.duration_ns);
                        self.grpc_values.extend_from_slice(b" bytes_rx=");
                        fast_write_u64(&mut self.grpc_values, rec.bytes_rx as u64);
                        self.grpc_values.extend_from_slice(b" bytes_tx=");
                        fast_write_u64(&mut self.grpc_values, rec.bytes_tx as u64);
                        self.grpc_values.extend_from_slice(b" client=");
                        fast_write_ip(&mut self.grpc_values, rec.peer_ip);
                        self.grpc_values.push(b':');
                        fast_write_u64(&mut self.grpc_values, rec.peer_port as u64);
                        self.grpc_values.extend_from_slice(b" endpoint=");
                        fast_write_ipv4(&mut self.grpc_values, rec.upstream_ip);
                        self.grpc_values.push(b':');
                        fast_write_u64(&mut self.grpc_values, rec.upstream_port as u64);
                        self.grpc_values.extend_from_slice(b" user_agent=");
                        self.grpc_values
                            .extend_from_slice(ua_fam.as_str().as_bytes());
                        self.grpc_values.push(b'/');
                        fast_write_u64(&mut self.grpc_values, rec.ua_major_version as u64);
                        self.grpc_values.extend_from_slice(b" completion=");
                        self.grpc_values
                            .extend_from_slice(completion.as_str().as_bytes());
                        self.grpc_values.extend_from_slice(b"\"]");
                    }
                    LogProtocol::RawTcp | LogProtocol::RawUdp => {
                        if !self.raw_values.is_empty() {
                            self.raw_values.push(b',');
                        }
                        self.raw_values.extend_from_slice(b"[\"");
                        fast_write_ts_nano(&mut self.raw_values, rec.timestamp_sec);
                        self.raw_values.extend_from_slice(b"\",\"request_id=");
                        fast_write_hex32(
                            &mut self.raw_values,
                            rec.timestamp_sec as u64,
                            rec.request_id,
                        );
                        self.raw_values.extend_from_slice(b" pipe=");
                        self.raw_values
                            .extend_from_slice(rec.pipe_name().as_bytes());
                        self.raw_values.extend_from_slice(b" route=");
                        velda_observability::with_route_name(rec.route_id, |name| {
                            self.raw_values.extend_from_slice(name.as_bytes());
                        });
                        self.raw_values.extend_from_slice(b" duration_ns=");
                        fast_write_u64(&mut self.raw_values, rec.duration_ns);
                        self.raw_values.extend_from_slice(b" bytes_rx=");
                        fast_write_u64(&mut self.raw_values, rec.bytes_rx as u64);
                        self.raw_values.extend_from_slice(b" bytes_tx=");
                        fast_write_u64(&mut self.raw_values, rec.bytes_tx as u64);
                        self.raw_values.extend_from_slice(b" client=");
                        fast_write_ip(&mut self.raw_values, rec.peer_ip);
                        self.raw_values.push(b':');
                        fast_write_u64(&mut self.raw_values, rec.peer_port as u64);
                        self.raw_values.extend_from_slice(b" endpoint=");
                        fast_write_ipv4(&mut self.raw_values, rec.upstream_ip);
                        self.raw_values.push(b':');
                        fast_write_u64(&mut self.raw_values, rec.upstream_port as u64);
                        self.raw_values.extend_from_slice(b" completion=");
                        self.raw_values
                            .extend_from_slice(completion.as_str().as_bytes());
                        self.raw_values.extend_from_slice(b"\"]");
                    }
                }
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
    batches: &[Arc<[AccessLogRecord]>],
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
            for rec in batch.iter() {
                total_records += 1;
                let downstream_protocol = rec.downstream_protocol();
                let completion = LogCompletionReason::from_u8(rec.completion);

                match downstream_protocol {
                    LogProtocol::H1 | LogProtocol::H2 | LogProtocol::H3 => {
                        if !http_values.is_empty() {
                            http_values.push(b',');
                        }
                        let method = LogHttpMethod::from_u8(rec.method);
                        let ua_fam = UaFamily::from_u8(rec.ua_family);
                        let ua_os = UaOs::from_u8(rec.ua_os);

                        http_values.extend_from_slice(b"[\"");
                        fast_write_ts_nano(http_values, rec.timestamp_sec);
                        http_values.extend_from_slice(b"\",\"request_id=");
                        fast_write_hex32(http_values, rec.timestamp_sec as u64, rec.request_id);
                        http_values.extend_from_slice(b" pipe=");
                        http_values.extend_from_slice(rec.pipe_name().as_bytes());
                        http_values.extend_from_slice(b" method=");
                        http_values.extend_from_slice(method.as_str().as_bytes());
                        http_values.extend_from_slice(b" status=");
                        fast_write_u64(http_values, rec.status_code as u64);
                        http_values.extend_from_slice(b" route_id=");
                        fast_write_u64(http_values, rec.route_id as u64);
                        http_values.extend_from_slice(b" duration_ns=");
                        fast_write_u64(http_values, rec.duration_ns);
                        http_values.extend_from_slice(b" rx=");
                        fast_write_u64(http_values, rec.bytes_rx as u64);
                        http_values.extend_from_slice(b" tx=");
                        fast_write_u64(http_values, rec.bytes_tx as u64);
                        http_values.extend_from_slice(b" client=");
                        fast_write_ip(http_values, rec.peer_ip);
                        http_values.push(b':');
                        fast_write_u64(http_values, rec.peer_port as u64);
                        http_values.extend_from_slice(b" endpoint=");
                        fast_write_ipv4(http_values, rec.upstream_ip);
                        http_values.push(b':');
                        fast_write_u64(http_values, rec.upstream_port as u64);
                        http_values.extend_from_slice(b" ua=");
                        http_values.extend_from_slice(ua_fam.as_str().as_bytes());
                        http_values.push(b'/');
                        fast_write_u64(http_values, rec.ua_major_version as u64);
                        http_values.extend_from_slice(b" os=");
                        http_values.extend_from_slice(ua_os.as_str().as_bytes());
                        http_values.extend_from_slice(b" completion=");
                        http_values.extend_from_slice(completion.as_str().as_bytes());
                        http_values.extend_from_slice(b"\"]");
                    }
                    LogProtocol::GrpcTcp | LogProtocol::GrpcUdp => {
                        if !grpc_values.is_empty() {
                            grpc_values.push(b',');
                        }
                        let grpc_st = LogGrpcStatus::from_u8(rec.grpc_status);
                        let ua_fam = UaFamily::from_u8(rec.ua_family);

                        grpc_values.extend_from_slice(b"[\"");
                        fast_write_ts_nano(grpc_values, rec.timestamp_sec);
                        grpc_values.extend_from_slice(b"\",\"request_id=");
                        fast_write_hex32(grpc_values, rec.timestamp_sec as u64, rec.request_id);
                        grpc_values.extend_from_slice(b" pipe=");
                        grpc_values.extend_from_slice(rec.pipe_name().as_bytes());
                        grpc_values.extend_from_slice(b" grpc_status=");
                        grpc_values.extend_from_slice(grpc_st.as_str().as_bytes());
                        grpc_values.extend_from_slice(b" http_status=");
                        fast_write_u64(grpc_values, rec.status_code as u64);
                        grpc_values.extend_from_slice(b" route_id=");
                        fast_write_u64(grpc_values, rec.route_id as u64);
                        grpc_values.extend_from_slice(b" duration_ns=");
                        fast_write_u64(grpc_values, rec.duration_ns);
                        grpc_values.extend_from_slice(b" rx=");
                        fast_write_u64(grpc_values, rec.bytes_rx as u64);
                        grpc_values.extend_from_slice(b" tx=");
                        fast_write_u64(grpc_values, rec.bytes_tx as u64);
                        grpc_values.extend_from_slice(b" msgs=");
                        fast_write_u64(grpc_values, rec.rpc_msgs_or_flags as u64);
                        grpc_values.extend_from_slice(b" client=");
                        fast_write_ip(grpc_values, rec.peer_ip);
                        grpc_values.push(b':');
                        fast_write_u64(grpc_values, rec.peer_port as u64);
                        grpc_values.extend_from_slice(b" endpoint=");
                        fast_write_ipv4(grpc_values, rec.upstream_ip);
                        grpc_values.push(b':');
                        fast_write_u64(grpc_values, rec.upstream_port as u64);
                        grpc_values.extend_from_slice(b" ua=");
                        grpc_values.extend_from_slice(ua_fam.as_str().as_bytes());
                        grpc_values.push(b'/');
                        fast_write_u64(grpc_values, rec.ua_major_version as u64);
                        grpc_values.extend_from_slice(b" completion=");
                        grpc_values.extend_from_slice(completion.as_str().as_bytes());
                        grpc_values.extend_from_slice(b"\"]");
                    }
                    LogProtocol::RawTcp | LogProtocol::RawUdp => {
                        if !raw_values.is_empty() {
                            raw_values.push(b',');
                        }
                        raw_values.extend_from_slice(b"[\"");
                        fast_write_ts_nano(raw_values, rec.timestamp_sec);
                        raw_values.extend_from_slice(b"\",\"request_id=");
                        fast_write_hex32(raw_values, rec.timestamp_sec as u64, rec.request_id);
                        raw_values.extend_from_slice(b" pipe=");
                        raw_values.extend_from_slice(rec.pipe_name().as_bytes());
                        raw_values.extend_from_slice(b" duration_ns=");
                        fast_write_u64(raw_values, rec.duration_ns);
                        raw_values.extend_from_slice(b" rx=");
                        fast_write_u64(raw_values, rec.bytes_rx as u64);
                        raw_values.extend_from_slice(b" tx=");
                        fast_write_u64(raw_values, rec.bytes_tx as u64);
                        raw_values.extend_from_slice(b" client=");
                        fast_write_ip(raw_values, rec.peer_ip);
                        raw_values.push(b':');
                        fast_write_u64(raw_values, rec.peer_port as u64);
                        raw_values.extend_from_slice(b" endpoint=");
                        fast_write_ipv4(raw_values, rec.upstream_ip);
                        raw_values.push(b':');
                        fast_write_u64(raw_values, rec.upstream_port as u64);
                        raw_values.extend_from_slice(b" completion=");
                        raw_values.extend_from_slice(completion.as_str().as_bytes());
                        raw_values.extend_from_slice(b"\"]");
                    }
                }
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
    use velda_observability::PipeKind;

    #[test]
    fn test_loki_json_formatter_hex32_and_ipv6() {
        let mut labels = HashMap::new();
        labels.insert("env".to_string(), "prod".to_string());
        let mut formatter = LokiJsonFormatter::new("test-service", &labels);

        let http_rec = AccessLogRecord::from_scalars(
            0xaaaa_bbbb_cccc_dddd,
            1_710_000_001,
            320,
            "192.168.1.10:50000".parse().unwrap(),
            Some("10.0.0.1:8080".parse().unwrap()),
            128,
            512,
            200,
            1,
            0,
            PipeKind::BufferedH2H1,
            LogCompletionReason::Complete,
            LogHttpMethod::Get,
            LogGrpcStatus::NonGrpc,
            b"curl/8.6.0",
        );

        let grpc_rec = AccessLogRecord::from_scalars(
            0x1111_2222_3333_4444,
            1_710_000_002,
            890,
            "[::1]:50001".parse().unwrap(),
            Some("10.0.0.2:9090".parse().unwrap()),
            64,
            256,
            200,
            2,
            4,
            PipeKind::DuplexGrpcTcpTcp,
            LogCompletionReason::Complete,
            LogHttpMethod::Post,
            LogGrpcStatus::Ok,
            b"grpc-go/1.64.0",
        );

        let batches = vec![Arc::from(vec![http_rec, grpc_rec])];
        let mut out = Vec::new();
        let count = formatter.format_batches(&batches, &mut out);
        assert_eq!(count, 2);

        let json_str = std::str::from_utf8(&out).expect("Valid JSON");
        assert!(json_str.starts_with("{\"streams\":["));
        assert!(json_str.contains("\"service\":\"test-service\""));
        assert!(json_str.contains("\"protocol\":\"http\""));
        assert!(json_str.contains("\"protocol\":\"grpc\""));
        assert!(json_str.contains("\"env\":\"prod\""));

        // Verify full 32-hex request_id: timestamp_sec (1_710_000_001 = 0x0000000065ec8781) + entropy
        assert!(json_str.contains("request_id=0000000065ec8781aaaabbbbccccdddd"));
        assert!(json_str.contains("request_id=0000000065ec87821111222233334444"));

        // Verify RFC 5952 IPv6 compression
        assert!(json_str.contains("client=::1:50001"));
        assert!(json_str.contains("client=192.168.1.10:50000"));
    }

    #[test]
    fn test_format_loki_push_payload_empty_batch() {
        let template = LokiStreamTemplate::new("test", &HashMap::new());
        let mut out = Vec::new();
        let batches: Vec<Arc<[AccessLogRecord]>> = Vec::new();
        let count = format_loki_push_payload(&batches, &template, &mut out);
        assert_eq!(count, 0);
        assert!(out.is_empty());
    }
}
