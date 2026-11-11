//! Wire Protobuf Encoder for Grafana Loki PushRequest (`loki_log_shipper::proto::template`).
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────────────────────────┐
//! │                    LOKI PROTOBUF WIRE SERIALIZATION ARCHITECTURE                       │
//! │                                                                                        │
//! │  1. Pre-Baked Prometheus Labels Syntax (LokiProtoLabels):                              │
//! │     http_labels: {service="velda-edge",protocol="http",...}                            │
//! │     grpc_labels: {service="velda-edge",protocol="grpc",...}                            │
//! │     raw_labels:  {service="velda-edge",protocol="raw",...}                             │
//! │                                                                                        │
//! │  2. Zero-Allocation Wire Serialization (LokiProtoFormatter):                           │
//! │     AccessLogRecord (64B SPSC Ring)                                                    │
//! │           │                                                                            │
//! │           ▼                                                                            │
//! │     Format log line text into scratch line_buf (32-hex request_id, RFC 5952 IP)         │
//! │           │                                                                            │
//! │           ▼                                                                            │
//! │     Encode EntryAdapter wire bytes directly into protocol stream buffer:               │
//! │     ┌────────────┬─────────────────────────────┬────────────────────────────────────┐  │
//! │     │ Tag 2 0x12 │ entry_len varint            │ EntryAdapter fields:               │  │
//! │     │            │                             │ ├─ Tag 1: Timestamp (sec, nanos)   │  │
//! │     │            │                             │ └─ Tag 2: Line (len + ASCII text)  │  │
//! │     └────────────┴─────────────────────────────┴────────────────────────────────────┘  │
//! │           │                                                                            │
//! │           ▼                                                                            │
//! │  3. Stream Assembly into Output Buffer (PushRequest):                                  │
//! │     out.extend_from_slice(StreamAdapter { Tag 1: labels, Tag 2: entries })            │
//! │     Guarantees 0 heap allocations during export flush cycle.                           │
//! └────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use velda_observability::{
    AccessLogRecord, LogCompletionReason, LogGrpcStatus, LogHttpMethod, LogProtocol, PipeStrategy,
    UaFamily, UaOs,
};

use super::super::lut::{fast_write_hex32, fast_write_ip, fast_write_ipv4, fast_write_u64};

/// Fast inline varint encoding for u64.
#[inline(always)]
pub fn write_varint(buf: &mut Vec<u8>, mut val: u64) {
    while val >= 0x80 {
        buf.push((val as u8 & 0x7F) | 0x80);
        val >>= 7;
    }
    buf.push(val as u8);
}

/// Computes length in bytes of a varint without writing it.
#[inline(always)]
pub const fn varint_len(mut val: u64) -> usize {
    let mut len = 1;
    while val >= 0x80 {
        len += 1;
        val >>= 7;
    }
    len
}

/// Pre-baked Prometheus label descriptors for HTTP, gRPC, and Raw streams.
#[derive(Debug, Clone)]
pub struct LokiProtoLabels {
    pub http_labels: String,
    pub grpc_labels: String,
    pub raw_labels: String,
}

impl LokiProtoLabels {
    /// Builds Prometheus labels syntax string: `{service="...",protocol="...",k="v"}`.
    pub fn new(service_name: &str, static_labels: &HashMap<String, String>) -> Self {
        fn build_labels(
            service_name: &str,
            protocol: &str,
            static_labels: &HashMap<String, String>,
        ) -> String {
            let mut s = String::with_capacity(128);
            s.push('{');
            s.push_str("service=\"");
            s.push_str(service_name);
            s.push_str("\",protocol=\"");
            s.push_str(protocol);
            s.push('"');

            let mut sorted_keys: Vec<&String> = static_labels.keys().collect();
            sorted_keys.sort();
            for k in sorted_keys {
                if let Some(v) = static_labels.get(k) {
                    s.push(',');
                    s.push_str(k);
                    s.push_str("=\"");
                    s.push_str(v);
                    s.push('"');
                }
            }
            s.push('}');
            s
        }

        Self {
            http_labels: build_labels(service_name, "http", static_labels),
            grpc_labels: build_labels(service_name, "grpc", static_labels),
            raw_labels: build_labels(service_name, "raw", static_labels),
        }
    }
}

/// High-performance Protobuf wire encoder for Grafana Loki PushRequest.
///
/// Encodes `PushRequest` directly into a wire buffer with zero intermediate AST allocation
/// and persistent scratch buffers.
pub struct LokiProtoFormatter {
    pub labels: LokiProtoLabels,
    pub http_entries_buf: Vec<u8>,
    pub grpc_entries_buf: Vec<u8>,
    pub raw_entries_buf: Vec<u8>,
    pub line_buf: Vec<u8>,
}

impl LokiProtoFormatter {
    /// Instantiates a new Protobuf formatter with pre-sized persistent scratch buffers.
    pub fn new(service_name: &str, static_labels: &HashMap<String, String>) -> Self {
        Self {
            labels: LokiProtoLabels::new(service_name, static_labels),
            http_entries_buf: Vec::with_capacity(16 * 1024),
            grpc_entries_buf: Vec::with_capacity(8 * 1024),
            raw_entries_buf: Vec::with_capacity(4 * 1024),
            line_buf: Vec::with_capacity(512),
        }
    }

    /// Wraps pre-baked [`LokiProtoLabels`] with persistent scratch buffers.
    pub fn with_labels(labels: LokiProtoLabels) -> Self {
        Self {
            labels,
            http_entries_buf: Vec::with_capacity(16 * 1024),
            grpc_entries_buf: Vec::with_capacity(8 * 1024),
            raw_entries_buf: Vec::with_capacity(4 * 1024),
            line_buf: Vec::with_capacity(512),
        }
    }

    /// Serializes batches of reference-counted [`AccessLogRecord`] into a binary Protobuf
    /// `logproto.PushRequest` wire payload.
    ///
    /// Guarantees 0 heap allocations during export flush cycle.
    pub fn format_batches(
        &mut self,
        batches: &[Arc<[AccessLogRecord]>],
        out: &mut Vec<u8>,
    ) -> usize {
        out.clear();
        self.http_entries_buf.clear();
        self.grpc_entries_buf.clear();
        self.raw_entries_buf.clear();

        let mut total_records = 0usize;

        for batch in batches {
            for rec in batch.iter() {
                total_records += 1;
                let proto = LogProtocol::from_u8(rec.downstream_proto);
                let up_proto = LogProtocol::from_u8(rec.upstream_proto);
                let pipe = PipeStrategy::from_discriminant(rec.pipe_strategy)
                    .unwrap_or(PipeStrategy::Buffered);
                let completion = LogCompletionReason::from_u8(rec.completion);
                let seconds = rec.timestamp_sec as i64;

                self.line_buf.clear();

                match proto {
                    LogProtocol::H1 | LogProtocol::H2 | LogProtocol::H3 => {
                        let method = LogHttpMethod::from_u8(rec.method);
                        let ua_fam = UaFamily::from_u8(rec.ua_family);
                        let ua_os = UaOs::from_u8(rec.ua_os);

                        self.line_buf.extend_from_slice(b"request_id=");
                        fast_write_hex32(
                            &mut self.line_buf,
                            rec.timestamp_sec as u64,
                            rec.request_id,
                        );
                        self.line_buf.extend_from_slice(b" proto=");
                        self.line_buf.extend_from_slice(proto.as_str().as_bytes());
                        self.line_buf.extend_from_slice(b" up_proto=");
                        self.line_buf
                            .extend_from_slice(up_proto.as_str().as_bytes());
                        self.line_buf.extend_from_slice(b" pipe=");
                        self.line_buf.extend_from_slice(pipe.as_str().as_bytes());
                        self.line_buf.extend_from_slice(b" method=");
                        self.line_buf.extend_from_slice(method.as_str().as_bytes());
                        self.line_buf.extend_from_slice(b" status=");
                        fast_write_u64(&mut self.line_buf, rec.status_code as u64);
                        self.line_buf.extend_from_slice(b" route_id=");
                        fast_write_u64(&mut self.line_buf, rec.route_id as u64);
                        self.line_buf.extend_from_slice(b" dur_us=");
                        fast_write_u64(&mut self.line_buf, rec.duration_us as u64);
                        self.line_buf.extend_from_slice(b" rx=");
                        fast_write_u64(&mut self.line_buf, rec.bytes_rx as u64);
                        self.line_buf.extend_from_slice(b" tx=");
                        fast_write_u64(&mut self.line_buf, rec.bytes_tx as u64);
                        self.line_buf.extend_from_slice(b" peer=");
                        fast_write_ip(&mut self.line_buf, rec.peer_ip);
                        self.line_buf.push(b':');
                        fast_write_u64(&mut self.line_buf, rec.peer_port as u64);
                        self.line_buf.extend_from_slice(b" upstream=");
                        fast_write_ipv4(&mut self.line_buf, rec.upstream_ip);
                        self.line_buf.push(b':');
                        fast_write_u64(&mut self.line_buf, rec.upstream_port as u64);
                        self.line_buf.extend_from_slice(b" ua=");
                        self.line_buf.extend_from_slice(ua_fam.as_str().as_bytes());
                        self.line_buf.push(b'/');
                        fast_write_u64(&mut self.line_buf, rec.ua_major_version as u64);
                        self.line_buf.extend_from_slice(b" os=");
                        self.line_buf.extend_from_slice(ua_os.as_str().as_bytes());
                        self.line_buf.extend_from_slice(b" completion=");
                        self.line_buf
                            .extend_from_slice(completion.as_str().as_bytes());

                        Self::encode_entry_into(
                            &mut self.http_entries_buf,
                            seconds,
                            0,
                            &self.line_buf,
                        );
                    }
                    LogProtocol::GrpcTcp | LogProtocol::GrpcUdp => {
                        let grpc_st = LogGrpcStatus::from_u8(rec.grpc_status);
                        let ua_fam = UaFamily::from_u8(rec.ua_family);

                        self.line_buf.extend_from_slice(b"request_id=");
                        fast_write_hex32(
                            &mut self.line_buf,
                            rec.timestamp_sec as u64,
                            rec.request_id,
                        );
                        self.line_buf.extend_from_slice(b" proto=");
                        self.line_buf.extend_from_slice(proto.as_str().as_bytes());
                        self.line_buf.extend_from_slice(b" grpc_status=");
                        self.line_buf.extend_from_slice(grpc_st.as_str().as_bytes());
                        self.line_buf.extend_from_slice(b" http_status=");
                        fast_write_u64(&mut self.line_buf, rec.status_code as u64);
                        self.line_buf.extend_from_slice(b" route_id=");
                        fast_write_u64(&mut self.line_buf, rec.route_id as u64);
                        self.line_buf.extend_from_slice(b" dur_us=");
                        fast_write_u64(&mut self.line_buf, rec.duration_us as u64);
                        self.line_buf.extend_from_slice(b" rx=");
                        fast_write_u64(&mut self.line_buf, rec.bytes_rx as u64);
                        self.line_buf.extend_from_slice(b" tx=");
                        fast_write_u64(&mut self.line_buf, rec.bytes_tx as u64);
                        self.line_buf.extend_from_slice(b" msgs=");
                        fast_write_u64(&mut self.line_buf, rec.rpc_msgs_or_flags as u64);
                        self.line_buf.extend_from_slice(b" peer=");
                        fast_write_ip(&mut self.line_buf, rec.peer_ip);
                        self.line_buf.push(b':');
                        fast_write_u64(&mut self.line_buf, rec.peer_port as u64);
                        self.line_buf.extend_from_slice(b" upstream=");
                        fast_write_ipv4(&mut self.line_buf, rec.upstream_ip);
                        self.line_buf.push(b':');
                        fast_write_u64(&mut self.line_buf, rec.upstream_port as u64);
                        self.line_buf.extend_from_slice(b" ua=");
                        self.line_buf.extend_from_slice(ua_fam.as_str().as_bytes());
                        self.line_buf.push(b'/');
                        fast_write_u64(&mut self.line_buf, rec.ua_major_version as u64);
                        self.line_buf.extend_from_slice(b" completion=");
                        self.line_buf
                            .extend_from_slice(completion.as_str().as_bytes());

                        Self::encode_entry_into(
                            &mut self.grpc_entries_buf,
                            seconds,
                            0,
                            &self.line_buf,
                        );
                    }
                    LogProtocol::RawTcp | LogProtocol::RawUdp => {
                        self.line_buf.extend_from_slice(b"request_id=");
                        fast_write_hex32(
                            &mut self.line_buf,
                            rec.timestamp_sec as u64,
                            rec.request_id,
                        );
                        self.line_buf.extend_from_slice(b" proto=");
                        self.line_buf.extend_from_slice(proto.as_str().as_bytes());
                        self.line_buf.extend_from_slice(b" dur_us=");
                        fast_write_u64(&mut self.line_buf, rec.duration_us as u64);
                        self.line_buf.extend_from_slice(b" rx=");
                        fast_write_u64(&mut self.line_buf, rec.bytes_rx as u64);
                        self.line_buf.extend_from_slice(b" tx=");
                        fast_write_u64(&mut self.line_buf, rec.bytes_tx as u64);
                        self.line_buf.extend_from_slice(b" peer=");
                        fast_write_ip(&mut self.line_buf, rec.peer_ip);
                        self.line_buf.push(b':');
                        fast_write_u64(&mut self.line_buf, rec.peer_port as u64);
                        self.line_buf.extend_from_slice(b" upstream=");
                        fast_write_ipv4(&mut self.line_buf, rec.upstream_ip);
                        self.line_buf.push(b':');
                        fast_write_u64(&mut self.line_buf, rec.upstream_port as u64);
                        self.line_buf.extend_from_slice(b" completion=");
                        self.line_buf
                            .extend_from_slice(completion.as_str().as_bytes());

                        Self::encode_entry_into(
                            &mut self.raw_entries_buf,
                            seconds,
                            0,
                            &self.line_buf,
                        );
                    }
                }
            }
        }

        if total_records == 0 {
            return 0;
        }

        if !self.http_entries_buf.is_empty() {
            Self::encode_stream_into(out, &self.labels.http_labels, &self.http_entries_buf);
        }

        if !self.grpc_entries_buf.is_empty() {
            Self::encode_stream_into(out, &self.labels.grpc_labels, &self.grpc_entries_buf);
        }

        if !self.raw_entries_buf.is_empty() {
            Self::encode_stream_into(out, &self.labels.raw_labels, &self.raw_entries_buf);
        }

        total_records
    }

    #[inline(always)]
    fn encode_entry_into(buf: &mut Vec<u8>, seconds: i64, nanos: i32, line: &[u8]) {
        // Timestamp message length
        let ts_msg_len = 1
            + varint_len(seconds as u64)
            + if nanos != 0 {
                1 + varint_len(nanos as u64)
            } else {
                0
            };

        // EntryAdapter fields length:
        // Tag 1 (Timestamp field): 0x0A, varint(ts_msg_len), then timestamp payload
        let ts_field_len = 1 + varint_len(ts_msg_len as u64) + ts_msg_len;
        // Tag 2 (line field): 0x12, varint(line.len()), then line bytes
        let line_field_len = 1 + varint_len(line.len() as u64) + line.len();

        let entry_msg_len = ts_field_len + line_field_len;

        // Tag 2 of StreamAdapter (entries repeated): 0x12, varint(entry_msg_len)
        buf.push(0x12);
        write_varint(buf, entry_msg_len as u64);

        // EntryAdapter field 1: Timestamp
        buf.push(0x0A);
        write_varint(buf, ts_msg_len as u64);
        buf.push(0x08);
        write_varint(buf, seconds as u64);
        if nanos != 0 {
            buf.push(0x10);
            write_varint(buf, nanos as u64);
        }

        // EntryAdapter field 2: line
        buf.push(0x12);
        write_varint(buf, line.len() as u64);
        buf.extend_from_slice(line);
    }

    #[inline(always)]
    fn encode_stream_into(out: &mut Vec<u8>, labels: &str, entries_buf: &[u8]) {
        let labels_bytes = labels.as_bytes();
        let labels_field_len = 1 + varint_len(labels_bytes.len() as u64) + labels_bytes.len();
        let stream_msg_len = labels_field_len + entries_buf.len();

        // PushRequest field 1: streams repeated (tag 1: 0x0A, varint(stream_msg_len))
        out.push(0x0A);
        write_varint(out, stream_msg_len as u64);

        // StreamAdapter field 1: labels
        out.push(0x0A);
        write_varint(out, labels_bytes.len() as u64);
        out.extend_from_slice(labels_bytes);

        // StreamAdapter field 2: entries
        out.extend_from_slice(entries_buf);
    }
}

/// Standalone formatting helper for Grafana Loki PushRequest wire format.
pub fn format_loki_records_proto(
    batches: &[Arc<[AccessLogRecord]>],
    labels: &LokiProtoLabels,
    out: &mut Vec<u8>,
) -> usize {
    let mut formatter = LokiProtoFormatter::with_labels(labels.clone());
    formatter.format_batches(batches, out)
}

#[cfg(test)]
mod tests {
    use super::super::model::PushRequest;
    use super::*;
    use prost::Message;

    #[test]
    fn test_proto_wire_formatter_decodes_cleanly() {
        let mut labels = HashMap::new();
        labels.insert("env".to_string(), "staging".to_string());
        let mut formatter = LokiProtoFormatter::new("my-service", &labels);

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
            LogProtocol::H2,
            LogProtocol::H1,
            PipeStrategy::Buffered,
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
            LogProtocol::GrpcTcp,
            LogProtocol::GrpcTcp,
            PipeStrategy::Duplex,
            LogCompletionReason::Complete,
            LogHttpMethod::Post,
            LogGrpcStatus::Ok,
            b"grpc-go/1.64.0",
        );

        let batches = vec![Arc::from(vec![http_rec, grpc_rec])];
        let mut wire_out = Vec::new();
        let count = formatter.format_batches(&batches, &mut wire_out);
        assert_eq!(count, 2);
        assert!(!wire_out.is_empty());

        // Decode binary wire_out with Prost PushRequest model!
        let decoded = PushRequest::decode(&wire_out[..]).expect("Valid Protobuf PushRequest");
        assert_eq!(decoded.streams.len(), 2);

        // Verify Stream 1 (HTTP)
        let s0 = &decoded.streams[0];
        assert_eq!(
            s0.labels,
            "{service=\"my-service\",protocol=\"http\",env=\"staging\"}"
        );
        assert_eq!(s0.entries.len(), 1);
        let e0 = &s0.entries[0];
        assert_eq!(e0.timestamp.as_ref().unwrap().seconds, 1_710_000_001);
        assert!(
            e0.line
                .contains("request_id=0000000065ec8781aaaabbbbccccdddd")
        );
        assert!(e0.line.contains("peer=192.168.1.10:50000"));

        // Verify Stream 2 (gRPC)
        let s1 = &decoded.streams[1];
        assert_eq!(
            s1.labels,
            "{service=\"my-service\",protocol=\"grpc\",env=\"staging\"}"
        );
        assert_eq!(s1.entries.len(), 1);
        let e1 = &s1.entries[0];
        assert_eq!(e1.timestamp.as_ref().unwrap().seconds, 1_710_000_002);
        assert!(
            e1.line
                .contains("request_id=0000000065ec87821111222233334444")
        );
        assert!(e1.line.contains("peer=::1:50001"));
        assert!(e1.line.contains("grpc_status=OK"));
    }
}
