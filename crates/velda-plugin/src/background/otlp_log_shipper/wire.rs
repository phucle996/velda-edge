//! Direct Protobuf Wire Format Serializer (`otlp_log_shipper::wire`).
//!
//! Encodes batches of reference-counted 64-byte [`AccessLogRecord`] directly into
//! compliant OpenTelemetry Protocol Buffers wire bytes (`ExportLogsServiceRequest`)
//! with ZERO intermediate Prost struct AST allocation and ZERO runtime Prost tree encoding.

use std::sync::Arc;
use velda_observability::{
    AccessLogRecord, LogGrpcStatus, LogHttpMethod, LogProtocol, UaFamily, UaOs,
};

use super::format::{fast_write_hex16, fast_write_ip128, fast_write_u64};

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

/// Helper to serialize a Protobuf IntValue attribute:
/// `KeyValue { key, value: Some(AnyValue { value: Some(IntValue(val)) }) }`
#[inline(always)]
fn write_int_attribute(buf: &mut Vec<u8>, key_wire_bytes: &[u8], val: i64) {
    let uval = val as u64;
    let v_len = varint_len(uval);
    let any_val_len = 1 + v_len; // tag 0x18 + varint
    let kv_len = key_wire_bytes.len() + 1 + varint_len(any_val_len as u64) + any_val_len;

    buf.push(0x32); // LogRecord field 6 (repeated KeyValue attributes)
    write_varint(buf, kv_len as u64);
    buf.extend_from_slice(key_wire_bytes); // 0x0A + len + key
    buf.push(0x12); // KeyValue field 2 (value: AnyValue)
    write_varint(buf, any_val_len as u64);
    buf.push(0x18); // AnyValue field 3 (int_value)
    write_varint(buf, uval);
}

/// Helper to serialize a Protobuf StringValue attribute:
/// `KeyValue { key, value: Some(AnyValue { value: Some(StringValue(s)) }) }`
#[inline(always)]
fn write_str_attribute(buf: &mut Vec<u8>, key_wire_bytes: &[u8], s: &[u8]) {
    let str_val_len = 1 + varint_len(s.len() as u64) + s.len(); // tag 0x0A + len + bytes
    let any_val_len = str_val_len;
    let kv_len = key_wire_bytes.len() + 1 + varint_len(any_val_len as u64) + any_val_len;

    buf.push(0x32); // LogRecord field 6
    write_varint(buf, kv_len as u64);
    buf.extend_from_slice(key_wire_bytes);
    buf.push(0x12); // KeyValue field 2
    write_varint(buf, any_val_len as u64);
    buf.push(0x0A); // AnyValue field 1 (string_value)
    write_varint(buf, s.len() as u64);
    buf.extend_from_slice(s);
}

// Pre-baked Key tags and names (tag 0x0A + len + string):
const KEY_STATUS_CODE: &[u8] = b"\x0a\x19http.response.status_code";
const KEY_DURATION_US: &[u8] = b"\x0a\x0bduration_us";
const KEY_RESP_BODY_SIZE: &[u8] = b"\x0a\x17http.response.body.size";
const KEY_REQ_BODY_SIZE: &[u8] = b"\x0a\x16http.request.body.size";
const KEY_ROUTE_ID: &[u8] = b"\x0a\x0evelda.route_id";
const KEY_CLIENT_ADDR: &[u8] = b"\x0a\x0eclient.address";
const KEY_CLIENT_PORT: &[u8] = b"\x0a\x0bclient.port";
const KEY_UA_MAJOR: &[u8] = b"\x0a\x18user_agent.major_version";

// Pre-baked request_id attribute prefix:
// KeyValue tag 0x32, len 32 (0x20), key tag 0x0A, len 10, "request_id", AnyValue tag 0x12, len 18, StringValue tag 0x0A, len 16
const ATTR_REQUEST_ID_PREFIX: &[u8] = b"\x32\x20\x0a\x0arequest_id\x12\x12\x0a\x10";

// Pre-baked static severity chunks:
const SEV_INFO: &[u8] = b"\x18\x09\x22\x04INFO";
const SEV_WARN: &[u8] = b"\x18\x0d\x22\x04WARN";
const SEV_ERROR: &[u8] = b"\x18\x11\x05\x22ERROR";

/// Pre-baked binary wire envelope for OTLP gRPC Protobuf logs.
#[derive(Debug, Clone)]
pub struct OtlpProtoWireEnvelope {
    pub resource_wire: Vec<u8>,
    pub scope_wire: Vec<u8>,
    // Pre-baked static attribute wire slices
    proto_attrs: [Vec<u8>; 8],
    method_attrs: [Vec<u8>; 8],
    grpc_attrs: [Vec<u8>; 18],
    ua_fam_attrs: [Vec<u8>; 7],
    ua_os_attrs: [Vec<u8>; 7],
}

impl OtlpProtoWireEnvelope {
    /// Constructs a new wire envelope, pre-baking static Resource, Scope, and enum attribute wire bytes once.
    pub fn new(service_name: &str) -> Self {
        use crate::background::otlp_log_shipper::proto::{
            InstrumentationScope, KeyValue, Resource,
        };
        use prost::Message;

        // 1. Bake Resource bytes once at startup using Prost:
        let resource = Resource {
            attributes: vec![
                KeyValue::string("service.name", service_name),
                KeyValue::string("telemetry.sdk.name", "velda-edge"),
                KeyValue::string("telemetry.sdk.language", "rust"),
            ],
            dropped_attributes_count: 0,
        };
        let mut resource_payload = Vec::new();
        resource.encode(&mut resource_payload).unwrap();

        let mut resource_wire = Vec::new();
        resource_wire.push(0x0A); // ResourceLogs field 1 (Resource resource)
        write_varint(&mut resource_wire, resource_payload.len() as u64);
        resource_wire.extend_from_slice(&resource_payload);

        // 2. Bake InstrumentationScope bytes once at startup using Prost:
        let scope = InstrumentationScope {
            name: "velda.access_log".to_string(),
            version: "0.1.0".to_string(),
            attributes: Vec::new(),
            dropped_attributes_count: 0,
        };
        let mut scope_payload = Vec::new();
        scope.encode(&mut scope_payload).unwrap();

        let mut scope_wire = Vec::new();
        scope_wire.push(0x0A); // ScopeLogs field 1 (InstrumentationScope scope)
        write_varint(&mut scope_wire, scope_payload.len() as u64);
        scope_wire.extend_from_slice(&scope_payload);

        // 3. Bake static attribute tables:
        let mut proto_attrs: [Vec<u8>; 8] = Default::default();
        for (i, slot) in proto_attrs.iter_mut().enumerate() {
            let p = LogProtocol::from_u8(i as u8);
            let mut buf = Vec::new();
            write_str_attribute(
                &mut buf,
                b"\x0a\x15network.protocol.name",
                p.as_str().as_bytes(),
            );
            *slot = buf;
        }

        let mut method_attrs: [Vec<u8>; 8] = Default::default();
        for (i, slot) in method_attrs.iter_mut().enumerate() {
            let m = LogHttpMethod::from_u8(i as u8);
            let mut buf = Vec::new();
            write_str_attribute(
                &mut buf,
                b"\x0a\x13http.request.method",
                m.as_str().as_bytes(),
            );
            *slot = buf;
        }

        let mut grpc_attrs: [Vec<u8>; 18] = Default::default();
        for (i, slot) in grpc_attrs.iter_mut().enumerate() {
            let g = LogGrpcStatus::from_u8(i as u8);
            let mut buf = Vec::new();
            write_str_attribute(
                &mut buf,
                b"\x0a\x14rpc.grpc.status_code",
                g.as_str().as_bytes(),
            );
            *slot = buf;
        }

        let mut ua_fam_attrs: [Vec<u8>; 7] = Default::default();
        for (i, slot) in ua_fam_attrs.iter_mut().enumerate() {
            let u = UaFamily::from_u8(i as u8);
            let mut buf = Vec::new();
            write_str_attribute(
                &mut buf,
                b"\x0a\x11user_agent.family",
                u.as_str().as_bytes(),
            );
            *slot = buf;
        }

        let mut ua_os_attrs: [Vec<u8>; 7] = Default::default();
        for (i, slot) in ua_os_attrs.iter_mut().enumerate() {
            let o = UaOs::from_u8(i as u8);
            let mut buf = Vec::new();
            write_str_attribute(&mut buf, b"\x0a\x0duser_agent.os", o.as_str().as_bytes());
            *slot = buf;
        }

        Self {
            resource_wire,
            scope_wire,
            proto_attrs,
            method_attrs,
            grpc_attrs,
            ua_fam_attrs,
            ua_os_attrs,
        }
    }

    /// Serializes a single [`AccessLogRecord`] directly into `out` with in-place length backpatching.
    #[inline(always)]
    pub fn encode_log_record_wire_direct(&self, rec: &AccessLogRecord, out: &mut Vec<u8>) {
        // Tag 2 for repeated LogRecord in ScopeLogs (wire type 2 = 0x12)
        out.push(0x12);
        // Reserve 2 bytes for the length varint (since a record is always between 128 and 16383 bytes)
        let len_offset = out.len();
        out.push(0);
        out.push(0);
        let start_rec = out.len();

        let proto = LogProtocol::from_u8(rec.downstream_proto);
        let method = LogHttpMethod::from_u8(rec.method);
        let grpc_st = LogGrpcStatus::from_u8(rec.grpc_status);
        let ts_nano = (rec.timestamp_sec as u64).saturating_mul(1_000_000_000);

        // 1. time_unix_nano (tag 1, wire type 1 = 0x09) & observed_time_unix_nano (tag 2, wire type 1 = 0x11)
        out.push(0x09);
        out.extend_from_slice(&ts_nano.to_le_bytes());
        out.push(0x11);
        out.extend_from_slice(&ts_nano.to_le_bytes());

        // 2. severity_number (tag 3, varint = 0x18) & severity_text (tag 4, string = 0x22)
        if rec.status_code >= 500
            || (grpc_st != LogGrpcStatus::Ok && grpc_st != LogGrpcStatus::NonGrpc)
        {
            out.extend_from_slice(SEV_ERROR);
        } else if rec.status_code >= 400 {
            out.extend_from_slice(SEV_WARN);
        } else {
            out.extend_from_slice(SEV_INFO);
        }

        // 3. body (tag 5, wire type 2 = 0x2A) -> AnyValue { string_value = ... }
        out.push(0x2A);
        let body_any_offset = out.len();
        out.push(0);
        out.push(0x0A);
        let body_str_offset = out.len();
        out.push(0);
        let body_start = out.len();

        out.extend_from_slice(b"request_id=");
        fast_write_hex16(out, rec.request_id);
        out.push(b' ');
        out.extend_from_slice(method.as_str().as_bytes());
        out.push(b' ');
        out.extend_from_slice(proto.as_str().as_bytes());
        out.extend_from_slice(b" status=");
        fast_write_u64(out, rec.status_code as u64);
        out.extend_from_slice(b" dur_us=");
        fast_write_u64(out, rec.duration_us as u64);
        out.extend_from_slice(b" peer=");
        fast_write_ip128(out, rec.peer_ip);
        out.push(b':');
        fast_write_u64(out, rec.peer_port as u64);

        let str_len = out.len() - body_start;
        out[body_str_offset] = str_len as u8;
        out[body_any_offset] = (str_len + 2) as u8;

        // 4. Attributes (tag 6, wire type 2 = 0x32):
        // Attr 0: request_id (hex16 string)
        out.extend_from_slice(ATTR_REQUEST_ID_PREFIX);
        fast_write_hex16(out, rec.request_id);

        // Attr 1: network.protocol.name
        let proto_idx = (rec.downstream_proto as usize).min(7);
        out.extend_from_slice(&self.proto_attrs[proto_idx]);

        // Attr 2: http.request.method
        let method_idx = (rec.method as usize).min(7);
        out.extend_from_slice(&self.method_attrs[method_idx]);

        // Attr 3: http.response.status_code
        write_int_attribute(out, KEY_STATUS_CODE, rec.status_code as i64);

        // Attr 4: rpc.grpc.status_code
        let grpc_idx = (rec.grpc_status as usize).min(17);
        out.extend_from_slice(&self.grpc_attrs[grpc_idx]);

        // Attr 5: duration_us
        write_int_attribute(out, KEY_DURATION_US, rec.duration_us as i64);

        // Attr 6: http.response.body.size
        write_int_attribute(out, KEY_RESP_BODY_SIZE, rec.bytes_tx as i64);

        // Attr 7: http.request.body.size
        write_int_attribute(out, KEY_REQ_BODY_SIZE, rec.bytes_rx as i64);

        // Attr 8: velda.route_id
        write_int_attribute(out, KEY_ROUTE_ID, rec.route_id as i64);

        // Attr 9: client.address
        out.push(0x32);
        let attr_offset = out.len();
        out.push(0);
        out.extend_from_slice(KEY_CLIENT_ADDR);
        out.push(0x12);
        let any_offset = out.len();
        out.push(0);
        out.push(0x0A);
        let ip_str_offset = out.len();
        out.push(0);
        let ip_start = out.len();
        fast_write_ip128(out, rec.peer_ip);
        let ip_len = out.len() - ip_start;
        out[ip_str_offset] = ip_len as u8;
        let any_val_len = ip_len + 2;
        out[any_offset] = any_val_len as u8;
        out[attr_offset] = (KEY_CLIENT_ADDR.len() + 1 + 1 + any_val_len) as u8;

        // Attr 10: client.port
        write_int_attribute(out, KEY_CLIENT_PORT, rec.peer_port as i64);

        // Attr 11: user_agent.family
        let ua_fam_idx = (rec.ua_family as usize).min(6);
        out.extend_from_slice(&self.ua_fam_attrs[ua_fam_idx]);

        // Attr 12: user_agent.major_version
        write_int_attribute(out, KEY_UA_MAJOR, rec.ua_major_version as i64);

        // Attr 13: user_agent.os
        let ua_os_idx = (rec.ua_os as usize).min(6);
        out.extend_from_slice(&self.ua_os_attrs[ua_os_idx]);

        // Backpatch the 2-byte varint length of LogRecord:
        let rec_len = out.len() - start_rec;
        out[len_offset] = (rec_len as u8 & 0x7F) | 0x80;
        out[len_offset + 1] = (rec_len >> 7) as u8;
    }

    /// Serializes a batch of [`AccessLogRecord`] directly into compliant Protobuf wire bytes (`out`).
    /// Returns total records formatted.
    #[inline(always)]
    pub fn format_records_wire(
        &self,
        batches: &[Arc<[AccessLogRecord]>],
        records_buf: &mut Vec<u8>,
        out: &mut Vec<u8>,
    ) -> usize {
        records_buf.clear();
        out.clear();

        let total_recs: usize = batches.iter().map(|b| b.len()).sum();
        if total_recs == 0 {
            return 0;
        }

        records_buf.reserve(total_recs * 450);

        for batch in batches {
            for rec in batch.iter() {
                self.encode_log_record_wire_direct(rec, records_buf);
            }
        }

        // Wrap into ScopeLogs, ResourceLogs, and ExportLogsServiceRequest:
        // ScopeLogs payload: scope_wire + records_buf
        let scope_payload_len = self.scope_wire.len() + records_buf.len();

        // ScopeLogs message in ResourceLogs: tag 2 (0x12) + varint(scope_payload_len) + scope_wire + records_buf
        let scope_msg_header_len = 1 + varint_len(scope_payload_len as u64);

        // ResourceLogs payload: resource_wire + scope_msg
        let res_payload_len = self.resource_wire.len() + scope_msg_header_len + scope_payload_len;

        // ExportLogsServiceRequest: tag 1 (0x0A) + varint(res_payload_len) + ResourceLogs payload
        let total_root_len = 1 + varint_len(res_payload_len as u64) + res_payload_len;
        out.reserve(total_root_len);

        out.push(0x0A); // ExportLogsServiceRequest field 1 (repeated ResourceLogs)
        write_varint(out, res_payload_len as u64);
        out.extend_from_slice(&self.resource_wire);
        out.push(0x12); // ResourceLogs field 2 (repeated ScopeLogs)
        write_varint(out, scope_payload_len as u64);
        out.extend_from_slice(&self.scope_wire);
        out.extend_from_slice(records_buf);

        total_recs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use velda_observability::{LogCompletionReason, PipeStrategy};

    #[test]
    fn test_otlp_proto_wire_encoder_decoding() {
        use crate::background::otlp_log_shipper::proto::{AnyValue, KeyValue, any_value};
        let kv = KeyValue {
            key: "request_id".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(
                    "4bf92f3577b34da6".to_string(),
                )),
            }),
        };
        let mut kv_bytes = Vec::new();
        kv.encode(&mut kv_bytes).unwrap();
        println!(
            "Prost KeyValue bytes (len {}): {:02x?}",
            kv_bytes.len(),
            kv_bytes
        );
        let envelope = OtlpProtoWireEnvelope::new("test-service");
        let rec = AccessLogRecord::from_scalars(
            0x4bf9_2f35_77b3_4da6,
            1_710_000_000,
            2500,
            "192.168.1.50:12345".parse().unwrap(),
            Some("10.0.0.10:8080".parse().unwrap()),
            100,
            200,
            200,
            1,
            0,
            LogProtocol::H1,
            LogProtocol::H1,
            PipeStrategy::Buffered,
            LogCompletionReason::Complete,
            LogHttpMethod::Get,
            LogGrpcStatus::NonGrpc,
            b"curl/8.6.0",
        );

        let batches = vec![Arc::from(vec![rec])];
        let mut records_buf = Vec::new();
        let mut out = Vec::new();

        let count = envelope.format_records_wire(&batches, &mut records_buf, &mut out);
        assert_eq!(count, 1);
        assert!(!out.is_empty());

        // Compare with Prost encoding of LogRecord
        let mut template = crate::background::otlp_log_shipper::proto::LogRecord::template();
        template.populate_from_record(&rec);
        let mut prost_rec_bytes = Vec::new();
        template.encode(&mut prost_rec_bytes).unwrap();

        let mut single_rec_buf = Vec::new();
        envelope.encode_log_record_wire_direct(&rec, &mut single_rec_buf);
        // Skip tag (0x12) and 2-byte varint length to decode the LogRecord message body
        let decoded_rec =
            crate::background::otlp_log_shipper::proto::LogRecord::decode(&single_rec_buf[3..])
                .expect("Valid single LogRecord");
        assert_eq!(decoded_rec.attributes.len(), 14);

        // Verify decoding via Prost Message::decode
        use crate::background::otlp_log_shipper::proto::ExportLogsServiceRequest;
        let decoded = ExportLogsServiceRequest::decode(&out[..]).expect("Valid protobuf");

        assert_eq!(decoded.resource_logs.len(), 1);
        let res_logs = &decoded.resource_logs[0];
        let res = res_logs.resource.as_ref().expect("Resource present");
        assert_eq!(res.attributes[0].key, "service.name");

        assert_eq!(res_logs.scope_logs.len(), 1);
        let scope_logs = &res_logs.scope_logs[0];
        let scope = scope_logs.scope.as_ref().expect("Scope present");
        assert_eq!(scope.name, "velda.access_log");

        assert_eq!(scope_logs.log_records.len(), 1);
        let log_rec = &scope_logs.log_records[0];
        assert_eq!(log_rec.time_unix_nano, 1_710_000_000_000_000_000);
        assert_eq!(log_rec.observed_time_unix_nano, 1_710_000_000_000_000_000);
        assert_eq!(log_rec.severity_number, 9);
        assert_eq!(log_rec.severity_text, "INFO");

        // Verify request_id attribute (slot 0)
        assert_eq!(log_rec.attributes.len(), 14);
        assert_eq!(log_rec.attributes[0].key, "request_id");
        if let Some(crate::background::otlp_log_shipper::proto::AnyValue {
            value:
                Some(crate::background::otlp_log_shipper::proto::any_value::Value::StringValue(ref s)),
        }) = log_rec.attributes[0].value
        {
            assert_eq!(s, "4bf92f3577b34da6");
        } else {
            panic!("Expected StringValue for request_id");
        }
    }
}
