//! OpenTelemetry ResourceSpans Protobuf Message Models & Serializer (`opentelemetry_tracer::proto`).
//!
//! Encodes slices of [`PackedOtlpSpan`] records into compliant OpenTelemetry binary Protocol Buffers:
//! `opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest`

use prost::Message;

use super::span::{OtlpSpanItem, PackedOtlpSpan, decode_hex};

/// Root export request for OpenTelemetry Trace Service.
#[derive(Clone, PartialEq, Message)]
pub struct ExportTraceServiceRequest {
    #[prost(message, repeated, tag = "1")]
    pub resource_spans: Vec<ResourceSpans>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ResourceSpans {
    #[prost(message, optional, tag = "1")]
    pub resource: Option<Resource>,
    #[prost(message, repeated, tag = "2")]
    pub scope_spans: Vec<ScopeSpans>,
    #[prost(string, tag = "3")]
    pub schema_url: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct Resource {
    #[prost(message, repeated, tag = "1")]
    pub attributes: Vec<KeyValue>,
    #[prost(uint32, tag = "2")]
    pub dropped_attributes_count: u32,
}

#[derive(Clone, PartialEq, Message)]
pub struct ScopeSpans {
    #[prost(message, optional, tag = "1")]
    pub scope: Option<InstrumentationScope>,
    #[prost(message, repeated, tag = "2")]
    pub spans: Vec<Span>,
    #[prost(string, tag = "3")]
    pub schema_url: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct InstrumentationScope {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub version: String,
    #[prost(message, repeated, tag = "3")]
    pub attributes: Vec<KeyValue>,
    #[prost(uint32, tag = "4")]
    pub dropped_attributes_count: u32,
}

#[derive(Clone, PartialEq, Message)]
pub struct Span {
    #[prost(bytes, tag = "1")]
    pub trace_id: Vec<u8>,
    #[prost(bytes, tag = "2")]
    pub span_id: Vec<u8>,
    #[prost(string, tag = "3")]
    pub trace_state: String,
    #[prost(bytes, tag = "4")]
    pub parent_span_id: Vec<u8>,
    #[prost(string, tag = "5")]
    pub name: String,
    #[prost(int32, tag = "6")]
    pub kind: i32,
    #[prost(fixed64, tag = "7")]
    pub start_time_unix_nano: u64,
    #[prost(fixed64, tag = "8")]
    pub end_time_unix_nano: u64,
    #[prost(message, repeated, tag = "9")]
    pub attributes: Vec<KeyValue>,
    #[prost(uint32, tag = "10")]
    pub dropped_attributes_count: u32,
    #[prost(message, optional, tag = "15")]
    pub status: Option<Status>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Status {
    #[prost(string, tag = "1")]
    pub message: String,
    #[prost(int32, tag = "2")]
    pub code: i32,
}

#[derive(Clone, PartialEq, Message)]
pub struct KeyValue {
    #[prost(string, tag = "1")]
    pub key: String,
    #[prost(message, optional, tag = "2")]
    pub value: Option<AnyValue>,
}

#[derive(Clone, PartialEq, Message)]
pub struct AnyValue {
    #[prost(oneof = "any_value::Value", tags = "1, 2, 3, 4")]
    pub value: Option<any_value::Value>,
}

pub mod any_value {
    use prost::Oneof;

    #[derive(Clone, PartialEq, Oneof)]
    pub enum Value {
        #[prost(string, tag = "1")]
        StringValue(String),
        #[prost(bool, tag = "2")]
        BoolValue(bool),
        #[prost(int64, tag = "3")]
        IntValue(i64),
        #[prost(double, tag = "4")]
        DoubleValue(f64),
    }
}

impl KeyValue {
    #[inline]
    pub fn new_string(key: impl Into<String>, val: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(val.into())),
            }),
        }
    }

    #[inline]
    pub fn new_int(key: impl Into<String>, val: i64) -> Self {
        Self {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::IntValue(val)),
            }),
        }
    }
}

/// Encodes a slice of [`PackedOtlpSpan`] into compliant binary Protobuf bytes.
pub fn format_packed_spans_proto(spans: &[PackedOtlpSpan], service_name: &str) -> Vec<u8> {
    let mut otlp_spans = Vec::with_capacity(spans.len());

    for item in spans {
        let status_code = if item.status_code < 400 { 1 } else { 2 };
        let method = item.method_str();
        let path = item.path();

        let mut attributes = vec![
            KeyValue::new_string("http.request.method", method),
            KeyValue::new_int("http.response.status_code", item.status_code as i64),
            KeyValue::new_string("url.path", path),
        ];

        if item.ip_kind != 0 {
            let mut ip_str = String::with_capacity(40);
            item.format_client_ip(&mut ip_str);
            attributes.push(KeyValue::new_string("client.address", ip_str));
        }

        let route = item.route_id();
        if !route.is_empty() {
            attributes.push(KeyValue::new_string("http.route", route));
        }

        let upstream = item.upstream();
        if !upstream.is_empty() {
            attributes.push(KeyValue::new_string("network.peer.address", upstream));
        }

        let parent_span_id = if item.has_parent == 1 {
            item.parent_span_id.to_vec()
        } else {
            Vec::new()
        };

        otlp_spans.push(Span {
            trace_id: item.trace_id.to_vec(),
            span_id: item.span_id.to_vec(),
            trace_state: String::new(),
            parent_span_id,
            name: format!("{method} {path}"),
            kind: 2, // SPAN_KIND_SERVER
            start_time_unix_nano: item.start_time_unix_nano,
            end_time_unix_nano: item.end_time_unix_nano,
            attributes,
            dropped_attributes_count: 0,
            status: Some(Status {
                message: String::new(),
                code: status_code,
            }),
        });
    }

    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue::new_string("service.name", service_name)],
                dropped_attributes_count: 0,
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "velda-edge".to_string(),
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    attributes: Vec::new(),
                    dropped_attributes_count: 0,
                }),
                spans: otlp_spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    let mut buf = Vec::with_capacity(request.encoded_len());
    request
        .encode(&mut buf)
        .expect("protobuf encoding is infallible");
    buf
}

/// Encodes a slice of legacy [`OtlpSpanItem`] records into compliant binary Protobuf bytes.
///
/// Retained for test backward-compatibility.
pub fn format_otlp_spans_proto(spans: &[OtlpSpanItem], service_name: &str) -> Vec<u8> {
    let mut packed_list = Vec::with_capacity(spans.len());
    for s in spans {
        let mut p = PackedOtlpSpan::empty();
        p.start_time_unix_nano = s.start_time_unix_nano as u64;
        p.end_time_unix_nano = s.end_time_unix_nano as u64;
        p.status_code = s.status_code;

        if let Ok(method) = s.method.parse::<http::Method>() {
            p.method = PackedOtlpSpan::encode_method(&method);
        }
        p.set_path(s.path.as_bytes());

        if let Some(ip) = &s.client_ip
            && let Ok(addr) = ip.parse::<std::net::IpAddr>()
        {
            p.set_client_ip(addr);
        }
        if let Some(route) = &s.route_id {
            p.set_route_id(route.as_bytes());
        }
        if let Some(up) = &s.upstream {
            p.set_upstream(up.as_bytes());
        }

        let mut trace_raw = [0u8; 16];
        if decode_hex(s.trace_id.as_bytes(), &mut trace_raw) == 16 {
            p.set_trace_id(&trace_raw);
        }

        let mut span_raw = [0u8; 8];
        if decode_hex(s.span_id.as_bytes(), &mut span_raw) == 8 {
            p.set_span_id(&span_raw);
        }

        if let Some(parent) = &s.parent_span_id {
            let mut parent_raw = [0u8; 8];
            if decode_hex(parent.as_bytes(), &mut parent_raw) == 8 {
                p.set_parent_span_id(&parent_raw);
            }
        }

        packed_list.push(p);
    }

    format_packed_spans_proto(&packed_list, service_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_otlp_spans_proto() {
        let span = OtlpSpanItem {
            trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".to_string(),
            span_id: "00f067aa0ba902b7".to_string(),
            parent_span_id: Some("5fb397be34d23b0f".to_string()),
            name: "GET /api/v1/checkout".to_string(),
            start_time_unix_nano: 1729579200000000000,
            end_time_unix_nano: 1729579200005000000,
            status_code: 200,
            method: "GET".to_string(),
            path: "/api/v1/checkout".to_string(),
            route_id: Some("checkout-route".to_string()),
            client_ip: Some("127.0.0.1".to_string()),
            upstream: Some("10.0.0.8:8080".to_string()),
        };

        let bytes = format_otlp_spans_proto(&[span], "checkout-service");
        assert!(!bytes.is_empty());

        let decoded = ExportTraceServiceRequest::decode(&bytes[..]).expect("Must decode");
        assert_eq!(decoded.resource_spans.len(), 1);
        let s = &decoded.resource_spans[0].scope_spans[0].spans[0];
        assert_eq!(s.name, "GET /api/v1/checkout");
        assert_eq!(s.trace_id.len(), 16);
        assert_eq!(s.span_id.len(), 8);
        assert_eq!(s.parent_span_id.len(), 8);
        assert_eq!(s.kind, 2);
    }
}
