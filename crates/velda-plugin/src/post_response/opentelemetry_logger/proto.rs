//! OpenTelemetry ResourceLogs Protobuf Message Models & Serializer (`opentelemetry_logger`).
//!
//! Encodes slices of [`OtlpLogItem`] records into compliant OpenTelemetry binary Protocol Buffers:
//! `opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest`

use prost::Message;

use super::record::OtlpLogItem;

/// Root export request for OpenTelemetry Logs Service.
#[derive(Clone, PartialEq, Message)]
pub struct ExportLogsServiceRequest {
    #[prost(message, repeated, tag = "1")]
    pub resource_logs: Vec<ResourceLogs>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ResourceLogs {
    #[prost(message, optional, tag = "1")]
    pub resource: Option<Resource>,
    #[prost(message, repeated, tag = "2")]
    pub scope_logs: Vec<ScopeLogs>,
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
pub struct ScopeLogs {
    #[prost(message, optional, tag = "1")]
    pub scope: Option<InstrumentationScope>,
    #[prost(message, repeated, tag = "2")]
    pub log_records: Vec<LogRecord>,
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
pub struct LogRecord {
    #[prost(fixed64, tag = "1")]
    pub time_unix_nano: u64,
    #[prost(fixed64, tag = "2")]
    pub observed_time_unix_nano: u64,
    #[prost(int32, tag = "3")]
    pub severity_number: i32,
    #[prost(string, tag = "4")]
    pub severity_text: String,
    #[prost(message, optional, tag = "5")]
    pub body: Option<AnyValue>,
    #[prost(message, repeated, tag = "6")]
    pub attributes: Vec<KeyValue>,
    #[prost(uint32, tag = "7")]
    pub dropped_attributes_count: u32,
    #[prost(uint32, tag = "8")]
    pub flags: u32,
    #[prost(bytes, tag = "9")]
    pub trace_id: Vec<u8>,
    #[prost(bytes, tag = "10")]
    pub span_id: Vec<u8>,
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

    #[inline]
    pub fn new_double(key: impl Into<String>, val: f64) -> Self {
        Self {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::DoubleValue(val)),
            }),
        }
    }
}

fn decode_hex(hex: &str, expected_len: usize) -> Vec<u8> {
    if hex.len() == expected_len * 2 {
        let mut out = Vec::with_capacity(expected_len);
        for i in 0..expected_len {
            if let Ok(b) = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16) {
                out.push(b);
            } else {
                return Vec::new();
            }
        }
        out
    } else {
        Vec::new()
    }
}

/// Encodes a slice of [`OtlpLogItem`] records into compliant OpenTelemetry binary Protobuf bytes.
pub fn format_otlp_logs_proto(records: &[OtlpLogItem], service_name: &str) -> Vec<u8> {
    let mut log_records = Vec::with_capacity(records.len());

    for item in records {
        let (severity_number, severity_text) = item.severity();
        let body_str = format!("{} {} -> {}", item.method, item.path, item.status);

        let mut attributes = vec![
            KeyValue::new_string("http.request.method", &item.method),
            KeyValue::new_int("http.response.status_code", item.status as i64),
            KeyValue::new_string("url.path", &item.path),
        ];

        if let Some(ip) = &item.client_ip {
            attributes.push(KeyValue::new_string("client.address", ip));
        }

        if let Some(route) = &item.route_id {
            attributes.push(KeyValue::new_string("http.route", route));
        }

        if let Some(upstream) = &item.upstream {
            attributes.push(KeyValue::new_string("network.peer.address", upstream));
        }

        if let Some(lat) = item.latency_ms {
            attributes.push(KeyValue::new_double("http.server.request.duration_ms", lat));
        }

        let trace_id_bytes = item
            .trace_id
            .as_deref()
            .map(|s| decode_hex(s, 16))
            .unwrap_or_default();
        let span_id_bytes = item
            .span_id
            .as_deref()
            .map(|s| decode_hex(s, 8))
            .unwrap_or_default();

        log_records.push(LogRecord {
            time_unix_nano: item.time_unix_nano as u64,
            observed_time_unix_nano: item.time_unix_nano as u64,
            severity_number: severity_number as i32,
            severity_text: severity_text.to_string(),
            body: Some(AnyValue {
                value: Some(any_value::Value::StringValue(body_str)),
            }),
            attributes,
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: trace_id_bytes,
            span_id: span_id_bytes,
        });
    }

    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue::new_string("service.name", service_name)],
                dropped_attributes_count: 0,
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "velda-edge".to_string(),
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    attributes: Vec::new(),
                    dropped_attributes_count: 0,
                }),
                log_records,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_otlp_logs_proto() {
        let item = OtlpLogItem {
            time_unix_nano: 1729579200000000000,
            status: 200,
            method: "GET".to_string(),
            path: "/api/v1/orders".to_string(),
            client_ip: Some("192.168.1.100".to_string()),
            route_id: Some("orders-route".to_string()),
            upstream: Some("10.0.0.5:8080".to_string()),
            latency_ms: Some(1.25),
            trace_id: Some("4bf92f3577b34da6a3ce929d0e0e4736".to_string()),
            span_id: Some("00f067aa0ba902b7".to_string()),
        };

        let bytes = format_otlp_logs_proto(&[item], "order-service");
        assert!(!bytes.is_empty());

        let decoded = ExportLogsServiceRequest::decode(&bytes[..]).expect("Must decode");
        assert_eq!(decoded.resource_logs.len(), 1);
        let lr = &decoded.resource_logs[0].scope_logs[0].log_records[0];
        assert_eq!(lr.severity_number, 9);
        assert_eq!(lr.severity_text, "INFO");
        assert_eq!(lr.trace_id.len(), 16);
        assert_eq!(lr.span_id.len(), 8);
    }
}
