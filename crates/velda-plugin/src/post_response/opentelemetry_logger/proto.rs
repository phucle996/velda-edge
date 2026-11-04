//! OpenTelemetry ResourceLogs Protobuf Message Models & Serializer (`opentelemetry_logger::proto`).
//!
//! Encodes slices of [`PackedOtlpRecord`] into compliant OpenTelemetry binary Protocol Buffers:
//! `opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest`

use prost::Message;

use super::record::PackedOtlpRecord;

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

/// Encodes a slice of [`PackedOtlpRecord`] directly into Protobuf wire bytes without intermediate string allocations.
pub fn format_otlp_records_proto(records: &[PackedOtlpRecord], service_name: &str) -> Vec<u8> {
    let mut log_records = Vec::with_capacity(records.len());
    let mut ip_buf = String::with_capacity(40);

    for item in records {
        let (severity_number, severity_text) = item.severity();
        let method = item.method_str();
        let path = item.path();
        let body_str = format!("{} {} -> {}", method, path, item.status);

        let mut attributes = vec![
            KeyValue::new_string("http.request.method", method),
            KeyValue::new_int("http.response.status_code", item.status as i64),
            KeyValue::new_string("url.path", path),
        ];

        if item.ip_kind != 0 {
            ip_buf.clear();
            item.format_client_ip(&mut ip_buf);
            attributes.push(KeyValue::new_string("client.address", ip_buf.as_str()));
        }

        let route = item.route_id();
        if !route.is_empty() {
            attributes.push(KeyValue::new_string("http.route", route));
        }

        let up = item.upstream();
        if !up.is_empty() {
            attributes.push(KeyValue::new_string("network.peer.address", up));
        }

        if item.duration_us > 0 {
            let lat_ms = item.duration_us as f64 / 1000.0;
            attributes.push(KeyValue::new_double(
                "http.server.request.duration_ms",
                lat_ms,
            ));
        }

        let trace_id_bytes = item.trace_id().to_vec();
        let span_id_bytes = item.span_id().to_vec();

        log_records.push(LogRecord {
            time_unix_nano: item.time_unix_nano,
            observed_time_unix_nano: item.time_unix_nano,
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
