//! OpenTelemetry ResourceLogs Protobuf Models & Serializer (`otlp_log_shipper::proto`).
//!
//! Encodes slices of reference-counted 64-byte [`AccessLogRecord`] into compliant OpenTelemetry binary Protocol Buffers:
//! `opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest`

use prost::Message;
use std::sync::Arc;
use velda_observability::{
    AccessLogRecord, LogGrpcStatus, LogHttpMethod, LogProtocol, UaFamily, UaOs,
};

use super::format::{fast_write_hex16, fast_write_ip128, fast_write_u64};

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
    #[prost(bytes = "vec", tag = "9")]
    pub trace_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "10")]
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
    #[prost(oneof = "any_value::Value", tags = "1, 2, 3, 4, 5, 6, 7")]
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
        #[prost(bytes = "vec", tag = "5")]
        BytesValue(Vec<u8>),
    }
}

impl KeyValue {
    #[inline]
    pub fn string(key: impl Into<String>, val: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(val.into())),
            }),
        }
    }

    #[inline]
    pub fn int(key: impl Into<String>, val: i64) -> Self {
        Self {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::IntValue(val)),
            }),
        }
    }
}

impl LogRecord {
    /// Pre-allocated template with capacity for all static fields and attributes.
    pub fn template() -> Self {
        let trace_id = Vec::new();

        let attributes = vec![
            KeyValue::string("request_id", String::with_capacity(16)),
            KeyValue::string("network.protocol.name", String::with_capacity(16)),
            KeyValue::string("http.request.method", String::with_capacity(16)),
            KeyValue::int("http.response.status_code", 0),
            KeyValue::string("rpc.grpc.status_code", String::with_capacity(16)),
            KeyValue::int("duration_us", 0),
            KeyValue::int("http.response.body.size", 0),
            KeyValue::int("http.request.body.size", 0),
            KeyValue::int("velda.route_id", 0),
            KeyValue::string("client.address", String::with_capacity(48)),
            KeyValue::int("client.port", 0),
            KeyValue::string("user_agent.family", String::with_capacity(32)),
            KeyValue::int("user_agent.major_version", 0),
            KeyValue::string("user_agent.os", String::with_capacity(32)),
        ];

        Self {
            time_unix_nano: 0,
            observed_time_unix_nano: 0,
            severity_number: 9,
            severity_text: String::with_capacity(8),
            body: Some(AnyValue {
                value: Some(any_value::Value::StringValue(String::with_capacity(128))),
            }),
            attributes,
            dropped_attributes_count: 0,
            flags: 0,
            trace_id,
            span_id: Vec::new(),
        }
    }

    /// Mutates this template in-place directly from a 64-byte [`AccessLogRecord`] with 0 heap allocations.
    #[inline(always)]
    pub fn populate_from_record(&mut self, rec: &AccessLogRecord) {
        let proto = LogProtocol::from_u8(rec.downstream_proto);
        let method = LogHttpMethod::from_u8(rec.method);
        let grpc_st = LogGrpcStatus::from_u8(rec.grpc_status);
        let ua_fam = UaFamily::from_u8(rec.ua_family);
        let ua_os = UaOs::from_u8(rec.ua_os);
        let ts_nano = (rec.timestamp_sec as u64).saturating_mul(1_000_000_000);

        let (severity_num, severity_txt) = if rec.status_code >= 500
            || (grpc_st != LogGrpcStatus::Ok && grpc_st != LogGrpcStatus::NonGrpc)
        {
            (17, "ERROR")
        } else if rec.status_code >= 400 {
            (13, "WARN")
        } else {
            (9, "INFO")
        };

        #[cfg(target_arch = "x86_64")]
        unsafe {
            use core::arch::x86_64::*;
            let ts_vec = _mm_set1_epi64x(ts_nano as i64);
            _mm_storeu_si128(&mut self.time_unix_nano as *mut u64 as *mut __m128i, ts_vec);
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            self.time_unix_nano = ts_nano;
            self.observed_time_unix_nano = ts_nano;
        }
        self.severity_number = severity_num;

        self.severity_text.clear();
        self.severity_text.push_str(severity_txt);

        // Body formatting in-place using LUT and static chunks (0 heap allocations)
        if let Some(AnyValue {
            value: Some(any_value::Value::StringValue(ref mut s)),
        }) = self.body
        {
            let bytes = unsafe { s.as_mut_vec() };
            bytes.clear();
            bytes.extend_from_slice(b"request_id=");
            fast_write_hex16(bytes, rec.request_id);
            bytes.push(b' ');
            bytes.extend_from_slice(method.as_str().as_bytes());
            bytes.push(b' ');
            bytes.extend_from_slice(proto.as_str().as_bytes());
            bytes.extend_from_slice(b" status=");
            fast_write_u64(bytes, rec.status_code as u64);
            bytes.extend_from_slice(b" dur_us=");
            fast_write_u64(bytes, rec.duration_us as u64);
            bytes.extend_from_slice(b" peer=");
            fast_write_ip128(bytes, rec.peer_ip);
            bytes.push(b':');
            fast_write_u64(bytes, rec.peer_port as u64);
        }

        // Trace ID remains empty (not spoofed with request_id)
        self.trace_id.clear();

        // Multi-offset parallel store coalescing across 14 fixed attribute slots:
        if let [a_req, a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12] =
            &mut self.attributes[0..14]
        {
            // request_id attribute (hex16 string):
            if let Some(AnyValue {
                value: Some(any_value::Value::StringValue(s_req)),
            }) = &mut a_req.value
            {
                let bytes = unsafe { s_req.as_mut_vec() };
                bytes.clear();
                fast_write_hex16(bytes, rec.request_id);
            }

            // Coalesce 4 contiguous int attributes (duration_us, bytes_tx, bytes_rx, route_id):
            if let (
                Some(AnyValue {
                    value: Some(any_value::Value::IntValue(v4)),
                }),
                Some(AnyValue {
                    value: Some(any_value::Value::IntValue(v5)),
                }),
                Some(AnyValue {
                    value: Some(any_value::Value::IntValue(v6)),
                }),
                Some(AnyValue {
                    value: Some(any_value::Value::IntValue(v7)),
                }),
            ) = (&mut a4.value, &mut a5.value, &mut a6.value, &mut a7.value)
            {
                *v4 = rec.duration_us as i64;
                *v5 = rec.bytes_tx as i64;
                *v6 = rec.bytes_rx as i64;
                *v7 = rec.route_id as i64;
            }

            // Coalesce remaining 3 int attributes (status_code, peer_port, ua_major_version):
            if let (
                Some(AnyValue {
                    value: Some(any_value::Value::IntValue(v2)),
                }),
                Some(AnyValue {
                    value: Some(any_value::Value::IntValue(v9)),
                }),
                Some(AnyValue {
                    value: Some(any_value::Value::IntValue(v11)),
                }),
            ) = (&mut a2.value, &mut a9.value, &mut a11.value)
            {
                *v2 = rec.status_code as i64;
                *v9 = rec.peer_port as i64;
                *v11 = rec.ua_major_version as i64;
            }

            // Coalesce string attributes without branches:
            if let (
                Some(AnyValue {
                    value: Some(any_value::Value::StringValue(s0)),
                }),
                Some(AnyValue {
                    value: Some(any_value::Value::StringValue(s1)),
                }),
                Some(AnyValue {
                    value: Some(any_value::Value::StringValue(s3)),
                }),
                Some(AnyValue {
                    value: Some(any_value::Value::StringValue(s10)),
                }),
                Some(AnyValue {
                    value: Some(any_value::Value::StringValue(s12)),
                }),
            ) = (
                &mut a0.value,
                &mut a1.value,
                &mut a3.value,
                &mut a10.value,
                &mut a12.value,
            ) {
                s0.clear();
                s0.push_str(proto.as_str());
                s1.clear();
                s1.push_str(method.as_str());
                s3.clear();
                s3.push_str(grpc_st.as_str());
                s10.clear();
                s10.push_str(ua_fam.as_str());
                s12.clear();
                s12.push_str(ua_os.as_str());
            }

            // Client IP:
            if let Some(AnyValue {
                value: Some(any_value::Value::StringValue(s8)),
            }) = &mut a8.value
            {
                let bytes = unsafe { s8.as_mut_vec() };
                bytes.clear();
                fast_write_ip128(bytes, rec.peer_ip);
            }
        }
    }
}

/// Pre-baked reusable Protobuf envelope for OpenTelemetry logs export.
/// Eliminates heap allocations by pre-allocating the request hierarchy,
/// resource descriptors, scope descriptors, and a reusable pool of [`LogRecord`] templates.
#[derive(Clone, Debug)]
pub struct OtlpProtoEnvelope {
    pub request: ExportLogsServiceRequest,
    record_pool: Vec<LogRecord>,
}

impl OtlpProtoEnvelope {
    /// Constructs a new envelope pre-baking static resource and scope headers.
    pub fn new(service_name: &str) -> Self {
        let resource = Resource {
            attributes: vec![
                KeyValue::string("service.name", service_name),
                KeyValue::string("telemetry.sdk.name", "velda-edge"),
                KeyValue::string("telemetry.sdk.language", "rust"),
            ],
            dropped_attributes_count: 0,
        };

        let scope_logs = ScopeLogs {
            scope: Some(InstrumentationScope {
                name: "velda.access_log".to_string(),
                version: "0.1.0".to_string(),
                attributes: Vec::new(),
                dropped_attributes_count: 0,
            }),
            log_records: Vec::with_capacity(1024),
            schema_url: String::new(),
        };

        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(resource),
                scope_logs: vec![scope_logs],
                schema_url: String::new(),
            }],
        };

        let mut record_pool = Vec::with_capacity(1024);
        for _ in 0..128 {
            record_pool.push(LogRecord::template());
        }

        Self {
            request,
            record_pool,
        }
    }

    /// Formats batches into the pre-allocated request hierarchy without heap reallocations.
    pub fn format_records(
        &mut self,
        batches: &[Arc<[AccessLogRecord]>],
    ) -> (&ExportLogsServiceRequest, usize) {
        let active_records = &mut self.request.resource_logs[0].scope_logs[0].log_records;
        // Return previous batch records back to the pool without deallocation
        self.record_pool.append(active_records);

        let total_needed: usize = batches.iter().map(|b| b.len()).sum();
        while self.record_pool.len() < total_needed {
            self.record_pool.push(LogRecord::template());
        }

        let mut count = 0;
        for batch in batches {
            for rec in batch.iter() {
                let mut log_rec = self.record_pool.pop().unwrap();
                log_rec.populate_from_record(rec);
                active_records.push(log_rec);
                count += 1;
            }
        }

        (&self.request, count)
    }
}

/// Encodes reference-counted 64-byte [`AccessLogRecord`] batches into an OTLP Protobuf request.
pub fn format_otlp_records_proto(
    batches: &[Arc<[AccessLogRecord]>],
    service_name: &str,
) -> (ExportLogsServiceRequest, usize) {
    let mut envelope = OtlpProtoEnvelope::new(service_name);
    let (_, count) = envelope.format_records(batches);
    (envelope.request, count)
}
