//! OpenTelemetry ResourceMetrics Protobuf Message Models & Serializer.
//!
//! Encodes point-in-time [`MetricsSnapshot`] values from [`velda_observability::MetricsStore`]
//! into compliant OpenTelemetry binary Protocol Buffers matching:
//! `opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest`

use prost::Message;
use velda_observability::{MetricsSnapshot, MetricsStore};

/// Root export request for OpenTelemetry Metrics Service.
#[derive(Clone, PartialEq, Message)]
pub struct ExportMetricsServiceRequest {
    #[prost(message, repeated, tag = "1")]
    pub resource_metrics: Vec<ResourceMetrics>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ResourceMetrics {
    #[prost(message, optional, tag = "1")]
    pub resource: Option<Resource>,
    #[prost(message, repeated, tag = "2")]
    pub scope_metrics: Vec<ScopeMetrics>,
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
pub struct ScopeMetrics {
    #[prost(message, optional, tag = "1")]
    pub scope: Option<InstrumentationScope>,
    #[prost(message, repeated, tag = "2")]
    pub metrics: Vec<Metric>,
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
pub struct Metric {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub description: String,
    #[prost(string, tag = "3")]
    pub unit: String,
    #[prost(oneof = "metric::Data", tags = "5, 7")]
    pub data: Option<metric::Data>,
}

pub mod metric {
    use prost::Oneof;
    #[derive(Clone, PartialEq, Oneof)]
    pub enum Data {
        #[prost(message, tag = "5")]
        Gauge(super::Gauge),
        #[prost(message, tag = "7")]
        Sum(super::Sum),
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct Gauge {
    #[prost(message, repeated, tag = "1")]
    pub data_points: Vec<NumberDataPoint>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Sum {
    #[prost(message, repeated, tag = "1")]
    pub data_points: Vec<NumberDataPoint>,
    #[prost(int32, tag = "2")]
    pub aggregation_temporality: i32,
    #[prost(bool, tag = "3")]
    pub is_monotonic: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct NumberDataPoint {
    #[prost(message, repeated, tag = "1")]
    pub attributes: Vec<KeyValue>,
    #[prost(fixed64, tag = "2")]
    pub start_time_unix_nano: u64,
    #[prost(fixed64, tag = "3")]
    pub time_unix_nano: u64,
    #[prost(oneof = "number_data_point::Value", tags = "4, 6")]
    pub value: Option<number_data_point::Value>,
    #[prost(uint32, tag = "8")]
    pub flags: u32,
}

pub mod number_data_point {
    use prost::Oneof;
    #[derive(Clone, PartialEq, Oneof)]
    pub enum Value {
        #[prost(double, tag = "4")]
        AsDouble(f64),
        #[prost(sfixed64, tag = "6")]
        AsInt(i64),
    }
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
}

/// Pre-allocated, zero-heap-churn template for OTLP Protobuf metrics export.
///
/// Pre-instantiates the full `ExportMetricsServiceRequest` AST structure
/// (including all Metric descriptors, Scopes, Resources, and all DataPoints with their
/// static KeyValue attributes) once at initialization.
///
/// On every export cycle, `update_and_encode` walks the pre-allocated DataPoints,
/// updates `time_unix_nano` and `value` in-place, and serializes directly into
/// a reusable `Vec<u8>` buffer without a single heap allocation.
pub struct OtlpProtoTemplate {
    request: ExportMetricsServiceRequest,
}

impl OtlpProtoTemplate {
    /// Creates a new template pre-populated with metric definitions and static dimension attributes.
    pub fn new(service_name: &str) -> Self {
        // Build Metric 0: velda.requests.total
        let mut request_datapoints = Vec::with_capacity(36 * 5);
        for slot_idx in 0..36 {
            let (down, up, pipe) = MetricsStore::decode_slot_index(slot_idx);
            let downstream_str = down.as_str();
            let upstream_str = up.as_str();
            let pipe_str = pipe.as_str();

            let classes = ["2xx", "3xx", "4xx", "5xx", "other"];
            for class in classes {
                request_datapoints.push(NumberDataPoint {
                    attributes: vec![
                        KeyValue::new_string("downstream", downstream_str),
                        KeyValue::new_string("upstream", upstream_str),
                        KeyValue::new_string("pipe", pipe_str),
                        KeyValue::new_string("status_class", class),
                    ],
                    start_time_unix_nano: 0,
                    time_unix_nano: 0,
                    value: Some(number_data_point::Value::AsInt(0)),
                    flags: 0,
                });
            }
        }

        let metric_requests = Metric {
            name: "velda.requests.total".to_string(),
            description: "Total number of HTTP requests processed across pipeline dimensions"
                .to_string(),
            unit: "1".to_string(),
            data: Some(metric::Data::Sum(Sum {
                data_points: request_datapoints,
                aggregation_temporality: 2, // AGGREGATION_TEMPORALITY_CUMULATIVE
                is_monotonic: true,
            })),
        };

        // Build Metric 1: velda.active_streams
        let mut active_datapoints = Vec::with_capacity(36);
        for slot_idx in 0..36 {
            let (down, up, pipe) = MetricsStore::decode_slot_index(slot_idx);
            active_datapoints.push(NumberDataPoint {
                attributes: vec![
                    KeyValue::new_string("downstream", down.as_str()),
                    KeyValue::new_string("upstream", up.as_str()),
                    KeyValue::new_string("pipe", pipe.as_str()),
                ],
                start_time_unix_nano: 0,
                time_unix_nano: 0,
                value: Some(number_data_point::Value::AsInt(0)),
                flags: 0,
            });
        }

        let metric_active = Metric {
            name: "velda.active_streams".to_string(),
            description: "Number of streams actively in-flight".to_string(),
            unit: "1".to_string(),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: active_datapoints,
            })),
        };

        // Build Metric 2: velda.bytes_received_total & Metric 3: velda.bytes_transmitted_total
        let mut rx_datapoints = Vec::with_capacity(36);
        let mut tx_datapoints = Vec::with_capacity(36);
        for slot_idx in 0..36 {
            let (down, up, _pipe) = MetricsStore::decode_slot_index(slot_idx);
            rx_datapoints.push(NumberDataPoint {
                attributes: vec![
                    KeyValue::new_string("downstream", down.as_str()),
                    KeyValue::new_string("upstream", up.as_str()),
                ],
                start_time_unix_nano: 0,
                time_unix_nano: 0,
                value: Some(number_data_point::Value::AsInt(0)),
                flags: 0,
            });
            tx_datapoints.push(NumberDataPoint {
                attributes: vec![
                    KeyValue::new_string("downstream", down.as_str()),
                    KeyValue::new_string("upstream", up.as_str()),
                ],
                start_time_unix_nano: 0,
                time_unix_nano: 0,
                value: Some(number_data_point::Value::AsInt(0)),
                flags: 0,
            });
        }

        let metric_rx = Metric {
            name: "velda.bytes_received_total".to_string(),
            description: "Total bytes received from downstream clients".to_string(),
            unit: "By".to_string(),
            data: Some(metric::Data::Sum(Sum {
                data_points: rx_datapoints,
                aggregation_temporality: 2,
                is_monotonic: true,
            })),
        };

        let metric_tx = Metric {
            name: "velda.bytes_transmitted_total".to_string(),
            description: "Total bytes transmitted to downstream clients".to_string(),
            unit: "By".to_string(),
            data: Some(metric::Data::Sum(Sum {
                data_points: tx_datapoints,
                aggregation_temporality: 2,
                is_monotonic: true,
            })),
        };

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![
                        KeyValue::new_string("service.name", service_name),
                        KeyValue::new_string("telemetry.sdk.name", "velda-edge"),
                        KeyValue::new_string("telemetry.sdk.language", "rust"),
                    ],
                    dropped_attributes_count: 0,
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "velda-edge".to_string(),
                        version: env!("CARGO_PKG_VERSION").to_string(),
                        attributes: Vec::new(),
                        dropped_attributes_count: 0,
                    }),
                    metrics: vec![metric_requests, metric_active, metric_rx, metric_tx],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        Self { request }
    }

    /// In-place updates all metric data points from `snapshot` and encodes into `buf` with zero heap allocations.
    pub fn update_and_encode(&mut self, snapshot: &MetricsSnapshot, buf: &mut Vec<u8>) {
        let now_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        let metrics = &mut self.request.resource_metrics[0].scope_metrics[0].metrics;

        // 1. velda.requests.total
        if let Some(metric::Data::Sum(ref mut sum)) = metrics[0].data {
            let mut dp_idx = 0;
            for p in &snapshot.pipelines {
                let classes = [
                    p.requests_2xx,
                    p.requests_3xx,
                    p.requests_4xx,
                    p.requests_5xx,
                    p.requests_other,
                ];
                for &count in &classes {
                    if let Some(dp) = sum.data_points.get_mut(dp_idx) {
                        dp.time_unix_nano = now_nanos;
                        dp.value = Some(number_data_point::Value::AsInt(count as i64));
                    }
                    dp_idx += 1;
                }
            }
        }

        // 2. velda.active_streams
        if let Some(metric::Data::Gauge(ref mut gauge)) = metrics[1].data {
            for (i, p) in snapshot.pipelines.iter().enumerate() {
                if let Some(dp) = gauge.data_points.get_mut(i) {
                    dp.time_unix_nano = now_nanos;
                    dp.value = Some(number_data_point::Value::AsInt(p.active_streams));
                }
            }
        }

        // 3. velda.bytes_received_total
        if let Some(metric::Data::Sum(ref mut sum)) = metrics[2].data {
            for (i, p) in snapshot.pipelines.iter().enumerate() {
                if let Some(dp) = sum.data_points.get_mut(i) {
                    dp.time_unix_nano = now_nanos;
                    dp.value = Some(number_data_point::Value::AsInt(p.bytes_rx as i64));
                }
            }
        }

        // 4. velda.bytes_transmitted_total
        if let Some(metric::Data::Sum(ref mut sum)) = metrics[3].data {
            for (i, p) in snapshot.pipelines.iter().enumerate() {
                if let Some(dp) = sum.data_points.get_mut(i) {
                    dp.time_unix_nano = now_nanos;
                    dp.value = Some(number_data_point::Value::AsInt(p.bytes_tx as i64));
                }
            }
        }

        buf.clear();
        self.request
            .encode(buf)
            .expect("protobuf encoding is infallible");
    }
}

/// Encodes a [`MetricsSnapshot`] into compliant OpenTelemetry binary Protobuf bytes.
pub fn format_otlp_metrics_proto(snapshot: &MetricsSnapshot, service_name: &str) -> Vec<u8> {
    let mut template = OtlpProtoTemplate::new(service_name);
    let mut buf = Vec::with_capacity(32 * 1024);
    template.update_and_encode(snapshot, &mut buf);
    buf
}

/// In-place updates an existing [`OtlpProtoTemplate`] and encodes into `buf` with zero allocations.
pub fn format_otlp_metrics_proto_into(
    template: &mut OtlpProtoTemplate,
    snapshot: &MetricsSnapshot,
    buf: &mut Vec<u8>,
) {
    template.update_and_encode(snapshot, buf);
}

#[cfg(test)]
mod tests {
    use super::*;
    use velda_observability::MetricsStore;

    #[test]
    fn test_format_otlp_metrics_proto() {
        let store = MetricsStore::new();
        let snapshot = store.snapshot();
        let bytes = format_otlp_metrics_proto(&snapshot, "test-service");
        assert!(!bytes.is_empty(), "Protobuf bytes must not be empty");

        // Verify round-trip decoding
        let decoded =
            ExportMetricsServiceRequest::decode(&bytes[..]).expect("Must decode successfully");
        assert_eq!(decoded.resource_metrics.len(), 1);
        let rm = &decoded.resource_metrics[0];
        assert_eq!(
            rm.resource.as_ref().unwrap().attributes[0].key,
            "service.name"
        );
        assert_eq!(
            rm.resource.as_ref().unwrap().attributes[0]
                .value
                .as_ref()
                .unwrap()
                .value,
            Some(any_value::Value::StringValue("test-service".to_string()))
        );

        let metrics = &rm.scope_metrics[0].metrics;
        assert_eq!(metrics.len(), 4);
        assert_eq!(metrics[0].name, "velda.requests.total");
        assert_eq!(metrics[1].name, "velda.active_streams");
        assert_eq!(metrics[2].name, "velda.bytes_received_total");
        assert_eq!(metrics[3].name, "velda.bytes_transmitted_total");
    }

    #[test]
    fn test_proto_template_reuse() {
        let store = MetricsStore::new();
        let mut template = OtlpProtoTemplate::new("reuse-service");
        let mut buf = Vec::with_capacity(32 * 1024);

        let snap1 = store.snapshot();
        template.update_and_encode(&snap1, &mut buf);
        assert!(!buf.is_empty());
        let len1 = buf.len();

        let snap2 = store.snapshot();
        template.update_and_encode(&snap2, &mut buf);
        assert_eq!(buf.len(), len1);
    }
}
