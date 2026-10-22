//! OpenTelemetry ResourceMetrics Protobuf Message Models & Serializer.
//!
//! Encodes point-in-time [`MetricsSnapshot`] values from [`velda_observability::MetricsStore`]
//! into compliant OpenTelemetry binary Protocol Buffers matching:
//! `opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest`

use prost::Message;
use velda_observability::MetricsSnapshot;

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

/// Encodes a [`MetricsSnapshot`] into compliant OpenTelemetry binary Protobuf bytes.
pub fn format_otlp_metrics_proto(snapshot: &MetricsSnapshot, service_name: &str) -> Vec<u8> {
    let now_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);

    let mut metrics = Vec::new();

    // 1. velda.requests.total (Cumulative Sum)
    let mut request_datapoints = Vec::new();
    for p in &snapshot.pipelines {
        let downstream_str = p.downstream.as_str();
        let upstream_str = p.upstream.as_str();
        let pipe_str = p.pipe.as_str();

        let classes = [
            ("2xx", p.requests_2xx),
            ("3xx", p.requests_3xx),
            ("4xx", p.requests_4xx),
            ("5xx", p.requests_5xx),
            ("other", p.requests_other),
        ];

        for (class, count) in classes {
            request_datapoints.push(NumberDataPoint {
                attributes: vec![
                    KeyValue::new_string("downstream", downstream_str),
                    KeyValue::new_string("upstream", upstream_str),
                    KeyValue::new_string("pipe", pipe_str),
                    KeyValue::new_string("status_class", class),
                ],
                start_time_unix_nano: 0,
                time_unix_nano: now_nanos,
                value: Some(number_data_point::Value::AsInt(count as i64)),
                flags: 0,
            });
        }
    }

    metrics.push(Metric {
        name: "velda.requests.total".to_string(),
        description: "Total number of HTTP requests processed across pipeline dimensions"
            .to_string(),
        unit: "1".to_string(),
        data: Some(metric::Data::Sum(Sum {
            data_points: request_datapoints,
            aggregation_temporality: 2, // AGGREGATION_TEMPORALITY_CUMULATIVE
            is_monotonic: true,
        })),
    });

    // 2. velda.active_streams (Gauge)
    let mut active_datapoints = Vec::new();
    for p in &snapshot.pipelines {
        active_datapoints.push(NumberDataPoint {
            attributes: vec![
                KeyValue::new_string("downstream", p.downstream.as_str()),
                KeyValue::new_string("upstream", p.upstream.as_str()),
                KeyValue::new_string("pipe", p.pipe.as_str()),
            ],
            start_time_unix_nano: 0,
            time_unix_nano: now_nanos,
            value: Some(number_data_point::Value::AsInt(p.active_streams)),
            flags: 0,
        });
    }

    metrics.push(Metric {
        name: "velda.active_streams".to_string(),
        description: "Number of streams actively in-flight".to_string(),
        unit: "1".to_string(),
        data: Some(metric::Data::Gauge(Gauge {
            data_points: active_datapoints,
        })),
    });

    // 3. velda.bytes_received_total & velda.bytes_transmitted_total
    let mut rx_datapoints = Vec::new();
    let mut tx_datapoints = Vec::new();
    for p in &snapshot.pipelines {
        rx_datapoints.push(NumberDataPoint {
            attributes: vec![
                KeyValue::new_string("downstream", p.downstream.as_str()),
                KeyValue::new_string("upstream", p.upstream.as_str()),
            ],
            start_time_unix_nano: 0,
            time_unix_nano: now_nanos,
            value: Some(number_data_point::Value::AsInt(p.bytes_rx as i64)),
            flags: 0,
        });
        tx_datapoints.push(NumberDataPoint {
            attributes: vec![
                KeyValue::new_string("downstream", p.downstream.as_str()),
                KeyValue::new_string("upstream", p.upstream.as_str()),
            ],
            start_time_unix_nano: 0,
            time_unix_nano: now_nanos,
            value: Some(number_data_point::Value::AsInt(p.bytes_tx as i64)),
            flags: 0,
        });
    }

    metrics.push(Metric {
        name: "velda.bytes_received_total".to_string(),
        description: "Total bytes received from downstream clients".to_string(),
        unit: "By".to_string(),
        data: Some(metric::Data::Sum(Sum {
            data_points: rx_datapoints,
            aggregation_temporality: 2,
            is_monotonic: true,
        })),
    });

    metrics.push(Metric {
        name: "velda.bytes_transmitted_total".to_string(),
        description: "Total bytes transmitted to downstream clients".to_string(),
        unit: "By".to_string(),
        data: Some(metric::Data::Sum(Sum {
            data_points: tx_datapoints,
            aggregation_temporality: 2,
            is_monotonic: true,
        })),
    });

    let request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![KeyValue::new_string("service.name", service_name)],
                dropped_attributes_count: 0,
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: "velda-edge".to_string(),
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    attributes: Vec::new(),
                    dropped_attributes_count: 0,
                }),
                metrics,
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
    }
}
