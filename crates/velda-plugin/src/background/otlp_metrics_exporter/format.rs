//! OpenTelemetry ResourceMetrics JSON Formatter (`otlp_metrics_exporter`).
//!
//! Encodes point-in-time [`MetricsSnapshot`] values from [`velda_observability::MetricsStore`]
//! into compliant OpenTelemetry ResourceMetrics JSON payloads (`POST /v1/metrics`).

use std::time::{SystemTime, UNIX_EPOCH};

use velda_observability::MetricsSnapshot;

/// Encodes a [`MetricsSnapshot`] into compliant OpenTelemetry ResourceMetrics JSON.
pub fn format_otlp_metrics_json(snapshot: &MetricsSnapshot, service_name: &str) -> String {
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos().to_string())
        .unwrap_or_else(|_| "0".to_string());

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
            request_datapoints.push(serde_json::json!({
                "timeUnixNano": now_nanos,
                "asInt": count.to_string(),
                "attributes": [
                    { "key": "downstream", "value": { "stringValue": downstream_str } },
                    { "key": "upstream", "value": { "stringValue": upstream_str } },
                    { "key": "pipe", "value": { "stringValue": pipe_str } },
                    { "key": "status_class", "value": { "stringValue": class } },
                ]
            }));
        }
    }

    metrics.push(serde_json::json!({
        "name": "velda.requests.total",
        "description": "Total number of HTTP requests processed across pipeline dimensions",
        "unit": "1",
        "sum": {
            "aggregationTemporality": 2, // AGGREGATION_TEMPORALITY_CUMULATIVE
            "isMonotonic": true,
            "dataPoints": request_datapoints
        }
    }));

    // 2. velda.active_streams (Gauge)
    let mut active_datapoints = Vec::new();
    for p in &snapshot.pipelines {
        active_datapoints.push(serde_json::json!({
            "timeUnixNano": now_nanos,
            "asInt": p.active_streams.to_string(),
            "attributes": [
                { "key": "downstream", "value": { "stringValue": p.downstream.as_str() } },
                { "key": "upstream", "value": { "stringValue": p.upstream.as_str() } },
                { "key": "pipe", "value": { "stringValue": p.pipe.as_str() } },
            ]
        }));
    }

    metrics.push(serde_json::json!({
        "name": "velda.active_streams",
        "description": "Number of streams actively in-flight",
        "unit": "1",
        "gauge": {
            "dataPoints": active_datapoints
        }
    }));

    // 3. velda.bytes_received_total & velda.bytes_transmitted_total
    let mut rx_datapoints = Vec::new();
    let mut tx_datapoints = Vec::new();
    for p in &snapshot.pipelines {
        rx_datapoints.push(serde_json::json!({
            "timeUnixNano": now_nanos,
            "asInt": p.bytes_rx.to_string(),
            "attributes": [
                { "key": "downstream", "value": { "stringValue": p.downstream.as_str() } },
                { "key": "upstream", "value": { "stringValue": p.upstream.as_str() } },
            ]
        }));
        tx_datapoints.push(serde_json::json!({
            "timeUnixNano": now_nanos,
            "asInt": p.bytes_tx.to_string(),
            "attributes": [
                { "key": "downstream", "value": { "stringValue": p.downstream.as_str() } },
                { "key": "upstream", "value": { "stringValue": p.upstream.as_str() } },
            ]
        }));
    }

    metrics.push(serde_json::json!({
        "name": "velda.bytes_received_total",
        "description": "Total payload bytes received from clients",
        "unit": "By",
        "sum": {
            "aggregationTemporality": 2,
            "isMonotonic": true,
            "dataPoints": rx_datapoints
        }
    }));

    metrics.push(serde_json::json!({
        "name": "velda.bytes_transmitted_total",
        "description": "Total payload bytes transmitted to clients",
        "unit": "By",
        "sum": {
            "aggregationTemporality": 2,
            "isMonotonic": true,
            "dataPoints": tx_datapoints
        }
    }));

    let otlp_doc = serde_json::json!({
        "resourceMetrics": [
            {
                "resource": {
                    "attributes": [
                        { "key": "service.name", "value": { "stringValue": service_name } },
                        { "key": "telemetry.sdk.name", "value": { "stringValue": "velda-edge" } },
                        { "key": "telemetry.sdk.language", "value": { "stringValue": "rust" } },
                    ]
                },
                "scopeMetrics": [
                    {
                        "scope": {
                            "name": "velda-edge",
                            "version": env!("CARGO_PKG_VERSION")
                        },
                        "metrics": metrics
                    }
                ]
            }
        ]
    });

    otlp_doc.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use velda_observability::MetricsStore;

    #[test]
    fn test_format_otlp_metrics_json() {
        let store = Arc::new(MetricsStore::new());
        let snapshot = store.snapshot();
        let json_str = format_otlp_metrics_json(&snapshot, "test-service");

        let parsed: serde_json::Value = serde_json::from_str(&json_str).expect("Valid JSON");
        assert!(parsed["resourceMetrics"].is_array());
        assert_eq!(
            parsed["resourceMetrics"][0]["resource"]["attributes"][0]["value"]["stringValue"],
            "test-service"
        );
        let metrics = &parsed["resourceMetrics"][0]["scopeMetrics"][0]["metrics"];
        assert!(metrics.as_array().unwrap().len() >= 4);
    }
}
