//! OpenTelemetry Structured Log Item and Serializer (`opentelemetry_logger`).

use serde::{Deserialize, Serialize};

/// Structured log record captured during `post_response` and queued for batch export.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OtlpLogItem {
    pub time_unix_nano: u128,
    pub status: u16,
    pub method: String,
    pub path: String,
    pub client_ip: Option<String>,
    pub route_id: Option<String>,
    pub upstream: Option<String>,
    pub latency_ms: Option<f64>,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
}

impl OtlpLogItem {
    /// Determines the standard OpenTelemetry severity number and text from HTTP status.
    #[inline]
    pub fn severity(&self) -> (u8, &'static str) {
        match self.status {
            100..=399 => (9, "INFO"),
            400..=499 => (13, "WARN"),
            _ => (17, "ERROR"),
        }
    }
}

/// Encodes a slice of [`OtlpLogItem`] records into compliant OpenTelemetry ResourceLogs JSON.
pub fn format_otlp_logs_json(records: &[OtlpLogItem], service_name: &str) -> String {
    let mut log_records = Vec::with_capacity(records.len());

    for item in records {
        let (severity_number, severity_text) = item.severity();
        let body_str = format!("{} {} -> {}", item.method, item.path, item.status);

        let mut attributes = vec![
            serde_json::json!({
                "key": "http.request.method",
                "value": { "stringValue": item.method }
            }),
            serde_json::json!({
                "key": "http.response.status_code",
                "value": { "intValue": item.status }
            }),
            serde_json::json!({
                "key": "url.path",
                "value": { "stringValue": item.path }
            }),
        ];

        if let Some(ip) = &item.client_ip {
            attributes.push(serde_json::json!({
                "key": "client.address",
                "value": { "stringValue": ip }
            }));
        }

        if let Some(route) = &item.route_id {
            attributes.push(serde_json::json!({
                "key": "http.route",
                "value": { "stringValue": route }
            }));
        }

        if let Some(up) = &item.upstream {
            attributes.push(serde_json::json!({
                "key": "server.address",
                "value": { "stringValue": up }
            }));
        }

        if let Some(lat) = item.latency_ms {
            attributes.push(serde_json::json!({
                "key": "http.duration_ms",
                "value": { "doubleValue": lat }
            }));
        }

        let mut record_obj = serde_json::json!({
            "timeUnixNano": item.time_unix_nano.to_string(),
            "severityNumber": severity_number,
            "severityText": severity_text,
            "body": { "stringValue": body_str },
            "attributes": attributes
        });

        if let Some(trace_id) = &item.trace_id {
            record_obj["traceId"] = serde_json::json!(trace_id);
        }

        if let Some(span_id) = &item.span_id {
            record_obj["spanId"] = serde_json::json!(span_id);
        }

        log_records.push(record_obj);
    }

    let otlp_doc = serde_json::json!({
        "resourceLogs": [
            {
                "resource": {
                    "attributes": [
                        { "key": "service.name", "value": { "stringValue": service_name } },
                        { "key": "telemetry.sdk.name", "value": { "stringValue": "velda-edge" } },
                        { "key": "telemetry.sdk.language", "value": { "stringValue": "rust" } },
                    ]
                },
                "scopeLogs": [
                    {
                        "scope": {
                            "name": "velda-edge",
                            "version": env!("CARGO_PKG_VERSION")
                        },
                        "logRecords": log_records
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

    #[test]
    fn test_format_otlp_logs_json() {
        let items = vec![OtlpLogItem {
            time_unix_nano: 1710000000000000000,
            status: 200,
            method: "GET".to_string(),
            path: "/api/v1/users".to_string(),
            client_ip: Some("127.0.0.1".to_string()),
            route_id: Some("route-main".to_string()),
            upstream: Some("10.0.0.1:8080".to_string()),
            latency_ms: Some(1.25),
            trace_id: Some("4bf92f3577b34da6a3ce929d0e0e4736".to_string()),
            span_id: Some("00f067aa0ba902b7".to_string()),
        }];

        let json_str = format_otlp_logs_json(&items, "test-service");
        let parsed: serde_json::Value = serde_json::from_str(&json_str).expect("Valid JSON");

        assert_eq!(
            parsed["resourceLogs"][0]["resource"]["attributes"][0]["value"]["stringValue"],
            "test-service"
        );
        let logs = &parsed["resourceLogs"][0]["scopeLogs"][0]["logRecords"];
        assert_eq!(logs.as_array().unwrap().len(), 1);
        assert_eq!(logs[0]["severityText"], "INFO");
        assert_eq!(logs[0]["traceId"], "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(logs[0]["spanId"], "00f067aa0ba902b7");
    }
}
