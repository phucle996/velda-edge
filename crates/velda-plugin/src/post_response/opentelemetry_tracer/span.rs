//! OpenTelemetry Span Item and Serializer (`opentelemetry_tracer`).

use serde::{Deserialize, Serialize};

/// Structured span record captured during `post_response` and queued for batch export.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OtlpSpanItem {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub name: String,
    pub start_time_unix_nano: u128,
    pub end_time_unix_nano: u128,
    pub status_code: u16,
    pub method: String,
    pub path: String,
    pub route_id: Option<String>,
    pub client_ip: Option<String>,
    pub upstream: Option<String>,
}

/// Encodes a slice of [`OtlpSpanItem`] records into compliant OpenTelemetry ResourceSpans JSON.
pub fn format_otlp_spans_json(spans: &[OtlpSpanItem], service_name: &str) -> String {
    let mut otlp_spans = Vec::with_capacity(spans.len());

    for item in spans {
        let status_code = if item.status_code < 400 {
            1 // STATUS_CODE_OK
        } else {
            2 // STATUS_CODE_ERROR
        };

        let mut attributes = vec![
            serde_json::json!({
                "key": "http.request.method",
                "value": { "stringValue": item.method }
            }),
            serde_json::json!({
                "key": "http.response.status_code",
                "value": { "intValue": item.status_code }
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

        let mut span_obj = serde_json::json!({
            "traceId": item.trace_id,
            "spanId": item.span_id,
            "name": item.name,
            "kind": 1, // SPAN_KIND_SERVER
            "startTimeUnixNano": item.start_time_unix_nano.to_string(),
            "endTimeUnixNano": item.end_time_unix_nano.to_string(),
            "attributes": attributes,
            "status": {
                "code": status_code
            }
        });

        if let Some(parent) = &item.parent_span_id {
            span_obj["parentSpanId"] = serde_json::json!(parent);
        }

        otlp_spans.push(span_obj);
    }

    let otlp_doc = serde_json::json!({
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        { "key": "service.name", "value": { "stringValue": service_name } },
                        { "key": "telemetry.sdk.name", "value": { "stringValue": "velda-edge" } },
                        { "key": "telemetry.sdk.language", "value": { "stringValue": "rust" } },
                    ]
                },
                "scopeSpans": [
                    {
                        "scope": {
                            "name": "velda-edge",
                            "version": env!("CARGO_PKG_VERSION")
                        },
                        "spans": otlp_spans
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
    fn test_format_otlp_spans_json() {
        let spans = vec![OtlpSpanItem {
            trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".to_string(),
            span_id: "00f067aa0ba902b7".to_string(),
            parent_span_id: Some("5fb397be34d23b0f".to_string()),
            name: "GET /api/v1/users".to_string(),
            start_time_unix_nano: 1710000000000000000,
            end_time_unix_nano: 1710000000025000000,
            status_code: 200,
            method: "GET".to_string(),
            path: "/api/v1/users".to_string(),
            route_id: Some("route-users".to_string()),
            client_ip: Some("127.0.0.1".to_string()),
            upstream: Some("10.0.0.2:8080".to_string()),
        }];

        let json_str = format_otlp_spans_json(&spans, "test-service");
        let parsed: serde_json::Value = serde_json::from_str(&json_str).expect("Valid JSON");

        assert_eq!(
            parsed["resourceSpans"][0]["resource"]["attributes"][0]["value"]["stringValue"],
            "test-service"
        );
        let list = &parsed["resourceSpans"][0]["scopeSpans"][0]["spans"];
        assert_eq!(list.as_array().unwrap().len(), 1);
        assert_eq!(list[0]["name"], "GET /api/v1/users");
        assert_eq!(list[0]["kind"], 1);
        assert_eq!(list[0]["status"]["code"], 1);
        assert_eq!(list[0]["parentSpanId"], "5fb397be34d23b0f");
    }
}
