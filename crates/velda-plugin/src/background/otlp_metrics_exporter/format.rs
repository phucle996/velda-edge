//! OpenTelemetry ResourceMetrics JSON Formatter (`otlp_metrics_exporter`).
//!
//! Encodes point-in-time [`MetricsSnapshot`] values from [`velda_observability::MetricsStore`]
//! into compliant OpenTelemetry ResourceMetrics JSON payloads (`POST /v1/metrics`)
//! via zero-allocation single-pass template byte injection.

use std::time::{SystemTime, UNIX_EPOCH};

use velda_observability::MetricsSnapshot;

#[inline(always)]
fn write_u64(buf: &mut Vec<u8>, mut n: u64) {
    if n == 0 {
        buf.push(b'0');
        return;
    }
    let mut temp = [0u8; 20];
    let mut i = 0;
    while n > 0 {
        temp[i] = b'0' + (n % 10) as u8;
        n /= 10;
        i += 1;
    }
    while i > 0 {
        i -= 1;
        buf.push(temp[i]);
    }
}

#[inline(always)]
fn write_i64(buf: &mut Vec<u8>, n: i64) {
    if n < 0 {
        buf.push(b'-');
        write_u64(buf, n.unsigned_abs());
    } else {
        write_u64(buf, n as u64);
    }
}

#[inline(always)]
fn write_u128(buf: &mut Vec<u8>, mut n: u128) {
    if n == 0 {
        buf.push(b'0');
        return;
    }
    let mut temp = [0u8; 40];
    let mut i = 0;
    while n > 0 {
        temp[i] = b'0' + (n % 10) as u8;
        n /= 10;
        i += 1;
    }
    while i > 0 {
        i -= 1;
        buf.push(temp[i]);
    }
}

/// Encodes a [`MetricsSnapshot`] into compliant OpenTelemetry ResourceMetrics JSON.
pub fn format_otlp_metrics_json(snapshot: &MetricsSnapshot, service_name: &str) -> String {
    let mut buf = Vec::with_capacity(64 * 1024);
    format_otlp_metrics_into(snapshot, service_name, &mut buf);
    // SAFETY: Output is strictly valid ASCII JSON
    unsafe { String::from_utf8_unchecked(buf) }
}

/// Encodes a [`MetricsSnapshot`] directly into the provided byte buffer without intermediate allocations.
pub fn format_otlp_metrics_into(snapshot: &MetricsSnapshot, service_name: &str, buf: &mut Vec<u8>) {
    buf.clear();

    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);

    let mut nanos_vec = Vec::with_capacity(40);
    write_u128(&mut nanos_vec, now_nanos);
    let nanos_bytes = nanos_vec.as_slice();

    // Root JSON envelope
    buf.extend_from_slice(b"{\"resourceMetrics\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"");
    buf.extend_from_slice(service_name.as_bytes());
    buf.extend_from_slice(b"\"}},{\"key\":\"telemetry.sdk.name\",\"value\":{\"stringValue\":\"velda-edge\"}},{\"key\":\"telemetry.sdk.language\",\"value\":{\"stringValue\":\"rust\"}}]},\"scopeMetrics\":[{\"scope\":{\"name\":\"velda-edge\",\"version\":\"");
    buf.extend_from_slice(env!("CARGO_PKG_VERSION").as_bytes());
    buf.extend_from_slice(b"\"},\"metrics\":[");

    // Metric 1: velda.requests.total
    buf.extend_from_slice(b"{\"name\":\"velda.requests.total\",\"description\":\"Total number of HTTP requests processed across pipeline dimensions\",\"unit\":\"1\",\"sum\":{\"aggregationTemporality\":2,\"isMonotonic\":true,\"dataPoints\":[");

    let mut first_dp = true;
    for p in &snapshot.pipelines {
        let d = p.downstream.as_str().as_bytes();
        let u = p.upstream.as_str().as_bytes();
        let pipe = p.pipe.as_str().as_bytes();

        let classes: &[(&[u8], u64)] = &[
            (b"2xx", p.requests_2xx),
            (b"3xx", p.requests_3xx),
            (b"4xx", p.requests_4xx),
            (b"5xx", p.requests_5xx),
            (b"other", p.requests_other),
        ];

        for (class, count) in classes {
            if !first_dp {
                buf.push(b',');
            } else {
                first_dp = false;
            }

            buf.extend_from_slice(b"{\"timeUnixNano\":\"");
            buf.extend_from_slice(nanos_bytes);
            buf.extend_from_slice(b"\",\"asInt\":\"");
            write_u64(buf, *count);
            buf.extend_from_slice(
                b"\",\"attributes\":[{\"key\":\"downstream\",\"value\":{\"stringValue\":\"",
            );
            buf.extend_from_slice(d);
            buf.extend_from_slice(b"\"}},{\"key\":\"upstream\",\"value\":{\"stringValue\":\"");
            buf.extend_from_slice(u);
            buf.extend_from_slice(b"\"}},{\"key\":\"pipe\",\"value\":{\"stringValue\":\"");
            buf.extend_from_slice(pipe);
            buf.extend_from_slice(b"\"}},{\"key\":\"status_class\",\"value\":{\"stringValue\":\"");
            buf.extend_from_slice(class);
            buf.extend_from_slice(b"\"}}]}");
        }
    }
    buf.extend_from_slice(b"]}},");

    // Metric 2: velda.active_streams
    buf.extend_from_slice(b"{\"name\":\"velda.active_streams\",\"description\":\"Number of streams actively in-flight\",\"unit\":\"1\",\"gauge\":{\"dataPoints\":[");
    first_dp = true;
    for p in &snapshot.pipelines {
        if !first_dp {
            buf.push(b',');
        } else {
            first_dp = false;
        }

        buf.extend_from_slice(b"{\"timeUnixNano\":\"");
        buf.extend_from_slice(nanos_bytes);
        buf.extend_from_slice(b"\",\"asInt\":\"");
        write_i64(buf, p.active_streams);
        buf.extend_from_slice(
            b"\",\"attributes\":[{\"key\":\"downstream\",\"value\":{\"stringValue\":\"",
        );
        buf.extend_from_slice(p.downstream.as_str().as_bytes());
        buf.extend_from_slice(b"\"}},{\"key\":\"upstream\",\"value\":{\"stringValue\":\"");
        buf.extend_from_slice(p.upstream.as_str().as_bytes());
        buf.extend_from_slice(b"\"}},{\"key\":\"pipe\",\"value\":{\"stringValue\":\"");
        buf.extend_from_slice(p.pipe.as_str().as_bytes());
        buf.extend_from_slice(b"\"}}]}");
    }
    buf.extend_from_slice(b"]}},");

    // Metric 3: velda.bytes_received_total
    buf.extend_from_slice(b"{\"name\":\"velda.bytes_received_total\",\"description\":\"Total payload bytes received from clients\",\"unit\":\"By\",\"sum\":{\"aggregationTemporality\":2,\"isMonotonic\":true,\"dataPoints\":[");
    first_dp = true;
    for p in &snapshot.pipelines {
        if !first_dp {
            buf.push(b',');
        } else {
            first_dp = false;
        }

        buf.extend_from_slice(b"{\"timeUnixNano\":\"");
        buf.extend_from_slice(nanos_bytes);
        buf.extend_from_slice(b"\",\"asInt\":\"");
        write_u64(buf, p.bytes_rx);
        buf.extend_from_slice(
            b"\",\"attributes\":[{\"key\":\"downstream\",\"value\":{\"stringValue\":\"",
        );
        buf.extend_from_slice(p.downstream.as_str().as_bytes());
        buf.extend_from_slice(b"\"}},{\"key\":\"upstream\",\"value\":{\"stringValue\":\"");
        buf.extend_from_slice(p.upstream.as_str().as_bytes());
        buf.extend_from_slice(b"\"}}]}");
    }
    buf.extend_from_slice(b"]}},");

    // Metric 4: velda.bytes_transmitted_total
    buf.extend_from_slice(b"{\"name\":\"velda.bytes_transmitted_total\",\"description\":\"Total payload bytes transmitted to clients\",\"unit\":\"By\",\"sum\":{\"aggregationTemporality\":2,\"isMonotonic\":true,\"dataPoints\":[");
    first_dp = true;
    for p in &snapshot.pipelines {
        if !first_dp {
            buf.push(b',');
        } else {
            first_dp = false;
        }

        buf.extend_from_slice(b"{\"timeUnixNano\":\"");
        buf.extend_from_slice(nanos_bytes);
        buf.extend_from_slice(b"\",\"asInt\":\"");
        write_u64(buf, p.bytes_tx);
        buf.extend_from_slice(
            b"\",\"attributes\":[{\"key\":\"downstream\",\"value\":{\"stringValue\":\"",
        );
        buf.extend_from_slice(p.downstream.as_str().as_bytes());
        buf.extend_from_slice(b"\"}},{\"key\":\"upstream\",\"value\":{\"stringValue\":\"");
        buf.extend_from_slice(p.upstream.as_str().as_bytes());
        buf.extend_from_slice(b"\"}}]}");
    }
    buf.extend_from_slice(b"]}}");

    // Close JSON envelope
    buf.extend_from_slice(b"]}]}]}");
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
