//! High-Performance Prometheus / OpenMetrics Text Formatter.
//!
//! Formats point-in-time [`MetricsSnapshot`] structs into compliant Prometheus / OpenMetrics
//! text format with bounded memory allocations and zero heap churn on repeated scrapes.
//!
//! # Architectural Invariants
//! 1. **Zero Dynamic Allocation**: Reuses an external `String` scratch buffer across scrapes.
//! 2. **Single-Pass Direct Byte Assembly**: Appends pre-structured static byte slices and formats
//!    integers via pure stack-based ASCII conversion, eliminating `std::fmt` runtime parsing.
//! 3. **Bounded Capacity**: Strictly enforces `max_buffer_size_bytes` to prevent memory exhaustion
//!    under adversarial scrape conditions.

use velda_observability::MetricsSnapshot;

use crate::error::PluginError;

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

/// Renders a [`MetricsSnapshot`] into the provided reusable String buffer.
///
/// # Errors
/// Returns [`PluginError::BufferOverflow`] if the formatted output exceeds `max_buffer_size_bytes`.
pub fn format_snapshot(
    snapshot: &MetricsSnapshot,
    buffer: &mut String,
    max_buffer_size_bytes: usize,
) -> Result<(), PluginError> {
    buffer.clear();
    // SAFETY: We only append valid ASCII bytes (Prometheus metric names, labels, and numbers)
    let buf = unsafe { buffer.as_mut_vec() };

    // 1. Pipeline Requests Total (Counter by status_class)
    buf.extend_from_slice(b"# HELP velda_requests_total Total number of requests processed across all pipeline dimensions.\n");
    buf.extend_from_slice(b"# TYPE velda_requests_total counter\n");
    for p in &snapshot.pipelines {
        let d = p.downstream.as_str().as_bytes();
        let u = p.upstream.as_str().as_bytes();
        let pipe = p.pipe.as_str().as_bytes();

        let prefix_start = b"velda_requests_total{downstream=\"";
        let sep_up = b"\",upstream=\"";
        let sep_pipe = b"\",pipe=\"";
        let sep_class = b"\",status_class=\"";

        // 2xx
        buf.extend_from_slice(prefix_start);
        buf.extend_from_slice(d);
        buf.extend_from_slice(sep_up);
        buf.extend_from_slice(u);
        buf.extend_from_slice(sep_pipe);
        buf.extend_from_slice(pipe);
        buf.extend_from_slice(sep_class);
        buf.extend_from_slice(b"2xx\"} ");
        write_u64(buf, p.requests_2xx);
        buf.push(b'\n');

        // 3xx
        buf.extend_from_slice(prefix_start);
        buf.extend_from_slice(d);
        buf.extend_from_slice(sep_up);
        buf.extend_from_slice(u);
        buf.extend_from_slice(sep_pipe);
        buf.extend_from_slice(pipe);
        buf.extend_from_slice(sep_class);
        buf.extend_from_slice(b"3xx\"} ");
        write_u64(buf, p.requests_3xx);
        buf.push(b'\n');

        // 4xx
        buf.extend_from_slice(prefix_start);
        buf.extend_from_slice(d);
        buf.extend_from_slice(sep_up);
        buf.extend_from_slice(u);
        buf.extend_from_slice(sep_pipe);
        buf.extend_from_slice(pipe);
        buf.extend_from_slice(sep_class);
        buf.extend_from_slice(b"4xx\"} ");
        write_u64(buf, p.requests_4xx);
        buf.push(b'\n');

        // 5xx
        buf.extend_from_slice(prefix_start);
        buf.extend_from_slice(d);
        buf.extend_from_slice(sep_up);
        buf.extend_from_slice(u);
        buf.extend_from_slice(sep_pipe);
        buf.extend_from_slice(pipe);
        buf.extend_from_slice(sep_class);
        buf.extend_from_slice(b"5xx\"} ");
        write_u64(buf, p.requests_5xx);
        buf.push(b'\n');

        // other
        buf.extend_from_slice(prefix_start);
        buf.extend_from_slice(d);
        buf.extend_from_slice(sep_up);
        buf.extend_from_slice(u);
        buf.extend_from_slice(sep_pipe);
        buf.extend_from_slice(pipe);
        buf.extend_from_slice(sep_class);
        buf.extend_from_slice(b"other\"} ");
        write_u64(buf, p.requests_other);
        buf.push(b'\n');
    }

    // 2. Active Streams (Gauge)
    buf.extend_from_slice(
        b"# HELP velda_active_streams Current number of active in-flight streams.\n",
    );
    buf.extend_from_slice(b"# TYPE velda_active_streams gauge\n");
    for p in &snapshot.pipelines {
        buf.extend_from_slice(b"velda_active_streams{downstream=\"");
        buf.extend_from_slice(p.downstream.as_str().as_bytes());
        buf.extend_from_slice(b"\",upstream=\"");
        buf.extend_from_slice(p.upstream.as_str().as_bytes());
        buf.extend_from_slice(b"\",pipe=\"");
        buf.extend_from_slice(p.pipe.as_str().as_bytes());
        buf.extend_from_slice(b"\"} ");
        write_i64(buf, p.active_streams);
        buf.push(b'\n');
    }

    // 3. Payload Bytes Received (Counter)
    buf.extend_from_slice(
        b"# HELP velda_bytes_rx_total Total payload bytes received from downstream.\n",
    );
    buf.extend_from_slice(b"# TYPE velda_bytes_rx_total counter\n");
    for p in &snapshot.pipelines {
        buf.extend_from_slice(b"velda_bytes_rx_total{downstream=\"");
        buf.extend_from_slice(p.downstream.as_str().as_bytes());
        buf.extend_from_slice(b"\",upstream=\"");
        buf.extend_from_slice(p.upstream.as_str().as_bytes());
        buf.extend_from_slice(b"\",pipe=\"");
        buf.extend_from_slice(p.pipe.as_str().as_bytes());
        buf.extend_from_slice(b"\"} ");
        write_u64(buf, p.bytes_rx);
        buf.push(b'\n');
    }

    // 4. Payload Bytes Transmitted (Counter)
    buf.extend_from_slice(
        b"# HELP velda_bytes_tx_total Total payload bytes transmitted to downstream.\n",
    );
    buf.extend_from_slice(b"# TYPE velda_bytes_tx_total counter\n");
    for p in &snapshot.pipelines {
        buf.extend_from_slice(b"velda_bytes_tx_total{downstream=\"");
        buf.extend_from_slice(p.downstream.as_str().as_bytes());
        buf.extend_from_slice(b"\",upstream=\"");
        buf.extend_from_slice(p.upstream.as_str().as_bytes());
        buf.extend_from_slice(b"\",pipe=\"");
        buf.extend_from_slice(p.pipe.as_str().as_bytes());
        buf.extend_from_slice(b"\"} ");
        write_u64(buf, p.bytes_tx);
        buf.push(b'\n');
    }

    // 5. Request Duration Histogram (Seconds)
    buf.extend_from_slice(
        b"# HELP velda_request_duration_seconds Request duration latency histogram in seconds.\n",
    );
    buf.extend_from_slice(b"# TYPE velda_request_duration_seconds histogram\n");
    for p in &snapshot.pipelines {
        let d = p.downstream.as_str().as_bytes();
        let u = p.upstream.as_str().as_bytes();
        let pipe = p.pipe.as_str().as_bytes();
        let total_count = p.duration_le_1000ms.saturating_add(p.duration_gt_1000ms);

        let buckets: &[(&[u8], u64)] = &[
            (b"0.001", p.duration_le_1ms),
            (b"0.005", p.duration_le_5ms),
            (b"0.01", p.duration_le_10ms),
            (b"0.05", p.duration_le_50ms),
            (b"0.25", p.duration_le_250ms),
            (b"1.0", p.duration_le_1000ms),
            (b"+Inf", total_count),
        ];

        for (le, count) in buckets {
            buf.extend_from_slice(b"velda_request_duration_seconds_bucket{downstream=\"");
            buf.extend_from_slice(d);
            buf.extend_from_slice(b"\",upstream=\"");
            buf.extend_from_slice(u);
            buf.extend_from_slice(b"\",pipe=\"");
            buf.extend_from_slice(pipe);
            buf.extend_from_slice(b"\",le=\"");
            buf.extend_from_slice(le);
            buf.extend_from_slice(b"\"} ");
            write_u64(buf, *count);
            buf.push(b'\n');
        }

        buf.extend_from_slice(b"velda_request_duration_seconds_count{downstream=\"");
        buf.extend_from_slice(d);
        buf.extend_from_slice(b"\",upstream=\"");
        buf.extend_from_slice(u);
        buf.extend_from_slice(b"\",pipe=\"");
        buf.extend_from_slice(pipe);
        buf.extend_from_slice(b"\"} ");
        write_u64(buf, total_count);
        buf.push(b'\n');
    }

    // 6. Request TTFB Histogram (Seconds)
    buf.extend_from_slice(
        b"# HELP velda_request_ttfb_seconds Time to first byte latency histogram in seconds.\n",
    );
    buf.extend_from_slice(b"# TYPE velda_request_ttfb_seconds histogram\n");
    for p in &snapshot.pipelines {
        let d = p.downstream.as_str().as_bytes();
        let u = p.upstream.as_str().as_bytes();
        let pipe = p.pipe.as_str().as_bytes();
        let total_ttfb = p.ttfb_le_250ms.saturating_add(p.ttfb_gt_250ms);

        let buckets: &[(&[u8], u64)] = &[
            (b"0.001", p.ttfb_le_1ms),
            (b"0.005", p.ttfb_le_5ms),
            (b"0.01", p.ttfb_le_10ms),
            (b"0.05", p.ttfb_le_50ms),
            (b"0.25", p.ttfb_le_250ms),
            (b"+Inf", total_ttfb),
        ];

        for (le, count) in buckets {
            buf.extend_from_slice(b"velda_request_ttfb_seconds_bucket{downstream=\"");
            buf.extend_from_slice(d);
            buf.extend_from_slice(b"\",upstream=\"");
            buf.extend_from_slice(u);
            buf.extend_from_slice(b"\",pipe=\"");
            buf.extend_from_slice(pipe);
            buf.extend_from_slice(b"\",le=\"");
            buf.extend_from_slice(le);
            buf.extend_from_slice(b"\"} ");
            write_u64(buf, *count);
            buf.push(b'\n');
        }

        buf.extend_from_slice(b"velda_request_ttfb_seconds_count{downstream=\"");
        buf.extend_from_slice(d);
        buf.extend_from_slice(b"\",upstream=\"");
        buf.extend_from_slice(u);
        buf.extend_from_slice(b"\",pipe=\"");
        buf.extend_from_slice(pipe);
        buf.extend_from_slice(b"\"} ");
        write_u64(buf, total_ttfb);
        buf.push(b'\n');
    }

    // 7. Global L4 and Transport Metrics
    buf.extend_from_slice(
        b"# HELP velda_connections_total Total established L4 downstream connections.\n",
    );
    buf.extend_from_slice(b"# TYPE velda_connections_total counter\n");
    buf.extend_from_slice(b"velda_connections_total ");
    write_u64(buf, snapshot.global.connections_total);
    buf.push(b'\n');

    buf.extend_from_slice(
        b"# HELP velda_connections_active Number of active L4 downstream connections.\n",
    );
    buf.extend_from_slice(b"# TYPE velda_connections_active gauge\n");
    buf.extend_from_slice(b"velda_connections_active ");
    write_i64(buf, snapshot.global.connections_active);
    buf.push(b'\n');

    buf.extend_from_slice(
        b"# HELP velda_connections_rejected_total Total rejected L4 connections.\n",
    );
    buf.extend_from_slice(b"# TYPE velda_connections_rejected_total counter\n");
    buf.extend_from_slice(b"velda_connections_rejected_total ");
    write_u64(buf, snapshot.global.connections_rejected);
    buf.push(b'\n');

    buf.extend_from_slice(b"# HELP velda_tls_handshakes_total Total successful TLS handshakes.\n");
    buf.extend_from_slice(b"# TYPE velda_tls_handshakes_total counter\n");
    buf.extend_from_slice(b"velda_tls_handshakes_total ");
    write_u64(buf, snapshot.global.tls_handshakes_total);
    buf.push(b'\n');

    buf.extend_from_slice(
        b"# HELP velda_tls_handshake_errors_total Total failed TLS handshakes.\n",
    );
    buf.extend_from_slice(b"# TYPE velda_tls_handshake_errors_total counter\n");
    buf.extend_from_slice(b"velda_tls_handshake_errors_total ");
    write_u64(buf, snapshot.global.tls_handshake_errors);
    buf.push(b'\n');

    buf.extend_from_slice(
        b"# HELP velda_h2_rst_stream_total Total HTTP/2 RST_STREAM frames encountered.\n",
    );
    buf.extend_from_slice(b"# TYPE velda_h2_rst_stream_total counter\n");
    buf.extend_from_slice(b"velda_h2_rst_stream_total ");
    write_u64(buf, snapshot.global.h2_rst_stream_total);
    buf.push(b'\n');

    buf.extend_from_slice(
        b"# HELP velda_config_reloads_total Total configuration reload cycles performed.\n",
    );
    buf.extend_from_slice(b"# TYPE velda_config_reloads_total counter\n");
    buf.extend_from_slice(b"velda_config_reloads_total ");
    write_u64(buf, snapshot.global.config_reloads_total);
    buf.push(b'\n');

    buf.extend_from_slice(
        b"# HELP velda_config_reload_errors_total Total failed configuration reload cycles.\n",
    );
    buf.extend_from_slice(b"# TYPE velda_config_reload_errors_total counter\n");
    buf.extend_from_slice(b"velda_config_reload_errors_total ");
    write_u64(buf, snapshot.global.config_reload_errors);
    buf.push(b'\n');

    // 8. Connection Pool Metrics (\"The Soul of Velda\")
    buf.extend_from_slice(b"# HELP velda_pool_containers_active Total active SubPool key containers resident in memory.\n");
    buf.extend_from_slice(b"# TYPE velda_pool_containers_active gauge\n");
    buf.extend_from_slice(b"velda_pool_containers_active ");
    write_u64(buf, snapshot.pool.pool_containers_total);
    buf.push(b'\n');

    buf.extend_from_slice(b"# HELP velda_pool_connections_idle Sequential upstream connections currently idle in pool.\n");
    buf.extend_from_slice(b"# TYPE velda_pool_connections_idle gauge\n");
    buf.extend_from_slice(b"velda_pool_connections_idle ");
    write_u64(buf, snapshot.pool.connections_idle);
    buf.push(b'\n');

    buf.extend_from_slice(b"# HELP velda_pool_connections_leased Sequential upstream connections actively checked out and in use.\n");
    buf.extend_from_slice(b"# TYPE velda_pool_connections_leased gauge\n");
    buf.extend_from_slice(b"velda_pool_connections_leased ");
    write_i64(buf, snapshot.pool.connections_leased);
    buf.push(b'\n');

    buf.extend_from_slice(b"# HELP velda_pool_connections_multiplexed Multiplexed upstream connections currently active.\n");
    buf.extend_from_slice(b"# TYPE velda_pool_connections_multiplexed gauge\n");
    buf.extend_from_slice(b"velda_pool_connections_multiplexed ");
    write_u64(buf, snapshot.pool.connections_multiplexed);
    buf.push(b'\n');

    buf.extend_from_slice(b"# HELP velda_pool_multiplexed_active_streams Active concurrent streams across all multiplexed connections.\n");
    buf.extend_from_slice(b"# TYPE velda_pool_multiplexed_active_streams gauge\n");
    buf.extend_from_slice(b"velda_pool_multiplexed_active_streams ");
    write_i64(buf, snapshot.pool.multiplexed_active_streams);
    buf.push(b'\n');

    buf.extend_from_slice(b"# HELP velda_pool_lease_hits_total Upstream connection leases satisfied from warm pool.\n");
    buf.extend_from_slice(b"# TYPE velda_pool_lease_hits_total counter\n");
    buf.extend_from_slice(b"velda_pool_lease_hits_total ");
    write_u64(buf, snapshot.pool.lease_hits_total);
    buf.push(b'\n');

    buf.extend_from_slice(b"# HELP velda_pool_lease_misses_total Upstream connection leases requiring fresh socket establishment.\n");
    buf.extend_from_slice(b"# TYPE velda_pool_lease_misses_total counter\n");
    buf.extend_from_slice(b"velda_pool_lease_misses_total ");
    write_u64(buf, snapshot.pool.lease_misses_total);
    buf.push(b'\n');

    buf.extend_from_slice(b"# HELP velda_pool_lease_timeouts_total Upstream connection leases that timed out waiting for connection.\n");
    buf.extend_from_slice(b"# TYPE velda_pool_lease_timeouts_total counter\n");
    buf.extend_from_slice(b"velda_pool_lease_timeouts_total ");
    write_u64(buf, snapshot.pool.lease_timeouts_total);
    buf.push(b'\n');

    buf.extend_from_slice(b"# HELP velda_pool_lease_releases_total Upstream connection leases returned to the pool.\n");
    buf.extend_from_slice(b"# TYPE velda_pool_lease_releases_total counter\n");
    buf.extend_from_slice(b"velda_pool_lease_releases_total ");
    write_u64(buf, snapshot.pool.lease_releases_total);
    buf.push(b'\n');

    buf.extend_from_slice(b"# HELP velda_pool_connections_created_total Cumulative count of physical upstream connections created.\n");
    buf.extend_from_slice(b"# TYPE velda_pool_connections_created_total counter\n");
    buf.extend_from_slice(b"velda_pool_connections_created_total ");
    write_u64(buf, snapshot.pool.connections_created_total);
    buf.push(b'\n');

    buf.extend_from_slice(b"# HELP velda_pool_evictions_total Cumulative connection evictions categorized by reason.\n");
    buf.extend_from_slice(b"# TYPE velda_pool_evictions_total counter\n");

    let reasons: &[(&[u8], u64)] = &[
        (b"idle_timeout", snapshot.pool.evictions_idle_timeout),
        (b"max_lifetime", snapshot.pool.evictions_max_lifetime),
        (b"goaway", snapshot.pool.evictions_goaway),
        (b"unhealthy", snapshot.pool.evictions_unhealthy),
        (
            b"capacity_exceeded",
            snapshot.pool.evictions_capacity_exceeded,
        ),
    ];
    for (r, count) in reasons {
        buf.extend_from_slice(b"velda_pool_evictions_total{reason=\"");
        buf.extend_from_slice(r);
        buf.extend_from_slice(b"\"} ");
        write_u64(buf, *count);
        buf.push(b'\n');
    }

    buf.extend_from_slice(b"# HELP velda_pool_connection_lifespan_seconds Upstream connection lifespan distribution in seconds.\n");
    buf.extend_from_slice(b"# TYPE velda_pool_connection_lifespan_seconds histogram\n");
    let total_lifespans = snapshot.pool.total_lifespan_recorded();

    let lifespan_buckets: &[(&[u8], u64)] = &[
        (b"5", snapshot.pool.lifespan_le_5s),
        (b"30", snapshot.pool.lifespan_le_30s),
        (b"300", snapshot.pool.lifespan_le_5m),
        (b"1800", snapshot.pool.lifespan_le_30m),
        (b"3600", snapshot.pool.lifespan_le_1h),
        (b"+Inf", total_lifespans),
    ];
    for (le, count) in lifespan_buckets {
        buf.extend_from_slice(b"velda_pool_connection_lifespan_seconds_bucket{le=\"");
        buf.extend_from_slice(le);
        buf.extend_from_slice(b"\"} ");
        write_u64(buf, *count);
        buf.push(b'\n');
    }

    buf.extend_from_slice(b"velda_pool_connection_lifespan_seconds_count ");
    write_u64(buf, total_lifespans);
    buf.push(b'\n');

    // Guard against buffer overflow
    if buf.len() > max_buffer_size_bytes {
        return Err(PluginError::BufferOverflow(max_buffer_size_bytes / 1024));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use velda_observability::MetricsStore;

    #[test]
    fn test_format_snapshot_structure() {
        let store = MetricsStore::new();
        let snapshot = store.snapshot();
        let mut buffer = String::with_capacity(32 * 1024);

        format_snapshot(&snapshot, &mut buffer, 1024 * 1024).expect("Formatting should succeed");

        assert!(buffer.contains("velda_requests_total{"));
        assert!(buffer.contains("velda_active_streams{"));
        assert!(buffer.contains("velda_bytes_rx_total{"));
        assert!(buffer.contains("velda_bytes_tx_total{"));
        assert!(buffer.contains("velda_request_duration_seconds_bucket{"));
        assert!(buffer.contains("velda_request_ttfb_seconds_bucket{"));
        assert!(buffer.contains("velda_connections_total 0"));
        assert!(buffer.contains("velda_pool_containers_active 0"));
        assert!(buffer.contains("velda_pool_evictions_total{reason=\"idle_timeout\"} 0"));
        assert!(buffer.contains("velda_pool_connection_lifespan_seconds_bucket{le=\"+Inf\"} 0"));
    }

    #[test]
    fn test_format_snapshot_overflow_guard() {
        let store = MetricsStore::new();
        let snapshot = store.snapshot();
        let mut buffer = String::with_capacity(128);

        // Max buffer 100 bytes should trigger overflow
        let err = format_snapshot(&snapshot, &mut buffer, 100).expect_err("Should overflow");
        assert!(matches!(err, PluginError::BufferOverflow(_)));
    }
}
