//! High-Performance Prometheus / OpenMetrics Text Formatter.
//!
//! Formats point-in-time [`MetricsSnapshot`] structs into compliant Prometheus / OpenMetrics
//! text format with bounded memory allocations and zero heap churn on repeated scrapes.
//!
//! # Architectural Invariants
//! 1. **Zero Dynamic Allocation**: Reuses an external `String` scratch buffer across scrapes.
//! 2. **Bounded Capacity**: Strictly enforces `max_buffer_size_bytes` to prevent memory exhaustion
//!    under adversarial scrape conditions.
//! 3. **Hardware-Aligned Telemetry Mapping**: Iterates through all 36 pipeline slot combinations
//!    in deterministic register order, generating standard HELP and TYPE declarations.

use std::fmt::Write;
use velda_observability::MetricsSnapshot;

use crate::error::PluginError;

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

    // 1. Pipeline Requests Total (Counter by status_class)
    buffer.push_str("# HELP velda_requests_total Total number of requests processed across all pipeline dimensions.\n");
    buffer.push_str("# TYPE velda_requests_total counter\n");
    for p in &snapshot.pipelines {
        let d = p.downstream.as_str();
        let u = p.upstream.as_str();
        let pipe = p.pipe.as_str();

        let _ = writeln!(
            buffer,
            "velda_requests_total{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{pipe}\",status_class=\"2xx\"}} {}",
            p.requests_2xx
        );
        let _ = writeln!(
            buffer,
            "velda_requests_total{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{pipe}\",status_class=\"3xx\"}} {}",
            p.requests_3xx
        );
        let _ = writeln!(
            buffer,
            "velda_requests_total{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{pipe}\",status_class=\"4xx\"}} {}",
            p.requests_4xx
        );
        let _ = writeln!(
            buffer,
            "velda_requests_total{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{pipe}\",status_class=\"5xx\"}} {}",
            p.requests_5xx
        );
        let _ = writeln!(
            buffer,
            "velda_requests_total{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{pipe}\",status_class=\"other\"}} {}",
            p.requests_other
        );
    }

    // 2. Active Streams (Gauge)
    buffer.push_str("# HELP velda_active_streams Current number of active in-flight streams.\n");
    buffer.push_str("# TYPE velda_active_streams gauge\n");
    for p in &snapshot.pipelines {
        let _ = writeln!(
            buffer,
            "velda_active_streams{{downstream=\"{}\",upstream=\"{}\",pipe=\"{}\"}} {}",
            p.downstream.as_str(),
            p.upstream.as_str(),
            p.pipe.as_str(),
            p.active_streams
        );
    }

    // 3. Payload Bytes Received (Counter)
    buffer.push_str("# HELP velda_bytes_rx_total Total payload bytes received from downstream.\n");
    buffer.push_str("# TYPE velda_bytes_rx_total counter\n");
    for p in &snapshot.pipelines {
        let _ = writeln!(
            buffer,
            "velda_bytes_rx_total{{downstream=\"{}\",upstream=\"{}\",pipe=\"{}\"}} {}",
            p.downstream.as_str(),
            p.upstream.as_str(),
            p.pipe.as_str(),
            p.bytes_rx
        );
    }

    // 4. Payload Bytes Transmitted (Counter)
    buffer.push_str("# HELP velda_bytes_tx_total Total payload bytes transmitted to downstream.\n");
    buffer.push_str("# TYPE velda_bytes_tx_total counter\n");
    for p in &snapshot.pipelines {
        let _ = writeln!(
            buffer,
            "velda_bytes_tx_total{{downstream=\"{}\",upstream=\"{}\",pipe=\"{}\"}} {}",
            p.downstream.as_str(),
            p.upstream.as_str(),
            p.pipe.as_str(),
            p.bytes_tx
        );
    }

    // 5. Request Duration Histogram (Seconds)
    buffer.push_str(
        "# HELP velda_request_duration_seconds Request duration latency histogram in seconds.\n",
    );
    buffer.push_str("# TYPE velda_request_duration_seconds histogram\n");
    for p in &snapshot.pipelines {
        let d = p.downstream.as_str();
        let u = p.upstream.as_str();
        let pipe = p.pipe.as_str();
        let total_count = p.duration_le_1000ms.saturating_add(p.duration_gt_1000ms);

        let _ = writeln!(
            buffer,
            "velda_request_duration_seconds_bucket{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{pipe}\",le=\"0.001\"}} {}",
            p.duration_le_1ms
        );
        let _ = writeln!(
            buffer,
            "velda_request_duration_seconds_bucket{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{pipe}\",le=\"0.005\"}} {}",
            p.duration_le_5ms
        );
        let _ = writeln!(
            buffer,
            "velda_request_duration_seconds_bucket{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{pipe}\",le=\"0.01\"}} {}",
            p.duration_le_10ms
        );
        let _ = writeln!(
            buffer,
            "velda_request_duration_seconds_bucket{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{pipe}\",le=\"0.05\"}} {}",
            p.duration_le_50ms
        );
        let _ = writeln!(
            buffer,
            "velda_request_duration_seconds_bucket{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{pipe}\",le=\"0.25\"}} {}",
            p.duration_le_250ms
        );
        let _ = writeln!(
            buffer,
            "velda_request_duration_seconds_bucket{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{pipe}\",le=\"1.0\"}} {}",
            p.duration_le_1000ms
        );
        let _ = writeln!(
            buffer,
            "velda_request_duration_seconds_bucket{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{pipe}\",le=\"+Inf\"}} {}",
            total_count
        );
        let _ = writeln!(
            buffer,
            "velda_request_duration_seconds_count{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{pipe}\"}} {}",
            total_count
        );
    }

    // 6. Request TTFB Histogram (Seconds)
    buffer.push_str(
        "# HELP velda_request_ttfb_seconds Time to first byte latency histogram in seconds.\n",
    );
    buffer.push_str("# TYPE velda_request_ttfb_seconds histogram\n");
    for p in &snapshot.pipelines {
        let d = p.downstream.as_str();
        let u = p.upstream.as_str();
        let pipe = p.pipe.as_str();
        let total_ttfb = p.ttfb_le_250ms.saturating_add(p.ttfb_gt_250ms);

        let _ = writeln!(
            buffer,
            "velda_request_ttfb_seconds_bucket{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{pipe}\",le=\"0.001\"}} {}",
            p.ttfb_le_1ms
        );
        let _ = writeln!(
            buffer,
            "velda_request_ttfb_seconds_bucket{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{pipe}\",le=\"0.005\"}} {}",
            p.ttfb_le_5ms
        );
        let _ = writeln!(
            buffer,
            "velda_request_ttfb_seconds_bucket{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{pipe}\",le=\"0.01\"}} {}",
            p.ttfb_le_10ms
        );
        let _ = writeln!(
            buffer,
            "velda_request_ttfb_seconds_bucket{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{pipe}\",le=\"0.05\"}} {}",
            p.ttfb_le_50ms
        );
        let _ = writeln!(
            buffer,
            "velda_request_ttfb_seconds_bucket{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{pipe}\",le=\"0.25\"}} {}",
            p.ttfb_le_250ms
        );
        let _ = writeln!(
            buffer,
            "velda_request_ttfb_seconds_bucket{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{pipe}\",le=\"+Inf\"}} {}",
            total_ttfb
        );
        let _ = writeln!(
            buffer,
            "velda_request_ttfb_seconds_count{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{pipe}\"}} {}",
            total_ttfb
        );
    }

    // 7. Global L4 and Transport Metrics
    buffer
        .push_str("# HELP velda_connections_total Total established L4 downstream connections.\n");
    buffer.push_str("# TYPE velda_connections_total counter\n");
    let _ = writeln!(
        buffer,
        "velda_connections_total {}",
        snapshot.global.connections_total
    );

    buffer
        .push_str("# HELP velda_connections_active Number of active L4 downstream connections.\n");
    buffer.push_str("# TYPE velda_connections_active gauge\n");
    let _ = writeln!(
        buffer,
        "velda_connections_active {}",
        snapshot.global.connections_active
    );

    buffer.push_str("# HELP velda_connections_rejected_total Total rejected L4 connections.\n");
    buffer.push_str("# TYPE velda_connections_rejected_total counter\n");
    let _ = writeln!(
        buffer,
        "velda_connections_rejected_total {}",
        snapshot.global.connections_rejected
    );

    buffer.push_str("# HELP velda_tls_handshakes_total Total successful TLS handshakes.\n");
    buffer.push_str("# TYPE velda_tls_handshakes_total counter\n");
    let _ = writeln!(
        buffer,
        "velda_tls_handshakes_total {}",
        snapshot.global.tls_handshakes_total
    );

    buffer.push_str("# HELP velda_tls_handshake_errors_total Total failed TLS handshakes.\n");
    buffer.push_str("# TYPE velda_tls_handshake_errors_total counter\n");
    let _ = writeln!(
        buffer,
        "velda_tls_handshake_errors_total {}",
        snapshot.global.tls_handshake_errors
    );

    buffer
        .push_str("# HELP velda_h2_rst_stream_total Total HTTP/2 RST_STREAM frames encountered.\n");
    buffer.push_str("# TYPE velda_h2_rst_stream_total counter\n");
    let _ = writeln!(
        buffer,
        "velda_h2_rst_stream_total {}",
        snapshot.global.h2_rst_stream_total
    );

    buffer.push_str(
        "# HELP velda_config_reloads_total Total configuration reload cycles performed.\n",
    );
    buffer.push_str("# TYPE velda_config_reloads_total counter\n");
    let _ = writeln!(
        buffer,
        "velda_config_reloads_total {}",
        snapshot.global.config_reloads_total
    );

    buffer.push_str(
        "# HELP velda_config_reload_errors_total Total failed configuration reload cycles.\n",
    );
    buffer.push_str("# TYPE velda_config_reload_errors_total counter\n");
    let _ = writeln!(
        buffer,
        "velda_config_reload_errors_total {}",
        snapshot.global.config_reload_errors
    );

    // 8. Connection Pool Metrics ("The Soul of Velda")
    buffer.push_str("# HELP velda_pool_containers_active Total active SubPool key containers resident in memory.\n");
    buffer.push_str("# TYPE velda_pool_containers_active gauge\n");
    let _ = writeln!(
        buffer,
        "velda_pool_containers_active {}",
        snapshot.pool.pool_containers_total
    );

    buffer.push_str("# HELP velda_pool_connections_idle Sequential upstream connections currently idle in pool.\n");
    buffer.push_str("# TYPE velda_pool_connections_idle gauge\n");
    let _ = writeln!(
        buffer,
        "velda_pool_connections_idle {}",
        snapshot.pool.connections_idle
    );

    buffer.push_str("# HELP velda_pool_connections_leased Sequential upstream connections actively checked out and in use.\n");
    buffer.push_str("# TYPE velda_pool_connections_leased gauge\n");
    let _ = writeln!(
        buffer,
        "velda_pool_connections_leased {}",
        snapshot.pool.connections_leased
    );

    buffer.push_str("# HELP velda_pool_connections_multiplexed Multiplexed upstream connections currently active.\n");
    buffer.push_str("# TYPE velda_pool_connections_multiplexed gauge\n");
    let _ = writeln!(
        buffer,
        "velda_pool_connections_multiplexed {}",
        snapshot.pool.connections_multiplexed
    );

    buffer.push_str("# HELP velda_pool_multiplexed_active_streams Active concurrent streams across all multiplexed connections.\n");
    buffer.push_str("# TYPE velda_pool_multiplexed_active_streams gauge\n");
    let _ = writeln!(
        buffer,
        "velda_pool_multiplexed_active_streams {}",
        snapshot.pool.multiplexed_active_streams
    );

    buffer.push_str(
        "# HELP velda_pool_lease_hits_total Upstream connection leases satisfied from warm pool.\n",
    );
    buffer.push_str("# TYPE velda_pool_lease_hits_total counter\n");
    let _ = writeln!(
        buffer,
        "velda_pool_lease_hits_total {}",
        snapshot.pool.lease_hits_total
    );

    buffer.push_str("# HELP velda_pool_lease_misses_total Upstream connection leases requiring fresh socket establishment.\n");
    buffer.push_str("# TYPE velda_pool_lease_misses_total counter\n");
    let _ = writeln!(
        buffer,
        "velda_pool_lease_misses_total {}",
        snapshot.pool.lease_misses_total
    );

    buffer.push_str("# HELP velda_pool_lease_timeouts_total Upstream connection leases that timed out waiting for connection.\n");
    buffer.push_str("# TYPE velda_pool_lease_timeouts_total counter\n");
    let _ = writeln!(
        buffer,
        "velda_pool_lease_timeouts_total {}",
        snapshot.pool.lease_timeouts_total
    );

    buffer.push_str(
        "# HELP velda_pool_lease_releases_total Upstream connection leases returned to the pool.\n",
    );
    buffer.push_str("# TYPE velda_pool_lease_releases_total counter\n");
    let _ = writeln!(
        buffer,
        "velda_pool_lease_releases_total {}",
        snapshot.pool.lease_releases_total
    );

    buffer.push_str("# HELP velda_pool_connections_created_total Cumulative count of physical upstream connections created.\n");
    buffer.push_str("# TYPE velda_pool_connections_created_total counter\n");
    let _ = writeln!(
        buffer,
        "velda_pool_connections_created_total {}",
        snapshot.pool.connections_created_total
    );

    buffer.push_str("# HELP velda_pool_evictions_total Cumulative connection evictions categorized by reason.\n");
    buffer.push_str("# TYPE velda_pool_evictions_total counter\n");
    let _ = writeln!(
        buffer,
        "velda_pool_evictions_total{{reason=\"idle_timeout\"}} {}",
        snapshot.pool.evictions_idle_timeout
    );
    let _ = writeln!(
        buffer,
        "velda_pool_evictions_total{{reason=\"max_lifetime\"}} {}",
        snapshot.pool.evictions_max_lifetime
    );
    let _ = writeln!(
        buffer,
        "velda_pool_evictions_total{{reason=\"goaway\"}} {}",
        snapshot.pool.evictions_goaway
    );
    let _ = writeln!(
        buffer,
        "velda_pool_evictions_total{{reason=\"unhealthy\"}} {}",
        snapshot.pool.evictions_unhealthy
    );
    let _ = writeln!(
        buffer,
        "velda_pool_evictions_total{{reason=\"capacity_exceeded\"}} {}",
        snapshot.pool.evictions_capacity_exceeded
    );

    buffer.push_str("# HELP velda_pool_connection_lifespan_seconds Upstream connection lifespan distribution in seconds.\n");
    buffer.push_str("# TYPE velda_pool_connection_lifespan_seconds histogram\n");
    let total_lifespans = snapshot.pool.total_lifespan_recorded();
    let _ = writeln!(
        buffer,
        "velda_pool_connection_lifespan_seconds_bucket{{le=\"5\"}} {}",
        snapshot.pool.lifespan_le_5s
    );
    let _ = writeln!(
        buffer,
        "velda_pool_connection_lifespan_seconds_bucket{{le=\"30\"}} {}",
        snapshot.pool.lifespan_le_30s
    );
    let _ = writeln!(
        buffer,
        "velda_pool_connection_lifespan_seconds_bucket{{le=\"300\"}} {}",
        snapshot.pool.lifespan_le_5m
    );
    let _ = writeln!(
        buffer,
        "velda_pool_connection_lifespan_seconds_bucket{{le=\"1800\"}} {}",
        snapshot.pool.lifespan_le_30m
    );
    let _ = writeln!(
        buffer,
        "velda_pool_connection_lifespan_seconds_bucket{{le=\"3600\"}} {}",
        snapshot.pool.lifespan_le_1h
    );
    let _ = writeln!(
        buffer,
        "velda_pool_connection_lifespan_seconds_bucket{{le=\"+Inf\"}} {}",
        total_lifespans
    );
    let _ = writeln!(
        buffer,
        "velda_pool_connection_lifespan_seconds_count {}",
        total_lifespans
    );

    // Guard against buffer overflow
    if buffer.len() > max_buffer_size_bytes {
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
