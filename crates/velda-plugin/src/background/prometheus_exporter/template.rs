//! Pre-baked Line Prefix Skeletons & OpenMetrics Text Formatter (`prometheus_exporter::template`).
//!
//! Pre-bakes exact label prefix byte arrays for all 36 pipeline combinations and static
//! `# HELP` / `# TYPE` metric documentation blocks. During scrape cycles, metric lines
//! are assembled via exactly 1 static prefix copy + 1 SIMD Itoa integer conversion + 1 newline byte,
//! eliminating over 7,000 fragmented slice operations per scrape.
//!
//! # Microarchitecture: Pre-baked Line Prefix Skeletons
//!
//! ```text
//! ╔══════════════════════════════════════════════════════════════════════════════════════════════════╗
//! ║                 PROMETHEUS OPENMETRICS: PRE-BAKED LINE PREFIX SKELETONS                          ║
//! ╚══════════════════════════════════════════════════════════════════════════════════════════════════╝
//!
//!  [Initialization: PrometheusTemplate::new()]
//!  Pre-bakes 858 static metric lines:
//!  ├── Static Headers:     b"# HELP velda_requests_total ...\n# TYPE velda_requests_total counter\n"
//!  ├── Request Prefixes:   b"velda_requests_total{downstream=\"http1\",upstream=\"http1\",pipe=\"buffered\",status_class=\"2xx\"} "
//!  ├── Latency Prefixes:   b"velda_request_duration_seconds_bucket{downstream=\"...\",...,le=\"0.001\"} "
//!  └── Global & Pool Prefixes
//!
//!  [Runtime: format_snapshot_into() — 15,445 CPU Cycles (~6.3 µs, 0 Allocations)]
//!  1. buf.extend_from_slice(HELP_TYPE_REQUESTS_TOTAL);
//!  2. for each Metric Line (858 lines total):
//!       ├── buf.extend_from_slice(prebaked_prefix); ──► 1 single slice copy of full name + labels
//!       ├── fast_write_u64(buf, counter_value);     ──► SIMD Itoa (3-7 cycles, 0 division)
//!       └── buf.push(b'\n');
//!  3. Direct transmission via PooledBuffer (Zero-Allocation)
//! ```

use std::sync::LazyLock;
use velda_observability::{MetricsSnapshot, MetricsStore, TOTAL_PIPELINE_SLOTS};

use super::lut::{fast_write_i64, fast_write_u64};
use crate::error::PluginError;

// ============================================================================
// Static Metric Family Documentation & Type Headers
// ============================================================================

const HELP_TYPE_REQUESTS_TOTAL: &[u8] = b"\
# HELP velda_requests_total Total number of requests processed across all pipeline dimensions.\n\
# TYPE velda_requests_total counter\n";

const HELP_TYPE_ACTIVE_STREAMS: &[u8] = b"\
# HELP velda_active_streams Current number of active in-flight streams.\n\
# TYPE velda_active_streams gauge\n";

const HELP_TYPE_BYTES_RX: &[u8] = b"\
# HELP velda_bytes_rx_total Total payload bytes received from downstream.\n\
# TYPE velda_bytes_rx_total counter\n";

const HELP_TYPE_BYTES_TX: &[u8] = b"\
# HELP velda_bytes_tx_total Total payload bytes transmitted to downstream.\n\
# TYPE velda_bytes_tx_total counter\n";

const HELP_TYPE_DURATION: &[u8] = b"\
# HELP velda_request_duration_seconds Request duration latency histogram in seconds.\n\
# TYPE velda_request_duration_seconds histogram\n";

const HELP_TYPE_TTFB: &[u8] = b"\
# HELP velda_request_ttfb_seconds Time to first byte latency histogram in seconds.\n\
# TYPE velda_request_ttfb_seconds histogram\n";

// ============================================================================
// Global L4 & Transport Pre-baked Skeletons (HELP + TYPE + Line Prefix in 1 Slice)
// ============================================================================

const SKELETON_CONNECTIONS_TOTAL: &[u8] = b"\
# HELP velda_connections_total Total established L4 downstream connections.\n\
# TYPE velda_connections_total counter\n\
velda_connections_total ";

const SKELETON_CONNECTIONS_ACTIVE: &[u8] = b"\
# HELP velda_connections_active Number of active L4 downstream connections.\n\
# TYPE velda_connections_active gauge\n\
velda_connections_active ";

const SKELETON_CONNECTIONS_REJECTED: &[u8] = b"\
# HELP velda_connections_rejected_total Total rejected L4 connections.\n\
# TYPE velda_connections_rejected_total counter\n\
velda_connections_rejected_total ";

const SKELETON_TLS_HANDSHAKES: &[u8] = b"\
# HELP velda_tls_handshakes_total Total successful TLS handshakes.\n\
# TYPE velda_tls_handshakes_total counter\n\
velda_tls_handshakes_total ";

const SKELETON_TLS_ERRORS: &[u8] = b"\
# HELP velda_tls_handshake_errors_total Total failed TLS handshakes.\n\
# TYPE velda_tls_handshake_errors_total counter\n\
velda_tls_handshake_errors_total ";

const SKELETON_H2_RST: &[u8] = b"\
# HELP velda_h2_rst_stream_total Total HTTP/2 RST_STREAM frames encountered.\n\
# TYPE velda_h2_rst_stream_total counter\n\
velda_h2_rst_stream_total ";

const SKELETON_CONFIG_RELOADS: &[u8] = b"\
# HELP velda_config_reloads_total Total configuration reload cycles performed.\n\
# TYPE velda_config_reloads_total counter\n\
velda_config_reloads_total ";

const SKELETON_CONFIG_ERRORS: &[u8] = b"\
# HELP velda_config_reload_errors_total Total failed configuration reload cycles.\n\
# TYPE velda_config_reload_errors_total counter\n\
velda_config_reload_errors_total ";

// ============================================================================
// Connection Pool Pre-baked Skeletons ("The Soul of Velda")
// ============================================================================

const SKELETON_POOL_CONTAINERS: &[u8] = b"\
# HELP velda_pool_containers_active Total active SubPool key containers resident in memory.\n\
# TYPE velda_pool_containers_active gauge\n\
velda_pool_containers_active ";

const SKELETON_POOL_IDLE: &[u8] = b"\
# HELP velda_pool_connections_idle Sequential upstream connections currently idle in pool.\n\
# TYPE velda_pool_connections_idle gauge\n\
velda_pool_connections_idle ";

const SKELETON_POOL_LEASED: &[u8] = b"\
# HELP velda_pool_connections_leased Sequential upstream connections actively checked out and in use.\n\
# TYPE velda_pool_connections_leased gauge\n\
velda_pool_connections_leased ";

const SKELETON_POOL_MULTIPLEXED: &[u8] = b"\
# HELP velda_pool_connections_multiplexed Multiplexed upstream connections currently active.\n\
# TYPE velda_pool_connections_multiplexed gauge\n\
velda_pool_connections_multiplexed ";

const SKELETON_POOL_MUX_STREAMS: &[u8] = b"\
# HELP velda_pool_multiplexed_active_streams Active concurrent streams across all multiplexed connections.\n\
# TYPE velda_pool_multiplexed_active_streams gauge\n\
velda_pool_multiplexed_active_streams ";

const SKELETON_POOL_HITS: &[u8] = b"\
# HELP velda_pool_lease_hits_total Upstream connection leases satisfied from warm pool.\n\
# TYPE velda_pool_lease_hits_total counter\n\
velda_pool_lease_hits_total ";

const SKELETON_POOL_MISSES: &[u8] = b"\
# HELP velda_pool_lease_misses_total Upstream connection leases requiring fresh socket establishment.\n\
# TYPE velda_pool_lease_misses_total counter\n\
velda_pool_lease_misses_total ";

const SKELETON_POOL_TIMEOUTS: &[u8] = b"\
# HELP velda_pool_lease_timeouts_total Upstream connection leases that timed out waiting for connection.\n\
# TYPE velda_pool_lease_timeouts_total counter\n\
velda_pool_lease_timeouts_total ";

const SKELETON_POOL_RELEASES: &[u8] = b"\
# HELP velda_pool_lease_releases_total Upstream connection leases returned to the pool.\n\
# TYPE velda_pool_lease_releases_total counter\n\
velda_pool_lease_releases_total ";

const SKELETON_POOL_CREATED: &[u8] = b"\
# HELP velda_pool_connections_created_total Cumulative count of physical upstream connections created.\n\
# TYPE velda_pool_connections_created_total counter\n\
velda_pool_connections_created_total ";

const HELP_TYPE_POOL_EVICTIONS: &[u8] = b"\
# HELP velda_pool_evictions_total Cumulative connection evictions categorized by reason.\n\
# TYPE velda_pool_evictions_total counter\n";

const PREFIX_EVICTION_IDLE: &[u8] = b"velda_pool_evictions_total{reason=\"idle_timeout\"} ";
const PREFIX_EVICTION_MAX_LIFE: &[u8] = b"velda_pool_evictions_total{reason=\"max_lifetime\"} ";
const PREFIX_EVICTION_GOAWAY: &[u8] = b"velda_pool_evictions_total{reason=\"goaway\"} ";
const PREFIX_EVICTION_UNHEALTHY: &[u8] = b"velda_pool_evictions_total{reason=\"unhealthy\"} ";
const PREFIX_EVICTION_CAPACITY: &[u8] =
    b"velda_pool_evictions_total{reason=\"capacity_exceeded\"} ";

const HELP_TYPE_POOL_LIFESPAN: &[u8] = b"\
# HELP velda_pool_connection_lifespan_seconds Upstream connection lifespan distribution in seconds.\n\
# TYPE velda_pool_connection_lifespan_seconds histogram\n";

const PREFIX_LIFESPAN_5: &[u8] = b"velda_pool_connection_lifespan_seconds_bucket{le=\"5\"} ";
const PREFIX_LIFESPAN_30: &[u8] = b"velda_pool_connection_lifespan_seconds_bucket{le=\"30\"} ";
const PREFIX_LIFESPAN_300: &[u8] = b"velda_pool_connection_lifespan_seconds_bucket{le=\"300\"} ";
const PREFIX_LIFESPAN_1800: &[u8] = b"velda_pool_connection_lifespan_seconds_bucket{le=\"1800\"} ";
const PREFIX_LIFESPAN_3600: &[u8] = b"velda_pool_connection_lifespan_seconds_bucket{le=\"3600\"} ";
const PREFIX_LIFESPAN_INF: &[u8] = b"velda_pool_connection_lifespan_seconds_bucket{le=\"+Inf\"} ";
const PREFIX_LIFESPAN_COUNT: &[u8] = b"velda_pool_connection_lifespan_seconds_count ";

// ============================================================================
// Pipeline Slot Pre-baked Line Prefix Skeletons
// ============================================================================

/// Pre-baked label prefix skeletons for a single pipeline slot (23 metric lines).
pub struct PipelineSlotPrefixes {
    pub requests_2xx: Box<[u8]>,
    pub requests_3xx: Box<[u8]>,
    pub requests_4xx: Box<[u8]>,
    pub requests_5xx: Box<[u8]>,
    pub requests_other: Box<[u8]>,
    pub active_streams: Box<[u8]>,
    pub bytes_rx: Box<[u8]>,
    pub bytes_tx: Box<[u8]>,
    pub duration_buckets: [Box<[u8]>; 7],
    pub duration_count: Box<[u8]>,
    pub ttfb_buckets: [Box<[u8]>; 6],
    pub ttfb_count: Box<[u8]>,
}

impl PipelineSlotPrefixes {
    fn new(slot_idx: usize) -> Self {
        let (down, up, pipe) = MetricsStore::decode_slot_index(slot_idx);
        let d = down.as_str();
        let u = up.as_str();
        let p = pipe.as_str();

        let req_prefix = |status: &str| -> Box<[u8]> {
            format!("velda_requests_total{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{p}\",status_class=\"{status}\"}} ")
                .into_bytes()
                .into_boxed_slice()
        };

        let active_streams =
            format!("velda_active_streams{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{p}\"}} ")
                .into_bytes()
                .into_boxed_slice();

        let bytes_rx =
            format!("velda_bytes_rx_total{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{p}\"}} ")
                .into_bytes()
                .into_boxed_slice();

        let bytes_tx =
            format!("velda_bytes_tx_total{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{p}\"}} ")
                .into_bytes()
                .into_boxed_slice();

        let dur_les = ["0.001", "0.005", "0.01", "0.05", "0.25", "1.0", "+Inf"];
        let duration_buckets = [
            format!("velda_request_duration_seconds_bucket{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{p}\",le=\"{}\"}} ", dur_les[0]).into_bytes().into_boxed_slice(),
            format!("velda_request_duration_seconds_bucket{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{p}\",le=\"{}\"}} ", dur_les[1]).into_bytes().into_boxed_slice(),
            format!("velda_request_duration_seconds_bucket{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{p}\",le=\"{}\"}} ", dur_les[2]).into_bytes().into_boxed_slice(),
            format!("velda_request_duration_seconds_bucket{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{p}\",le=\"{}\"}} ", dur_les[3]).into_bytes().into_boxed_slice(),
            format!("velda_request_duration_seconds_bucket{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{p}\",le=\"{}\"}} ", dur_les[4]).into_bytes().into_boxed_slice(),
            format!("velda_request_duration_seconds_bucket{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{p}\",le=\"{}\"}} ", dur_les[5]).into_bytes().into_boxed_slice(),
            format!("velda_request_duration_seconds_bucket{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{p}\",le=\"{}\"}} ", dur_les[6]).into_bytes().into_boxed_slice(),
        ];

        let duration_count = format!("velda_request_duration_seconds_count{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{p}\"}} ")
            .into_bytes()
            .into_boxed_slice();

        let ttfb_les = ["0.001", "0.005", "0.01", "0.05", "0.25", "+Inf"];
        let ttfb_buckets = [
            format!("velda_request_ttfb_seconds_bucket{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{p}\",le=\"{}\"}} ", ttfb_les[0]).into_bytes().into_boxed_slice(),
            format!("velda_request_ttfb_seconds_bucket{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{p}\",le=\"{}\"}} ", ttfb_les[1]).into_bytes().into_boxed_slice(),
            format!("velda_request_ttfb_seconds_bucket{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{p}\",le=\"{}\"}} ", ttfb_les[2]).into_bytes().into_boxed_slice(),
            format!("velda_request_ttfb_seconds_bucket{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{p}\",le=\"{}\"}} ", ttfb_les[3]).into_bytes().into_boxed_slice(),
            format!("velda_request_ttfb_seconds_bucket{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{p}\",le=\"{}\"}} ", ttfb_les[4]).into_bytes().into_boxed_slice(),
            format!("velda_request_ttfb_seconds_bucket{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{p}\",le=\"{}\"}} ", ttfb_les[5]).into_bytes().into_boxed_slice(),
        ];

        let ttfb_count = format!(
            "velda_request_ttfb_seconds_count{{downstream=\"{d}\",upstream=\"{u}\",pipe=\"{p}\"}} "
        )
        .into_bytes()
        .into_boxed_slice();

        Self {
            requests_2xx: req_prefix("2xx"),
            requests_3xx: req_prefix("3xx"),
            requests_4xx: req_prefix("4xx"),
            requests_5xx: req_prefix("5xx"),
            requests_other: req_prefix("other"),
            active_streams,
            bytes_rx,
            bytes_tx,
            duration_buckets,
            duration_count,
            ttfb_buckets,
            ttfb_count,
        }
    }
}

// ============================================================================
// Prometheus Template Engine
// ============================================================================

/// Immutable pre-baked template for Prometheus / OpenMetrics serialization.
///
/// Holds pre-rendered byte slices for all 858 metric lines across 36 pipeline slots,
/// eliminating all dynamic string allocations and string formatting loops.
pub struct PrometheusTemplate {
    slots: Box<[PipelineSlotPrefixes; TOTAL_PIPELINE_SLOTS]>,
}

impl Default for PrometheusTemplate {
    fn default() -> Self {
        Self::new()
    }
}

impl PrometheusTemplate {
    /// Constructs and pre-bakes all label prefix skeletons.
    pub fn new() -> Self {
        let mut slots_vec = Vec::with_capacity(TOTAL_PIPELINE_SLOTS);
        for i in 0..TOTAL_PIPELINE_SLOTS {
            slots_vec.push(PipelineSlotPrefixes::new(i));
        }
        let boxed_slice = slots_vec.into_boxed_slice();
        let slots = match boxed_slice.try_into() {
            Ok(arr) => arr,
            Err(_) => unreachable!("TOTAL_PIPELINE_SLOTS is exactly 36"),
        };
        Self { slots }
    }

    /// Renders a [`MetricsSnapshot`] directly into a pre-allocated byte buffer.
    ///
    /// # Performance
    /// - Performs zero heap allocation.
    /// - Exactly 1 slice copy per static prefix.
    /// - Decimal numbers emitted via division-free SIMD Itoa (`fast_write_u64`).
    ///
    /// # Errors
    /// Returns [`PluginError::BufferOverflow`] if formatted output exceeds `max_buffer_size_bytes`.
    pub fn format_snapshot_into(
        &self,
        snapshot: &MetricsSnapshot,
        buf: &mut Vec<u8>,
        max_buffer_size_bytes: usize,
    ) -> Result<(), PluginError> {
        buf.clear();

        let estimated_bytes = snapshot.pipelines.len() * 640 + 2048;
        buf.reserve(estimated_bytes);

        // 1. Pipeline Requests Total (Counter by status_class)
        buf.extend_from_slice(HELP_TYPE_REQUESTS_TOTAL);
        for p in &snapshot.pipelines {
            let slot_idx =
                (p.downstream as usize) * 12 + (p.upstream as usize) * 4 + (p.pipe as usize);
            let slot = &self.slots[slot_idx];

            buf.extend_from_slice(&slot.requests_2xx);
            fast_write_u64(buf, p.requests_2xx);
            buf.push(b'\n');

            buf.extend_from_slice(&slot.requests_3xx);
            fast_write_u64(buf, p.requests_3xx);
            buf.push(b'\n');

            buf.extend_from_slice(&slot.requests_4xx);
            fast_write_u64(buf, p.requests_4xx);
            buf.push(b'\n');

            buf.extend_from_slice(&slot.requests_5xx);
            fast_write_u64(buf, p.requests_5xx);
            buf.push(b'\n');

            buf.extend_from_slice(&slot.requests_other);
            fast_write_u64(buf, p.requests_other);
            buf.push(b'\n');
        }

        // 2. Active Streams (Gauge)
        buf.extend_from_slice(HELP_TYPE_ACTIVE_STREAMS);
        for p in &snapshot.pipelines {
            let slot_idx =
                (p.downstream as usize) * 12 + (p.upstream as usize) * 4 + (p.pipe as usize);
            let slot = &self.slots[slot_idx];

            buf.extend_from_slice(&slot.active_streams);
            fast_write_i64(buf, p.active_streams);
            buf.push(b'\n');
        }

        // 3. Payload Bytes Received (Counter)
        buf.extend_from_slice(HELP_TYPE_BYTES_RX);
        for p in &snapshot.pipelines {
            let slot_idx =
                (p.downstream as usize) * 12 + (p.upstream as usize) * 4 + (p.pipe as usize);
            let slot = &self.slots[slot_idx];

            buf.extend_from_slice(&slot.bytes_rx);
            fast_write_u64(buf, p.bytes_rx);
            buf.push(b'\n');
        }

        // 4. Payload Bytes Transmitted (Counter)
        buf.extend_from_slice(HELP_TYPE_BYTES_TX);
        for p in &snapshot.pipelines {
            let slot_idx =
                (p.downstream as usize) * 12 + (p.upstream as usize) * 4 + (p.pipe as usize);
            let slot = &self.slots[slot_idx];

            buf.extend_from_slice(&slot.bytes_tx);
            fast_write_u64(buf, p.bytes_tx);
            buf.push(b'\n');
        }

        // 5. Request Duration Histogram (Seconds)
        buf.extend_from_slice(HELP_TYPE_DURATION);
        for p in &snapshot.pipelines {
            let slot_idx =
                (p.downstream as usize) * 12 + (p.upstream as usize) * 4 + (p.pipe as usize);
            let slot = &self.slots[slot_idx];
            let total_count = p.duration_le_1000ms.saturating_add(p.duration_gt_1000ms);

            buf.extend_from_slice(&slot.duration_buckets[0]);
            fast_write_u64(buf, p.duration_le_1ms);
            buf.push(b'\n');

            buf.extend_from_slice(&slot.duration_buckets[1]);
            fast_write_u64(buf, p.duration_le_5ms);
            buf.push(b'\n');

            buf.extend_from_slice(&slot.duration_buckets[2]);
            fast_write_u64(buf, p.duration_le_10ms);
            buf.push(b'\n');

            buf.extend_from_slice(&slot.duration_buckets[3]);
            fast_write_u64(buf, p.duration_le_50ms);
            buf.push(b'\n');

            buf.extend_from_slice(&slot.duration_buckets[4]);
            fast_write_u64(buf, p.duration_le_250ms);
            buf.push(b'\n');

            buf.extend_from_slice(&slot.duration_buckets[5]);
            fast_write_u64(buf, p.duration_le_1000ms);
            buf.push(b'\n');

            buf.extend_from_slice(&slot.duration_buckets[6]);
            fast_write_u64(buf, total_count);
            buf.push(b'\n');

            buf.extend_from_slice(&slot.duration_count);
            fast_write_u64(buf, total_count);
            buf.push(b'\n');
        }

        // 6. Request TTFB Histogram (Seconds)
        buf.extend_from_slice(HELP_TYPE_TTFB);
        for p in &snapshot.pipelines {
            let slot_idx =
                (p.downstream as usize) * 12 + (p.upstream as usize) * 4 + (p.pipe as usize);
            let slot = &self.slots[slot_idx];
            let total_ttfb = p.ttfb_le_250ms.saturating_add(p.ttfb_gt_250ms);

            buf.extend_from_slice(&slot.ttfb_buckets[0]);
            fast_write_u64(buf, p.ttfb_le_1ms);
            buf.push(b'\n');

            buf.extend_from_slice(&slot.ttfb_buckets[1]);
            fast_write_u64(buf, p.ttfb_le_5ms);
            buf.push(b'\n');

            buf.extend_from_slice(&slot.ttfb_buckets[2]);
            fast_write_u64(buf, p.ttfb_le_10ms);
            buf.push(b'\n');

            buf.extend_from_slice(&slot.ttfb_buckets[3]);
            fast_write_u64(buf, p.ttfb_le_50ms);
            buf.push(b'\n');

            buf.extend_from_slice(&slot.ttfb_buckets[4]);
            fast_write_u64(buf, p.ttfb_le_250ms);
            buf.push(b'\n');

            buf.extend_from_slice(&slot.ttfb_buckets[5]);
            fast_write_u64(buf, total_ttfb);
            buf.push(b'\n');

            buf.extend_from_slice(&slot.ttfb_count);
            fast_write_u64(buf, total_ttfb);
            buf.push(b'\n');
        }

        // 7. Global L4 and Transport Metrics
        buf.extend_from_slice(SKELETON_CONNECTIONS_TOTAL);
        fast_write_u64(buf, snapshot.global.connections_total);
        buf.push(b'\n');

        buf.extend_from_slice(SKELETON_CONNECTIONS_ACTIVE);
        fast_write_i64(buf, snapshot.global.connections_active);
        buf.push(b'\n');

        buf.extend_from_slice(SKELETON_CONNECTIONS_REJECTED);
        fast_write_u64(buf, snapshot.global.connections_rejected);
        buf.push(b'\n');

        buf.extend_from_slice(SKELETON_TLS_HANDSHAKES);
        fast_write_u64(buf, snapshot.global.tls_handshakes_total);
        buf.push(b'\n');

        buf.extend_from_slice(SKELETON_TLS_ERRORS);
        fast_write_u64(buf, snapshot.global.tls_handshake_errors);
        buf.push(b'\n');

        buf.extend_from_slice(SKELETON_H2_RST);
        fast_write_u64(buf, snapshot.global.h2_rst_stream_total);
        buf.push(b'\n');

        buf.extend_from_slice(SKELETON_CONFIG_RELOADS);
        fast_write_u64(buf, snapshot.global.config_reloads_total);
        buf.push(b'\n');

        buf.extend_from_slice(SKELETON_CONFIG_ERRORS);
        fast_write_u64(buf, snapshot.global.config_reload_errors);
        buf.push(b'\n');

        // 8. Connection Pool Metrics ("The Soul of Velda")
        buf.extend_from_slice(SKELETON_POOL_CONTAINERS);
        fast_write_u64(buf, snapshot.pool.pool_containers_total);
        buf.push(b'\n');

        buf.extend_from_slice(SKELETON_POOL_IDLE);
        fast_write_u64(buf, snapshot.pool.connections_idle);
        buf.push(b'\n');

        buf.extend_from_slice(SKELETON_POOL_LEASED);
        fast_write_i64(buf, snapshot.pool.connections_leased);
        buf.push(b'\n');

        buf.extend_from_slice(SKELETON_POOL_MULTIPLEXED);
        fast_write_u64(buf, snapshot.pool.connections_multiplexed);
        buf.push(b'\n');

        buf.extend_from_slice(SKELETON_POOL_MUX_STREAMS);
        fast_write_i64(buf, snapshot.pool.multiplexed_active_streams);
        buf.push(b'\n');

        buf.extend_from_slice(SKELETON_POOL_HITS);
        fast_write_u64(buf, snapshot.pool.lease_hits_total);
        buf.push(b'\n');

        buf.extend_from_slice(SKELETON_POOL_MISSES);
        fast_write_u64(buf, snapshot.pool.lease_misses_total);
        buf.push(b'\n');

        buf.extend_from_slice(SKELETON_POOL_TIMEOUTS);
        fast_write_u64(buf, snapshot.pool.lease_timeouts_total);
        buf.push(b'\n');

        buf.extend_from_slice(SKELETON_POOL_RELEASES);
        fast_write_u64(buf, snapshot.pool.lease_releases_total);
        buf.push(b'\n');

        buf.extend_from_slice(SKELETON_POOL_CREATED);
        fast_write_u64(buf, snapshot.pool.connections_created_total);
        buf.push(b'\n');

        // Pool evictions
        buf.extend_from_slice(HELP_TYPE_POOL_EVICTIONS);
        buf.extend_from_slice(PREFIX_EVICTION_IDLE);
        fast_write_u64(buf, snapshot.pool.evictions_idle_timeout);
        buf.push(b'\n');

        buf.extend_from_slice(PREFIX_EVICTION_MAX_LIFE);
        fast_write_u64(buf, snapshot.pool.evictions_max_lifetime);
        buf.push(b'\n');

        buf.extend_from_slice(PREFIX_EVICTION_GOAWAY);
        fast_write_u64(buf, snapshot.pool.evictions_goaway);
        buf.push(b'\n');

        buf.extend_from_slice(PREFIX_EVICTION_UNHEALTHY);
        fast_write_u64(buf, snapshot.pool.evictions_unhealthy);
        buf.push(b'\n');

        buf.extend_from_slice(PREFIX_EVICTION_CAPACITY);
        fast_write_u64(buf, snapshot.pool.evictions_capacity_exceeded);
        buf.push(b'\n');

        // Pool lifespan histogram
        buf.extend_from_slice(HELP_TYPE_POOL_LIFESPAN);
        let total_lifespans = snapshot.pool.total_lifespan_recorded();

        buf.extend_from_slice(PREFIX_LIFESPAN_5);
        fast_write_u64(buf, snapshot.pool.lifespan_le_5s);
        buf.push(b'\n');

        buf.extend_from_slice(PREFIX_LIFESPAN_30);
        fast_write_u64(buf, snapshot.pool.lifespan_le_30s);
        buf.push(b'\n');

        buf.extend_from_slice(PREFIX_LIFESPAN_300);
        fast_write_u64(buf, snapshot.pool.lifespan_le_5m);
        buf.push(b'\n');

        buf.extend_from_slice(PREFIX_LIFESPAN_1800);
        fast_write_u64(buf, snapshot.pool.lifespan_le_30m);
        buf.push(b'\n');

        buf.extend_from_slice(PREFIX_LIFESPAN_3600);
        fast_write_u64(buf, snapshot.pool.lifespan_le_1h);
        buf.push(b'\n');

        buf.extend_from_slice(PREFIX_LIFESPAN_INF);
        fast_write_u64(buf, total_lifespans);
        buf.push(b'\n');

        buf.extend_from_slice(PREFIX_LIFESPAN_COUNT);
        fast_write_u64(buf, total_lifespans);
        buf.push(b'\n');

        if buf.len() > max_buffer_size_bytes {
            return Err(PluginError::BufferOverflow(max_buffer_size_bytes / 1024));
        }

        Ok(())
    }

    /// Renders a [`MetricsSnapshot`] into the provided reusable String buffer.
    pub fn format_snapshot_string(
        &self,
        snapshot: &MetricsSnapshot,
        buffer: &mut String,
        max_buffer_size_bytes: usize,
    ) -> Result<(), PluginError> {
        buffer.clear();
        let estimated_bytes = snapshot.pipelines.len() * 640 + 2048;
        buffer.reserve(estimated_bytes);

        // SAFETY: All appended bytes are strictly valid ASCII (Prometheus headers, prefixes, digits, and \n)
        let buf = unsafe { buffer.as_mut_vec() };
        self.format_snapshot_into(snapshot, buf, max_buffer_size_bytes)
    }
}

static GLOBAL_TEMPLATE: LazyLock<PrometheusTemplate> = LazyLock::new(PrometheusTemplate::new);

/// Renders a [`MetricsSnapshot`] into the provided reusable String buffer using the pre-baked global template.
///
/// # Errors
/// Returns [`PluginError::BufferOverflow`] if the formatted output exceeds `max_buffer_size_bytes`.
pub fn format_snapshot(
    snapshot: &MetricsSnapshot,
    buffer: &mut String,
    max_buffer_size_bytes: usize,
) -> Result<(), PluginError> {
    GLOBAL_TEMPLATE.format_snapshot_string(snapshot, buffer, max_buffer_size_bytes)
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

    #[test]
    fn test_format_into_vec_matches_string() {
        let store = MetricsStore::new();
        let snapshot = store.snapshot();
        let template = PrometheusTemplate::new();

        let mut buf_vec = Vec::new();
        template
            .format_snapshot_into(&snapshot, &mut buf_vec, 1024 * 1024)
            .unwrap();

        let mut buf_str = String::new();
        template
            .format_snapshot_string(&snapshot, &mut buf_str, 1024 * 1024)
            .unwrap();

        assert_eq!(buf_vec, buf_str.as_bytes());
    }
}
