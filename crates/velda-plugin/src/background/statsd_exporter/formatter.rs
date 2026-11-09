//! Zero-Allocation StatsD & DogStatsD Wire Formatter.
//!
//! Formats point-in-time [`MetricsSnapshot`] data into compliant StatsD or DogStatsD
//! line protocol datagrams with zero heap allocation per line and strictly bounded UDP packet sizes.
//!
//! # Dialects Supported
//! 1. **`DogStatsD`**: Datadog-compatible metric line with `#tag1:val1,tag2:val2` suffix.
//! 2. **`Standard`**: Classic Etsy StatsD / Graphite dot-notation metric names (`prefix.dim1.dim2:value|type`).
//!
//! # Wire Protocol Specifications
//! - Counter: `<metric_name>:<value>|c[|#tags]\n`
//! - Gauge:   `<metric_name>:<value>|g[|#tags]\n`

use std::str::FromStr;
use velda_observability::MetricsSnapshot;

/// Two-digit ASCII lookup table covering numbers from 00 to 99.
const DIGIT_PAIRS: &[u8; 200] = b"\
    00010203040506070809\
    10111213141516171819\
    20212223242526272829\
    30313233343536373839\
    40414243444546474849\
    50515253545556575859\
    60616263646566676869\
    70717273747576777879\
    80818283848586878889\
    90919293949596979899";

/// Serializes an unsigned 64-bit integer into ASCII bytes at the end of `buf`.
#[inline(always)]
pub fn fast_write_u64(buf: &mut Vec<u8>, mut n: u64) {
    if n < 10 {
        buf.push(b'0' + n as u8);
        return;
    }

    let mut temp = [0u8; 20];
    let mut curr = 20;

    while n >= 100 {
        let rem = (n % 100) as usize * 2;
        n /= 100;
        curr -= 2;
        temp[curr] = DIGIT_PAIRS[rem];
        temp[curr + 1] = DIGIT_PAIRS[rem + 1];
    }

    if n < 10 {
        curr -= 1;
        temp[curr] = b'0' + n as u8;
    } else {
        let rem = n as usize * 2;
        curr -= 2;
        temp[curr] = DIGIT_PAIRS[rem];
        temp[curr + 1] = DIGIT_PAIRS[rem + 1];
    }

    buf.extend_from_slice(&temp[curr..20]);
}

/// Serializes a signed 64-bit integer into ASCII bytes.
#[inline(always)]
pub fn fast_write_i64(buf: &mut Vec<u8>, n: i64) {
    if n < 0 {
        buf.push(b'-');
        fast_write_u64(buf, n.unsigned_abs());
    } else {
        fast_write_u64(buf, n as u64);
    }
}

/// Supported StatsD wire line dialect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatsdDialect {
    /// Datadog DogStatsD dialect with tags (`metric:val|c|#tag:val`).
    DogStatsd,
    /// Classic Etsy StatsD dialect with dot notation (`metric.tag_val:val|c`).
    Standard,
}

impl FromStr for StatsdDialect {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "standard" => Ok(Self::Standard),
            _ => Ok(Self::DogStatsd),
        }
    }
}

impl StatsdDialect {
    /// Parses dialect from case-insensitive string.
    pub fn from_dialect_str(s: &str) -> Self {
        s.parse().unwrap_or(Self::DogStatsd)
    }
}

/// Tuple of dimension byte slices for pipeline metrics.
#[derive(Clone, Copy)]
struct PipelineDims<'a> {
    d: &'a [u8],
    u: &'a [u8],
    pipe: &'a [u8],
}

/// Reusable batch collector serializing and chunking metric lines into MTU-safe datagrams.
pub struct StatsdBatchWriter<'a, F: FnMut(&[u8])> {
    packet_buffer: &'a mut Vec<u8>,
    max_packet_size: usize,
    on_packet: F,
    packets_emitted: usize,
    metrics_emitted: usize,
}

impl<'a, F: FnMut(&[u8])> StatsdBatchWriter<'a, F> {
    /// Creates a new batch writer with the target packet buffer and flush callback.
    pub fn new(packet_buffer: &'a mut Vec<u8>, max_packet_size: usize, on_packet: F) -> Self {
        packet_buffer.clear();
        Self {
            packet_buffer,
            max_packet_size,
            on_packet,
            packets_emitted: 0,
            metrics_emitted: 0,
        }
    }

    /// Appends a formatted line into the active packet buffer, flushing if MTU would be exceeded.
    #[inline(always)]
    pub fn append_line(&mut self, line: &[u8]) {
        if self.packet_buffer.len() + line.len() > self.max_packet_size
            && !self.packet_buffer.is_empty()
        {
            (self.on_packet)(self.packet_buffer);
            self.packet_buffer.clear();
            self.packets_emitted += 1;
        }
        self.packet_buffer.extend_from_slice(line);
        self.metrics_emitted += 1;
    }

    /// Flushes any remaining bytes in the packet buffer.
    pub fn flush(&mut self) {
        if !self.packet_buffer.is_empty() {
            (self.on_packet)(self.packet_buffer);
            self.packet_buffer.clear();
            self.packets_emitted += 1;
        }
    }

    /// Returns total packets emitted.
    pub fn packets_emitted(&self) -> usize {
        self.packets_emitted
    }

    /// Returns total metric lines emitted.
    pub fn metrics_emitted(&self) -> usize {
        self.metrics_emitted
    }
}

/// Serializes all metrics from a [`MetricsSnapshot`] and emits them in MTU-safe packets.
pub fn format_statsd_snapshot<F: FnMut(&[u8])>(
    snapshot: &MetricsSnapshot,
    prefix: &str,
    dialect: StatsdDialect,
    packet_buf: &mut Vec<u8>,
    max_packet_size: usize,
    on_packet: F,
) -> (usize, usize) {
    let mut writer = StatsdBatchWriter::new(packet_buf, max_packet_size, on_packet);
    let mut line = Vec::with_capacity(256);
    let pfx = prefix.as_bytes();

    // ------------------------------------------------------------------------
    // 1. Global Ingress & System Metrics
    // ------------------------------------------------------------------------
    emit_global_metric(
        &mut writer,
        &mut line,
        pfx,
        b"connections.total",
        snapshot.global.connections_total,
        b"c",
    );
    emit_global_metric_i64(
        &mut writer,
        &mut line,
        pfx,
        b"connections.active",
        snapshot.global.connections_active,
        b"g",
    );
    emit_global_metric(
        &mut writer,
        &mut line,
        pfx,
        b"connections.rejected",
        snapshot.global.connections_rejected,
        b"c",
    );
    emit_global_metric(
        &mut writer,
        &mut line,
        pfx,
        b"tls.handshakes.total",
        snapshot.global.tls_handshakes_total,
        b"c",
    );
    emit_global_metric(
        &mut writer,
        &mut line,
        pfx,
        b"tls.handshakes.errors",
        snapshot.global.tls_handshake_errors,
        b"c",
    );
    emit_global_metric(
        &mut writer,
        &mut line,
        pfx,
        b"h2.rst_stream.total",
        snapshot.global.h2_rst_stream_total,
        b"c",
    );
    emit_global_metric(
        &mut writer,
        &mut line,
        pfx,
        b"config.reloads.total",
        snapshot.global.config_reloads_total,
        b"c",
    );
    emit_global_metric(
        &mut writer,
        &mut line,
        pfx,
        b"config.reloads.errors",
        snapshot.global.config_reload_errors,
        b"c",
    );

    // ------------------------------------------------------------------------
    // 2. Upstream Connection Pool Metrics
    // ------------------------------------------------------------------------
    emit_global_metric(
        &mut writer,
        &mut line,
        pfx,
        b"pool.containers.total",
        snapshot.pool.pool_containers_total,
        b"g",
    );
    emit_global_metric(
        &mut writer,
        &mut line,
        pfx,
        b"pool.connections.idle",
        snapshot.pool.connections_idle,
        b"g",
    );
    emit_global_metric_i64(
        &mut writer,
        &mut line,
        pfx,
        b"pool.connections.leased",
        snapshot.pool.connections_leased,
        b"g",
    );
    emit_global_metric(
        &mut writer,
        &mut line,
        pfx,
        b"pool.connections.multiplexed",
        snapshot.pool.connections_multiplexed,
        b"g",
    );
    emit_global_metric_i64(
        &mut writer,
        &mut line,
        pfx,
        b"pool.multiplexed_active_streams",
        snapshot.pool.multiplexed_active_streams,
        b"g",
    );
    emit_global_metric(
        &mut writer,
        &mut line,
        pfx,
        b"pool.lease_hits.total",
        snapshot.pool.lease_hits_total,
        b"c",
    );
    emit_global_metric(
        &mut writer,
        &mut line,
        pfx,
        b"pool.lease_misses.total",
        snapshot.pool.lease_misses_total,
        b"c",
    );
    emit_global_metric(
        &mut writer,
        &mut line,
        pfx,
        b"pool.lease_timeouts.total",
        snapshot.pool.lease_timeouts_total,
        b"c",
    );
    emit_global_metric(
        &mut writer,
        &mut line,
        pfx,
        b"pool.lease_releases.total",
        snapshot.pool.lease_releases_total,
        b"c",
    );
    emit_global_metric(
        &mut writer,
        &mut line,
        pfx,
        b"pool.connections_created.total",
        snapshot.pool.connections_created_total,
        b"c",
    );

    // Pool Evictions (DogStatsD tags vs Standard dot path)
    emit_eviction(
        &mut writer,
        &mut line,
        pfx,
        dialect,
        b"idle_timeout",
        snapshot.pool.evictions_idle_timeout,
    );
    emit_eviction(
        &mut writer,
        &mut line,
        pfx,
        dialect,
        b"max_lifetime",
        snapshot.pool.evictions_max_lifetime,
    );
    emit_eviction(
        &mut writer,
        &mut line,
        pfx,
        dialect,
        b"goaway",
        snapshot.pool.evictions_goaway,
    );
    emit_eviction(
        &mut writer,
        &mut line,
        pfx,
        dialect,
        b"unhealthy",
        snapshot.pool.evictions_unhealthy,
    );
    emit_eviction(
        &mut writer,
        &mut line,
        pfx,
        dialect,
        b"capacity_exceeded",
        snapshot.pool.evictions_capacity_exceeded,
    );

    // ------------------------------------------------------------------------
    // 3. Pipeline Metrics (36 slots)
    // ------------------------------------------------------------------------
    for p in &snapshot.pipelines {
        let dims = PipelineDims {
            d: p.downstream.as_str().as_bytes(),
            u: p.upstream.as_str().as_bytes(),
            pipe: p.pipe.as_str().as_bytes(),
        };

        match dialect {
            StatsdDialect::DogStatsd => {
                emit_dogstatsd_pipeline_status(
                    &mut writer,
                    &mut line,
                    pfx,
                    dims,
                    b"2xx",
                    p.requests_2xx,
                );
                emit_dogstatsd_pipeline_status(
                    &mut writer,
                    &mut line,
                    pfx,
                    dims,
                    b"3xx",
                    p.requests_3xx,
                );
                emit_dogstatsd_pipeline_status(
                    &mut writer,
                    &mut line,
                    pfx,
                    dims,
                    b"4xx",
                    p.requests_4xx,
                );
                emit_dogstatsd_pipeline_status(
                    &mut writer,
                    &mut line,
                    pfx,
                    dims,
                    b"5xx",
                    p.requests_5xx,
                );
                emit_dogstatsd_pipeline_status(
                    &mut writer,
                    &mut line,
                    pfx,
                    dims,
                    b"other",
                    p.requests_other,
                );

                emit_dogstatsd_pipeline_i64(
                    &mut writer,
                    &mut line,
                    pfx,
                    b"streams.active",
                    dims,
                    p.active_streams,
                    b"g",
                );
                emit_dogstatsd_pipeline_u64(
                    &mut writer,
                    &mut line,
                    pfx,
                    b"bytes.rx",
                    dims,
                    p.bytes_rx,
                    b"c",
                );
                emit_dogstatsd_pipeline_u64(
                    &mut writer,
                    &mut line,
                    pfx,
                    b"bytes.tx",
                    dims,
                    p.bytes_tx,
                    b"c",
                );

                emit_dogstatsd_pipeline_bucket(
                    &mut writer,
                    &mut line,
                    pfx,
                    b"duration_ms",
                    dims,
                    b"1",
                    p.duration_le_1ms,
                );
                emit_dogstatsd_pipeline_bucket(
                    &mut writer,
                    &mut line,
                    pfx,
                    b"duration_ms",
                    dims,
                    b"5",
                    p.duration_le_5ms,
                );
                emit_dogstatsd_pipeline_bucket(
                    &mut writer,
                    &mut line,
                    pfx,
                    b"duration_ms",
                    dims,
                    b"10",
                    p.duration_le_10ms,
                );
                emit_dogstatsd_pipeline_bucket(
                    &mut writer,
                    &mut line,
                    pfx,
                    b"duration_ms",
                    dims,
                    b"50",
                    p.duration_le_50ms,
                );
                emit_dogstatsd_pipeline_bucket(
                    &mut writer,
                    &mut line,
                    pfx,
                    b"duration_ms",
                    dims,
                    b"250",
                    p.duration_le_250ms,
                );
                emit_dogstatsd_pipeline_bucket(
                    &mut writer,
                    &mut line,
                    pfx,
                    b"duration_ms",
                    dims,
                    b"1000",
                    p.duration_le_1000ms,
                );
                emit_dogstatsd_pipeline_bucket(
                    &mut writer,
                    &mut line,
                    pfx,
                    b"duration_ms",
                    dims,
                    b"inf",
                    p.duration_gt_1000ms,
                );
            }
            StatsdDialect::Standard => {
                emit_standard_pipeline_status(
                    &mut writer,
                    &mut line,
                    pfx,
                    dims,
                    b"2xx",
                    p.requests_2xx,
                );
                emit_standard_pipeline_status(
                    &mut writer,
                    &mut line,
                    pfx,
                    dims,
                    b"3xx",
                    p.requests_3xx,
                );
                emit_standard_pipeline_status(
                    &mut writer,
                    &mut line,
                    pfx,
                    dims,
                    b"4xx",
                    p.requests_4xx,
                );
                emit_standard_pipeline_status(
                    &mut writer,
                    &mut line,
                    pfx,
                    dims,
                    b"5xx",
                    p.requests_5xx,
                );
                emit_standard_pipeline_status(
                    &mut writer,
                    &mut line,
                    pfx,
                    dims,
                    b"other",
                    p.requests_other,
                );

                emit_standard_pipeline_i64(
                    &mut writer,
                    &mut line,
                    pfx,
                    b"streams.active",
                    dims,
                    p.active_streams,
                    b"g",
                );
                emit_standard_pipeline_u64(
                    &mut writer,
                    &mut line,
                    pfx,
                    b"bytes.rx",
                    dims,
                    p.bytes_rx,
                    b"c",
                );
                emit_standard_pipeline_u64(
                    &mut writer,
                    &mut line,
                    pfx,
                    b"bytes.tx",
                    dims,
                    p.bytes_tx,
                    b"c",
                );

                emit_standard_pipeline_bucket(
                    &mut writer,
                    &mut line,
                    pfx,
                    b"duration_ms",
                    dims,
                    b"1",
                    p.duration_le_1ms,
                );
                emit_standard_pipeline_bucket(
                    &mut writer,
                    &mut line,
                    pfx,
                    b"duration_ms",
                    dims,
                    b"5",
                    p.duration_le_5ms,
                );
                emit_standard_pipeline_bucket(
                    &mut writer,
                    &mut line,
                    pfx,
                    b"duration_ms",
                    dims,
                    b"10",
                    p.duration_le_10ms,
                );
                emit_standard_pipeline_bucket(
                    &mut writer,
                    &mut line,
                    pfx,
                    b"duration_ms",
                    dims,
                    b"50",
                    p.duration_le_50ms,
                );
                emit_standard_pipeline_bucket(
                    &mut writer,
                    &mut line,
                    pfx,
                    b"duration_ms",
                    dims,
                    b"250",
                    p.duration_le_250ms,
                );
                emit_standard_pipeline_bucket(
                    &mut writer,
                    &mut line,
                    pfx,
                    b"duration_ms",
                    dims,
                    b"1000",
                    p.duration_le_1000ms,
                );
                emit_standard_pipeline_bucket(
                    &mut writer,
                    &mut line,
                    pfx,
                    b"duration_ms",
                    dims,
                    b"inf",
                    p.duration_gt_1000ms,
                );
            }
        }
    }

    writer.flush();
    (writer.packets_emitted(), writer.metrics_emitted())
}

#[inline(always)]
fn emit_global_metric<F: FnMut(&[u8])>(
    writer: &mut StatsdBatchWriter<'_, F>,
    line: &mut Vec<u8>,
    prefix: &[u8],
    name: &[u8],
    val: u64,
    mtype: &[u8],
) {
    line.clear();
    line.extend_from_slice(prefix);
    line.push(b'.');
    line.extend_from_slice(name);
    line.push(b':');
    fast_write_u64(line, val);
    line.push(b'|');
    line.extend_from_slice(mtype);
    line.push(b'\n');
    writer.append_line(line);
}

#[inline(always)]
fn emit_global_metric_i64<F: FnMut(&[u8])>(
    writer: &mut StatsdBatchWriter<'_, F>,
    line: &mut Vec<u8>,
    prefix: &[u8],
    name: &[u8],
    val: i64,
    mtype: &[u8],
) {
    line.clear();
    line.extend_from_slice(prefix);
    line.push(b'.');
    line.extend_from_slice(name);
    line.push(b':');
    fast_write_i64(line, val);
    line.push(b'|');
    line.extend_from_slice(mtype);
    line.push(b'\n');
    writer.append_line(line);
}

#[inline(always)]
fn emit_eviction<F: FnMut(&[u8])>(
    writer: &mut StatsdBatchWriter<'_, F>,
    line: &mut Vec<u8>,
    prefix: &[u8],
    dialect: StatsdDialect,
    reason: &[u8],
    count: u64,
) {
    line.clear();
    line.extend_from_slice(prefix);
    line.extend_from_slice(b".pool.evictions.total:");
    fast_write_u64(line, count);
    line.extend_from_slice(b"|c");
    match dialect {
        StatsdDialect::DogStatsd => {
            line.extend_from_slice(b"|#reason:");
            line.extend_from_slice(reason);
        }
        StatsdDialect::Standard => {
            line.clear();
            line.extend_from_slice(prefix);
            line.extend_from_slice(b".pool.evictions.");
            line.extend_from_slice(reason);
            line.push(b':');
            fast_write_u64(line, count);
            line.extend_from_slice(b"|c");
        }
    }
    line.push(b'\n');
    writer.append_line(line);
}

#[inline(always)]
fn emit_dogstatsd_pipeline_status<F: FnMut(&[u8])>(
    writer: &mut StatsdBatchWriter<'_, F>,
    line: &mut Vec<u8>,
    prefix: &[u8],
    dims: PipelineDims<'_>,
    status: &[u8],
    count: u64,
) {
    line.clear();
    line.extend_from_slice(prefix);
    line.extend_from_slice(b".requests.total:");
    fast_write_u64(line, count);
    line.extend_from_slice(b"|c|#downstream:");
    line.extend_from_slice(dims.d);
    line.extend_from_slice(b",upstream:");
    line.extend_from_slice(dims.u);
    line.extend_from_slice(b",pipe:");
    line.extend_from_slice(dims.pipe);
    line.extend_from_slice(b",status:");
    line.extend_from_slice(status);
    line.push(b'\n');
    writer.append_line(line);
}

#[inline(always)]
fn emit_dogstatsd_pipeline_u64<F: FnMut(&[u8])>(
    writer: &mut StatsdBatchWriter<'_, F>,
    line: &mut Vec<u8>,
    prefix: &[u8],
    metric_name: &[u8],
    dims: PipelineDims<'_>,
    val: u64,
    mtype: &[u8],
) {
    line.clear();
    line.extend_from_slice(prefix);
    line.push(b'.');
    line.extend_from_slice(metric_name);
    line.push(b':');
    fast_write_u64(line, val);
    line.push(b'|');
    line.extend_from_slice(mtype);
    line.extend_from_slice(b"|#downstream:");
    line.extend_from_slice(dims.d);
    line.extend_from_slice(b",upstream:");
    line.extend_from_slice(dims.u);
    line.extend_from_slice(b",pipe:");
    line.extend_from_slice(dims.pipe);
    line.push(b'\n');
    writer.append_line(line);
}

#[inline(always)]
fn emit_dogstatsd_pipeline_i64<F: FnMut(&[u8])>(
    writer: &mut StatsdBatchWriter<'_, F>,
    line: &mut Vec<u8>,
    prefix: &[u8],
    metric_name: &[u8],
    dims: PipelineDims<'_>,
    val: i64,
    mtype: &[u8],
) {
    line.clear();
    line.extend_from_slice(prefix);
    line.push(b'.');
    line.extend_from_slice(metric_name);
    line.push(b':');
    fast_write_i64(line, val);
    line.push(b'|');
    line.extend_from_slice(mtype);
    line.extend_from_slice(b"|#downstream:");
    line.extend_from_slice(dims.d);
    line.extend_from_slice(b",upstream:");
    line.extend_from_slice(dims.u);
    line.extend_from_slice(b",pipe:");
    line.extend_from_slice(dims.pipe);
    line.push(b'\n');
    writer.append_line(line);
}

#[inline(always)]
fn emit_dogstatsd_pipeline_bucket<F: FnMut(&[u8])>(
    writer: &mut StatsdBatchWriter<'_, F>,
    line: &mut Vec<u8>,
    prefix: &[u8],
    metric_name: &[u8],
    dims: PipelineDims<'_>,
    le: &[u8],
    val: u64,
) {
    line.clear();
    line.extend_from_slice(prefix);
    line.push(b'.');
    line.extend_from_slice(metric_name);
    line.extend_from_slice(b".bucket:");
    fast_write_u64(line, val);
    line.extend_from_slice(b"|c|#downstream:");
    line.extend_from_slice(dims.d);
    line.extend_from_slice(b",upstream:");
    line.extend_from_slice(dims.u);
    line.extend_from_slice(b",pipe:");
    line.extend_from_slice(dims.pipe);
    line.extend_from_slice(b",le:");
    line.extend_from_slice(le);
    line.push(b'\n');
    writer.append_line(line);
}

#[inline(always)]
fn emit_standard_pipeline_status<F: FnMut(&[u8])>(
    writer: &mut StatsdBatchWriter<'_, F>,
    line: &mut Vec<u8>,
    prefix: &[u8],
    dims: PipelineDims<'_>,
    status: &[u8],
    count: u64,
) {
    line.clear();
    line.extend_from_slice(prefix);
    line.extend_from_slice(b".requests.");
    line.extend_from_slice(dims.d);
    line.push(b'.');
    line.extend_from_slice(dims.u);
    line.push(b'.');
    line.extend_from_slice(dims.pipe);
    line.push(b'.');
    line.extend_from_slice(status);
    line.push(b':');
    fast_write_u64(line, count);
    line.extend_from_slice(b"|c\n");
    writer.append_line(line);
}

#[inline(always)]
fn emit_standard_pipeline_u64<F: FnMut(&[u8])>(
    writer: &mut StatsdBatchWriter<'_, F>,
    line: &mut Vec<u8>,
    prefix: &[u8],
    metric_name: &[u8],
    dims: PipelineDims<'_>,
    val: u64,
    mtype: &[u8],
) {
    line.clear();
    line.extend_from_slice(prefix);
    line.push(b'.');
    line.extend_from_slice(metric_name);
    line.push(b'.');
    line.extend_from_slice(dims.d);
    line.push(b'.');
    line.extend_from_slice(dims.u);
    line.push(b'.');
    line.extend_from_slice(dims.pipe);
    line.push(b':');
    fast_write_u64(line, val);
    line.push(b'|');
    line.extend_from_slice(mtype);
    line.push(b'\n');
    writer.append_line(line);
}

#[inline(always)]
fn emit_standard_pipeline_i64<F: FnMut(&[u8])>(
    writer: &mut StatsdBatchWriter<'_, F>,
    line: &mut Vec<u8>,
    prefix: &[u8],
    metric_name: &[u8],
    dims: PipelineDims<'_>,
    val: i64,
    mtype: &[u8],
) {
    line.clear();
    line.extend_from_slice(prefix);
    line.push(b'.');
    line.extend_from_slice(metric_name);
    line.push(b'.');
    line.extend_from_slice(dims.d);
    line.push(b'.');
    line.extend_from_slice(dims.u);
    line.push(b'.');
    line.extend_from_slice(dims.pipe);
    line.push(b':');
    fast_write_i64(line, val);
    line.push(b'|');
    line.extend_from_slice(mtype);
    line.push(b'\n');
    writer.append_line(line);
}

#[inline(always)]
fn emit_standard_pipeline_bucket<F: FnMut(&[u8])>(
    writer: &mut StatsdBatchWriter<'_, F>,
    line: &mut Vec<u8>,
    prefix: &[u8],
    metric_name: &[u8],
    dims: PipelineDims<'_>,
    le: &[u8],
    val: u64,
) {
    line.clear();
    line.extend_from_slice(prefix);
    line.push(b'.');
    line.extend_from_slice(metric_name);
    line.push(b'.');
    line.extend_from_slice(dims.d);
    line.push(b'.');
    line.extend_from_slice(dims.u);
    line.push(b'.');
    line.extend_from_slice(dims.pipe);
    line.push(b'.');
    line.extend_from_slice(le);
    line.push(b':');
    fast_write_u64(line, val);
    line.extend_from_slice(b"|c\n");
    writer.append_line(line);
}
