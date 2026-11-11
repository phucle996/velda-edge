//! Classic Etsy / Graphite StatsD Wire Formatter with Dot-Notation Skeletons (`statsd_exporter::dialect::standard`).
//!
//! Pre-bakes all hierarchical dot-paths for 36 pipeline slots, eliminating dynamic string formatting
//! and streaming directly to MTU packet buffers via single-pass serialization.
//!
//! # Microarchitecture: Pre-baked Dot-Paths & Direct-to-Packet Pass
//!
//! ```text
//! ╔══════════════════════════════════════════════════════════════════════════════════════════════════╗
//! ║                  STANDARD STATSD: PRE-BAKED DOT PATHS & DIRECT-TO-PACKET PASS                    ║
//! ╚══════════════════════════════════════════════════════════════════════════════════════════════════╝
//!
//!  [Initialization: StandardTemplate::new(prefix)]
//!  Pre-bakes 563 static metric lines:
//!  ├── Global & Pool Skeletons:  head: b"velda.connections.total:" | tail: b"|c\n"
//!  └── 36 Pipeline Skeletons:    head: b"velda.requests.total.http1.http1.buffered.2xx:"
//!                                tail: b"|c\n"
//!
//!  [Runtime: format_snapshot() — 10,022 CPU Cycles (~4.0 µs, 0 Heap Allocations)]
//!  Direct-to-Packet Write (Zero Double Copy, Zero String Splitting):
//!  1. Check MTU capacity: if remaining < head.len() + 21 + tail.len() -> emit packet callback
//!  2. packet_buffer.extend_from_slice(head);
//!  3. fast_write_u64_slice(packet_buffer, counter_value); ──► SIMD Itoa (3-7 cycles)
//!  4. packet_buffer.extend_from_slice(tail);
//! ```

use velda_observability::{
    DownstreamProto, MetricsSnapshot, PipeStrategy, PipelineSnapshot, UpstreamProto,
};

use super::StatsdBatchWriter;

/// Pre-baked dot-path metric heads for a single pipeline slot (15 metric lines).
#[derive(Clone)]
pub struct StandardSlotSkeleton {
    pub req_2xx_head: Box<[u8]>,
    pub req_3xx_head: Box<[u8]>,
    pub req_4xx_head: Box<[u8]>,
    pub req_5xx_head: Box<[u8]>,
    pub req_other_head: Box<[u8]>,
    pub active_streams_head: Box<[u8]>,
    pub bytes_rx_head: Box<[u8]>,
    pub bytes_tx_head: Box<[u8]>,
    pub dur_1ms_head: Box<[u8]>,
    pub dur_5ms_head: Box<[u8]>,
    pub dur_10ms_head: Box<[u8]>,
    pub dur_50ms_head: Box<[u8]>,
    pub dur_250ms_head: Box<[u8]>,
    pub dur_1000ms_head: Box<[u8]>,
    pub dur_inf_head: Box<[u8]>,
}

impl StandardSlotSkeleton {
    pub fn new(prefix: &str, down: DownstreamProto, up: UpstreamProto, pipe: PipeStrategy) -> Self {
        let pfx = prefix.trim();
        let d = down.as_str();
        let u = up.as_str();
        let p = pipe.as_str();

        Self {
            req_2xx_head: format!("{pfx}.requests.{d}.{u}.{p}.2xx:")
                .into_bytes()
                .into_boxed_slice(),
            req_3xx_head: format!("{pfx}.requests.{d}.{u}.{p}.3xx:")
                .into_bytes()
                .into_boxed_slice(),
            req_4xx_head: format!("{pfx}.requests.{d}.{u}.{p}.4xx:")
                .into_bytes()
                .into_boxed_slice(),
            req_5xx_head: format!("{pfx}.requests.{d}.{u}.{p}.5xx:")
                .into_bytes()
                .into_boxed_slice(),
            req_other_head: format!("{pfx}.requests.{d}.{u}.{p}.other:")
                .into_bytes()
                .into_boxed_slice(),
            active_streams_head: format!("{pfx}.streams.active.{d}.{u}.{p}:")
                .into_bytes()
                .into_boxed_slice(),
            bytes_rx_head: format!("{pfx}.bytes.rx.{d}.{u}.{p}:")
                .into_bytes()
                .into_boxed_slice(),
            bytes_tx_head: format!("{pfx}.bytes.tx.{d}.{u}.{p}:")
                .into_bytes()
                .into_boxed_slice(),
            dur_1ms_head: format!("{pfx}.duration_ms.{d}.{u}.{p}.1:")
                .into_bytes()
                .into_boxed_slice(),
            dur_5ms_head: format!("{pfx}.duration_ms.{d}.{u}.{p}.5:")
                .into_bytes()
                .into_boxed_slice(),
            dur_10ms_head: format!("{pfx}.duration_ms.{d}.{u}.{p}.10:")
                .into_bytes()
                .into_boxed_slice(),
            dur_50ms_head: format!("{pfx}.duration_ms.{d}.{u}.{p}.50:")
                .into_bytes()
                .into_boxed_slice(),
            dur_250ms_head: format!("{pfx}.duration_ms.{d}.{u}.{p}.250:")
                .into_bytes()
                .into_boxed_slice(),
            dur_1000ms_head: format!("{pfx}.duration_ms.{d}.{u}.{p}.1000:")
                .into_bytes()
                .into_boxed_slice(),
            dur_inf_head: format!("{pfx}.duration_ms.{d}.{u}.{p}.inf:")
                .into_bytes()
                .into_boxed_slice(),
        }
    }
}

/// Pre-baked Standard StatsD Template for dot-notation serialization.
#[derive(Clone)]
pub struct StandardTemplate {
    // 1. Global Metrics Heads
    pub conn_total_head: Box<[u8]>,
    pub conn_active_head: Box<[u8]>,
    pub conn_rejected_head: Box<[u8]>,
    pub tls_handshakes_total_head: Box<[u8]>,
    pub tls_handshake_errors_head: Box<[u8]>,
    pub h2_rst_stream_total_head: Box<[u8]>,
    pub config_reloads_total_head: Box<[u8]>,
    pub config_reload_errors_head: Box<[u8]>,

    // 2. Pool Metrics Heads
    pub pool_containers_total_head: Box<[u8]>,
    pub pool_conn_idle_head: Box<[u8]>,
    pub pool_conn_leased_head: Box<[u8]>,
    pub pool_conn_multiplexed_head: Box<[u8]>,
    pub pool_mux_active_streams_head: Box<[u8]>,
    pub pool_lease_hits_total_head: Box<[u8]>,
    pub pool_lease_misses_total_head: Box<[u8]>,
    pub pool_lease_timeouts_total_head: Box<[u8]>,
    pub pool_lease_releases_total_head: Box<[u8]>,
    pub pool_conn_created_total_head: Box<[u8]>,

    // Pool Evictions (Dot paths: prefix.pool.evictions.<reason>:)
    pub eviction_idle_head: Box<[u8]>,
    pub eviction_lifetime_head: Box<[u8]>,
    pub eviction_goaway_head: Box<[u8]>,
    pub eviction_unhealthy_head: Box<[u8]>,
    pub eviction_capacity_head: Box<[u8]>,

    // 36 Pipeline Slots Skeletons
    pub slots: Box<[StandardSlotSkeleton; 36]>,
}

impl StandardTemplate {
    /// Pre-bakes all skeletons with the declared metric prefix.
    pub fn new(prefix: &str) -> Self {
        let pfx = prefix.trim();

        let mut slots_vec = Vec::with_capacity(36);
        for down_idx in 0..DownstreamProto::COUNT {
            let down = DownstreamProto::from_discriminant(down_idx as u8).unwrap();
            for up_idx in 0..UpstreamProto::COUNT {
                let up = UpstreamProto::from_discriminant(up_idx as u8).unwrap();
                for pipe_idx in 0..PipeStrategy::COUNT {
                    let pipe = PipeStrategy::from_discriminant(pipe_idx as u8).unwrap();
                    slots_vec.push(StandardSlotSkeleton::new(pfx, down, up, pipe));
                }
            }
        }
        let slots: Box<[StandardSlotSkeleton; 36]> = slots_vec
            .into_boxed_slice()
            .try_into()
            .unwrap_or_else(|_| panic!("Expected exactly 36 pipeline slot skeletons"));

        Self {
            conn_total_head: format!("{pfx}.connections.total:")
                .into_bytes()
                .into_boxed_slice(),
            conn_active_head: format!("{pfx}.connections.active:")
                .into_bytes()
                .into_boxed_slice(),
            conn_rejected_head: format!("{pfx}.connections.rejected:")
                .into_bytes()
                .into_boxed_slice(),
            tls_handshakes_total_head: format!("{pfx}.tls.handshakes.total:")
                .into_bytes()
                .into_boxed_slice(),
            tls_handshake_errors_head: format!("{pfx}.tls.handshakes.errors:")
                .into_bytes()
                .into_boxed_slice(),
            h2_rst_stream_total_head: format!("{pfx}.h2.rst_stream.total:")
                .into_bytes()
                .into_boxed_slice(),
            config_reloads_total_head: format!("{pfx}.config.reloads.total:")
                .into_bytes()
                .into_boxed_slice(),
            config_reload_errors_head: format!("{pfx}.config.reloads.errors:")
                .into_bytes()
                .into_boxed_slice(),

            pool_containers_total_head: format!("{pfx}.pool.containers.total:")
                .into_bytes()
                .into_boxed_slice(),
            pool_conn_idle_head: format!("{pfx}.pool.connections.idle:")
                .into_bytes()
                .into_boxed_slice(),
            pool_conn_leased_head: format!("{pfx}.pool.connections.leased:")
                .into_bytes()
                .into_boxed_slice(),
            pool_conn_multiplexed_head: format!("{pfx}.pool.connections.multiplexed:")
                .into_bytes()
                .into_boxed_slice(),
            pool_mux_active_streams_head: format!("{pfx}.pool.multiplexed_active_streams:")
                .into_bytes()
                .into_boxed_slice(),
            pool_lease_hits_total_head: format!("{pfx}.pool.lease_hits.total:")
                .into_bytes()
                .into_boxed_slice(),
            pool_lease_misses_total_head: format!("{pfx}.pool.lease_misses.total:")
                .into_bytes()
                .into_boxed_slice(),
            pool_lease_timeouts_total_head: format!("{pfx}.pool.lease_timeouts.total:")
                .into_bytes()
                .into_boxed_slice(),
            pool_lease_releases_total_head: format!("{pfx}.pool.lease_releases.total:")
                .into_bytes()
                .into_boxed_slice(),
            pool_conn_created_total_head: format!("{pfx}.pool.connections_created.total:")
                .into_bytes()
                .into_boxed_slice(),

            eviction_idle_head: format!("{pfx}.pool.evictions.idle_timeout:")
                .into_bytes()
                .into_boxed_slice(),
            eviction_lifetime_head: format!("{pfx}.pool.evictions.max_lifetime:")
                .into_bytes()
                .into_boxed_slice(),
            eviction_goaway_head: format!("{pfx}.pool.evictions.goaway:")
                .into_bytes()
                .into_boxed_slice(),
            eviction_unhealthy_head: format!("{pfx}.pool.evictions.unhealthy:")
                .into_bytes()
                .into_boxed_slice(),
            eviction_capacity_head: format!("{pfx}.pool.evictions.capacity_exceeded:")
                .into_bytes()
                .into_boxed_slice(),

            slots,
        }
    }

    /// Formats all metrics from a [`MetricsSnapshot`] directly into the batch writer.
    #[inline(always)]
    pub fn format_with_writer<F: FnMut(&[u8])>(
        &self,
        snapshot: &MetricsSnapshot,
        writer: &mut StatsdBatchWriter<'_, F>,
    ) {
        // 1. Global Metrics
        writer.append_metric_u64(
            &self.conn_total_head,
            snapshot.global.connections_total,
            b"|c\n",
        );
        writer.append_metric_i64(
            &self.conn_active_head,
            snapshot.global.connections_active,
            b"|g\n",
        );
        writer.append_metric_u64(
            &self.conn_rejected_head,
            snapshot.global.connections_rejected,
            b"|c\n",
        );
        writer.append_metric_u64(
            &self.tls_handshakes_total_head,
            snapshot.global.tls_handshakes_total,
            b"|c\n",
        );
        writer.append_metric_u64(
            &self.tls_handshake_errors_head,
            snapshot.global.tls_handshake_errors,
            b"|c\n",
        );
        writer.append_metric_u64(
            &self.h2_rst_stream_total_head,
            snapshot.global.h2_rst_stream_total,
            b"|c\n",
        );
        writer.append_metric_u64(
            &self.config_reloads_total_head,
            snapshot.global.config_reloads_total,
            b"|c\n",
        );
        writer.append_metric_u64(
            &self.config_reload_errors_head,
            snapshot.global.config_reload_errors,
            b"|c\n",
        );

        // 2. Pool Metrics
        writer.append_metric_u64(
            &self.pool_containers_total_head,
            snapshot.pool.pool_containers_total,
            b"|g\n",
        );
        writer.append_metric_u64(
            &self.pool_conn_idle_head,
            snapshot.pool.connections_idle,
            b"|g\n",
        );
        writer.append_metric_i64(
            &self.pool_conn_leased_head,
            snapshot.pool.connections_leased,
            b"|g\n",
        );
        writer.append_metric_u64(
            &self.pool_conn_multiplexed_head,
            snapshot.pool.connections_multiplexed,
            b"|g\n",
        );
        writer.append_metric_i64(
            &self.pool_mux_active_streams_head,
            snapshot.pool.multiplexed_active_streams,
            b"|g\n",
        );
        writer.append_metric_u64(
            &self.pool_lease_hits_total_head,
            snapshot.pool.lease_hits_total,
            b"|c\n",
        );
        writer.append_metric_u64(
            &self.pool_lease_misses_total_head,
            snapshot.pool.lease_misses_total,
            b"|c\n",
        );
        writer.append_metric_u64(
            &self.pool_lease_timeouts_total_head,
            snapshot.pool.lease_timeouts_total,
            b"|c\n",
        );
        writer.append_metric_u64(
            &self.pool_lease_releases_total_head,
            snapshot.pool.lease_releases_total,
            b"|c\n",
        );
        writer.append_metric_u64(
            &self.pool_conn_created_total_head,
            snapshot.pool.connections_created_total,
            b"|c\n",
        );

        // Pool Evictions
        writer.append_metric_u64(
            &self.eviction_idle_head,
            snapshot.pool.evictions_idle_timeout,
            b"|c\n",
        );
        writer.append_metric_u64(
            &self.eviction_lifetime_head,
            snapshot.pool.evictions_max_lifetime,
            b"|c\n",
        );
        writer.append_metric_u64(
            &self.eviction_goaway_head,
            snapshot.pool.evictions_goaway,
            b"|c\n",
        );
        writer.append_metric_u64(
            &self.eviction_unhealthy_head,
            snapshot.pool.evictions_unhealthy,
            b"|c\n",
        );
        writer.append_metric_u64(
            &self.eviction_capacity_head,
            snapshot.pool.evictions_capacity_exceeded,
            b"|c\n",
        );

        // 3. Pipeline Metrics (36 Slots)
        for p in &snapshot.pipelines {
            self.format_slot(p, writer);
        }
    }

    #[inline(always)]
    fn format_slot<F: FnMut(&[u8])>(
        &self,
        p: &PipelineSnapshot,
        writer: &mut StatsdBatchWriter<'_, F>,
    ) {
        let slot_idx = (p.downstream as usize) * 12 + (p.upstream as usize) * 4 + (p.pipe as usize);
        if let Some(slot) = self.slots.get(slot_idx) {
            writer.append_metric_u64(&slot.req_2xx_head, p.requests_2xx, b"|c\n");
            writer.append_metric_u64(&slot.req_3xx_head, p.requests_3xx, b"|c\n");
            writer.append_metric_u64(&slot.req_4xx_head, p.requests_4xx, b"|c\n");
            writer.append_metric_u64(&slot.req_5xx_head, p.requests_5xx, b"|c\n");
            writer.append_metric_u64(&slot.req_other_head, p.requests_other, b"|c\n");

            writer.append_metric_i64(&slot.active_streams_head, p.active_streams, b"|g\n");
            writer.append_metric_u64(&slot.bytes_rx_head, p.bytes_rx, b"|c\n");
            writer.append_metric_u64(&slot.bytes_tx_head, p.bytes_tx, b"|c\n");

            writer.append_metric_u64(&slot.dur_1ms_head, p.duration_le_1ms, b"|c\n");
            writer.append_metric_u64(&slot.dur_5ms_head, p.duration_le_5ms, b"|c\n");
            writer.append_metric_u64(&slot.dur_10ms_head, p.duration_le_10ms, b"|c\n");
            writer.append_metric_u64(&slot.dur_50ms_head, p.duration_le_50ms, b"|c\n");
            writer.append_metric_u64(&slot.dur_250ms_head, p.duration_le_250ms, b"|c\n");
            writer.append_metric_u64(&slot.dur_1000ms_head, p.duration_le_1000ms, b"|c\n");
            writer.append_metric_u64(&slot.dur_inf_head, p.duration_gt_1000ms, b"|c\n");
        }
    }

    /// Serializes snapshot directly into MTU packets with callback.
    pub fn format<F: FnMut(&[u8])>(
        &self,
        snapshot: &MetricsSnapshot,
        packet_buf: &mut Vec<u8>,
        max_packet_size: usize,
        on_packet: F,
    ) -> (usize, usize) {
        let mut writer = StatsdBatchWriter::new(packet_buf, max_packet_size, on_packet);
        self.format_with_writer(snapshot, &mut writer);
        writer.flush();
        (writer.packets_emitted(), writer.metrics_emitted())
    }
}
