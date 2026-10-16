//! Zero-Cost Telemetry Abstraction: ZST Noop vs Live Computation.
//!
//! Provides the generic `Telemetry` trait along with two canonical implementations:
//! 1. `NoopTelemetry`: A Zero-Sized Type (ZST) with size = 0 bytes.
//!    When compiled with monomorphization, LLVM dead-code elimination erases 100% of telemetry
//!    instructions from the generated binary.
//! 2. `LiveTelemetry`: Straight-line active computation that records timestamps, bytes, and latencies
//!    directly into the `MetricsStore` without runtime `if enabled` branch checks.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::store::MetricsStore;
use crate::types::{DownstreamProto, PipeStrategy, StreamCompletion, UpstreamProto};

/// Generic telemetry interface for stream and request processing pipelines.
pub trait Telemetry: Send + Sync + 'static {
    /// Invoked when request processing begins.
    fn on_start(&mut self);

    /// Invoked when the first byte of response payload is received from upstream.
    fn on_ttfb(&mut self);

    /// Invoked when payload bytes are received from downstream.
    fn on_bytes_rx(&mut self, bytes: usize);

    /// Invoked when payload bytes are transmitted to downstream.
    fn on_bytes_tx(&mut self, bytes: usize);

    /// Invoked when the stream terminates to flush metrics.
    fn on_finish(self, status: u16);
}

/// Zero-Sized Type (ZST) for true zero-cost telemetry when metrics are disabled.
///
/// # Hardware Invariants
/// - Memory Size: `std::mem::size_of::<NoopTelemetry>() == 0`.
/// - Registers: Consumes zero CPU registers.
/// - Assembly: All methods are marked `#[inline(always)]` and empty; LLVM completely
///   erases all call sites and generates ZERO machine code instructions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NoopTelemetry;

impl Telemetry for NoopTelemetry {
    #[inline(always)]
    fn on_start(&mut self) {}

    #[inline(always)]
    fn on_ttfb(&mut self) {}

    #[inline(always)]
    fn on_bytes_rx(&mut self, _bytes: usize) {}

    #[inline(always)]
    fn on_bytes_tx(&mut self, _bytes: usize) {}

    #[inline(always)]
    fn on_finish(self, _status: u16) {}
}

/// Active telemetry context recording straight-line metrics into a `MetricsStore`.
///
/// # Performance
/// Executes straight-line instructions without runtime `if enabled` branch checks.
/// Thread-safe and lock-free.
pub struct LiveTelemetry {
    store: Arc<MetricsStore>,
    downstream: DownstreamProto,
    upstream: UpstreamProto,
    pipe: PipeStrategy,
    start_time: Option<Instant>,
    ttfb: Option<Duration>,
    bytes_rx: u64,
    bytes_tx: u64,
}

impl LiveTelemetry {
    /// Creates a new active telemetry context bound to a target pipeline slot.
    #[inline(always)]
    pub fn new(
        store: Arc<MetricsStore>,
        downstream: DownstreamProto,
        upstream: UpstreamProto,
        pipe: PipeStrategy,
    ) -> Self {
        store.record_stream_start(downstream, upstream, pipe);
        Self {
            store,
            downstream,
            upstream,
            pipe,
            start_time: None,
            ttfb: None,
            bytes_rx: 0,
            bytes_tx: 0,
        }
    }
}

impl Telemetry for LiveTelemetry {
    #[inline(always)]
    fn on_start(&mut self) {
        self.start_time = Some(Instant::now());
    }

    #[inline(always)]
    fn on_ttfb(&mut self) {
        if let Some(start) = self.start_time {
            self.ttfb = Some(start.elapsed());
        }
    }

    #[inline(always)]
    fn on_bytes_rx(&mut self, bytes: usize) {
        self.bytes_rx = self.bytes_rx.saturating_add(bytes as u64);
    }

    #[inline(always)]
    fn on_bytes_tx(&mut self, bytes: usize) {
        self.bytes_tx = self.bytes_tx.saturating_add(bytes as u64);
    }

    #[inline(always)]
    fn on_finish(self, status: u16) {
        let duration = self
            .start_time
            .map(|s| s.elapsed())
            .unwrap_or(Duration::ZERO);

        let completion =
            StreamCompletion::new(status, self.bytes_rx, self.bytes_tx, duration, self.ttfb);

        self.store
            .record_stream_completion(self.downstream, self.upstream, self.pipe, completion);
    }
}
