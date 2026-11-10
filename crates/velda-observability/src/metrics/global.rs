//! Global L4 Ingress & System Metrics Slot.
//!
//! Provides thread-safe, atomic telemetry counters for connection lifecycle,
//! TLS handshakes, protocol-level anomalies (e.g. HTTP/2 Rapid Reset), and configuration reload events.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// Cacheline-aligned global metrics structure for L4 connection and transport events.
#[repr(align(64))]
pub struct GlobalMetrics {
    /// Total number of client TCP/TLS/QUIC connections established since gateway start.
    pub connections_total: AtomicU64,
    /// Instantaneous number of active client L4 connections.
    pub connections_active: AtomicI64,
    /// Connections rejected early due to limits, shedder, or backlog overflow.
    pub connections_rejected: AtomicU64,

    /// Successful TLS handshakes negotiated.
    pub tls_handshakes_total: AtomicU64,
    /// Failed TLS handshakes (unsupported cipher, certificate error, timeout).
    pub tls_handshake_errors: AtomicU64,

    /// Number of HTTP/2 RST_STREAM frames received/sent (Rapid Reset detection).
    pub h2_rst_stream_total: AtomicU64,

    /// Total configuration reload events applied.
    pub config_reloads_total: AtomicU64,
    /// Failed configuration reload attempts.
    pub config_reload_errors: AtomicU64,
}

impl Default for GlobalMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl GlobalMetrics {
    /// Creates a zeroed global metrics instance.
    pub const fn new() -> Self {
        Self {
            connections_total: AtomicU64::new(0),
            connections_active: AtomicI64::new(0),
            connections_rejected: AtomicU64::new(0),
            tls_handshakes_total: AtomicU64::new(0),
            tls_handshake_errors: AtomicU64::new(0),
            h2_rst_stream_total: AtomicU64::new(0),
            config_reloads_total: AtomicU64::new(0),
            config_reload_errors: AtomicU64::new(0),
        }
    }

    /// Records a newly accepted downstream client connection.
    #[inline(always)]
    pub fn connection_opened(&self) {
        self.connections_total.fetch_add(1, Ordering::Relaxed);
        self.connections_active.fetch_add(1, Ordering::Relaxed);
    }

    /// Records the closing of an accepted downstream client connection.
    #[inline(always)]
    pub fn connection_closed(&self) {
        self.connections_active.fetch_sub(1, Ordering::Relaxed);
    }

    /// Records a rejected connection at L4 ingress.
    #[inline(always)]
    pub fn connection_rejected(&self) {
        self.connections_rejected.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a successful TLS handshake.
    #[inline(always)]
    pub fn tls_handshake_success(&self) {
        self.tls_handshakes_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a failed TLS handshake.
    #[inline(always)]
    pub fn tls_handshake_error(&self) {
        self.tls_handshake_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Records an HTTP/2 RST_STREAM frame.
    #[inline(always)]
    pub fn h2_rst_stream(&self) {
        self.h2_rst_stream_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a runtime configuration reload outcome.
    #[inline(always)]
    pub fn record_config_reload(&self, success: bool) {
        self.config_reloads_total.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.config_reload_errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}
