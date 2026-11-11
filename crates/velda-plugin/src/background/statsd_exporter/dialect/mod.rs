//! StatsD Wire Protocol Dialects & Batch Serialization (`statsd_exporter::dialect`).
//!
//! Provides protocol implementations for Datadog DogStatsD (tag-based) and
//! classic Etsy / Graphite StatsD (dot-notation), using pre-baked line skeletons
//! and single-pass MTU packet chunking.

pub mod dogstatsd;
pub mod standard;

use std::cell::RefCell;
use std::str::FromStr;

pub use dogstatsd::DogStatsdTemplate;
pub use standard::StandardTemplate;
use velda_observability::MetricsSnapshot;

use crate::background::statsd_exporter::lut::{
    fast_write_i64, fast_write_i64_slice, fast_write_u64, fast_write_u64_slice,
};

/// Supported StatsD wire line dialect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatsdDialect {
    /// Datadog DogStatsD dialect with tags (`metric:val|c|#tag:val`).
    DogStatsd,
    /// Classic Etsy StatsD dialect with dot notation (`metric.tag_val:val|c`).
    Standard,
}

impl FromStr for StatsdDialect {
    type Err = String;

    /// Strict, case-insensitive parse. Unknown values are rejected (no silent fallback).
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "dogstatsd" => Ok(Self::DogStatsd),
            "standard" => Ok(Self::Standard),
            other => Err(format!(
                "unknown statsd dialect '{other}' (expected 'dogstatsd' or 'standard')"
            )),
        }
    }
}

/// Reusable batch collector serializing and chunking metric lines into MTU-safe datagrams
/// using Direct-to-Packet Single Pass serialization.
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

    /// Appends an unsigned 64-bit integer metric line directly into the packet buffer (Direct-to-Packet Single Pass).
    #[inline(always)]
    pub fn append_metric_u64(&mut self, head: &[u8], val: u64, tail: &[u8]) {
        // Fast path: if conservative max size fits into current MTU packet, write directly with 0 copies!
        if self.packet_buffer.len() + head.len() + 21 + tail.len() <= self.max_packet_size {
            self.packet_buffer.extend_from_slice(head);
            fast_write_u64(self.packet_buffer, val);
            self.packet_buffer.extend_from_slice(tail);
            self.metrics_emitted += 1;
        } else {
            let mut num_buf = [0u8; 24];
            let num_len = fast_write_u64_slice(&mut num_buf, val);
            let line_len = head.len() + num_len + tail.len();
            if self.packet_buffer.len() + line_len > self.max_packet_size
                && !self.packet_buffer.is_empty()
            {
                (self.on_packet)(self.packet_buffer);
                self.packet_buffer.clear();
                self.packets_emitted += 1;
            }
            self.packet_buffer.extend_from_slice(head);
            self.packet_buffer.extend_from_slice(&num_buf[..num_len]);
            self.packet_buffer.extend_from_slice(tail);
            self.metrics_emitted += 1;
        }
    }

    /// Appends a signed 64-bit integer metric line directly into the packet buffer (Direct-to-Packet Single Pass).
    #[inline(always)]
    pub fn append_metric_i64(&mut self, head: &[u8], val: i64, tail: &[u8]) {
        // Fast path: conservative 22 bytes for signed i64 (sign + 20 digits)
        if self.packet_buffer.len() + head.len() + 22 + tail.len() <= self.max_packet_size {
            self.packet_buffer.extend_from_slice(head);
            fast_write_i64(self.packet_buffer, val);
            self.packet_buffer.extend_from_slice(tail);
            self.metrics_emitted += 1;
        } else {
            let mut num_buf = [0u8; 24];
            let num_len = fast_write_i64_slice(&mut num_buf, val);
            let line_len = head.len() + num_len + tail.len();
            if self.packet_buffer.len() + line_len > self.max_packet_size
                && !self.packet_buffer.is_empty()
            {
                (self.on_packet)(self.packet_buffer);
                self.packet_buffer.clear();
                self.packets_emitted += 1;
            }
            self.packet_buffer.extend_from_slice(head);
            self.packet_buffer.extend_from_slice(&num_buf[..num_len]);
            self.packet_buffer.extend_from_slice(tail);
            self.metrics_emitted += 1;
        }
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

thread_local! {
    static DOGSTATSD_CACHE: RefCell<Option<(String, DogStatsdTemplate)>> = const { RefCell::new(None) };
    static STANDARD_CACHE: RefCell<Option<(String, StandardTemplate)>> = const { RefCell::new(None) };
}

/// Serializes all metrics from a [`MetricsSnapshot`] and emits them in MTU-safe packets.
pub fn format_statsd_snapshot<F: FnMut(&[u8])>(
    snapshot: &MetricsSnapshot,
    prefix: &str,
    dialect: StatsdDialect,
    packet_buf: &mut Vec<u8>,
    max_packet_size: usize,
    mut on_packet: F,
) -> (usize, usize) {
    match dialect {
        StatsdDialect::DogStatsd => DOGSTATSD_CACHE.with(|cell| {
            let mut borrowed = cell.borrow_mut();
            let need_rebuild = match &*borrowed {
                Some((p, _)) => p != prefix,
                None => true,
            };
            if need_rebuild {
                *borrowed = Some((prefix.to_string(), DogStatsdTemplate::new(prefix)));
            }
            let (_, template) = borrowed.as_ref().unwrap();
            let mut writer = StatsdBatchWriter::new(packet_buf, max_packet_size, &mut on_packet);
            template.format_with_writer(snapshot, &mut writer);
            writer.flush();
            (writer.packets_emitted(), writer.metrics_emitted())
        }),
        StatsdDialect::Standard => STANDARD_CACHE.with(|cell| {
            let mut borrowed = cell.borrow_mut();
            let need_rebuild = match &*borrowed {
                Some((p, _)) => p != prefix,
                None => true,
            };
            if need_rebuild {
                *borrowed = Some((prefix.to_string(), StandardTemplate::new(prefix)));
            }
            let (_, template) = borrowed.as_ref().unwrap();
            let mut writer = StatsdBatchWriter::new(packet_buf, max_packet_size, &mut on_packet);
            template.format_with_writer(snapshot, &mut writer);
            writer.flush();
            (writer.packets_emitted(), writer.metrics_emitted())
        }),
    }
}
