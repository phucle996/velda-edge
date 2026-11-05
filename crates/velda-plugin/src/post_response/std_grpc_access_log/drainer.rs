//! Background Shadow Logistics Drainer for gRPC (`std_grpc_access_log::drainer`).
//!
//! Autonomous background task running on companion Shadow Logistics Workers pinned to
//! the same CPU cores under `SCHED_IDLE`. Drains per-lane SPSC ring buffers and serializes
//! gRPC audit logs directly to standard streams (`stdout` or `stderr`) via single-syscall batching.
//!
//! # Micro-Architectural Guarantees
//! - **Zero Frontline Contention**: Frontline traffic workers never execute write syscalls.
//! - **Single Syscall Batching**: Accumulates up to 64KB of formatted log bytes, flushing via
//!   a single `libc::write` call to `stdout` (fd 1) or `stderr` (fd 2).
//! - **Zero Buffer Re-allocations**: `write_buf` is pre-allocated to 64KB at bootstrap; calls to
//!   `write_buf.clear()` retain the physical heap buffer without triggering `realloc` or `free`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use super::config::TargetStream;
use super::record::GrpcLogFormat;
use super::ring_buffer::ShardedRingBuffer;

const BATCH_DRAIN_SIZE: usize = 256;
const WRITE_BUFFER_CAPACITY: usize = 64 * 1024; // 64 KB

/// Background task draining gRPC access log records to standard output streams.
pub struct ShadowGrpcLogDrainer {
    target: TargetStream,
    format: GrpcLogFormat,
    ring_buffer: Arc<ShardedRingBuffer>,
    shutdown: Arc<AtomicBool>,
}

impl ShadowGrpcLogDrainer {
    pub fn new(
        target: TargetStream,
        format: GrpcLogFormat,
        ring_buffer: Arc<ShardedRingBuffer>,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        Self {
            target,
            format,
            ring_buffer,
            shutdown,
        }
    }

    /// Spawns the autonomous drainer loop on the Tokio runtime.
    pub fn spawn(self) {
        tokio::spawn(async move {
            self.run().await;
        });
    }

    /// Main loop executed by the Shadow Logistics Worker.
    pub async fn run(self) {
        let fd = match self.target {
            TargetStream::Stdout => 1,
            TargetStream::Stderr => 2,
        };

        let mut record_batch = Vec::with_capacity(BATCH_DRAIN_SIZE);
        let mut write_buf = Vec::with_capacity(WRITE_BUFFER_CAPACITY);
        let flush_interval = Duration::from_millis(50);

        while !self.shutdown.load(Ordering::Relaxed) {
            record_batch.clear();
            let count = self
                .ring_buffer
                .drain_shards(&mut record_batch, BATCH_DRAIN_SIZE);

            if count == 0 {
                tokio::time::sleep(flush_interval).await;
                continue;
            }

            write_buf.clear();
            for record in &record_batch {
                match self.format {
                    GrpcLogFormat::Json => record.write_json(&mut write_buf),
                    GrpcLogFormat::Text => record.write_text(&mut write_buf),
                }

                if write_buf.len() >= WRITE_BUFFER_CAPACITY - 1024 {
                    Self::write_to_fd(fd, &write_buf);
                    write_buf.clear();
                }
            }

            if !write_buf.is_empty() {
                Self::write_to_fd(fd, &write_buf);
                write_buf.clear();
            }
        }

        // Final shutdown drain
        record_batch.clear();
        while self
            .ring_buffer
            .drain_shards(&mut record_batch, BATCH_DRAIN_SIZE)
            > 0
        {
            write_buf.clear();
            for record in &record_batch {
                match self.format {
                    GrpcLogFormat::Json => record.write_json(&mut write_buf),
                    GrpcLogFormat::Text => record.write_text(&mut write_buf),
                }
            }
            if !write_buf.is_empty() {
                Self::write_to_fd(fd, &write_buf);
                write_buf.clear();
            }
            record_batch.clear();
        }
    }

    /// Synchronous helper to drain currently enqueued records into a memory buffer (used in tests).
    pub fn drain_to_buffer(&self, out: &mut Vec<u8>) -> usize {
        let mut record_batch = Vec::new();
        let count = self.ring_buffer.drain_shards(&mut record_batch, 1024);
        for record in &record_batch {
            match self.format {
                GrpcLogFormat::Json => record.write_json(out),
                GrpcLogFormat::Text => record.write_text(out),
            }
        }
        count
    }

    #[inline(always)]
    fn write_to_fd(fd: i32, bytes: &[u8]) {
        #[cfg(unix)]
        unsafe {
            let mut written = 0;
            while written < bytes.len() {
                let res = libc::write(
                    fd,
                    bytes[written..].as_ptr() as *const libc::c_void,
                    bytes.len() - written,
                );
                if res > 0 {
                    written += res as usize;
                } else {
                    break;
                }
            }
        }

        #[cfg(not(unix))]
        {
            use std::io::Write;
            if fd == 1 {
                let _ = std::io::stdout().write_all(bytes);
            } else {
                let _ = std::io::stderr().write_all(bytes);
            }
        }
    }
}
