//! Background Shadow Logistics Drainer (`std_http_access_log::drainer`).
//!
//! Owns asynchronous batch draining from SPSC ring buffers, slot-filling byte formatting,
//! and single-syscall flushing to standard output streams (`stdout` / `stderr`).
//!
//! # Architecture Invariant
//! - **Hot Path Isolation**: Draining executes strictly on background Shadow Worker threads
//!   or Tokio tasks, completely isolated from frontline traffic worker threads.
//! - **Single Syscall Batching**: Flushes entire batches (up to 64KB) in a single `libc::write` syscall.
//! - **Zero Allocation Recycling**: The 64KB scratch buffer is allocated once and reused indefinitely.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use super::config::TargetStream;
use super::record::{LogFormat, PackedLogRecord};
use super::ring_buffer::ShardedRingBuffer;

/// Background log drainer task running on Shadow Logistics workers.
pub struct ShadowLogDrainer {
    target: TargetStream,
    format: LogFormat,
    ring_buffer: Arc<ShardedRingBuffer>,
    shutdown: Arc<AtomicBool>,
}

impl ShadowLogDrainer {
    /// Constructs a new `ShadowLogDrainer`.
    pub fn new(
        target: TargetStream,
        format: LogFormat,
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

    /// Spawns the background draining loop into an active Tokio runtime.
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            self.run().await;
        })
    }

    /// Continuously drains batches and flushes to standard stream until shutdown.
    pub async fn run(self) {
        // Reusable 64KB batch write buffer (zero allocations after initialization)
        let mut write_buf = Vec::with_capacity(65536);
        let mut batch = [PackedLogRecord::empty(); 128];
        let fd = match self.target {
            TargetStream::Stdout => libc::STDOUT_FILENO,
            TargetStream::Stderr => libc::STDERR_FILENO,
        };

        while !self.shutdown.load(Ordering::Relaxed) {
            let count = self.ring_buffer.pop_all(&mut batch);
            if count > 0 {
                write_buf.clear();
                for record in &batch[..count] {
                    match self.format {
                        LogFormat::Json => record.write_json(&mut write_buf),
                        LogFormat::Text => record.write_text(&mut write_buf),
                        LogFormat::Clf => record.write_clf(&mut write_buf),
                    }
                }

                // Single syscall atomic flush to standard stream
                flush_fd(fd, &write_buf);

                // If buffer ballooned under extreme traffic, trim back to 64KB
                if write_buf.capacity() > 131072 {
                    write_buf.shrink_to(65536);
                }
            } else {
                // Yield to kernel SCHED_IDLE / sleep briefly when idle
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        // Final shutdown drain: flush any remaining records in ring buffer
        loop {
            let count = self.ring_buffer.pop_all(&mut batch);
            if count == 0 {
                break;
            }
            write_buf.clear();
            for record in &batch[..count] {
                match self.format {
                    LogFormat::Json => record.write_json(&mut write_buf),
                    LogFormat::Text => record.write_text(&mut write_buf),
                    LogFormat::Clf => record.write_clf(&mut write_buf),
                }
            }
            flush_fd(fd, &write_buf);
        }
    }

    /// Drains available records into a provided memory buffer (useful for unit testing).
    pub fn drain_to_buffer(&self, buf: &mut Vec<u8>) -> usize {
        let mut batch = [PackedLogRecord::empty(); 128];
        let mut total = 0;
        loop {
            let count = self.ring_buffer.pop_all(&mut batch);
            if count == 0 {
                break;
            }
            for record in &batch[..count] {
                match self.format {
                    LogFormat::Json => record.write_json(buf),
                    LogFormat::Text => record.write_text(buf),
                    LogFormat::Clf => record.write_clf(buf),
                }
            }
            total += count;
        }
        total
    }
}

/// Flushes a byte slice to an OS file descriptor via single libc syscall.
#[inline]
fn flush_fd(fd: libc::c_int, mut bytes: &[u8]) {
    while !bytes.is_empty() {
        let ret = unsafe { libc::write(fd, bytes.as_ptr() as *const libc::c_void, bytes.len()) };
        if ret <= 0 {
            // EPIPE, EBADF, or interrupted: break to prevent infinite hang
            break;
        }
        bytes = &bytes[ret as usize..];
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_drain_to_buffer_json() {
        let ring = Arc::new(ShardedRingBuffer::new(2, 64));
        let shutdown = Arc::new(AtomicBool::new(false));
        let drainer = ShadowLogDrainer::new(
            TargetStream::Stdout,
            LogFormat::Json,
            ring.clone(),
            shutdown,
        );

        let mut rec = PackedLogRecord::empty();
        rec.status = 200;
        rec.set_request_id(b"req-1");
        rec.set_user_agent(b"UnitTester");
        assert!(ring.try_push(&rec).is_ok());

        let mut out = Vec::new();
        let drained = drainer.drain_to_buffer(&mut out);
        assert_eq!(drained, 1);

        let s = std::str::from_utf8(&out).unwrap();
        assert!(s.contains("\"status\":200"));
        assert!(s.contains("\"request_id\":\"req-1\""));
        assert!(s.contains("\"user_agent\":\"UnitTester\""));
    }
}
