//! Persistent Output Stream Sink (`std_log_shipper::sink`).
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────────────────────────┐
//! │                              PERSISTENT LOG SINK ARCHITECTURE                          │
//! │                                                                                        │
//! │   TargetStream Configuration                                                           │
//! │        │                                                                               │
//! │        ├── Stdout ──► tokio::io::Stdout (Asynchronous standard output handle)          │
//! │        ├── Stderr ──► tokio::io::Stderr (Asynchronous standard error handle)           │
//! │        └── File   ──► tokio::fs::File   (Persistent open descriptor: O(1) startup open)│
//! │                                                                                        │
//! │   Periodic Batch Flush Cycle (Every 50-100ms):                                         │
//! │   ┌────────────────────────────────────────────────────────────────────────────────┐   │
//! │   │ Batched out_buf (Contiguous Spliced UTF-8 Bytes)                               │   │
//! │   │    │                                                                           │   │
//! │   │    ▼                                                                           │   │
//! │   │ sink.write_all(&out_buf).await ──► Single direct OS write syscall (No open/close)│ │
//! │   │ sink.flush().await             ──► Flushes buffered user-space bytes           │   │
//! │   └────────────────────────────────────────────────────────────────────────────────┘   │
//! └────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use tokio::fs::File;
use tokio::io::{self, AsyncWriteExt, Stderr, Stdout};

use super::config::TargetStream;
use crate::error::PluginError;

/// Persistent output stream sink for formatted standard access logs.
///
/// Holds open async handles to `stdout`, `stderr`, or a persistent filesystem `File`
/// descriptor across the entire plugin lifecycle, completely eliminating per-cycle
/// open/write/close file descriptor churn.
pub enum StdLogSink {
    Stdout(Stdout),
    Stderr(Stderr),
    File(File),
}

impl StdLogSink {
    /// Initializes a persistent sink instance matching the declared [`TargetStream`].
    ///
    /// For file targets, executes a single `open(O_CREAT | O_APPEND)` system call at startup
    /// and maintains the descriptor open indefinitely.
    pub async fn init(target: &TargetStream) -> Result<Self, PluginError> {
        match target {
            TargetStream::Stdout => Ok(Self::Stdout(io::stdout())),
            TargetStream::Stderr => Ok(Self::Stderr(io::stderr())),
            TargetStream::File(path) => {
                let file = tokio::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                    .await?;
                Ok(Self::File(file))
            }
        }
    }

    /// Alias for [`Self::init`].
    pub async fn new(target: &TargetStream) -> Result<Self, PluginError> {
        Self::init(target).await
    }

    /// Asynchronously writes raw bytes to the underlying stream and flushes.
    #[inline(always)]
    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), std::io::Error> {
        match self {
            Self::Stdout(s) => {
                s.write_all(buf).await?;
                s.flush().await?;
            }
            Self::Stderr(s) => {
                s.write_all(buf).await?;
                s.flush().await?;
            }
            Self::File(f) => {
                f.write_all(buf).await?;
                f.flush().await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_file_sink_persistent_append() {
        let temp_dir = tempfile::tempdir().unwrap();
        let log_file = temp_dir.path().join("test_access.log");
        let target = TargetStream::File(log_file.clone());

        let mut sink = StdLogSink::init(&target).await.expect("Must open sink");

        sink.write_all(b"line 1\n").await.expect("Write 1 ok");
        sink.write_all(b"line 2\n").await.expect("Write 2 ok");

        let content = tokio::fs::read_to_string(&log_file).await.expect("Read ok");
        assert_eq!(content, "line 1\nline 2\n");
    }

    #[tokio::test]
    async fn test_stdout_stderr_sink_init() {
        let mut stdout_sink = StdLogSink::init(&TargetStream::Stdout).await.unwrap();
        assert!(stdout_sink.write_all(b"").await.is_ok());

        let mut stderr_sink = StdLogSink::init(&TargetStream::Stderr).await.unwrap();
        assert!(stderr_sink.write_all(b"").await.is_ok());
    }
}
