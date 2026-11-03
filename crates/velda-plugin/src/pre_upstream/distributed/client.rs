//! High-Performance Non-Blocking Redis RESP Client for Shadow Logistics.
//!
//! # Objective & Mission
//! Provides an ultra-lightweight, zero-overhead Redis client tailored exclusively
//! for asynchronous batched delta pipelining (`HINCRBY`) on background shadow threads.
//!
//! # Hardware & Performance Invariants
//! - Zero Hot-Path Invariant: Used ONLY by background Shadow Logistics Workers; NEVER called
//!   by frontline traffic workers serving customer requests.
//! - Pipelined Round-Trip: Consolidates hundreds of tenant quota/credit updates into a single
//!   TCP network frame using standard Redis Serialization Protocol (RESP2).
//! - Non-Blocking Resilient Reconnect: Automatically falls back and retries connection on
//!   network partitions without panicking or blocking data plane execution.

use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

/// Lightweight Redis client connection dedicated to batched sync.
pub struct RedisClient {
    addr: SocketAddr,
    stream: Option<TcpStream>,
    buf: Vec<u8>,
    read_buf: Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
pub enum RedisError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Operation timed out")]
    Timeout,
    #[error("Protocol error: {0}")]
    Protocol(String),
    #[error("Connection disconnected")]
    Disconnected,
}

impl RedisClient {
    /// Creates a new uninitialized client pointing to the target Redis address.
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            stream: None,
            buf: Vec::with_capacity(8192),
            read_buf: Vec::with_capacity(8192),
        }
    }

    /// Establishes connection to Redis if not currently connected.
    pub async fn ensure_connected(&mut self) -> Result<(), RedisError> {
        if self.stream.is_some() {
            return Ok(());
        }

        let stream = match timeout(Duration::from_millis(1000), TcpStream::connect(self.addr)).await
        {
            Ok(Ok(s)) => {
                let _ = s.set_nodelay(true);
                s
            }
            Ok(Err(e)) => return Err(RedisError::Io(e)),
            Err(_) => return Err(RedisError::Timeout),
        };

        self.stream = Some(stream);
        Ok(())
    }

    /// Executes a batch of `HINCRBY key field delta` commands within a single pipelined TCP payload.
    ///
    /// Returns an array of updated 64-bit integer values returned by Redis for each command.
    pub async fn pipelined_hincrby(
        &mut self,
        commands: &[(&str, &str, i64)],
    ) -> Result<Vec<i64>, RedisError> {
        if commands.is_empty() {
            return Ok(Vec::new());
        }

        self.ensure_connected().await?;

        // 1. Format all commands into a single contiguous RESP buffer
        self.buf.clear();
        for &(key, field, delta) in commands {
            let delta_str = itoa::Buffer::new().format(delta).to_string();

            // *4\r\n$7\r\nHINCRBY\r\n${key.len}\r\n{key}\r\n${field.len}\r\n{field}\r\n${delta.len}\r\n{delta}\r\n
            self.buf.extend_from_slice(b"*4\r\n$7\r\nHINCRBY\r\n$");
            self.buf
                .extend_from_slice(itoa::Buffer::new().format(key.len()).as_bytes());
            self.buf.extend_from_slice(b"\r\n");
            self.buf.extend_from_slice(key.as_bytes());
            self.buf.extend_from_slice(b"\r\n$");
            self.buf
                .extend_from_slice(itoa::Buffer::new().format(field.len()).as_bytes());
            self.buf.extend_from_slice(b"\r\n");
            self.buf.extend_from_slice(field.as_bytes());
            self.buf.extend_from_slice(b"\r\n$");
            self.buf
                .extend_from_slice(itoa::Buffer::new().format(delta_str.len()).as_bytes());
            self.buf.extend_from_slice(b"\r\n");
            self.buf.extend_from_slice(delta_str.as_bytes());
            self.buf.extend_from_slice(b"\r\n");
        }

        // 2. Transmit pipelined batch in a single write operation
        let stream = match self.stream.as_mut() {
            Some(s) => s,
            None => return Err(RedisError::Disconnected),
        };

        if let Err(e) = stream.write_all(&self.buf).await {
            self.stream = None;
            return Err(RedisError::Io(e));
        }

        // 3. Read back pipelined integer responses
        let mut results = Vec::with_capacity(commands.len());
        self.read_buf.clear();

        let mut read_scratch = [0u8; 4096];
        while results.len() < commands.len() {
            let n = match timeout(Duration::from_millis(2000), stream.read(&mut read_scratch)).await
            {
                Ok(Ok(0)) => {
                    self.stream = None;
                    return Err(RedisError::Disconnected);
                }
                Ok(Ok(n)) => n,
                Ok(Err(e)) => {
                    self.stream = None;
                    return Err(RedisError::Io(e));
                }
                Err(_) => {
                    self.stream = None;
                    return Err(RedisError::Timeout);
                }
            };

            self.read_buf.extend_from_slice(&read_scratch[..n]);

            // Parse available responses from read buffer
            let mut cursor = 0;
            while cursor < self.read_buf.len() && results.len() < commands.len() {
                let remaining = &self.read_buf[cursor..];
                if remaining.is_empty() {
                    break;
                }

                // Check for newline
                if let Some(pos) = remaining.windows(2).position(|w| w == b"\r\n") {
                    let line = &remaining[..pos];
                    cursor += pos + 2;

                    if line.starts_with(b":") {
                        // Integer response :<number>
                        if let Ok(num_str) = std::str::from_utf8(&line[1..]) {
                            let val = num_str.parse::<i64>().unwrap_or(0);
                            results.push(val);
                        }
                    } else if line.starts_with(b"-") {
                        // Error response -ERR ...
                        let err_msg = std::str::from_utf8(&line[1..]).unwrap_or("unknown error");
                        tracing::warn!(
                            error = err_msg,
                            "Redis returned error for command in batch"
                        );
                        results.push(0);
                    }
                } else {
                    // Incomplete line, await next chunk
                    break;
                }
            }

            if cursor > 0 {
                self.read_buf.drain(..cursor);
            }
        }

        Ok(results)
    }
}
