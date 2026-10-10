//! Bidirectional byte forwarding for L4 stream proxying.

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

use crate::connection::Connection;
use crate::error::{Result, TransportError};

/// Transfer statistics for bidirectional stream forwarding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TransferStats {
    /// Bytes transmitted from client to server (upstream).
    pub client_to_server_bytes: u64,
    /// Bytes transmitted from server (upstream) to client.
    pub server_to_client_bytes: u64,
}

impl TransferStats {
    /// Creates a new transfer statistics record.
    #[inline]
    pub const fn new(client_to_server_bytes: u64, server_to_client_bytes: u64) -> Self {
        Self {
            client_to_server_bytes,
            server_to_client_bytes,
        }
    }

    /// Returns the total bytes transferred across both directions.
    #[inline]
    pub const fn total_bytes(&self) -> u64 {
        self.client_to_server_bytes + self.server_to_client_bytes
    }
}

/// Forwards bytes bidirectionally between two asynchronous streams using [`tokio::io::copy_bidirectional_with_sizes`].
pub async fn forward_bidirectional<C, S>(
    client: &mut C,
    server: &mut S,
    buffer_size: usize,
) -> Result<TransferStats>
where
    C: AsyncRead + AsyncWrite + Unpin + ?Sized,
    S: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    let (client_to_server, server_to_client) =
        tokio::io::copy_bidirectional_with_sizes(client, server, buffer_size, buffer_size)
            .await
            .map_err(TransportError::Forward)?;

    Ok(TransferStats::new(client_to_server, server_to_client))
}

#[cfg(target_os = "linux")]
struct SplicePipe {
    read_fd: libc::c_int,
    write_fd: libc::c_int,
}

#[cfg(target_os = "linux")]
impl SplicePipe {
    fn new(pipe_size: usize) -> std::io::Result<Self> {
        let mut fds = [0 as libc::c_int; 2];
        let ret = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_NONBLOCK | libc::O_CLOEXEC) };
        if ret != 0 {
            return Err(std::io::Error::last_os_error());
        }

        let pipe = Self {
            read_fd: fds[0],
            write_fd: fds[1],
        };

        if pipe_size > 0 {
            unsafe {
                libc::fcntl(pipe.write_fd, libc::F_SETPIPE_SZ, pipe_size as libc::c_int);
            }
        }

        Ok(pipe)
    }
}

#[cfg(target_os = "linux")]
impl Drop for SplicePipe {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.read_fd);
            libc::close(self.write_fd);
        }
    }
}

#[cfg(target_os = "linux")]
async fn splice_stream_to_stream(
    from: &TcpStream,
    to: &TcpStream,
    pipe: &SplicePipe,
    chunk_size: usize,
    last_activity_ms: Option<&std::sync::atomic::AtomicU64>,
    start_instant: std::time::Instant,
) -> std::io::Result<u64> {
    use std::os::fd::AsRawFd;
    let from_fd = from.as_raw_fd();
    let to_fd = to.as_raw_fd();
    let mut total_bytes = 0u64;

    loop {
        // Read from source socket into pipe write end
        let n = from
            .async_io(tokio::io::Interest::READABLE, || {
                let res = unsafe {
                    libc::splice(
                        from_fd,
                        std::ptr::null_mut(),
                        pipe.write_fd,
                        std::ptr::null_mut(),
                        chunk_size,
                        libc::SPLICE_F_MOVE | libc::SPLICE_F_NONBLOCK,
                    )
                };
                if res < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(res as usize)
            })
            .await?;

        if n == 0 {
            break;
        }

        if let Some(act) = last_activity_ms {
            act.store(
                start_instant.elapsed().as_millis() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
        }

        // Drain entire pipe into destination socket
        let mut written = 0;
        while written < n {
            let m = to
                .async_io(tokio::io::Interest::WRITABLE, || {
                    let res = unsafe {
                        libc::splice(
                            pipe.read_fd,
                            std::ptr::null_mut(),
                            to_fd,
                            std::ptr::null_mut(),
                            n - written,
                            libc::SPLICE_F_MOVE | libc::SPLICE_F_NONBLOCK,
                        )
                    };
                    if res < 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(res as usize)
                })
                .await?;

            if m == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "zero bytes spliced to target socket",
                ));
            }
            written += m;
        }

        total_bytes += n as u64;
    }

    Ok(total_bytes)
}

/// Bidirectional zero-copy byte streaming between two TCP sockets using Linux `splice(2)`.
///
/// Keeps data entirely within kernel pipe buffers without copying into user-space RAM.
#[cfg(target_os = "linux")]
async fn splice_bidirectional(
    client: &mut TcpStream,
    server: &mut TcpStream,
    chunk_size: usize,
    last_activity_ms: Option<&std::sync::atomic::AtomicU64>,
    start_instant: std::time::Instant,
) -> std::io::Result<TransferStats> {
    use std::os::fd::AsRawFd;
    let client_fd = client.as_raw_fd();
    let server_fd = server.as_raw_fd();

    let pipe_c2s = SplicePipe::new(chunk_size)?;
    let pipe_s2c = SplicePipe::new(chunk_size)?;

    let (c2s_res, s2c_res) = tokio::join!(
        async {
            let res = splice_stream_to_stream(
                client,
                server,
                &pipe_c2s,
                chunk_size,
                last_activity_ms,
                start_instant,
            )
            .await;
            if res.is_ok() {
                unsafe {
                    libc::shutdown(server_fd, libc::SHUT_WR);
                }
            }
            res
        },
        async {
            let res = splice_stream_to_stream(
                server,
                client,
                &pipe_s2c,
                chunk_size,
                last_activity_ms,
                start_instant,
            )
            .await;
            if res.is_ok() {
                unsafe {
                    libc::shutdown(client_fd, libc::SHUT_WR);
                }
            }
            res
        }
    );

    let client_to_server = c2s_res?;
    let server_to_client = s2c_res?;

    Ok(TransferStats::new(client_to_server, server_to_client))
}

/// Forwards bytes bidirectionally between an active [`Connection`] and an upstream [`TcpStream`]
/// using the specified buffer size and an optional idle timeout enforced via a low-overhead Sleeping Watchdog.
///
/// On Linux, attempts kernel-space zero-copy splicing via `splice(2)`.
/// Gracefully falls back to asynchronous user-space copy if splicing is unavailable or unsupported.
pub async fn forward_connection(
    mut client: Connection,
    mut server: TcpStream,
    buffer_size: usize,
    idle_timeout: Option<std::time::Duration>,
) -> Result<TransferStats> {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Instant;

    let start_instant = Instant::now();
    let last_activity_ms = Arc::new(AtomicU64::new(0));

    let act_clone = last_activity_ms.clone();
    let forward_fut = async {
        #[cfg(target_os = "linux")]
        {
            match splice_bidirectional(
                client.stream_mut(),
                &mut server,
                buffer_size,
                Some(&act_clone),
                start_instant,
            )
            .await
            {
                Ok(stats) => return Ok(stats),
                Err(err) => {
                    tracing::debug!(
                        error = %err,
                        "splice_bidirectional unavailable, falling back to copy_bidirectional"
                    );
                }
            }
        }

        forward_bidirectional(client.stream_mut(), &mut server, buffer_size).await
    };

    let stats = if let Some(timeout) = idle_timeout {
        let timeout_ms = timeout.as_millis() as u64;
        let watchdog = async {
            loop {
                tokio::time::sleep(timeout).await;
                let current_elapsed_ms = start_instant.elapsed().as_millis() as u64;
                let last = last_activity_ms.load(Ordering::Relaxed);
                if current_elapsed_ms.saturating_sub(last) >= timeout_ms {
                    return Err(TransportError::Forward(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "L4 raw TCP stream idle timeout exceeded",
                    )));
                }
            }
        };

        tokio::select! {
            res = forward_fut => res?,
            err = watchdog => err?,
        }
    } else {
        forward_fut.await?
    };

    client.add_bytes_transferred(stats.client_to_server_bytes, stats.server_to_client_bytes);
    Ok(stats)
}

/// Canonical alias maintaining backwards compatibility.
pub use forward_connection as forward_connection_with_timeout;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transfer_stats() {
        let stats = TransferStats::new(1024, 2048);
        assert_eq!(stats.client_to_server_bytes, 1024);
        assert_eq!(stats.server_to_client_bytes, 2048);
        assert_eq!(stats.total_bytes(), 3072);
    }
}
