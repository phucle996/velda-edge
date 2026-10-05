//! Bidirectional byte forwarding for L4 stream proxying.

use std::net::SocketAddr;
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

/// Forwards bytes bidirectionally between two asynchronous streams using custom buffer sizes.
pub async fn forward_bidirectional_with_sizes<C, S>(
    client: &mut C,
    server: &mut S,
    client_buf_size: usize,
    server_buf_size: usize,
) -> Result<TransferStats>
where
    C: AsyncRead + AsyncWrite + Unpin + ?Sized,
    S: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    let (client_to_server, server_to_client) =
        tokio::io::copy_bidirectional_with_sizes(client, server, client_buf_size, server_buf_size)
            .await
            .map_err(TransportError::Forward)?;

    Ok(TransferStats {
        client_to_server_bytes: client_to_server,
        server_to_client_bytes: server_to_client,
    })
}

/// Forwards bytes bidirectionally between two asynchronous streams using
/// [`tokio::io::copy_bidirectional`] with automatic half-close (TCP FIN) handling.
///
/// Uses standard 64 KB buffers for high-bandwidth L4 stream forwarding.
pub async fn forward_bidirectional<C, S>(client: &mut C, server: &mut S) -> Result<TransferStats>
where
    C: AsyncRead + AsyncWrite + Unpin + ?Sized,
    S: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    forward_bidirectional_with_sizes(client, server, 65536, 65536).await
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
pub async fn splice_bidirectional(
    client: &mut TcpStream,
    server: &mut TcpStream,
    chunk_size: usize,
) -> std::io::Result<TransferStats> {
    use std::os::fd::AsRawFd;
    let client_fd = client.as_raw_fd();
    let server_fd = server.as_raw_fd();

    let pipe_c2s = SplicePipe::new(chunk_size)?;
    let pipe_s2c = SplicePipe::new(chunk_size)?;

    let (c2s_res, s2c_res) = tokio::join!(
        async {
            let res = splice_stream_to_stream(client, server, &pipe_c2s, chunk_size).await;
            if res.is_ok() {
                unsafe {
                    libc::shutdown(server_fd, libc::SHUT_WR);
                }
            }
            res
        },
        async {
            let res = splice_stream_to_stream(server, client, &pipe_s2c, chunk_size).await;
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
/// using the specified buffer size (e.g. from [`super::config::TcpListenerConfig::copy_buffer_size`]).
///
/// On Linux, attempts kernel-space zero-copy splicing via [`splice_bidirectional`].
/// Gracefully falls back to asynchronous user-space copy if splicing is unavailable or unsupported.
pub async fn forward_connection_with_size(
    mut client: Connection,
    mut server: TcpStream,
    buffer_size: usize,
) -> Result<TransferStats> {
    #[cfg(target_os = "linux")]
    {
        match splice_bidirectional(client.stream_mut(), &mut server, buffer_size).await {
            Ok(stats) => {
                client.add_bytes_transferred(
                    stats.client_to_server_bytes,
                    stats.server_to_client_bytes,
                );
                return Ok(stats);
            }
            Err(err) => {
                tracing::debug!(
                    error = %err,
                    "splice_bidirectional unavailable, falling back to copy_bidirectional"
                );
            }
        }
    }

    let stats = forward_bidirectional_with_sizes(
        client.stream_mut(),
        &mut server,
        buffer_size,
        buffer_size,
    )
    .await?;
    client.add_bytes_transferred(stats.client_to_server_bytes, stats.server_to_client_bytes);
    Ok(stats)
}

/// Forwards bytes bidirectionally between an active [`Connection`] and an upstream [`TcpStream`].
///
/// Updates the internal byte counters of the client [`Connection`].
pub async fn forward_connection(client: Connection, server: TcpStream) -> Result<TransferStats> {
    forward_connection_with_size(client, server, 65536).await
}

/// Connects to a target upstream address and pumps bytes bidirectionally with the client connection.
pub async fn connect_and_forward(
    client: Connection,
    upstream_addr: SocketAddr,
) -> Result<TransferStats> {
    let server = TcpStream::connect(upstream_addr)
        .await
        .map_err(|e| TransportError::Connect {
            addr: upstream_addr,
            source: e,
        })?;

    let _ = server.set_nodelay(true);

    forward_connection_with_size(client, server, 65536).await
}

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
