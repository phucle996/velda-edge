//! StatsD / DogStatsD UDP Datagram Batch Wire Exchange (`statsd_exporter::wire`).
//!
//! ```text
//!   flush_snapshot(&etp, prefix, dialect, max_pkt, snapshot, &mut packet_buf)
//!         │
//!         ├─► format_statsd_snapshot() ──► packed contiguous buffer + (start, end) ranges
//!         │
//!         ▼
//!     etp.acquire().await ──► StatsdLease (Slot[idx] UdpSocket in-place)
//!         │
//!         ├─► send_batch(&mut lease, &slices)
//!         │     ├─ #[cfg(target_os = "linux")]     : writable().await + libc::sendmmsg(64)
//!         │     └─ #[cfg(not(target_os = "linux"))]: sequential lease.send().await loop
//!         │
//!         └─► Err on send ─► lease.reconnect().await (in-place Slot[idx]) + retry once
//!                                └─ still failing ─► lease.recreate_in_background()
//!
//!   Vectorized Linux `sendmmsg` Batch Frame Layout (Stack-Allocated, Zero Heap):
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ iovecs:  [libc::iovec; 64]   { iov_base: pkt.as_ptr(), iov_len: pkt.len() } │
//!   │ msgs:    [libc::mmsghdr; 64] { msg_name: null (connect'd UDP), msg_iov }   │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::sync::Arc;

use velda_etp::statsd::{StatsdEtp, StatsdLease};
use velda_observability::MetricsSnapshot;

use crate::error::PluginError;

use super::dialect::{StatsdDialect, format_statsd_snapshot};

/// Transmits MTU-chunked datagrams through a borrowed pre-connected [`StatsdLease`]
/// (`Deref<Target = tokio::net::UdpSocket>` at `Slot[idx]` of [`StatsdEtp`]).
///
/// On Linux (`target_os = "linux"`), batches up to 64 datagrams per kernel transition via
/// `libc::sendmmsg` after awaiting socket writability. On non-Linux targets, sends each datagram
/// sequentially via `lease.send().await`.
pub async fn send_batch(
    lease: &mut StatsdLease,
    packets: &[impl AsRef<[u8]>],
) -> Result<usize, PluginError> {
    if packets.is_empty() {
        return Ok(0);
    }

    #[cfg(target_os = "linux")]
    {
        send_batch_linux(lease, packets).await
    }

    #[cfg(not(target_os = "linux"))]
    {
        let mut sent = 0usize;
        for packet in packets {
            lease.send(packet.as_ref()).await?;
            sent += 1;
        }
        Ok(sent)
    }
}

#[cfg(target_os = "linux")]
async fn send_batch_linux(
    lease: &mut StatsdLease,
    packets: &[impl AsRef<[u8]>],
) -> Result<usize, PluginError> {
    use std::os::fd::AsRawFd;

    let fd = lease.as_raw_fd();
    let total = packets.len();
    let mut total_sent = 0usize;

    while total_sent < total {
        lease.writable().await?;

        let Some(chunk) = packets.get(total_sent..) else {
            break;
        };
        match try_sendmmsg_chunk(fd, chunk) {
            Ok(0) => {
                tokio::task::yield_now().await;
            }
            Ok(sent) => {
                total_sent += sent;
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                tokio::task::yield_now().await;
            }
            Err(err) => return Err(err.into()),
        }
    }

    Ok(total_sent)
}

#[cfg(target_os = "linux")]
#[inline]
fn try_sendmmsg_chunk(
    fd: std::os::fd::RawFd,
    chunk: &[impl AsRef<[u8]>],
) -> Result<usize, std::io::Error> {
    let chunk_len = chunk.len().min(64);
    let mut iovecs: [libc::iovec; 64] = unsafe { std::mem::zeroed() };
    for (i, p) in chunk
        .get(..chunk_len)
        .unwrap_or_default()
        .iter()
        .enumerate()
    {
        let slice = p.as_ref();
        if let Some(iov) = iovecs.get_mut(i) {
            iov.iov_base = slice.as_ptr() as *mut libc::c_void;
            iov.iov_len = slice.len();
        }
    }

    let mut msgs: [libc::mmsghdr; 64] = unsafe { std::mem::zeroed() };
    for (i, msg) in msgs.iter_mut().take(chunk_len).enumerate() {
        // Pre-connected UDP socket caches the destination tuple in kernel sock -> msg_name is null.
        msg.msg_hdr.msg_name = std::ptr::null_mut();
        msg.msg_hdr.msg_namelen = 0;
        msg.msg_hdr.msg_iov = iovecs
            .get_mut(i)
            .map(|v| v as *mut libc::iovec)
            .unwrap_or(std::ptr::null_mut());
        msg.msg_hdr.msg_iovlen = 1;
        msg.msg_hdr.msg_control = std::ptr::null_mut();
        msg.msg_hdr.msg_controllen = 0;
        msg.msg_hdr.msg_flags = 0;
        msg.msg_len = 0;
    }

    let res = unsafe {
        libc::sendmmsg(
            fd,
            msgs.as_mut_ptr(),
            chunk_len as libc::c_uint,
            libc::MSG_DONTWAIT,
        )
    };

    if res >= 0 {
        Ok(res as usize)
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// Formats `snapshot` into MTU-bounded packets, borrows an in-place [`StatsdLease`] from `etp`,
/// and transmits all datagrams, retrying once via in-place `lease.reconnect().await` on send error.
pub async fn flush_snapshot(
    etp: &Arc<StatsdEtp>,
    prefix: &str,
    dialect: StatsdDialect,
    max_packet_size: usize,
    snapshot: &MetricsSnapshot,
    packet_buf: &mut Vec<u8>,
) -> Result<(usize, usize), PluginError> {
    let mut packed = Vec::with_capacity(max_packet_size.saturating_mul(16).max(4096));
    let mut ranges: Vec<(usize, usize)> = Vec::with_capacity(32);

    let (packets_emitted, metrics_emitted) = format_statsd_snapshot(
        snapshot,
        prefix,
        dialect,
        packet_buf,
        max_packet_size,
        &mut |pkt| {
            let start = packed.len();
            packed.extend_from_slice(pkt);
            ranges.push((start, packed.len()));
        },
    );

    if packets_emitted == 0 || ranges.is_empty() {
        return Ok((0, metrics_emitted));
    }

    let mut slices: Vec<&[u8]> = Vec::with_capacity(ranges.len());
    for &(start, end) in &ranges {
        if let Some(slice) = packed.get(start..end) {
            slices.push(slice);
        }
    }
    if slices.is_empty() {
        return Ok((0, metrics_emitted));
    }

    let mut lease = etp
        .acquire()
        .await
        .map_err(|e| PluginError::Http(format!("StatsD ETP acquire error: {e}")))?;

    match send_batch(&mut lease, &slices).await {
        Ok(sent) => Ok((sent, metrics_emitted)),
        Err(first_err) => {
            if let Err(reconn_err) = lease.reconnect().await {
                lease.recreate_in_background();
                return Err(PluginError::Http(format!(
                    "StatsD UDP reconnect failed after send error ({first_err}): {reconn_err}"
                )));
            }
            match send_batch(&mut lease, &slices).await {
                Ok(sent) => Ok((sent, metrics_emitted)),
                Err(retry_err) => {
                    lease.recreate_in_background();
                    Err(retry_err)
                }
            }
        }
    }
}
