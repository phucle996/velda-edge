//! Valkey RESP2 pipelined `HINCRBY` client (`client.rs`) supporting `standalone` and
//! `cluster` (`CRC16` + `-MOVED` / `-ASK`) topologies without Lua scripts.
//!
//! ```text
//!   flush_deltas(&etp, key, deltas)
//!     ├─► !is_cluster() ──► etp.acquire() ──► codec::send_hincrby_on_lease()
//!     └─►  is_cluster() ──► crc16_slot(key) -> etp.acquire_for_slot(slot)
//!                             └─ -MOVED / -ASK ──► report_moved() / ASKING & retry
//! ```

use std::sync::Arc;

use velda_etp::EtpError;
use velda_etp::valkey::{ValkeyEtp, crc16_slot};

use super::codec::{BatchReplyOutcome, encode_hincrby_batch, send_hincrby_on_lease};

const MAX_CLUSTER_REDIRECTS: usize = 2;

/// Pushes account credit deltas to Valkey via pipelined `HINCRBY` and returns consumed totals.
pub async fn flush_deltas(
    etp: &Arc<ValkeyEtp>,
    key: &str,
    deltas: &[(u64, i64)],
    cmd_buf: &mut Vec<u8>,
    read_buf: &mut Vec<u8>,
) -> Result<Vec<i64>, EtpError> {
    if !etp.is_cluster() {
        encode_hincrby_batch(key, deltas, false, cmd_buf);
        let mut lease = etp.acquire().await?;
        match send_hincrby_on_lease(
            &mut lease,
            &etp.config.id,
            cmd_buf,
            deltas.len(),
            false,
            read_buf,
        )
        .await?
        {
            BatchReplyOutcome::Values(v) => return Ok(v),
            BatchReplyOutcome::Moved { slot, addr } => {
                return Err(etp.report_moved(slot, &addr).unwrap_err());
            }
            BatchReplyOutcome::Ask { addr } => {
                return Err(EtpError::Config(format!(
                    "Valkey ETP '{}' received cluster -ASK {addr} while configured with mode='standalone'; set mode='cluster' in etp.json",
                    etp.config.id
                )));
            }
        }
    }

    let key_slot = crc16_slot(key.as_bytes());
    let mut lease = etp.acquire_for_slot(key_slot).await?;
    let mut with_asking = false;

    for _ in 0..=MAX_CLUSTER_REDIRECTS {
        encode_hincrby_batch(key, deltas, with_asking, cmd_buf);
        match send_hincrby_on_lease(
            &mut lease,
            &etp.config.id,
            cmd_buf,
            deltas.len(),
            with_asking,
            read_buf,
        )
        .await?
        {
            BatchReplyOutcome::Values(vals) => return Ok(vals),
            BatchReplyOutcome::Moved { slot, addr } => {
                let next_node = etp.report_moved(slot, &addr)?;
                lease = etp.acquire_for_node(next_node).await?;
                with_asking = false;
            }
            BatchReplyOutcome::Ask { addr } => {
                lease = etp.acquire_for_addr(&addr).await?;
                with_asking = true;
            }
        }
    }

    Err(EtpError::Io(std::io::Error::other(format!(
        "Valkey Cluster '{}' exceeded max redirect hops ({MAX_CLUSTER_REDIRECTS}) for metering key '{key}'",
        etp.config.id
    ))))
}
