//! Standalone single-endpoint quota lease executor (`standalone.rs`) for GCP Memorystore.
//!
//! ```text
//!   flush_standalone() ──► etp.acquire() ──► codec::execute_lease_group()
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use velda_etp::gcp_memorystore::GcpMemorystoreEtp;

use super::codec::{ShardItemOutcome, execute_lease_group};

pub(crate) async fn flush_standalone(
    etp: &Arc<GcpMemorystoreEtp>,
    batch: &[(usize, String)],
    batch_i64: i64,
    batch_str: &str,
    cmd_buf: &mut Vec<u8>,
    resp_buf: &mut Vec<u8>,
    connected: &AtomicBool,
) -> Option<Vec<ShardItemOutcome>> {
    let Ok(lease) = etp.acquire().await else {
        connected.store(false, Ordering::Relaxed);
        return None;
    };
    match execute_lease_group(
        etp, lease, batch, batch_i64, batch_str, false, cmd_buf, resp_buf,
    )
    .await
    {
        Ok(res) => Some(res),
        Err(err) => {
            tracing::warn!(id = %etp.config.id, error = %err, "GcpMemorystore quota DECRBY batch failed");
            connected.store(false, Ordering::Relaxed);
            None
        }
    }
}
