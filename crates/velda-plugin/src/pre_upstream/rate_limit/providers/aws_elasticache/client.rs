//! AWS ElastiCache RESP2 pipelined atomic counter upsert & cluster-total readback (`client.rs`).
//!
//! ```text
//!   push_and_read_totals(&etp, key_prefix, deltas)
//!     ├─► !is_cluster() ──► standalone::push_standalone()
//!     └─►  is_cluster() ──► cluster::push_cluster()
//! ```

use std::sync::Arc;
use std::time::Duration;

use velda_etp::EtpError;
use velda_etp::aws_elasticache::AwsElastiCacheEtp;

use super::cluster::push_cluster;
use super::standalone::push_standalone;
use crate::pre_upstream::rate_limit::arena::{DirtyKeyDelta, ReconciledClusterTotal};

/// Atomically initializes (if absent with TTL) and increments each key's epoch counter in
/// AWS ElastiCache Standalone or Cluster without Lua scripts.
pub async fn push_and_read_totals(
    etp: &Arc<AwsElastiCacheEtp>,
    key_prefix: &str,
    deltas: &[DirtyKeyDelta],
    io_timeout: Duration,
) -> Result<Vec<ReconciledClusterTotal>, EtpError> {
    if deltas.is_empty() {
        return Ok(Vec::new());
    }

    if !etp.is_cluster() {
        return push_standalone(etp, key_prefix, deltas, io_timeout).await;
    }

    push_cluster(etp, key_prefix, deltas, io_timeout).await
}
