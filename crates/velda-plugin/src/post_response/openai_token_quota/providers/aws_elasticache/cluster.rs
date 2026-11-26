//! AWS ElastiCache Cluster (`CRC16` shard grouping, parallel RTT + `-MOVED`/`-ASK`)
//! executor (`cluster.rs`).
//!
//! ```text
//!   flush_cluster()
//!     ├── ensure_initialized() -> shard_groups via crc16_slot()
//!     ├── 1 shard: direct execute_lease_group() | >1 shard: parallel tokio::spawn
//!     └── resolve LeaseRedirect::Moved / LeaseRedirect::Ask up to MAX_CLUSTER_REDIRECTS
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use velda_etp::aws_elasticache::{AwsElastiCacheEtp, crc16_slot};

use super::codec::{LeaseRedirect, ShardItemOutcome, execute_lease_group};

const MAX_CLUSTER_REDIRECTS: usize = 2;

pub(crate) async fn flush_cluster(
    etp: &Arc<AwsElastiCacheEtp>,
    batch: &[(usize, String)],
    batch_i64: i64,
    batch_str: &str,
    cmd_buf: &mut Vec<u8>,
    resp_buf: &mut Vec<u8>,
    connected: &AtomicBool,
) -> Option<Vec<ShardItemOutcome>> {
    if let Some(cluster) = etp.cluster()
        && cluster.ensure_initialized().await.is_err()
    {
        connected.store(false, Ordering::Relaxed);
        return None;
    }

    let mut shard_groups: BTreeMap<usize, Vec<(usize, String)>> = BTreeMap::new();
    for (slot_idx, key) in batch {
        let slot = crc16_slot(key.as_bytes());
        let node_idx = etp.cluster_node_for_slot(slot).unwrap_or(0);
        shard_groups
            .entry(node_idx)
            .or_default()
            .push((*slot_idx, key.clone()));
    }

    let mut gathered = Vec::with_capacity(batch.len());
    if shard_groups.len() <= 1 {
        if let Some((node_idx, group)) = shard_groups.into_iter().next() {
            let Ok(lease) = etp.acquire_for_node(node_idx).await else {
                connected.store(false, Ordering::Relaxed);
                return None;
            };
            match execute_lease_group(
                etp, lease, &group, batch_i64, batch_str, false, cmd_buf, resp_buf,
            )
            .await
            {
                Ok(res) => gathered.extend(res),
                Err(err) => {
                    tracing::warn!(id = %etp.config.id, error = %err, "AwsElastiCache Cluster quota shard batch failed");
                    connected.store(false, Ordering::Relaxed);
                    return None;
                }
            }
        }
    } else {
        let mut handles = Vec::with_capacity(shard_groups.len());
        for (node_idx, group) in shard_groups {
            let etp_c = Arc::clone(etp);
            let b_str = batch_str.to_string();
            handles.push(tokio::spawn(async move {
                let lease = etp_c.acquire_for_node(node_idx).await?;
                let mut c_buf = Vec::with_capacity(1024);
                let mut r_buf = Vec::with_capacity(1024);
                execute_lease_group(
                    &etp_c, lease, &group, batch_i64, &b_str, false, &mut c_buf, &mut r_buf,
                )
                .await
            }));
        }
        for h in handles {
            match h.await {
                Ok(Ok(sub)) => gathered.extend(sub),
                _ => {
                    connected.store(false, Ordering::Relaxed);
                    return None;
                }
            }
        }
    }

    let mut final_outcomes = Vec::with_capacity(batch.len());
    let mut redirects = Vec::new();
    for item in gathered {
        match item {
            ShardItemOutcome::Leased { slot_idx, leased } => {
                final_outcomes.push(ShardItemOutcome::Leased { slot_idx, leased });
            }
            ShardItemOutcome::Redirect {
                slot_idx,
                key,
                redirect,
            } => redirects.push((slot_idx, key, redirect)),
        }
    }

    for (slot_idx, key, mut redirect) in redirects {
        let mut resolved = 0i64;
        for _ in 0..MAX_CLUSTER_REDIRECTS {
            let (lease_res, ask_flag) = match redirect {
                LeaseRedirect::Moved { slot, ref addr } => match etp.report_moved(slot, addr) {
                    Ok(next_idx) => (etp.acquire_for_node(next_idx).await, false),
                    Err(_) => break,
                },
                LeaseRedirect::Ask { ref addr } => (etp.acquire_for_addr(addr).await, true),
            };
            let Ok(lease) = lease_res else {
                break;
            };
            let single = [(slot_idx, key.clone())];
            match execute_lease_group(
                etp, lease, &single, batch_i64, batch_str, ask_flag, cmd_buf, resp_buf,
            )
            .await
            {
                Ok(mut sub) => match sub.pop() {
                    Some(ShardItemOutcome::Leased { leased, .. }) => {
                        resolved = leased;
                        break;
                    }
                    Some(ShardItemOutcome::Redirect { redirect: next, .. }) => redirect = next,
                    None => break,
                },
                Err(_) => break,
            }
        }
        final_outcomes.push(ShardItemOutcome::Leased {
            slot_idx,
            leased: resolved,
        });
    }

    Some(final_outcomes)
}
