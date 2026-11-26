//! GCP Memorystore Cluster (`CRC16` shard grouping, parallel RTT + `-MOVED`/`-ASK`)
//! executor (`cluster.rs`).
//!
//! ```text
//!   push_cluster()
//!     ├── ensure_initialized() -> crc16_slot(key) -> BTreeMap<node_idx, group>
//!     ├── 1 shard: push_cluster_shard_batch() | >1 shard: parallel tokio::spawn
//!     └── resolve KeyRedirect::Moved / KeyRedirect::Ask up to MAX_CLUSTER_REDIRECTS
//! ```

use std::collections::BTreeMap;
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::time::Duration;

use velda_etp::EtpError;
use velda_etp::gcp_memorystore::{GcpMemorystoreEtp, GcpMemorystoreLease, crc16_slot};

use super::codec::{
    KeyRedirect, SubItemOutcome, effective_prefix_bytes, encode_shard_group,
    encode_single_redirect, parse_redirect, parse_resp_int, read_resp_line, write_rl_key,
};
use crate::pre_upstream::rate_limit::arena::{DirtyKeyDelta, ReconciledClusterTotal};

const MAX_CLUSTER_REDIRECTS: usize = 2;

pub(crate) async fn push_cluster(
    etp: &Arc<GcpMemorystoreEtp>,
    key_prefix: &str,
    deltas: &[DirtyKeyDelta],
    io_timeout: Duration,
) -> Result<Vec<ReconciledClusterTotal>, EtpError> {
    if let Some(cluster) = etp.cluster() {
        cluster.ensure_initialized().await?;
    }

    let prefix = effective_prefix_bytes(key_prefix);
    let mut shard_groups: BTreeMap<usize, Vec<(Vec<u8>, DirtyKeyDelta)>> = BTreeMap::new();

    for &delta in deltas {
        let mut key_buf = Vec::with_capacity(prefix.len() + 40);
        write_rl_key(prefix, &delta, &mut key_buf);
        let slot = crc16_slot(&key_buf);
        let node_idx = etp.cluster_node_for_slot(slot).unwrap_or(0);
        shard_groups
            .entry(node_idx)
            .or_default()
            .push((key_buf, delta));
    }

    let mut out = Vec::with_capacity(deltas.len());
    let mut redirects = Vec::new();

    if shard_groups.len() <= 1 {
        if let Some((node_idx, group)) = shard_groups.into_iter().next() {
            let sub = push_cluster_shard_batch(etp, node_idx, group, io_timeout).await?;
            for item in sub {
                match item {
                    SubItemOutcome::Done(t) => out.push(t),
                    SubItemOutcome::Redirect {
                        delta,
                        key,
                        redirect,
                    } => redirects.push((delta, key, redirect)),
                }
            }
        }
    } else {
        let mut handles = Vec::with_capacity(shard_groups.len());
        for (node_idx, group) in shard_groups {
            let etp_c = Arc::clone(etp);
            handles.push(tokio::spawn(async move {
                push_cluster_shard_batch(&etp_c, node_idx, group, io_timeout).await
            }));
        }
        for handle in handles {
            let sub = handle.await.map_err(|e| {
                EtpError::Io(Error::other(format!("Cluster shard task join: {e}")))
            })??;
            for item in sub {
                match item {
                    SubItemOutcome::Done(t) => out.push(t),
                    SubItemOutcome::Redirect {
                        delta,
                        key,
                        redirect,
                    } => redirects.push((delta, key, redirect)),
                }
            }
        }
    }

    let mut redir_wire = Vec::with_capacity(256);
    for (delta, key, mut redirect) in redirects {
        let mut resolved = None;
        for _ in 0..MAX_CLUSTER_REDIRECTS {
            match redirect {
                KeyRedirect::Moved { slot, ref addr } => {
                    let next_node = etp.report_moved(slot, addr)?;
                    let lease = etp.acquire_for_node(next_node).await?;
                    match execute_single_key_redirect(
                        lease,
                        &key,
                        delta,
                        false,
                        io_timeout,
                        &etp.config.id,
                        &mut redir_wire,
                    )
                    .await?
                    {
                        SubItemOutcome::Done(total) => {
                            resolved = Some(total);
                            break;
                        }
                        SubItemOutcome::Redirect { redirect: next, .. } => redirect = next,
                    }
                }
                KeyRedirect::Ask { ref addr } => {
                    let lease = etp.acquire_for_addr(addr).await?;
                    match execute_single_key_redirect(
                        lease,
                        &key,
                        delta,
                        true,
                        io_timeout,
                        &etp.config.id,
                        &mut redir_wire,
                    )
                    .await?
                    {
                        SubItemOutcome::Done(total) => {
                            resolved = Some(total);
                            break;
                        }
                        SubItemOutcome::Redirect { redirect: next, .. } => redirect = next,
                    }
                }
            }
        }
        let final_total = resolved.ok_or_else(|| {
            EtpError::Io(Error::other(format!(
                "GcpMemorystore Cluster '{}' exceeded max redirect hops ({MAX_CLUSTER_REDIRECTS}) for key '{}'",
                etp.config.id,
                String::from_utf8_lossy(&key)
            )))
        })?;
        out.push(final_total);
    }

    Ok(out)
}

async fn push_cluster_shard_batch(
    etp: &Arc<GcpMemorystoreEtp>,
    node_idx: usize,
    group: Vec<(Vec<u8>, DirtyKeyDelta)>,
    io_timeout: Duration,
) -> Result<Vec<SubItemOutcome>, EtpError> {
    let mut wire = Vec::with_capacity(group.len() * 144);
    encode_shard_group(&group, &mut wire);

    let mut lease = etp.acquire_for_node(node_idx).await?;
    let mut last_err = EtpError::Timeout {
        id: etp.config.id.clone(),
        timeout_ms: u64::try_from(io_timeout.as_millis()).unwrap_or(u64::MAX),
    };

    for attempt in 0..2 {
        let res: Result<Result<Vec<SubItemOutcome>, EtpError>, _> =
            tokio::time::timeout(io_timeout, async {
                lease.write_all(&wire).await?;
                lease.flush().await?;

                let mut items = Vec::with_capacity(group.len());
                for (key, delta) in &group {
                    let set_reply = read_resp_line(&mut lease).await?;
                    let incr_reply = read_resp_line(&mut lease).await?;
                    if let Some(redir) =
                        parse_redirect(&set_reply).or_else(|| parse_redirect(&incr_reply))
                    {
                        items.push(SubItemOutcome::Redirect {
                            delta: *delta,
                            key: key.clone(),
                            redirect: redir,
                        });
                        continue;
                    }
                    if set_reply.starts_with('-') {
                        return Err(EtpError::Io(Error::new(
                            ErrorKind::InvalidData,
                            format!("GcpMemorystore Cluster SET NX error: {set_reply}"),
                        )));
                    }
                    if incr_reply.starts_with('-') {
                        return Err(EtpError::Io(Error::new(
                            ErrorKind::InvalidData,
                            format!("GcpMemorystore Cluster INCRBY error: {incr_reply}"),
                        )));
                    }
                    let total = parse_resp_int(&incr_reply)?;
                    items.push(SubItemOutcome::Done(ReconciledClusterTotal {
                        key_hash: delta.key_hash,
                        tier: delta.tier,
                        pushed_delta: delta.delta,
                        window_epoch: delta.window_epoch,
                        cluster_total: total,
                    }));
                }
                Ok(items)
            })
            .await;

        match res {
            Ok(Ok(items)) => return Ok(items),
            Ok(Err(err)) => {
                last_err = err;
                if attempt == 0 && lease.reconnect().await.is_ok() {
                    continue;
                }
                lease.recreate_in_background();
                return Err(last_err);
            }
            Err(_) => {
                lease.recreate_in_background();
                return Err(last_err);
            }
        }
    }

    Err(last_err)
}

async fn execute_single_key_redirect(
    mut lease: GcpMemorystoreLease,
    key: &[u8],
    delta: DirtyKeyDelta,
    send_asking: bool,
    io_timeout: Duration,
    id: &str,
    wire_buf: &mut Vec<u8>,
) -> Result<SubItemOutcome, EtpError> {
    encode_single_redirect(key, delta.delta, send_asking, wire_buf);

    let timeout_ms = u64::try_from(io_timeout.as_millis()).unwrap_or(u64::MAX);
    let res: Result<Result<SubItemOutcome, EtpError>, _> =
        tokio::time::timeout(io_timeout, async {
            lease.write_all(wire_buf).await?;
            lease.flush().await?;

            if send_asking {
                let ask1 = read_resp_line(&mut lease).await?;
                if ask1.starts_with('-') {
                    return Err(EtpError::Io(Error::other(format!(
                        "GcpMemorystore ASKING error: {ask1}"
                    ))));
                }
            }
            let set_reply = read_resp_line(&mut lease).await?;

            if send_asking {
                let ask2 = read_resp_line(&mut lease).await?;
                if ask2.starts_with('-') {
                    return Err(EtpError::Io(Error::other(format!(
                        "GcpMemorystore ASKING error: {ask2}"
                    ))));
                }
            }
            let incr_reply = read_resp_line(&mut lease).await?;

            if let Some(redir) = parse_redirect(&set_reply).or_else(|| parse_redirect(&incr_reply))
            {
                return Ok(SubItemOutcome::Redirect {
                    delta,
                    key: key.to_vec(),
                    redirect: redir,
                });
            }
            if set_reply.starts_with('-') {
                return Err(EtpError::Io(Error::new(
                    ErrorKind::InvalidData,
                    format!("GcpMemorystore redirect SET NX error: {set_reply}"),
                )));
            }
            if incr_reply.starts_with('-') {
                return Err(EtpError::Io(Error::new(
                    ErrorKind::InvalidData,
                    format!("GcpMemorystore redirect INCRBY error: {incr_reply}"),
                )));
            }
            let total = parse_resp_int(&incr_reply)?;
            Ok(SubItemOutcome::Done(ReconciledClusterTotal {
                key_hash: delta.key_hash,
                tier: delta.tier,
                pushed_delta: delta.delta,
                window_epoch: delta.window_epoch,
                cluster_total: total,
            }))
        })
        .await;

    match res {
        Ok(Ok(outcome)) => Ok(outcome),
        Ok(Err(err)) => {
            lease.recreate_in_background();
            Err(err)
        }
        Err(_) => {
            lease.recreate_in_background();
            Err(EtpError::Timeout {
                id: id.to_string(),
                timeout_ms,
            })
        }
    }
}
