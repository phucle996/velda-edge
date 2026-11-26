//! Standalone single-endpoint pipelined counter upsert (`standalone.rs`) for Valkey.
//!
//! ```text
//!   push_standalone() ──► etp.acquire() ──► BakedWire SET EX 15 NX + INCRBY (1 RTT) -> totals
//! ```

use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::time::Duration;

use velda_etp::EtpError;
use velda_etp::valkey::ValkeyEtp;

use super::codec::{
    KeyRedirect, build_pipelined_set_nx_incrby, parse_redirect, parse_resp_int, read_resp_line,
};
use crate::pre_upstream::rate_limit::arena::{DirtyKeyDelta, ReconciledClusterTotal};

pub(crate) async fn push_standalone(
    etp: &Arc<ValkeyEtp>,
    key_prefix: &str,
    deltas: &[DirtyKeyDelta],
    io_timeout: Duration,
) -> Result<Vec<ReconciledClusterTotal>, EtpError> {
    let mut wire = Vec::with_capacity(deltas.len() * 144);
    build_pipelined_set_nx_incrby(key_prefix, deltas, &mut wire);
    let mut lease = etp.acquire().await?;

    let mut last_err = EtpError::Timeout {
        id: etp.config.id.clone(),
        timeout_ms: u64::try_from(io_timeout.as_millis()).unwrap_or(u64::MAX),
    };

    for attempt in 0..2 {
        let res: Result<Result<Vec<ReconciledClusterTotal>, EtpError>, _> =
            tokio::time::timeout(io_timeout, async {
                lease.write_all(&wire).await?;
                lease.flush().await?;

                let mut out = Vec::with_capacity(deltas.len());
                for d in deltas {
                    let set_reply = read_resp_line(&mut lease).await?;
                    let incr_reply = read_resp_line(&mut lease).await?;
                    if let Some(KeyRedirect::Moved { slot, addr }) =
                        parse_redirect(&set_reply).or_else(|| parse_redirect(&incr_reply))
                    {
                        return Err(etp.report_moved(slot, &addr).unwrap_err());
                    }
                    if set_reply.starts_with('-') {
                        return Err(EtpError::Io(Error::new(
                            ErrorKind::InvalidData,
                            format!("Valkey SET NX error: {set_reply}"),
                        )));
                    }
                    if incr_reply.starts_with('-') {
                        return Err(EtpError::Io(Error::new(
                            ErrorKind::InvalidData,
                            format!("Valkey INCRBY error: {incr_reply}"),
                        )));
                    }
                    let total = parse_resp_int(&incr_reply)?;
                    out.push(ReconciledClusterTotal {
                        key_hash: d.key_hash,
                        tier: d.tier,
                        pushed_delta: d.delta,
                        window_epoch: d.window_epoch,
                        cluster_total: total,
                    });
                }
                Ok(out)
            })
            .await;

        match res {
            Ok(Ok(totals)) => return Ok(totals),
            Ok(Err(err)) => {
                if matches!(err, EtpError::Config(_)) {
                    lease.recreate_in_background();
                    return Err(err);
                }
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
