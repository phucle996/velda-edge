//! Redis RESP2 pipelined `HINCRBY` client (`client.rs`) supporting `standalone` and
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
use velda_etp::redis::{RedisEtp, crc16_slot};

use super::codec::{BatchReplyOutcome, encode_hincrby_batch, send_hincrby_on_lease};

const MAX_CLUSTER_REDIRECTS: usize = 2;

/// Pushes account credit deltas to Redis via pipelined `HINCRBY` and returns global consumed totals.
pub async fn flush_deltas(
    etp: &Arc<RedisEtp>,
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
                    "Redis ETP '{}' received cluster -ASK {addr} while configured with mode='standalone'; set mode='cluster' in etp.json",
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
        "Redis Cluster '{}' exceeded max redirect hops ({MAX_CLUSTER_REDIRECTS}) for metering key '{key}'",
        etp.config.id
    ))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use velda_etp::{EtpEntryConfig, EtpKind, EtpPoolSettings, EtpTransportSecurity};

    #[tokio::test]
    async fn test_api_metering_cluster_hincrby_and_moved_redirect() {
        let l0 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let p0 = l0.local_addr().unwrap().port();
        let l1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let p1 = l1.local_addr().unwrap().port();

        let key = "velda:metering:consumed";
        let key_slot = crc16_slot(key.as_bytes());

        let srv0 = tokio::spawn(async move {
            let slots_resp =
                format!("*1\r\n*3\r\n:0\r\n:16383\r\n*2\r\n$9\r\n127.0.0.1\r\n:{p0}\r\n");
            while let Ok((mut sock, _)) = l0.accept().await {
                let slots_resp = slots_resp.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 2048];
                    while let Ok(n) = sock.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                        let wire = String::from_utf8_lossy(&buf[..n]);
                        if wire.contains("SLOTS") {
                            let _ = sock.write_all(slots_resp.as_bytes()).await;
                        } else {
                            let count = wire.matches("HINCRBY").count();
                            let reply =
                                format!("-MOVED {key_slot} 127.0.0.1:{p1}\r\n").repeat(count);
                            let _ = sock.write_all(reply.as_bytes()).await;
                        }
                    }
                });
            }
        });

        let srv1 = tokio::spawn(async move {
            let slots_resp =
                format!("*1\r\n*3\r\n:0\r\n:16383\r\n*2\r\n$9\r\n127.0.0.1\r\n:{p1}\r\n");
            while let Ok((mut sock, _)) = l1.accept().await {
                let slots_resp = slots_resp.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 2048];
                    while let Ok(n) = sock.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                        let wire = String::from_utf8_lossy(&buf[..n]);
                        if wire.contains("SLOTS") {
                            let _ = sock.write_all(slots_resp.as_bytes()).await;
                        } else {
                            let _ = sock.write_all(b":42\r\n:84\r\n").await;
                        }
                    }
                });
            }
        });

        let entry = EtpEntryConfig {
            id: "redis-meter-cluster".into(),
            kind: EtpKind::Redis,
            security: EtpTransportSecurity::Plain,
            pool: EtpPoolSettings {
                pool_size: 2,
                connect_timeout_ms: 500,
                idle_timeout_ms: 5000,
            },
            tls: None,
            settings: serde_json::json!({
                "mode": "cluster",
                "seed_nodes": [format!("127.0.0.1:{p0}")]
            }),
        };
        let etp = Arc::new(RedisEtp::from_entry(&entry).unwrap());
        let mut cmd_buf = Vec::new();
        let mut read_buf = Vec::new();
        let out = flush_deltas(
            &etp,
            key,
            &[(1001, 10), (1002, 20)],
            &mut cmd_buf,
            &mut read_buf,
        )
        .await
        .unwrap();
        assert_eq!(out, vec![42, 84]);

        srv0.abort();
        srv1.abort();
    }
}
