//! Redis-compatible RESP2 pipelined atomic counter upsert & cluster-total readback (`client.rs`).
//!
//! ```text
//!   push_and_read_totals(&etp, key_prefix, deltas)
//!     ├─► !is_cluster() ──► standalone::push_standalone()
//!     └─►  is_cluster() ──► cluster::push_cluster()
//! ```

use std::sync::Arc;
use std::time::Duration;

use velda_etp::EtpError;
use velda_etp::redis::RedisEtp;

use super::cluster::push_cluster;
use super::standalone::push_standalone;
use crate::pre_upstream::rate_limit::arena::{DirtyKeyDelta, ReconciledClusterTotal};

/// Atomically initializes (if absent with TTL) and increments each key's epoch counter in
/// a single pipelined RTT per Master shard, returning authoritative cluster totals.
pub async fn push_and_read_totals(
    etp: &Arc<RedisEtp>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pre_upstream::rate_limit::providers::redis::codec::build_pipelined_set_nx_incrby;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use velda_etp::redis::crc16_slot;
    use velda_etp::{EtpEntryConfig, EtpKind, EtpPoolSettings, EtpTransportSecurity};

    #[tokio::test]
    async fn test_pipelined_set_nx_incrby_handles_new_and_existing_keys() {
        let deltas = vec![
            DirtyKeyDelta {
                key_hash: 0x0000_0000_0000_0001,
                tier: 0,
                delta: 5,
                window_epoch: 42,
            },
            DirtyKeyDelta {
                key_hash: 0x0000_0000_0000_0002,
                tier: 1,
                delta: 7,
                window_epoch: 42,
            },
        ];
        let mut wire_bytes = Vec::new();
        build_pipelined_set_nx_incrby("velda:rl:", &deltas, &mut wire_bytes);
        let wire = String::from_utf8(wire_bytes).unwrap();
        assert!(wire.contains(
            "$3\r\nSET\r\n$30\r\nvelda:rl:0:0000000000000001:42\r\n$1\r\n0\r\n$2\r\nEX\r\n$2\r\n15\r\n$2\r\nNX\r\n"
        ));
        assert!(
            wire.contains("$6\r\nINCRBY\r\n$30\r\nvelda:rl:1:0000000000000002:42\r\n$1\r\n7\r\n")
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let srv = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 2048];
            let _ = sock.read(&mut buf).await.unwrap();
            let _ = sock.write_all(b"+OK\r\n:5\r\n$-1\r\n:19\r\n").await;
        });

        let entry = EtpEntryConfig {
            id: "redis-test".into(),
            kind: EtpKind::Redis,
            security: EtpTransportSecurity::Plain,
            pool: EtpPoolSettings {
                pool_size: 2,
                connect_timeout_ms: 500,
                idle_timeout_ms: 5000,
            },
            tls: None,
            settings: serde_json::json!({ "host": "127.0.0.1", "port": port }),
        };
        let pool = Arc::new(RedisEtp::from_entry(&entry).unwrap());
        let totals = push_and_read_totals(&pool, "velda:rl:", &deltas, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(totals.len(), 2);
        assert_eq!(totals[0].cluster_total, 5);
        assert_eq!(totals[1].cluster_total, 19);

        srv.abort();
    }

    #[tokio::test]
    async fn test_cluster_multi_shard_batch_with_moved_redirect() {
        let l0 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let p0 = l0.local_addr().unwrap().port();
        let l1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let p1 = l1.local_addr().unwrap().port();

        let d_a = DirtyKeyDelta {
            key_hash: 0x1111,
            tier: 0,
            delta: 3,
            window_epoch: 100,
        };
        let d_b = DirtyKeyDelta {
            key_hash: 0x2222,
            tier: 0,
            delta: 4,
            window_epoch: 100,
        };
        let slot_a = crc16_slot(b"velda:rl:0:0000000000001111:100");
        let slot_b = crc16_slot(b"velda:rl:0:0000000000002222:100");

        let srv0 = tokio::spawn(async move {
            let slots_resp =
                format!("*1\r\n*3\r\n:0\r\n:16383\r\n*2\r\n$9\r\n127.0.0.1\r\n:{p0}\r\n");
            let moved_key_marker = "0000000000002222";
            while let Ok((mut sock, _)) = l0.accept().await {
                let slots_resp = slots_resp.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    while let Ok(n) = sock.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                        let wire = String::from_utf8_lossy(&buf[..n]);
                        if wire.contains("SLOTS") {
                            let _ = sock.write_all(slots_resp.as_bytes()).await;
                            continue;
                        }
                        let mut reply = String::new();
                        let mut cursor = wire.as_ref();
                        while let Some(set_pos) = cursor.find("$3\r\nSET\r\n") {
                            let after_set = &cursor[set_pos..];
                            let incr_pos = after_set
                                .find("$6\r\nINCRBY\r\n")
                                .unwrap_or(after_set.len());
                            let set_slice = &after_set[..incr_pos];
                            if set_slice.contains(moved_key_marker) {
                                reply.push_str(&format!("-MOVED {slot_b} 127.0.0.1:{p1}\r\n-MOVED {slot_b} 127.0.0.1:{p1}\r\n"));
                            } else {
                                reply.push_str("+OK\r\n:33\r\n");
                            }
                            cursor = &after_set[incr_pos.min(after_set.len())..];
                            if let Some(next_off) = cursor.find("$6\r\nINCRBY\r\n") {
                                cursor = &cursor[next_off + 12..];
                            }
                        }
                        let _ = sock.write_all(reply.as_bytes()).await;
                    }
                });
            }
        });

        let srv1 = tokio::spawn(async move {
            let slots_resp = format!(
                "*2\r\n*3\r\n:0\r\n:8191\r\n*2\r\n$9\r\n127.0.0.1\r\n:{p0}\r\n\
                 *3\r\n:8192\r\n:16383\r\n*2\r\n$9\r\n127.0.0.1\r\n:{p1}\r\n"
            );
            while let Ok((mut sock, _)) = l1.accept().await {
                let slots_resp = slots_resp.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    while let Ok(n) = sock.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                        let wire = String::from_utf8_lossy(&buf[..n]);
                        if wire.contains("SLOTS") {
                            let _ = sock.write_all(slots_resp.as_bytes()).await;
                        } else {
                            let _ = sock.write_all(b"+OK\r\n:77\r\n").await;
                        }
                    }
                });
            }
        });

        let entry = EtpEntryConfig {
            id: "redis-cluster-test".into(),
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
        let pool = Arc::new(RedisEtp::from_entry(&entry).unwrap());
        let totals = push_and_read_totals(&pool, "velda:rl:", &[d_a, d_b], Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(totals.len(), 2);
        let t_a = totals.iter().find(|t| t.key_hash == 0x1111).unwrap();
        let t_b = totals.iter().find(|t| t.key_hash == 0x2222).unwrap();
        assert_eq!(t_a.cluster_total, 33);
        assert_eq!(t_b.cluster_total, 77);
        let _ = (slot_a, slot_b);

        srv0.abort();
        srv1.abort();
    }
}
