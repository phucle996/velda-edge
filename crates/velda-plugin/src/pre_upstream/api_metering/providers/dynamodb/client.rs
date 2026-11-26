//! AWS DynamoDB `api_metering` UpdateItem workflow (`client.rs`).
//!
//! ```text
//!   flush_deltas(&etp, key_prefix, deltas)
//!     ├─► pre-baked table/expression prefix chunks + JSON-escaped prefix once/batch
//!     └─► per delta: BakedWire UpdateItem JSON ─► DynamoDbSigV4Signer (kSigning cache)
//! ```

use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::time::Duration;

use velda_core::lut::decimal::fast_write_u64_slice;
use velda_etp::EtpError;
use velda_etp::dynamodb::DynamoDbEtp;

use crate::pre_upstream::api_metering::providers::dynamodb::sigv4::{
    DynamoDbSigV4Signer, read_http1_response,
};
use crate::wire_baked::{BakedWire, json_escape_bytes};

const DDB_UPDATE_PREFIX: &[u8] = br#"{"TableName":"velda_api_metering","Key":{"pk":{"S":""#;
const DDB_UPDATE_MID: &[u8] = br##"","}},"UpdateExpression":"ADD #c :d SET #u = :u","ExpressionAttributeNames":{"#c":"consumed","#u":"updated_at"},"ExpressionAttributeValues":{":d":{"N":""##;
const DDB_UPDATE_U_PRE: &[u8] = br#"","},":u":{"N":""#;
const DDB_UPDATE_TAIL: &[u8] = br#""}},"ReturnValues":"UPDATED_NEW"}"#;

/// Pushes account credit consumption deltas to DynamoDB `UpdateItem`.
pub async fn flush_deltas(
    etp: &Arc<DynamoDbEtp>,
    key_prefix: &str,
    deltas: &[(u64, i64)],
    io_timeout: Duration,
) -> Result<Vec<i64>, EtpError> {
    if deltas.is_empty() {
        return Ok(Vec::new());
    }

    let now_unix = velda_core::clock::coarse_sec();
    let mut now_buf = [0u8; 24];
    let now_len = fast_write_u64_slice(&mut now_buf, now_unix);
    let now_bytes = &now_buf[..now_len];

    let escaped_prefix = json_escape_bytes(key_prefix);
    let mut signer = DynamoDbSigV4Signer::new(&etp.config);
    let mut lease = etp.acquire().await?;
    let mut resp_buf = Vec::with_capacity(2048);
    let mut body_buf = Vec::with_capacity(384);
    let mut creq_buf = Vec::with_capacity(320);
    let mut req_buf = Vec::with_capacity(896);
    let mut last_err = EtpError::Timeout {
        id: etp.config.id.clone(),
        timeout_ms: u64::try_from(io_timeout.as_millis()).unwrap_or(u64::MAX),
    };

    for attempt in 0..2 {
        let res: Result<Result<Vec<i64>, EtpError>, _> = tokio::time::timeout(io_timeout, async {
            let mut out = Vec::with_capacity(deltas.len());
            for &(acc_hash, delta) in deltas {
                {
                    let mut bw = BakedWire::cleared(&mut body_buf, 384);
                    bw.push_chunk(DDB_UPDATE_PREFIX);
                    bw.push_chunk(&escaped_prefix);
                    bw.push_chunk(b":metering:");
                    bw.push_u64(acc_hash);
                    bw.push_chunk(DDB_UPDATE_MID);
                    bw.push_i64(delta);
                    bw.push_chunk(DDB_UPDATE_U_PRE);
                    bw.push_chunk(now_bytes);
                    bw.push_chunk(DDB_UPDATE_TAIL);
                }

                signer.write_signed_request(
                    b"DynamoDB_20120810.UpdateItem",
                    &body_buf,
                    now_unix,
                    &mut creq_buf,
                    &mut req_buf,
                );
                lease.write_all(&req_buf).await?;
                lease.flush().await?;

                let (status, resp_body) = read_http1_response(&mut lease, &mut resp_buf).await?;
                if !(200..300).contains(&status) {
                    return Err(EtpError::Io(Error::other(format!(
                        "DynamoDB UpdateItem returned HTTP {status}: {}",
                        String::from_utf8_lossy(&resp_body)
                    ))));
                }
                let parsed: serde_json::Value =
                    serde_json::from_slice(&resp_body).map_err(|e| {
                        EtpError::Io(Error::new(
                            ErrorKind::InvalidData,
                            format!("DynamoDB invalid JSON response: {e}"),
                        ))
                    })?;
                let consumed = parsed
                    .get("Attributes")
                    .and_then(|a| a.get("consumed"))
                    .and_then(|c| c.get("N"))
                    .and_then(|n| n.as_str())
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(delta);
                out.push(consumed);
            }
            Ok(out)
        })
        .await;

        match res {
            Ok(Ok(vals)) => return Ok(vals),
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
