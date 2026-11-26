//! ClickHouse `SummingMergeTree` table bootstrap (`schema.rs`) for `rate_limit`.
//!
//! ```text
//!   ensure_table(&etp, timeout)
//!     └──► CREATE TABLE IF NOT EXISTS <db>.velda_rate_limits (k String, epoch UInt64, delta UInt64)
//! ```

use std::io::Error;
use std::sync::Arc;
use std::time::Duration;

use velda_etp::EtpError;
use velda_etp::clickhouse::ClickHouseEtp;

use super::http::{format_auth_headers, read_http1_response};

pub(crate) const TABLE_NAME: &str = "velda_rate_limits";

/// Ensures the ClickHouse SummingMergeTree table exists idempotently.
pub async fn ensure_table(etp: &Arc<ClickHouseEtp>, io_timeout: Duration) -> Result<(), EtpError> {
    let db = &etp.config.database;
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {db}.{TABLE_NAME} \
         (k String, epoch UInt64, delta UInt64) \
         ENGINE = SummingMergeTree(delta) ORDER BY (k, epoch) SETTINGS index_granularity = 8192"
    );
    let auth = format_auth_headers(&etp.config);
    let req = format!(
        "POST {}/ HTTP/1.1\r\nHost: {}\r\nContent-Type: text/plain; charset=utf-8\r\n\
         {auth}Connection: keep-alive\r\nContent-Length: {}\r\n\r\n{sql}",
        etp.config.base_path,
        etp.config.host_header,
        sql.len()
    );
    let mut lease = etp.acquire().await?;
    let mut resp_buf = Vec::with_capacity(1024);
    let res = tokio::time::timeout(io_timeout, async {
        lease.write_all(req.as_bytes()).await?;
        lease.flush().await?;
        let (status, body) = read_http1_response(&mut lease, &mut resp_buf).await?;
        if !(200..300).contains(&status) {
            return Err(EtpError::Io(Error::other(format!(
                "ClickHouse DDL HTTP {status}: {}",
                String::from_utf8_lossy(&body)
            ))));
        }
        Ok(())
    })
    .await;

    match res {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => {
            lease.recreate_in_background();
            Err(e)
        }
        Err(_) => {
            lease.recreate_in_background();
            Err(EtpError::Timeout {
                id: etp.config.id.clone(),
                timeout_ms: u64::try_from(io_timeout.as_millis()).unwrap_or(u64::MAX),
            })
        }
    }
}
