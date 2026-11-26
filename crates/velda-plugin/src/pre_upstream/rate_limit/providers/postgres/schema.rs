//! PostgreSQL schema & table bootstrap (`schema.rs`) for `rate_limit`.
//!
//! ```text
//!   ensure_table(&etp, timeout)
//!     └──► const PG_DDL_WIRE (.rodata 'Q' frame):
//!          CREATE SCHEMA IF NOT EXISTS velda;
//!          CREATE TABLE IF NOT EXISTS velda.velda_rate_limit (k TEXT PK, cnt BIGINT, exp BIGINT)
//! ```

use std::sync::Arc;
use std::time::Duration;

use velda_etp::EtpError;
use velda_etp::postgres::PostgresEtp;

use super::wire::read_until_ready;
use crate::wire_baked::{build_pg_simple_query, pg_simple_query_len};

const PG_SCHEMA_AND_TABLE_DDL: &str = "\
CREATE SCHEMA IF NOT EXISTS velda; \
CREATE TABLE IF NOT EXISTS velda.velda_rate_limit (\
k TEXT PRIMARY KEY, cnt BIGINT NOT NULL DEFAULT 0, exp BIGINT NOT NULL)";

const PG_DDL_WIRE: [u8; pg_simple_query_len(PG_SCHEMA_AND_TABLE_DDL)] =
    build_pg_simple_query(PG_SCHEMA_AND_TABLE_DDL);

/// Ensures schema `velda` and table `velda.velda_rate_limit` exist via idempotent wire v3 DDL.
pub async fn ensure_table(etp: &Arc<PostgresEtp>, io_timeout: Duration) -> Result<(), EtpError> {
    let mut lease = etp.acquire().await?;
    let res = tokio::time::timeout(io_timeout, async {
        lease.write_all(&PG_DDL_WIRE).await?;
        lease.flush().await?;
        let _ = read_until_ready(&mut lease).await?;
        Ok::<(), EtpError>(())
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
