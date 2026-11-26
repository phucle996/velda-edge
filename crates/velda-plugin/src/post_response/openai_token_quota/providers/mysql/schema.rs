//! MySQL schema & table bootstrap (`schema.rs`) for `openai_token_quota`.
//!
//! ```text
//!   ensure_table(&etp, timeout)
//!     ├── CREATE SCHEMA IF NOT EXISTS velda
//!     └── CREATE TABLE IF NOT EXISTS velda.velda_openai_token_quota (k VARCHAR(255) PK, remaining BIGINT, updated_at BIGINT)
//! ```

use std::sync::Arc;
use std::time::Duration;

use velda_etp::EtpError;
use velda_etp::mysql::MysqlEtp;

use super::wire::execute_com_query;

const MY_SCHEMA_DDL: &str = "CREATE SCHEMA IF NOT EXISTS velda";
const MY_TABLE_DDL: &str = "\
CREATE TABLE IF NOT EXISTS velda.velda_openai_token_quota (\
k VARCHAR(255) PRIMARY KEY, remaining BIGINT NOT NULL DEFAULT 0, updated_at BIGINT NOT NULL)";

/// Ensures schema `velda` and table `velda.velda_openai_token_quota` exist in MySQL.
pub async fn ensure_table(etp: &Arc<MysqlEtp>, io_timeout: Duration) -> Result<(), EtpError> {
    let mut lease = etp.acquire().await?;
    let res = tokio::time::timeout(io_timeout, async {
        execute_com_query(&mut lease, MY_SCHEMA_DDL).await?;
        execute_com_query(&mut lease, MY_TABLE_DDL).await?;
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
