//! DynamoDB table bootstrap (`schema.rs`) for `rate_limit`.
//!
//! ```text
//!   ensure_table(&etp, timeout)
//!     ├── static const DESCRIBE_BODY -> DescribeTable("velda_rate_limit")
//!     └── ResourceNotFoundException ──► static const CREATE_BODY -> CreateTable
//! ```

use std::io::Error;
use std::sync::Arc;
use std::time::Duration;

use velda_etp::EtpError;
use velda_etp::dynamodb::DynamoDbEtp;

use super::sigv4::{DynamoDbSigV4Signer, read_http1_response};

const DESCRIBE_BODY: &[u8] = b"{\"TableName\":\"velda_rate_limit\"}";
const CREATE_BODY: &[u8] = b"{\"TableName\":\"velda_rate_limit\",\"AttributeDefinitions\":[{\"AttributeName\":\"pk\",\"AttributeType\":\"S\"}],\"KeySchema\":[{\"AttributeName\":\"pk\",\"KeyType\":\"HASH\"}],\"BillingMode\":\"PAY_PER_REQUEST\"}";

/// Ensures `velda_rate_limit` exists in DynamoDB (`DescribeTable` -> `CreateTable` if missing).
pub async fn ensure_table(etp: &Arc<DynamoDbEtp>, io_timeout: Duration) -> Result<(), EtpError> {
    let now_unix = velda_core::clock::coarse_sec();
    let mut signer = DynamoDbSigV4Signer::new(&etp.config);
    let mut creq_buf = Vec::with_capacity(320);
    let mut desc_req = Vec::with_capacity(512);
    let mut create_req = Vec::with_capacity(768);
    signer.write_signed_request(
        b"DynamoDB_20120810.DescribeTable",
        DESCRIBE_BODY,
        now_unix,
        &mut creq_buf,
        &mut desc_req,
    );
    signer.write_signed_request(
        b"DynamoDB_20120810.CreateTable",
        CREATE_BODY,
        now_unix,
        &mut creq_buf,
        &mut create_req,
    );

    let mut lease = etp.acquire().await?;
    let mut resp_buf = Vec::with_capacity(2048);
    let res = tokio::time::timeout(io_timeout, async {
        lease.write_all(&desc_req).await?;
        lease.flush().await?;
        let (st, body) = read_http1_response(&mut lease, &mut resp_buf).await?;
        if (200..300).contains(&st) {
            return Ok(());
        }
        let err_s = String::from_utf8_lossy(&body);
        if !err_s.contains("ResourceNotFoundException") {
            return Err(EtpError::Io(Error::other(format!(
                "DynamoDB DescribeTable HTTP {st}: {err_s}"
            ))));
        }

        lease.write_all(&create_req).await?;
        lease.flush().await?;
        let (cst, cbody) = read_http1_response(&mut lease, &mut resp_buf).await?;
        let cerr = String::from_utf8_lossy(&cbody);
        if (200..300).contains(&cst) || cerr.contains("ResourceInUseException") {
            Ok(())
        } else {
            Err(EtpError::Io(Error::other(format!(
                "DynamoDB CreateTable HTTP {cst}: {cerr}"
            ))))
        }
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
