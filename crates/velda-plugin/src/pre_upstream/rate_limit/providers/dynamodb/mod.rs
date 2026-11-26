//! AWS DynamoDB State-of-Truth (`velda_etp::dynamodb::DynamoDbEtp`) adapter for `rate_limit`.
//!
//! ```text
//!   providers/dynamodb/
//!     ├─► mod.rs    (module root export)
//!     ├─► sigv4.rs  (HMAC-SHA256 SigV4 signer + HTTP/1.1 response parser)
//!     ├─► schema.rs (velda_rate_limit DescribeTable / CreateTable DDL)
//!     ├─► client.rs (UpdateItem ADD + ReturnValues=UPDATED_NEW)
//!     └─► sync.rs   (Shadow Worker drain -> push/reconcile -> health flag)
//! ```

pub mod client;
pub mod schema;
pub mod sigv4;
pub mod sync;
