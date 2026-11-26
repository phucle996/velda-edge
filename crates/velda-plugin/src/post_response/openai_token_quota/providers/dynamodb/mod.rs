//! AWS DynamoDB (`velda_etp::dynamodb::DynamoDbEtp`) adapter for `openai_token_quota`.
//!
//! ```text
//!   providers/dynamodb/
//!     ├─► mod.rs    (module root export)
//!     ├─► sigv4.rs  (HMAC-SHA256 SigV4 signer + HTTP/1.1 response parser)
//!     ├─► schema.rs (velda_openai_token_quota DescribeTable / CreateTable DDL)
//!     ├─► client.rs (UpdateItem ADD -batch + compensating refund micro-quota lease)
//!     └─► sync.rs   (Shadow Worker ensure_table + refill/ticker -> flush_batch)
//! ```

pub mod client;
pub mod schema;
pub mod sigv4;
pub mod sync;
