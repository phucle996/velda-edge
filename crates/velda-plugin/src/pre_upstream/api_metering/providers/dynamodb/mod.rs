//! AWS DynamoDB (`velda_etp::dynamodb::DynamoDbEtp`) adapter for `api_metering`.
//!
//! ```text
//!   providers/dynamodb/
//!     ├─► mod.rs    (module root)
//!     ├─► sigv4.rs  (AWS4-HMAC-SHA256 signer + HTTP/1.1 response reader)
//!     ├─► schema.rs (DescribeTable / CreateTable "velda_api_metering")
//!     ├─► client.rs (UpdateItem ADD consumed workflow)
//!     └─► sync.rs   (Shadow Worker tick loop)
//! ```

pub mod client;
pub mod schema;
pub mod sigv4;
pub mod sync;
