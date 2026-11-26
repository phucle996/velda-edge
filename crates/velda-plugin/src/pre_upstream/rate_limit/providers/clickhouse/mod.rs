//! ClickHouse State-of-Truth (`velda_etp::clickhouse::ClickHouseEtp`) adapter for `rate_limit`.
//!
//! ```text
//!   providers/clickhouse/
//!     ├─► mod.rs    (module root export)
//!     ├─► http.rs   (X-ClickHouse-* auth headers + HTTP/1.1 response reader)
//!     ├─► schema.rs (velda_rate_limits SummingMergeTree DDL bootstrap)
//!     ├─► client.rs (HTTP/1.1 TabSeparated INSERT + SELECT sum(delta))
//!     └─► sync.rs   (Shadow Worker drain -> push/reconcile -> health flag)
//! ```

pub mod client;
pub mod http;
pub mod schema;
pub mod sync;
