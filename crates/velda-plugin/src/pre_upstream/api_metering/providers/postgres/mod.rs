//! PostgreSQL (`velda_etp::postgres::PostgresEtp`) adapter for `api_metering`.
//!
//! ```text
//!   providers/postgres/
//!     ├─► mod.rs    (module root)
//!     ├─► wire.rs   (PostgreSQL Wire v3 'Q'/'P'/'B'/'E'/'S' & 'D'/'E'/'Z' codec)
//!     ├─► schema.rs (idempotent velda.velda_api_metering DDL)
//!     ├─► client.rs (CTE consumed-credit accumulator workflow)
//!     └─► sync.rs   (Shadow Worker tick loop)
//! ```

pub mod client;
pub mod schema;
pub mod sync;
pub mod wire;
