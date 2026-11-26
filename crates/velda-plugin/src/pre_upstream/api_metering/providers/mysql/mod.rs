//! MySQL (`velda_etp::mysql::MysqlEtp`) adapter for `api_metering`.
//!
//! ```text
//!   providers/mysql/
//!     ├─► mod.rs    (module root)
//!     ├─► wire.rs   (COM_QUERY + COM_STMT_PREPARE/EXECUTE/CLOSE + binary decoder)
//!     ├─► schema.rs (idempotent velda.velda_api_metering DDL)
//!     ├─► client.rs (CTE consumed-credit accumulator workflow)
//!     └─► sync.rs   (Shadow Worker tick loop)
//! ```

pub mod client;
pub mod schema;
pub mod sync;
pub mod wire;
