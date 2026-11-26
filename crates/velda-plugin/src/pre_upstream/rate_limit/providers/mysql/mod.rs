//! MySQL / MariaDB State-of-Truth (`velda_etp::mysql::MysqlEtp`) adapter for `rate_limit`.
//!
//! ```text
//!   providers/mysql/
//!     ├─► mod.rs    (module root export)
//!     ├─► wire.rs   (COM_QUERY & COM_STMT_PREPARE/EXECUTE binary codec)
//!     ├─► schema.rs (velda.velda_rate_limit DDL bootstrap)
//!     ├─► client.rs (COM_STMT CTE ON DUPLICATE KEY UPDATE + SELECT cnt)
//!     └─► sync.rs   (Shadow Worker drain -> push/reconcile -> health flag)
//! ```

pub mod client;
pub mod schema;
pub mod sync;
pub mod wire;
