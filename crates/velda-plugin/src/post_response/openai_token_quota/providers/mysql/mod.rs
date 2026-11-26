//! MySQL (`velda_etp::mysql::MysqlEtp`) adapter for `openai_token_quota`.
//!
//! ```text
//!   providers/mysql/
//!     ├─► mod.rs    (module root export)
//!     ├─► wire.rs   (COM_QUERY & COM_STMT_PREPARE/EXECUTE binary codec)
//!     ├─► schema.rs (velda.velda_openai_token_quota DDL bootstrap)
//!     ├─► client.rs (Transaction + FOR UPDATE CTE micro-quota lease)
//!     └─► sync.rs   (Shadow Worker ensure_table + refill/ticker -> flush_batch)
//! ```

pub mod client;
pub mod schema;
pub mod sync;
pub mod wire;
