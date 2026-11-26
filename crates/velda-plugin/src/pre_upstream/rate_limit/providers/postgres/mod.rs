//! PostgreSQL State-of-Truth (`velda_etp::postgres::PostgresEtp`) adapter for `rate_limit`.
//!
//! ```text
//!   providers/postgres/
//!     ├─► mod.rs    (module root export)
//!     ├─► wire.rs   (PG wire v3 Simple & Extended Query framing)
//!     ├─► schema.rs (velda.velda_rate_limit DDL bootstrap)
//!     ├─► client.rs (Extended Query CTE atomic ON CONFLICT UPSERT RETURNING cnt)
//!     └─► sync.rs   (Shadow Worker drain -> push/reconcile -> health flag)
//! ```

pub mod client;
pub mod schema;
pub mod sync;
pub mod wire;
