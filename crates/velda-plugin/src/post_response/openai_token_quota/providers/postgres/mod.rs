//! PostgreSQL (`velda_etp::postgres::PostgresEtp`) adapter for `openai_token_quota`.
//!
//! ```text
//!   providers/postgres/
//!     ├─► mod.rs    (module root export)
//!     ├─► wire.rs   (Postgres wire v3 Simple & Extended Query framing)
//!     ├─► schema.rs (velda.velda_openai_token_quota DDL bootstrap)
//!     ├─► client.rs (Extended Query CTE atomic FOR UPDATE micro-quota lease)
//!     └─► sync.rs   (Shadow Worker ensure_table + refill/ticker -> flush_batch)
//! ```

pub mod client;
pub mod schema;
pub mod sync;
pub mod wire;
