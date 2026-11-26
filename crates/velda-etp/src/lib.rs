//! Velda External Third-Party (`velda-etp`) Connection & Lifecycle Engine.
//!
//! ```text
//!   Caller / Plugin matches EtpInstance.backend (EtpBackend) directly
//!         │
//!         ├─► registry::build_registry() / reconcile(&current, new_cfgs, prewarm)
//!         │     ├─ unchanged fingerprint ──► Arc::clone(existing) [0 reconnect]
//!         │     └─ changed / new         ──► build_instance() -> warm_instance()
//!         │
//!         └─► <Kind>Etp::acquire() / warm() ──► EtpPageArena ((required_pages + 1) * 4096B)
//!               ├─ Ready hit (TZCNT) ──► borrow Slot[idx] in-place ──► <Kind>Lease
//!               └─ Miss / Burst      ──► connect.rs (LKG IP / TLS pin) ──► <Kind>Lease
//!
//!   Crate Ownership & Memory Architecture:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ Root Lifecycle: error.rs | config.rs | connect.rs | arena.rs | registry.rs │
//!   ├────────────────────────────────────────────────────────────────────────────┤
//!   │ Isolated Per-ETP Subfolders (4096B Page-Aligned Bitmap Arena & Leases):    │
//!   │   redis/ | clickhouse/ | s3/ | loki/ | elasticsearch/ | otlp/ | kafka/     │
//!   │   statsd/ (pre-connected UDP socket arena)                                 │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

pub mod arena;
pub mod config;
pub mod connect;
pub mod error;
pub mod registry;
pub mod stream_pool;

pub mod auth0;
pub mod authelia;
pub mod aws_elasticache;
pub mod aws_secrets_manager;
pub mod clickhouse;
pub mod cognito;
pub mod dragonfly;
pub mod dynamodb;
pub mod elasticsearch;
pub mod entra_id;
pub mod gcp_memorystore;
pub mod hydra;
pub mod kafka;
pub mod keycloak;
pub mod loki;
pub mod memcached;
pub mod mysql;
pub mod nats;
pub mod oidc;
pub mod okta;
pub mod otlp;
pub mod postgres;
pub mod redis;
pub mod s3;
pub mod statsd;
pub mod upstash;
pub mod valkey;
pub mod vault;

pub use config::{EtpEntryConfig, EtpKind, EtpPoolSettings};
pub use connect::{EtpTlsConfig, EtpTlsVersion, EtpTransportSecurity};
pub use error::EtpError;
pub use registry::{EtpBackend, EtpInstance, EtpRegistry};
pub use stream_pool::{StreamLease, StreamPool};
