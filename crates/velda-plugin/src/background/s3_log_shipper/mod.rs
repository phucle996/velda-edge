//! S3-Compatible Object Storage Background Log Shipper (`s3_log_shipper`).
//!
//! ```text
//!   TelemetryStore ──► format_ndjson ──► wire::upload_batch (&mut S3WireState)
//!                                               │
//!                                               ▼
//!                                     S3Etp::acquire() ──► S3Lease (Slot[idx])
//!
//!   Module Boundaries:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ config.rs | validation.rs | format.rs | sigv4.rs | wire.rs | plugin.rs     │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

pub mod config;
pub mod format;
pub mod plugin;
pub mod sigv4;
pub mod validation;
pub mod wire;

pub use config::{S3Compression, S3LogShipperConfig};
pub use plugin::S3LogShipperPlugin;
pub use validation::validate_s3_log_shipper_config;
