//! OpenTelemetry Log Shipper Background Plugin (`otlp_log_shipper`).
//!
//! ```text
//!   TelemetryStore ──► JSON / Protobuf Wire Encode ──► wire::push_logs(&etp, ...)
//!                                                             │
//!                                                             ▼
//!                                               OtlpEtp::acquire() ──► OtlpLease
//!
//!   Module Boundaries:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ config.rs | validation.rs | json/ | proto/ | wire.rs | plugin.rs           │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

pub mod config;
pub mod json;
pub mod plugin;
pub mod proto;
pub mod validation;
pub mod wire;

pub use config::OtlpLogShipperConfig;
pub use plugin::OtlpLogShipperPlugin;
pub use validation::validate_otlp_log_shipper_config;
