//! Kafka Log Shipper Background Plugin (`kafka_log_shipper`).
//!
//! ```text
//!   TelemetryStore ──► RecordBatch v2 ──► KafkaWireState::produce_records()
//!                                                │
//!                                                ▼
//!                                     KafkaEtp::acquire() ──► KafkaLease (Slot[idx])
//!
//!   Module Boundaries:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ config.rs | validation.rs | compress.rs | format/ | protocol.rs            │
//!   │ wire.rs (KafkaWireState + KafkaEtp/KafkaLease) | plugin.rs (coordinator)   │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

pub mod compress;
pub mod config;
pub mod format;
pub mod plugin;
pub mod protocol;
pub mod validation;
pub mod wire;

pub use config::KafkaLogShipperConfig;
pub use plugin::KafkaLogShipperPlugin;
pub use validation::validate_kafka_log_shipper_config;
