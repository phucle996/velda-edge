//! Kafka Log Shipper Background Plugin (`phase: "background"`).

pub mod client;
pub mod compress;
pub mod config;
pub mod format;
pub mod plugin;
pub mod protocol;
pub mod validation;

pub use config::KafkaLogShipperConfig;
pub use plugin::KafkaLogShipperPlugin;
