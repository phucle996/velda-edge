//! S3-Compatible Object Storage Background Log Shipper.

pub mod client;
pub mod config;
pub mod format;
pub mod plugin;
pub mod sigv4;
pub mod validation;

pub use config::{S3AddressingStyle, S3Compression, S3LogShipperConfig};
pub use plugin::S3LogShipperPlugin;
