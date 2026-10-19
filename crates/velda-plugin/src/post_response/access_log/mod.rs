//! Access Log Plugin Module (`access_log`).

pub mod plugin;
pub mod record;

pub use plugin::{AccessLogConfig, AccessLogPlugin};
pub use record::{LogFormat, LogRecord};
