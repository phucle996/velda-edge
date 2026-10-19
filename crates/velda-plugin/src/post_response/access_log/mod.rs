//! Post-Response Access Log Submodule (`access_log`).

pub mod plugin;

pub use plugin::{AccessLogConfig, AccessLogPlugin, LogFormat, LogRecord};
