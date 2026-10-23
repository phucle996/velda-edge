//! Pre-Route Web Application Firewall Lite Submodule (`waf_lite`).

pub mod dictionary;
pub mod plugin;
pub mod scanner;

pub use dictionary::ThreatVector;
pub use plugin::{WafLitePlugin, WafMode, WafScope};
