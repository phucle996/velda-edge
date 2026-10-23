//! CSRF Protection Plugin (`phase: "pre_upstream"`).

pub mod plugin;

pub use plugin::CsrfPlugin;
pub use velda_sync::post_sync::plugin::pre_upstream::csrf::CsrfConfig;
