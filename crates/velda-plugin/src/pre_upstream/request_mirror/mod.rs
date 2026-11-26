//! Request Mirroring (Traffic Shadowing) Pre-Upstream Submodule (`request_mirror`).
//!
//! ```text
//!   validate_request_mirror_config() ─► RequestMirrorConfig + MethodMask + threshold_u64
//!   RequestMirrorPlugin::from_config(cfg, upstream_ids) ─► dense mirror_upstream_id: u16
//!   on_upstream_request(req, targets: &mut u32) ─► targets[31..16] = mirror_upstream_id
//! ```

pub mod config;
pub mod plugin;
pub mod validation;

pub use config::RequestMirrorConfig;
pub use plugin::RequestMirrorPlugin;
