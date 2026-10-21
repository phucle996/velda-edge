//! Request Mirroring (Traffic Shadowing) Pre-Upstream Submodule (`request_mirror`).

pub mod plugin;

pub use plugin::{
    MirroredRequest, RequestMirrorConfig, RequestMirrorPlugin, get_mirror_sender, init_mirror_queue,
};
