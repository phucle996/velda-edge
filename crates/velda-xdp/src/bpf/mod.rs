//! # eBPF & XDP Program Management
//!
//! Owns loading, attaching, and lifecycle management of XDP filter programs,
//! XSK maps, and BPF sockmaps.

pub mod loader;
pub mod sockmap;
pub mod xsk_map;

pub use loader::{XdpProgramAttachment, attach_xdp_program, detach_xdp_program};
pub use sockmap::BpfSockMap;
pub use xsk_map::BpfXskMap;
