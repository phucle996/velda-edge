//! Layer 7 HTTP/3 Pipeline & Bridging Architecture.
//!
//! Operates strictly for listeners explicitly configured with `protocol = "http3"`.
//! Routes matched via `velda-router::Http3Router`, forwarded natively over QUIC or bridged
//! into HTTP/2 and HTTP/1.1 upstreams.
//!
//! ### Zero-Disruption Reload Invariant:
//! Active HTTP/3 QUIC state machine engines persist across configuration reloads.
//! When routes, upstreams, or plugins reload, active client QUIC connections
//! are NOT disconnected; new incoming requests on existing connections seamlessly
//! evaluate against the latest swapped runtime snapshot.

pub mod downstream;
pub mod engine;
pub mod pipe;

pub use downstream::handle_http3_udp;
pub use engine::{
    H3EngineShards, clear_h3_engines, get_or_init_h3_engine, get_or_init_h3_engine_for_peer,
    has_h3_engine, init_h3_engine,
};
