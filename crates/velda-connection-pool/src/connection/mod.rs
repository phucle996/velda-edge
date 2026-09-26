//! Connection profiles and protocol reuse semantics.
//!
//! Caller selects reuse semantics strictly through [`ConnectionProfile`] ([`profile`]):
//!
//! ```text
//!                      Caller (Upstream / Protocol Layer)
//!                                     │
//!                                     ▼
//!                       ConnectionProfile (profile.rs)
//!                                     │
//!             ┌───────────────────────┼───────────────────────┐
//!             ▼                       ▼                       ▼
//!        Exclusive               Sequential              Multiplexed
//!     (exclusive.rs)          (sequential.rs)         (multiplexed.rs)
//!     Raw TCP / Tunnel        HTTP/1.1 Keep-Alive     HTTP/2 & HTTP/3
//!     1:1 Exclusive Lease     1:1 LIFO Queue Lease    1:N Concurrent Stream Lease
//! ```
//!
//! Invariants:
//! - The pool provides and manages profile semantics; it NEVER establishes connections.
//! - The caller/creator establishes the physical socket on MISS and registers it with the pool.

pub mod exclusive;
pub mod lease;
pub mod multiplexed;
pub mod profile;
pub mod sequential;

pub use exclusive::ExclusiveLease;
pub use lease::ConnectionLease;
pub use multiplexed::{MultiplexedConnection, MultiplexedPool, StreamLease};
pub use profile::{ConnectionProfile, ReuseMode};
pub use sequential::SequentialLease;
