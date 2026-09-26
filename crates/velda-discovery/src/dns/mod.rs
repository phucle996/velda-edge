//! DNS discovery subsystem organized across 4 sequential processing phases.
//!
//! # The 4-Phase DNS Architecture
//!
//! Resolution and lifecycle management in `velda-discovery` follow a strict 4-phase pipeline:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │ Phase 1: Bootstrap (`bootstrap`)                            │
//! │ - Eagerly loads `/etc/hosts` into in-memory table.          │
//! │ - Eagerly loads `/etc/resolv.conf` nameserver definitions.  │
//! │ - Ensures zero disk I/O during serving runtime.             │
//! └──────────────────────────────┬──────────────────────────────┘
//!                                │
//!                                ▼
//! ┌─────────────────────────────────────────────────────────────┐
//! │ Phase 2: DNS Server Connect (`server`)                      │
//! │ - Manages upstream nameserver targets (IP or Host:Port).    │
//! │ - Resolves server targets via static hosts or bootstrap.    │
//! │ - Validates and establishes UDP/TCP socket connectivity.    │
//! └──────────────────────────────┬──────────────────────────────┘
//!                                │
//!                                ▼
//! ┌─────────────────────────────────────────────────────────────┐
//! │ Phase 3: Resolver (`resolver`)                              │
//! │ - Orchestrates backend A/AAAA record wire resolution.       │
//! │ - Executes nameserver sequential failover via `DnsTransport`│
//! │ - Engages Last-Known-Good (LKG) fallback on network outages.│
//! └──────────────────────────────┬──────────────────────────────┘
//!                                │
//!                                ▼
//! ┌─────────────────────────────────────────────────────────────┐
//! │ Phase 4: Cache (`cache`)                                    │
//! │ - Acts as the in-memory Single Source of Truth (`DnsCache`).│
//! │ - Enforces positive TTL for valid records.                  │
//! │ - Enforces negative TTL for NXDOMAIN protection storms.     │
//! │ - Shields hot paths with sub-microsecond zero-IO hits.      │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Runtime Hot-Path Flow
//!
//! During request processing, the pipeline executes in reverse-lookup order to maximize performance:
//! 1. **Phase 4 (Cache)** is checked first: on a cache hit, addresses return immediately with zero I/O.
//! 2. **Phase 1 (Bootstrap)** is checked on cache miss: static `/etc/hosts` entries are retrieved and populated into Phase 4 cache.
//! 3. **Phase 3 (Resolver)** executes wire queries against nameservers prepared in **Phase 2 (Server Connect)** only if both cache and hosts miss.
//! 4. Resolution results (or NXDOMAIN errors) are written back into **Phase 4 (Cache)**.

pub mod bootstrap;
pub mod cache;
pub mod resolver;
pub mod server;
pub mod transport;

// Re-exports organized by phase
pub use bootstrap::{HostsFileSource, ResolvConfServerProvider};
pub use cache::{CacheLookup, DnsCache};
pub use resolver::{DnsResolverConfig, DnsResolverProvider, DnsTransport};
pub use server::{DnsServer, DnsServerProvider, DnsServerTarget, StaticServerProvider};
pub use transport::{SystemDnsTransport, UdpDnsTransport};
