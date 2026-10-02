//! Stage 3: Post-Sync (Domain Compilation & Persistence).
//!
//! Subsystem domain compilers that independently convert raw JSON configurations
//! into optimized, validated, and checksummed binary runtime artifacts (`*.bin`).
//!
//! ### Domain Branches:
//! - **`route`**: L7 routing rules, path prefix matching, header matches, and upstream targets.
//! - **`listener`**: Network bind sockets, protocol options (HTTP/HTTPS/TCP), and TLS bindings.
//! - **`upstream`**: Backend service pools, target endpoints, load-balancing algorithms, and timeouts.
//! - **`plugin`**: Request/response lifecycle hook policies (rate limiting, auth, WAF).
//! - **`tls`**: TLS certificates, private keys, and SNI profile catalogs.
//!
//! ### Standard 5-Phase Pipeline for Each Domain Branch:
//! 1. **Phase 1: Entity & Schema Definitions** — Typed domain structs and configuration file schemas.
//! 2. **Phase 2: Ingest & Parse** (`parse_*`) — Deserializes raw JSON bytes into domain structs.
//! 3. **Phase 3: Semantic Validation** (`validate_*`) — Enforces structural and business invariants.
//! 4. **Phase 4: Binary Compilation & Unpack** (`compile_*`, `unpack_*`) — Emits `DomainHeader` + Bincode.
//! 5. **Phase 5: Atomic Persistence** (`persist_*`) — Atomically writes `config/*.json` and `runtime/*.bin`.

pub mod listener;
pub mod plugin;
pub mod policy;
pub mod route;
pub mod tls;
pub mod upstream;

pub use policy::validate_streaming_policy;
