//! # Post-Sync Subsystem Domains
//!
//! Autonomous, vertically isolated domain owners (`listener`, `route`, `upstream`, `plugin`, `tls`).
//! Each domain owns its full lifecycle from schema definition to binary persistence.
//!
//! ```text
//! post_sync/
//! │
//! ├── 1. Logical Dependency Tree (Cross-Domain References)
//! │   │
//! │   ├── route ────────────────► references:
//! │   │                           ├── listener (by listener ID)
//! │   │                           ├── upstream (by upstream ID)
//! │   │                           └── plugin   (by plugin ID list)
//! │   │
//! │   ├── listener ─────────────► references:
//! │   │                           └── tls      (by TLS profile name)
//! │   │
//! │   ├── upstream ─────────────► (leaf / target backend)
//! │   ├── plugin ───────────────► (leaf / policy hook)
//! │   └── tls ──────────────────► (leaf / certificate catalog)
//! │
//! └── 2. Domain Caller Tree (Submodule API Lifecycle)
//!     │
//!     ├── listener/
//!     │   ├── Types:      ListenersFile, ListenerConfig, ListenerTlsConfig, DomainHeader
//!     │   ├── [P2] Parse:    parse_listeners(&[u8]) -> Result<Vec<ListenerConfig>>
//!     │   ├── [P3] Validate: validate_listeners(&mut [ListenerConfig]) -> Result<()>
//!     │   ├── [P4] Compile:  compile_listeners_to_binary(&[ListenerConfig], rev, hash) -> Result<Vec<u8>>
//!     │   │                  unpack_listeners_from_binary(&[u8]) -> Result<(DomainHeader, Vec<ListenerConfig>)>
//!     │   └── [P5] Persist:  persist_listeners(storage_dir, raw_json, bin_bytes) -> Result<()>
//!     │
//!     ├── route/
//!     │   ├── Types:      RoutesFile, RouteConfig, RouteMatch, RouteTimeouts, DomainHeader
//!     │   ├── [P2] Parse:    parse_routes(&[u8]) -> Result<Vec<RouteConfig>>
//!     │   ├── [P3] Validate: validate_routes(&mut [RouteConfig]) -> Result<()>
//!     │   ├── [P4] Compile:  compile_routes_to_binary(&[RouteConfig], rev, hash) -> Result<Vec<u8>>
//!     │   │                  unpack_routes_from_binary(&[u8]) -> Result<(DomainHeader, Vec<RouteConfig>)>
//!     │   └── [P5] Persist:  persist_routes(storage_dir, raw_json, bin_bytes) -> Result<()>
//!     │
//!     ├── upstream/
//!     │   ├── Types:      UpstreamsFile, UpstreamConfig, EndpointConfig, LoadBalancerConfig, DomainHeader
//!     │   ├── [P2] Parse:    parse_upstreams(&[u8]) -> Result<Vec<UpstreamConfig>>
//!     │   ├── [P3] Validate: validate_upstreams(&mut [UpstreamConfig]) -> Result<()>
//!     │   ├── [P4] Compile:  compile_upstreams_to_binary(&[UpstreamConfig], rev, hash) -> Result<Vec<u8>>
//!     │   │                  unpack_upstreams_from_binary(&[u8]) -> Result<(DomainHeader, Vec<UpstreamConfig>)>
//!     │   └── [P5] Persist:  persist_upstreams(storage_dir, raw_json, bin_bytes) -> Result<()>
//!     │
//!     ├── plugin/
//!     │   ├── Types:      PluginsFile, PluginConfig, DomainHeader
//!     │   ├── [P2] Parse:    parse_plugins(&[u8]) -> Result<Vec<PluginConfig>>
//!     │   ├── [P3] Validate: validate_plugins(&mut [PluginConfig]) -> Result<()>
//!     │   ├── [P4] Compile:  compile_plugins_to_binary(&[PluginConfig], rev, hash) -> Result<Vec<u8>>
//!     │   │                  unpack_plugins_from_binary(&[u8]) -> Result<(DomainHeader, Vec<PluginConfig>)>
//!     │   └── [P5] Persist:  persist_plugins(storage_dir, raw_json, bin_bytes) -> Result<()>
//!     │
//!     └── tls/
//!         ├── Types:      TlsFile, TlsProfileConfig, CertificateFiles, DomainHeader
//!         ├── [P2] Parse:    parse_tls(&[u8]) -> Result<TlsFile>
//!         ├── [P3] Validate: validate_tls(&mut TlsFile) -> Result<()>
//!         ├── [P4] Compile:  compile_tls_to_binary(&TlsFile, rev, hash) -> Result<Vec<u8>>
//!         │                  unpack_tls_from_binary(&[u8]) -> Result<(DomainHeader, TlsFile)>
//!         └── [P5] Persist:  persist_tls(storage_dir, raw_json, bin_bytes) -> Result<()>
//! ```

pub mod listener;
pub mod plugin;
pub mod route;
pub mod tls;
pub mod upstream;
