//! HashiCorp Vault / OpenBao External Third-Party (`velda-etp::vault`) pool and session auth.
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::Vault) ──► VaultEtpConfig ──► VaultEtp
//!         │
//!         ├─► warm() / acquire() -> StreamLease (&mut EtpStream)
//!         └─► resolve_token()    -> StaticToken | AppRole auto-login & renew
//! ```

pub mod config;
pub mod pool;

pub use config::{VaultAuthConfig, VaultEtpConfig};
pub use pool::VaultEtp;
