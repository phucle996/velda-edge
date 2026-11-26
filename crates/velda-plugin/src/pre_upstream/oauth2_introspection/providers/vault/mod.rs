//! HashiCorp Vault / OpenBao Token & OIDC Introspection Provider (`providers::vault`).
//!
//! ```text
//!   providers::vault
//!        ├─ client.rs   ──► VaultEtp StreamLease HTTP/1.1 Keep-Alive (+ AppRole 403 retry)
//!        └─ response.rs ──► Vault `data` / OIDC JSON parse, policy eval, direct cache commit
//! ```

pub mod client;
pub mod response;

pub use response::{execute, revalidate};
