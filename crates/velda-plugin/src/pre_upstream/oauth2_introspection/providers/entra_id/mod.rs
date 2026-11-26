//! Microsoft Entra ID Introspection & OIDC UserInfo Provider (`providers::entra_id`).
//!
//! ```text
//!   providers::entra_id
//!        ├─ client.rs   ──► EntraIdEtp StreamLease HTTP/1.1 Keep-Alive (GET /oidc/userinfo | POST)
//!        └─ response.rs ──► Entra ID JSON decode, policy eval, direct cache commit
//! ```

pub mod client;
pub mod response;

pub use response::{execute, revalidate};
