//! Auth0 Introspection & UserInfo Provider (`providers::auth0`).
//!
//! ```text
//!   providers::auth0
//!        ├─ client.rs   ──► Auth0Etp StreamLease HTTP/1.1 Keep-Alive (GET /userinfo | POST custom)
//!        └─ response.rs ──► Auth0 JSON decode, policy eval, direct cache commit
//! ```

pub mod client;
pub mod response;

pub use response::{execute, revalidate};
