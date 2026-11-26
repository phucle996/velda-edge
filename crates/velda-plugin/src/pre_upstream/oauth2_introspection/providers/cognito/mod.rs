//! AWS Cognito Introspection & UserInfo Provider (`providers::cognito`).
//!
//! ```text
//!   providers::cognito
//!        ├─ client.rs   ──► CognitoEtp StreamLease HTTP/1.1 Keep-Alive (GET /oauth2/userInfo | POST)
//!        └─ response.rs ──► Cognito JSON decode, policy eval, direct cache commit
//! ```

pub mod client;
pub mod response;

pub use response::{execute, revalidate};
