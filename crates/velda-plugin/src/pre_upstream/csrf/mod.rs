//! Cross-Site Request Forgery (CSRF) Protection Plugin (`csrf`).
//!
//! ```text
//! Inbound L7Request (HTTP/1.1, HTTP/2, HTTP/3 | AccessLevel::Read, 0 alloc, 0 SystemTime)
//!   ├─► Gate 1: MethodMask (`GET | HEAD | OPTIONS | TRACE`)           -> Continue (1-inst bitmask)
//!   ├─► Gate 2: Sec-Fetch-Site (`same-origin | none | same-site | cross-site`)
//!   ├─► Gate 3: Fallback Origin / Referer vs Host/:authority (when Sec-Fetch-Site absent)
//!   └─► Gate 4: Optional Legacy Double-Submit (`cookie_name == header_name` across all COOKIE lines)
//! ```

pub mod plugin;
pub mod validation;

pub use plugin::CsrfPlugin;
pub use validation::{CsrfConfig, CsrfDoubleSubmitConfig, validate_csrf_config};
