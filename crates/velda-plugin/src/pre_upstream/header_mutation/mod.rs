//! Header Mutation Plugin Module (`header_mutation`).

pub mod plugin;
pub mod rule;
pub mod variable;

pub use plugin::{HeaderMutationConfig, HeaderMutationPlugin};
pub use rule::strip_hop_by_hop_headers;
pub use variable::{DynamicVar, HeaderValueSpec};
