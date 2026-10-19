//! Response Header Mutation Post-Response Plugin Module (`response_header_mutation`).

pub mod plugin;
pub mod variable;

pub use plugin::{ResponseHeaderMutationConfig, ResponseHeaderMutationPlugin};
pub use variable::{ResponseDynamicVar, ResponseHeaderValueSpec};
