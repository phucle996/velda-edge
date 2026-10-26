//! Response Header Mutation Post-Response Plugin Module (`response_header_mutation`).

pub mod plugin;

pub use plugin::{
    ResponseDynamicVar, ResponseHeaderMutationConfig, ResponseHeaderMutationPlugin,
    ResponseHeaderValueSpec,
};
