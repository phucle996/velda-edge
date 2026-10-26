//! Header Mutation Plugin Module (`header_mutation`).

pub mod plugin;

pub use plugin::{
    DynamicVar, HeaderMutationConfig, HeaderMutationPlugin, HeaderValueSpec,
    is_protected_from_connection_strip, strip_hop_by_hop_headers,
};
