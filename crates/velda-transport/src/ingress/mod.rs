//! Ingress traffic reception and classification.

pub mod classifier;
pub mod listener;

pub use classifier::{PathKind, classify_bytes, peek_and_classify};
pub use listener::{IngressBinding, IngressListener};
