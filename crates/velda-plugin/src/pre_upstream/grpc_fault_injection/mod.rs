//! gRPC Fault Injection Plugin (`grpc_fault_injection`).
//!
//! Provides enterprise-grade Chaos Engineering and Microservice Resilience Testing
//! specifically engineered for gRPC RPC traffic over TCP (HTTP/2) and UDP (HTTP/3 QUIC).

pub mod plugin;

pub use plugin::{GrpcAbortConfig, GrpcDelayConfig, GrpcFaultInjectionPlugin};
