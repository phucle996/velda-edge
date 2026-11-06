//! # Canonical Pipeline Classification: Acceleration Pipe (Hardware Kernel-Bypass)
//!
//! Unlike **Standard Bridge Pipes** (`*_udp_tcp`) that bridge downstream UDP datagrams to the Linux kernel
//! TCP stack, this is an **Acceleration Bridge Pipe** forwarding requests natively over `velda-rdma`
//! hardware Verbs (RoCEv2 / InfiniBand), bypassing the OS kernel entirely on the upstream leg.
//!
//! ### Activation Conditions (Điều kiện kích hoạt)
//! 1. **Upstream Configuration**: Route resolves to an upstream backend with `protocol.transport = "rdma"`.
//! 2. **Hardware & Driver Availability**: Host possesses an RDMA-capable NIC (`libibverbs.so.1`) or runs in mock simulation mode.
//! 3. **Backend Target Protocol**: Target physical endpoint responds to OOB QP handshakes and speaks HTTP/2 over RDMA.
//! 4. **Pre-warmed Core Lane State**: Ready connection leased in `< 10ns` from the local core lane shard pre-populated by Shadow Worker.
//! 5. **Pipe Pattern Match**: Upstream strategy explicitly configured as `Duplex` (Bidirectional streaming).
//!
//! Self-contained pipeline adhering to the Hourglass Architecture & Flat Workflow.

use std::sync::Arc;

use velda_core::{L7Request, L7Response};
use velda_grpc::tcp::client::request::GrpcClientRequest;
use velda_grpc::{GrpcConfig, GrpcError};

use crate::upstream::grpc::GrpcRdmaUpstream;

/// Serves a bidirectional streaming gRPC request over UDP bridged to an RDMA backend.
pub async fn serve(
    req: &L7Request,
    upstream: &Arc<GrpcRdmaUpstream>,
    config: &GrpcConfig,
) -> Result<L7Response, GrpcError> {
    let client_req = GrpcClientRequest::from_l7(req);

    let mut client = upstream.acquire(config).await.map_err(|e| {
        tracing::warn!(
            error = %e,
            upstream = %upstream.id(),
            "Failed to acquire upstream gRPC over RDMA connection"
        );
        GrpcError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            e.to_string(),
        ))
    })?;

    client
        .invoke_client_request(&client_req, config)
        .await
        .map(|resp| resp.into_l7())
}
