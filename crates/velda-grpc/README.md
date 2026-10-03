# velda-grpc

Dedicated high-performance Layer 7 gRPC Protocol Engine for the Velda Edge Data Plane.

> [!IMPORTANT]
> **Subsystem Invariant**: `gRPC is gRPC; HTTP is HTTP`.
> `velda-grpc` is completely decoupled from HTTP crates. It owns length-prefixed message (LPM) framing, canonical status code handling, downstream HTTP/2 connection lifecycles, and bidirectional streaming proxying.
> **Zero dynamic protocol sniffing on the request serving hot path.**

---

## 1. Subsystem Architecture

`velda-grpc` implements full client and server proxying across all 4 canonical RPC patterns:
- **Unary RPC**: Single request $\rightarrow$ Single response (with multiplexed client connection reuse).
- **Server Streaming RPC**: Single request $\rightarrow$ Continuous stream of response frames.
- **Client Streaming RPC**: Continuous stream of request frames $\rightarrow$ Single response.
- **Bidirectional Streaming RPC**: Full-duplex asynchronous pipe between downstream and upstream.

```text
Downstream Client                      Velda Edge                       Upstream Backend
 (H2 Connection)                   (velda-grpc Engine)                   (gRPC Service)
        │                                   │                                   │
        │─── HEADERS (path=/s.S/M) ────────►│                                   │
        │─── DATA (5-byte LPM Frame) ──────►│ (pipe strategy execution)         │
        │                                   │─── HEADERS (path=/s.S/M) ────────►│
        │                                   │─── DATA (5-byte LPM Frame) ──────►│
        │                                   │                                   │
        │                                   │◄── HEADERS (grpc-status=0) ───────│
        │                                   │◄── DATA (Response Frame) ─────────│
        │◄── HEADERS + DATA + TRAILERS ─────│                                   │
```

---

## 2. Core Modules

| Module | Scope & Responsibility |
| :--- | :--- |
| [`server/`](src/server/) | Downstream HTTP/2 ingress: accepts connections, decodes request streams, provides [`GrpcResponder`]. |
| [`client/`](src/client/) | Upstream egress: [`GrpcUpstreamConnector`] managing persistent HTTP/2 client connections & unary calls. |
| [`pipe/`](src/pipe/) | Protocol forwarding pipelines for 4 streaming modes ([`pipe_buffered`], [`pipe_server_stream`], [`pipe_client_stream`], [`pipe_duplex`]). |
| [`frame.rs`](src/frame.rs) | 5-byte Length-Prefixed Message (LPM) encoder and zero-copy decoder ([`GrpcFrame`]). |
| [`status.rs`](src/status.rs) | Canonical [`GrpcStatus`] codes (RFC/gRPC spec: `OK`, `NOT_FOUND`, `UNIMPLEMENTED`, etc.) and trailer serialization. |
| [`wire.rs`](src/wire.rs) | Constants, pseudo-headers, content-types (`application/grpc`), and wire-level protocol helpers. |

---

## 3. Invariants & Guarantees

1. **Protocol Isolation**:
   Operates strictly on dedicated gRPC ingress listeners. Does not evaluate standard HTTP routes or fall back into REST semantics.
2. **Length-Prefixed Framing (RFC)**:
   Every gRPC message is prefixed with a 5-byte header: `[1-byte compression flag][4-byte big-endian length]`.
3. **Canonical Status Trailers**:
   Status is authoritatively communicated via `grpc-status` and optional `grpc-message` in HTTP/2 trailers, never in the HTTP status line.
4. **Copy & Zero-Allocation Configuration**:
   [`GrpcConfig`] derives `Copy` and contains purely primitive limits (`max_message_size`, timeouts, window sizes) to eliminate runtime heap allocations.
