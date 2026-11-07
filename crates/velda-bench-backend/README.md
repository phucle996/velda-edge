# velda-bench-backend

High-Performance Real-Stream Multi-Protocol Benchmark Backend for the Velda Edge Data Plane.

---

## 1. Overview

`velda-bench-backend` is an autonomous mock upstream service used in end-to-end integration tests, load testing, and streaming benchmark suites. It supports all major edge protocols across plain text, TLS, and QUIC transports:

- **HTTP/1.1**: Port 8081 (Plaintext TCP), Port 8441 (TLS)
- **HTTP/2**: Port 8082 (Cleartext H2C), Port 8442 (TLS ALPN `h2`)
- **HTTP/3**: Port 8443 (QUIC UDP ALPN `h3`)
- **gRPC**: Canonical RPC methods over HTTP/2 binary framing and UDP
- **L4 TCP & UDP**: Echo, streaming, and throughput verification

---

## 2. Supported Streaming Pipeline Strategies

Each protocol router serves the 4 canonical streaming pipeline strategies:

| Strategy | Downstream Request | Upstream Response | Validation Focus |
|---|---|---|---|
| **`buffered`** | Fully buffered payload | Fully buffered response | Small payload latencies, header transformation |
| **`server_stream`** | Single request | Streaming chunks/events | SSE, long-polling, chunked downloads |
| **`client_stream`** | Streaming request body | Single response | Chunked uploads, ingestion pipelines |
| **`duplex`** | Bidirectional streams | Bidirectional streams | Full-duplex proxying, WebSockets, gRPC bidi |

---

## 3. Running the Backend

```bash
# Run locally in development
cargo run -p velda-bench-backend

# Check and lint
cargo check -p velda-bench-backend
cargo clippy -p velda-bench-backend -- -D warnings
```
