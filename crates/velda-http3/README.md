# velda-http3

Dedicated high-performance Layer 7 HTTP/3 Protocol Engine for the Velda Edge Data Plane (RFC 9114 & RFC 9000).

> [!IMPORTANT]
> **Subsystem Invariant**: `HTTP/3 = QUIC-Driven L7 State Machine`.
> `velda-http3` owns HTTP/3 frame encoding/decoding, QPACK static header table codecs, QUIC packet-driven event processing, and streaming pipe execution over UDP datagrams.

---

## 1. Subsystem Architecture

`velda-http3` operates as a transparent two-way ingress proxy over QUIC:

```text
Downstream Peer                        Velda Edge                        Upstream Target
 (UDP Datagrams)                   (velda-http3 Engine)                   (HTTP/3 Backend)
        │                                   │                                    │
        │─── QUIC Initial / Handshake ─────►│                                    │
        │─── QUIC 1-RTT (HEADERS Frame) ───►│ (route + pipe dispatch)            │
        │                                   │─── QUIC 1-RTT (HEADERS Frame) ────►│
        │                                   │─── QUIC 1-RTT (DATA Frames) ──────►│
        │                                   │                                    │
        │                                   │◄── QUIC 1-RTT (Response Headers) ──│
        │                                   │◄── QUIC 1-RTT (Response Data) ─────│
        │◄── Response UDP Datagrams ────────│                                    │
```

---

## 2. Core Modules

| Module | Scope & Responsibility |
| :--- | :--- |
| [`server/`](src/server/) | Downstream QUIC state machine engine: [`Http3Engine`] handling UDP datagram ingestion, stream reassembly, and outgoing responses. |
| [`client/`](src/client/) | Upstream HTTP/3 client: [`Http3Client`] managing outbound QUIC connections, flow control, and stream forwarding. |
| [`pipe/`](src/pipe/) | Stream forwarding pipelines ([`pipe_buffered`], [`pipe_server_stream`], [`pipe_client_stream`], [`pipe_duplex`]). |
| [`frame.rs`](src/frame.rs) | RFC 9114 HTTP/3 frames (`DATA`, `HEADERS`, `SETTINGS`, `GOAWAY`) and RFC 9000 varint codecs. |
| [`qpack/`](src/qpack/) | RFC 9204 QPACK encoder/decoder with static table decompression. |
| [`huffman.rs`](src/huffman.rs) | RFC 7541 / RFC 9204 canonical Huffman encoder and decoder. |
| [`config.rs`](src/config.rs) | Tunable settings ([`Http3Config`]) deriving `Copy` for zero-allocation hot-path execution. |

---

## 3. Invariants & Zero-Disruption Reload

1. **Persistent QUIC State Across Reloads**:
   HTTP/3 QUIC connection state machines persist across configuration reloads. When routes or upstreams reload, active client QUIC connections are **not** disconnected; new requests on existing connections evaluate seamlessly against the swapped snapshot.
2. **Deterministic Sharding**:
   QUIC engines are sharded by remote client socket address (`peer_addr`) to eliminate lock contention across worker threads.
3. **Strict UDP Transport**:
   HTTP/3 strictly requires UDP transport. Declaring HTTP/3 over TCP is validated and rejected during bootstrap.
