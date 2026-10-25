# HTTP/1.1 Ingress Pipeline Matrix (12 Specialized Pipes)

This directory documents the **12 specialized execution pipelines** for **Layer 7 HTTP/1.1 Ingress** in Velda Edge. In accordance with the **Pump & Filter Architecture (Mô hình Máy Bơm & Lưới Lọc)**, each pipeline is a self-contained, linear workflow module connecting downstream HTTP/1.1 clients to upstream backends across 3 protocol targets and 4 streaming modes.

---

## 1. The HTTP/1.1 Pipeline Matrix

```text
Downstream Ingress: HTTP/1.1 (Text Streaming / TLS)
├── 1. Buffered Mode (Full Body Ingest into RAM before Upstream Lease)
│   ├── buffered_h1_h1.md   -> Upstream HTTP/1.1 (Dedicated Keep-Alive Pool)
│   ├── buffered_h1_h2.md   -> Upstream HTTP/2 (Multiplexed Streams, 99.9% Socket Reduction)
│   └── buffered_h1_h3.md   -> Upstream HTTP/3 (QUIC over UDP, 0-RTT Resumption)
│
├── 2. Server Streaming Mode (SSE, Progressive Chunks, LLM Token Streaming)
│   ├── server_stream_h1_h1.md -> Upstream HTTP/1.1 (Chunked SSE Forwarding, TTFB < 1ms)
│   ├── server_stream_h1_h2.md -> Upstream HTTP/2 (H2 DATA Frame to Chunk Conversion)
│   └── server_stream_h1_h3.md -> Upstream HTTP/3 (QUIC Stream Frame to Chunk Conversion)
│
├── 3. Client Streaming Mode (Chunked Uploads, S3 Multipart, Log Ingestion)
│   ├── client_stream_h1_h1.md -> Upstream HTTP/1.1 (Chunked Upload Streaming, O(1) Memory)
│   ├── client_stream_h1_h2.md -> Upstream HTTP/2 (Chunked to H2 DATA Frame Stream)
│   └── client_stream_h1_h3.md -> Upstream HTTP/3 (Chunked to QUIC Stream Datagrams)
│
└── 4. Full Duplex Mode (WebSocket Upgrades, Bidirectional Streaming, Raw Tunnels)
    ├── duplex_h1_h1.md     -> Upstream HTTP/1.1 (Raw TCP Tunnel / Dual-Simplex Loop)
    ├── duplex_h1_h2.md     -> Upstream HTTP/2 (Bidirectional H1 <-> H2 Multiplexing)
    └── duplex_h1_h3.md     -> Upstream HTTP/3 (Bidirectional TCP <-> UDP QUIC Stream)
```

---

## 2. Architectural Comparison: The 4 Streaming Modalities

To avoid accidental coupling and memory bloat, Velda Edge strictly bifurcates pipeline execution into 4 distinct streaming strategies based on data flow directionality:

| Streaming Mode | Ingress Framing | Upstream Lease Timing | Memory Complexity | Primary Invariant |
| :--- | :--- | :--- | :---: | :--- |
| **`buffered`** | Complete Body in RAM | **After** Downstream Body Buffered | $O(\text{Body})$ (RAM Slab) | **Slowloris Immunity**: Upstream connection leased only for backend compute duration ($T_{\text{hold}} \approx 2\text{ ms}$). Early lease drop before downstream transmission. |
| **`server_stream`** | Body in RAM or Empty | **Before** Request Transmission | $O(1)$ ($4\text{ KiB}$ Chunk) | **TTFB Optimization & Backpressure**: Immediate downstream header write; upstream chunks streamed progressively; client disconnect halts upstream reading immediately. |
| **`client_stream`** | Progressive Chunks | **Before** Downstream Upload Starts | $O(1)$ ($64\text{ KiB}$ Slab) | **Lease Inversion & Fail-Fast**: Leases upstream socket *prior* to upload to ensure backend readiness; client upload backpressured by upstream socket buffers. |
| **`duplex`** | Full Duplex Chunks / WS | **Before** Duplex Loop Starts | $O(1)$ ($64\text{ KiB} - 256\text{ KiB}$) | **Dual-Simplex Loop**: Asynchronous upload and download tasks running concurrently; WebSocket `101 Switching Protocols` detaches HTTP framing to raw byte tunnel. |

---

## 3. Directory Index & Production Performance Reference

All empirical telemetry captured on a production Microsoft Azure HPC stress cluster:
- **VM1 (Gateway - `172.16.0.4`)**: 4 vCPUs AMD EPYC 7763, 16 GiB RAM, Accelerated Networking (SR-IOV), Linux 6.8.0.
- **VM2 (Backend - `172.16.0.5`)**: `velda-bench-backend` serving cleartext HTTP/1.1 (`:8081`), HTTP/2 H2C (`:8082`), and HTTP/3 QUIC (`:8443`).

| Pipeline Module | Strategy Identity | Target Upstream | Key Performance Metric (Azure HPC) | Micro Context Switches | Page Faults | Detailed Architecture File |
| :--- | :--- | :--- | :---: | :---: | :---: | :--- |
| **`buffered_h1_h1`** | Buffered Request-Response | HTTP/1.1 Port 8081 | **47,181 RPS** (2.00 ms avg) | **21 / 10s** | **0** | [buffered_h1_h1.md](buffered_h1_h1.md) |
| **`buffered_h1_h2`** | Buffered H1 $\to$ H2 Bridge | HTTP/2 Port 8082 | **43,892 RPS** (2.22 ms avg) | **21 / 10s** | **0** | [buffered_h1_h2.md](buffered_h1_h2.md) |
| **`buffered_h1_h3`** | Buffered TCP $\to$ QUIC Bridge| HTTP/3 Port 8443 | **34,965 RPS** (2.82 ms avg) | **30 / 10s** | **0** | [buffered_h1_h3.md](buffered_h1_h3.md) |
| **`server_stream_h1_h1`**| Server Streaming (SSE) | HTTP/1.1 Port 8081 | **38,412 RPS** (0.82 ms TTFB) | **24 / 10s** | **0** | [server_stream_h1_h1.md](server_stream_h1_h1.md) |
| **`server_stream_h1_h2`**| H2 DATA $\to$ Chunked SSE | HTTP/2 Port 8082 | **36,894 RPS** (0.94 ms TTFB) | **23 / 10s** | **0** | [server_stream_h1_h2.md](server_stream_h1_h2.md) |
| **`server_stream_h1_h3`**| QUIC $\to$ Chunked SSE | HTTP/3 Port 8443 | **31,245 RPS** (1.12 ms TTFB) | **29 / 10s** | **0** | [server_stream_h1_h3.md](server_stream_h1_h3.md) |
| **`client_stream_h1_h1`**| Client Chunked Upload | HTTP/1.1 Port 8081 | **34,182 RPS** (2.88 ms avg) | **25 / 10s** | **0** | [client_stream_h1_h1.md](client_stream_h1_h1.md) |
| **`client_stream_h1_h2`**| Chunked $\to$ H2 DATA Stream| HTTP/2 Port 8082 | **32,841 RPS** (3.02 ms avg) | **26 / 10s** | **0** | [client_stream_h1_h2.md](client_stream_h1_h2.md) |
| **`client_stream_h1_h3`**| Chunked $\to$ QUIC Stream | HTTP/3 Port 8443 | **28,914 RPS** (3.42 ms avg) | **31 / 10s** | **0** | [client_stream_h1_h3.md](client_stream_h1_h3.md) |
| **`duplex_h1_h1`** | Full Duplex / WebSocket | HTTP/1.1 Port 8081 | **31,894 RPS** (3.12 ms avg) | **28 / 10s** | **0** | [duplex_h1_h1.md](duplex_h1_h1.md) |
| **`duplex_h1_h2`** | Duplex H1 $\leftrightarrow$ H2 Stream | HTTP/2 Port 8082 | **30,412 RPS** (3.28 ms avg) | **27 / 10s** | **0** | [duplex_h1_h2.md](duplex_h1_h2.md) |
| **`duplex_h1_h3`** | Duplex TCP $\leftrightarrow$ QUIC Stream| HTTP/3 Port 8443 | **27,814 RPS** (3.65 ms avg) | **32 / 10s** | **0** | [duplex_h1_h3.md](duplex_h1_h3.md) |

---

## 4. Key Cross-Subsystem Architectural Invariants

Every pipeline in this directory adheres to non-negotiable systems engineering invariants:
1. **The Early Lease Drop Invariant (Buffered Mode)**: The upstream socket lease is dropped *immediately* once the response headers and payload are resident in RAM, *before* calling `write_vectored` downstream. Slow client download speeds do not tie up backend resources.
2. **The Lease Inversion Invariant (Streaming Modes)**: In `client_stream` and `duplex` modes, the upstream connection is leased *prior* to reading data chunks, ensuring the backend is listening and fail-fast errors trigger before bandwidth is consumed.
3. **The Truncation Safety Invariant (Streaming Modes)**: If an upstream backend terminates unexpectedly mid-stream, the gateway terminates the downstream connection with a TCP `RST` or hard close. It **MUST NEVER emit `0\r\n\r\n`**, preventing downstream clients from assuming a truncated stream succeeded.
4. **The Zero Dynamic Sniffing Invariant (`AGENTS.md §2.7`)**: Protocols are declared explicitly on listeners (`raw`, `http`, `grpc`). The serving hot path never dynamically inspects payloads or headers to switch protocols.
5. **Zero Heap Allocations on Hot Path**: Pre-allocated slab memory buffers (`BytesMut`) with capacity reservation and zero-copy slicing eliminate runtime page faults (`0` page faults measured across millions of requests).
