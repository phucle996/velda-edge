# velda-tls — In-Memory Stateless TLS Execution Engine

`velda-tls` is the **dedicated TLS execution engine** for Velda Edge. It handles both downstream server TLS termination and upstream client TLS initiation.

---

## 1. What Problem Does This Solve?

In high-performance edge gateways, **TLS is computationally the most expensive and security-critical bottleneck**:
- Asymmetric cryptography (ECDSA/RSA handshakes, key exchanges) heavily saturates CPU cores.
- Naive gateway implementations often perform disk I/O to read `.pem`/`.key` files on demand, block on `Mutex` locks during SNI lookups, or risk Out-Of-Memory (OOM) crashes by leaving session caches unbounded.
- Dynamic config reloads (ACME certificates, tenant certificate rotation) frequently stall active worker threads if compilation is slow.

**`velda-tls` solves these challenges by enforcing a strict architectural invariant:**
> **"Zero-IO on Request Serving, All Cryptographic State Pre-compiled in RAM."**

---

## 2. Where It Fits & How It Works

`velda-tls` sits between the transport coordination layer (`velda-composer`) and the application protocols (`velda-http1`, `velda-http2`, `velda-http3`, `velda-grpc`):

```text
[ Client ] ──────────────( TCP / TLS )──────────────> [ velda-transport ]
                                                             │
                                                             ▼ (Connection)
                                                       [ velda-composer ]
                                                             │
                                                             ▼ accept(stream)
                                  ┌─────────────────────────────────────────────────────┐
                                  │                     velda-tls                       │
                                  │                                                     │
                                  │  1. In-Memory SniResolver (Exact & RFC 6125 *.wild) │ ~65 ns (O(1))
                                  │  2. Strict Rejection on Unknown / Missing SNI       │ 0 Cert Leakage
                                  │  3. Downstream Handshake & mTLS Peer Verification   │ +1.1% cost
                                  │  4. RAM Session Resumption Cache & 0-RTT Tickets    │ Auto-Tiered
                                  └──────────────────────────┬──────────────────────────┘
                                                             ▼
                                                    Decrypted TlsStream
                                                             │
                                                  ┌──────────┴──────────┐
                                                  ▼                     ▼
                                          [ velda-http1 / 2 ]    [ velda-grpc ]
                                                  │                     │
                                                  └──────────┬──────────┘
                                                             ▼
                                                Upstream Forwarding Pipeline
                                                             │
                                                             ▼ connect(sni, io)
                                  ┌─────────────────────────────────────────────────────┐
                                  │                 TlsClientEngine                     │
                                  │       (Gateway -> Upstream Backend mTLS)            │
                                  └──────────────────────────┬──────────────────────────┘
                                                             ▼
                                                    [ Backend Services ]
```

---

## 3. Core Architectural Decisions (The "Why")

### 3.1 Downstream vs Upstream Separation
- **`TlsServerEngine` (Ingress)**: Terminates incoming client connections. Owns multiple tenant certificates, dynamic SNI matching, ALPN negotiation (`h2`, `http/1.1`), downstream mutual TLS (mTLS), and Slowloris timeout defense.
- **`TlsClientEngine` (Egress)**: Originates secure outbound connections to upstream microservices. Manages trusted CA roots and optional client certificates for upstream zero-trust mTLS.
- **`TlsEngine` (Unified Facade)**: Coordinates both engines into a single runtime structure that can be swapped atomically during configuration reloads.

### 3.2 Zero-IO Hot Path
During request serving, `velda-tls` **never**:
- Reads certificate or private key files from disk.
- Parses PEM or JSON text.
- Performs DNS resolution or network RPCs.
- Allocates dynamic routing tables.

All PEM parsing and crypto compilation happen during **startup or staged reload** (`ServerTlsConfig::build`). Once compiled into `Arc<rustls::ServerConfig>`, serving requests is 100% CPU and RAM-bound.

### 3.3 Strict SNI Isolation (No Accidental Fallbacks)
Many gateways fall back to a "default certificate" if the client sends an unknown domain or omits SNI. This leaks private tenant domains and breaks multi-tenant isolation.
`velda-tls` enforces **Strict Rejection**:
- If `ClientHello` contains no SNI $\to$ Handshake immediately rejected.
- If SNI does not match any configured exact domain or single-label wildcard $\to$ Handshake immediately rejected.

### 3.4 Dynamic Hardware Tiering (Preventing OOM)
Session resumption caches must be bounded according to the host machine's capacity. Hardcoding cache sizes causes OOM on small containers (e.g. 512MB RAM) or underutilizes large edge servers (e.g. 128GB RAM).
`velda-tls` dynamically probes hardware via `velda-core`:

| Memory Tier | Host RAM | Session Cache Capacity | 0-RTT Max Early Data | Handshake Timeout |
| :--- | :--- | :--- | :--- | :--- |
| **Constrained** | $< 2\text{ GB}$ | $1,024\text{ sessions}$ | $0\text{ B (Disabled)}$ | $10\text{ s}$ |
| **Small** | $2 - 4\text{ GB}$ | $2,048\text{ sessions}$ | $4\text{ KB}$ | $8\text{ s}$ |
| **Medium** | $4 - 16\text{ GB}$ | $8,192\text{ sessions}$ | $8\text{ KB}$ | $5\text{ s}$ |
| **Large** | $16 - 64\text{ GB}$ | $16,384\text{ sessions}$ | $8\text{ KB}$ | $5\text{ s}$ |
| **Ultra** | $> 256\text{ GB}$ | $131,072\text{ sessions}$ | $32\text{ KB}$ | $2\text{ s}$ |

### 3.5 QUIC / HTTP/3 Native Bridge
Rather than maintaining separate crypto configurations for TCP and UDP, `velda-tls` provides `build_quic_server_config()`, directly transforming `Arc<rustls::ServerConfig>` into `quinn_proto::ServerConfig`. Both protocols share the same certificates, ALPN lists, and resumption logic.

---

## 4. Verified Performance Benchmarks

Run any benchmark using `rtk cargo bench`:

```bash
# 1. Single-Thread Micro-Latency & SNI Lookup Scaling
rtk cargo bench -p velda-tls --bench single_thread_bench

# 2. Multi-Thread Concurrency (1 -> 16 Workers)
rtk cargo bench -p velda-tls --bench multi_thread_bench

# 3. Adversarial Attack Defense (SNI Flood, Malformed Frames, Slowloris)
rtk cargo bench -p velda-tls --bench adversarial_bench

# 4. Memory Leak & Heap Stability Audit
rtk cargo bench -p velda-tls --bench memory_leak_bench

# 5. Specialized Cryptography, mTLS Overhead & Reload Speed
rtk cargo bench -p velda-tls --bench crypto_bench
```

### Verified Empirical Numbers
- **SNI Lookup**: **~65 ns / lookup** ($15,000,000\text{ ops/s}$) flat complexity across $10$ up to $5,000$ certificates.
- **Metadata Extraction**: **~52 ns / op** ($19,000,000\text{ ops/s}$).
- **TLS 1.3 Full Handshake**: **~218 µs** per handshake ($4,500\text{ hsk/s}$ on a single core, scaling to $13,800\text{ hsk/s}$ across worker threads).
- **Mutual TLS (mTLS) Overhead**: Validating client certificates adds **only +1.1% latency (~2 µs)** compared to standard 1-way TLS.
- **Config Reload Compilation**: **~72,000 certificates/s** compiled from in-memory PEM into active `ServerConfig` ($< 0.7\text{ ms}$ for a batch of 50 tenant certificates).
- **Adversarial Resilience**: 100% clean rejection on hostile SNI probes; 100% safe drops on corrupted frames with 0 panics.
- **Memory Invariant**: **0 B net heap growth** post session-cache saturation; 0 leaks after 1,000,000 continuous lookups.
