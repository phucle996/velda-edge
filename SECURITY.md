# Security Policy

The Velda Edge maintainers take the security of our high-performance edge platform and its users extremely seriously. This document describes our security guarantees, supported versions, and the process for reporting vulnerabilities.

---

## 1. Supported Versions

Security updates are actively maintained for the following versions:

| Version | Supported |
|---|---|
| `0.1.x` (Main branch) | :white_check_mark: Yes |
| `< 0.1.0` | :x: No |

---

## 2. Reporting a Vulnerability

If you discover a security vulnerability or suspect a security flaw in Velda Edge, **please do NOT open a public GitHub issue**.

Instead, report it responsibly via our coordinated disclosure process:

1. **Email**: Send detailed security reports to 10k.polime.vnd@gmail.com (or maintainer contact).
2. **Details to Include**:
   - Subsystem affected (e.g. `crates/velda-tls`, `crates/velda-http1`, `control-plane`).
   - Type of vulnerability (e.g. Denial of Service, Protocol Confusion, Memory Leak, Authentication Bypass).
   - Minimal reproduction steps, proof-of-concept (PoC), or trace logs.
   - Any proposed patch or mitigation.
3. **Response Timeline**:
   - An initial acknowledgement will be provided within **24 hours**.
   - A triage assessment and coordination update will be provided within **72 hours**.
   - A security patch will be prepared and published alongside a CVE advisory upon coordinated release.

---

## 3. Data Plane Security Guarantees & Invariants

Velda Edge is engineered with defensive-in-depth principles:

### 3.1 Memory Safety
- The Data Plane request serving hot path is written in **100% Safe Rust**.
- `#![forbid(unsafe_code)]` or strictly audited narrow boundaries ensure zero buffer overflows, zero use-after-free, and zero uninitialized memory access.

### 3.2 Cryptographic Standards
- Powered by `rustls` for all downstream and upstream TLS handshakes.
- Strictly supports **TLS 1.2** and **TLS 1.3** with modern, secure cipher suites (ChaCha20-Poly1305, AES-GCM). Insecure legacy protocols (SSLv3, TLS 1.0, TLS 1.1) and broken ciphers (RC4, 3DES, CBC) are completely omitted.

### 3.3 Strict ALPN Enforcement & Protocol Isolation
- Listeners declare their expected application protocol (`http`, `grpc`, `raw`).
- Downstream TLS connections strictly validate Application-Layer Protocol Negotiation (ALPN) tokens. Any mismatch (e.g. attempting to send raw HTTP to a gRPC listener) results in immediate connection termination before any payload processing.

### 3.4 Denial of Service (DoS) & Slowloris Mitigations
- Socket accept loops and connection pipelines enforce hardware-tier timeouts (`connect_timeout`, `idle_timeout`, `request_timeout`).
- Headers and body sizes are strictly bounded via pre-allocated ring buffers to prevent memory exhaustion attacks.
