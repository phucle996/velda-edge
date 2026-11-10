# ACME Certificate Rotator — Test Plan & Verification Specification

This document details the test suites, verification strategy, and coverage matrix for the **ACME Automated Certificate Rotator Plugin**.

---

## 1. Test Architecture & Coverage Matrix

| Test Layer | File / Harness | Verification Target |
| :--- | :--- | :--- |
| **Unit: Cron** | `cron.rs` | 5-field UTC syntax, steps (`*/15`), lists (`3,4`), Sunday aliases (`0`/`7`), leap day (`2028-02-29`). |
| **Unit: Challenge** | `challenge.rs` | Pre-baked wire response, lock-free `ArcSwap` lookup, RAII teardown on guard drop. |
| **Unit: Crypto** | `crypto.rs` | ECDSA P-256 keygen, PEM roundtrip, RFC 7638 thumbprint, JWS ES256 signing, CSR DER generation. |
| **Unit: Expiry** | `expiry.rs` | X.509 ASN.1 `notAfter` timestamp extraction via `x509-parser`, renewal window calculation. |
| **Unit: Storage** | `storage.rs` | Atomic file write (tmp + rename), account key reuse, fullchain and privkey persistence. |
| **Unit: Hot-Swap** | `hot_swap.rs` | In-memory atomic certificate injection into `velda-tls::SniResolver`. |
| **Unit: Route Guard**| `route_guard.rs` | Ephemeral route injection into `Router`, pre-baked byte serving, unconditional cleanup on drop. |
| **Integration** | `tests/acme_cert_rotator_test.rs` | End-to-end RFC 8555 lifecycle against mock ACME CA server: order, challenge, CSR, issuance, hot-swap. |
| **Machine Code** | `harness/machine_code_test.rs` | RDTSC cycle profiling, PMU hardware counters, 3 live chaos assault scenarios. |

---

## 2. Running Test Suites

### Unit Tests
```bash
cargo test -p velda-plugin --lib acme_cert_rotator
```

### End-to-End Integration Test
```bash
cargo test -p velda-plugin --test acme_cert_rotator_test
```

### Live Chaos Profiling & Hardware PMU Counters
```bash
./crates/velda-plugin/src/background/acme_cert_rotator/harness/run_machine_code_analysis.sh
```
