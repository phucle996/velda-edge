# HTTP Method Filter (`method_filter`) — Deep Architecture Specification

This document specifies the bitmask memory representation, integer classification mechanics, and protocol isolation invariants of **`method_filter`**.

---

## 1. 16-Bit Bitmask Layout & Integer Classification

`method_filter` reuses `velda_core::lut::HttpMethod` (`#[repr(u8)]`, discriminants `0..=9`) to map ASCII method slices directly to bit positions in a single `u16` register:

```text
+─────────────────────────────────────────────────────────────────────────────+
|                    16-BIT METHOD ALLOWLIST BITMASK (u16)                    |
+─────────────────────────────────────────────────────────────────────────────+
| Bit: | 15..10 |  9  |   8   |  7  |   6   |  5  |  4  |  3 |  2 |  1 |  0  |
| Verb:| Unused |TRACE|CONNECT|PATCH|OPTIONS|HEAD | DEL |PUT |POST|GET | Unk |
+─────────────────────────────────────────────────────────────────────────────+

Hot-Path Evaluation:
  id  = HttpMethod::from_bytes(req.method.as_str().as_bytes()) as u8
  bit = 1u16 << id
  (allowed_mask & bit) != 0   ──►  Single-instruction ALU test (bt / test)
```

### Bit `0` (`HttpMethod::Unknown`) Fast-Fail Property
- Bit `0` of `allowed_mask` is always `0`.
- Any unrecognized method (`TRACK`, `DEBUG`, `PROPFIND`, malformed verbs) maps to `id = 0` (`bit = 1`).
- When `custom_methods` is empty (standard REST workloads), the CPU rejects all unknown verbs in **1 bitwise check** without string iteration or heap memory access.

---

## 2. Protocol Isolation Invariant (`ALL_HTTP`)

- **HTTP vs gRPC Separation**: All gRPC over HTTP/2 traffic carries `:method = POST` at the wire layer, while the RPC method name lives in `:path` (`/<Service>/<Method>`).
- `method_filter` declares `SupportedProtocols::ALL_HTTP`, ensuring dedicated gRPC listeners bypass HTTP verb filtering and delegate RPC method routing strictly to `GrpcRouter`.
