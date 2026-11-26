# Header Mutation Plugin (`header_mutation`)

Explicit static request header transformation plugin evaluated during the `pre_upstream` phase directly against `HeaderWorkspace`. Executes only the configured delete (`remove_headers`), overwrite (`set_headers`), and add (`append_headers`) rules — with no implicit hop-by-hop stripping, no hidden header reads, and no dynamic variable fallbacks.

```text
+-----------------------------------------------------------------------------+
|                        HEADER MUTATION PIPELINE                             |
+-----------------------------------------------------------------------------+
|  L7Request.headers (HeaderWorkspace)                                        |
|    |                                                                        |
|    +---> [0] op_mask == 0? -------- yes -------> Action::Continue           |
|    |                                                                        |
|    +---> [1] OP_REMOVE  --> contains_prehashed(&key) --> remove(&key.name)  |
|    |                        (removes even TAG_BYTE_BYPASS_BIT entries)      |
|    +---> [2] OP_SET     --> insert_prehashed(&key, static_val)              |
|    |                        (overwrites all existing matching slots)        |
|    +---> [3] OP_APPEND  --> append(name, static_val)                        |
|    |                        (appends a new slot preserving prior values)    |
|    +---> Action::Continue                                                   |
+-----------------------------------------------------------------------------+
```

---

## 1. What This Plugin Does

- **Static Header Deletion (`remove_headers`)**: Checks `HeaderWorkspace` via pre-hashed `HeaderKey` and removes matching header entries, including entries carrying `TAG_BYTE_BYPASS_BIT` (such as `x-request-id`, `x-forwarded-for`, or `host`) when explicitly configured.
- **Static Header Overwrite (`set_headers`)**: Overwrites or inserts configured static `HeaderValue` entries using `HeaderWorkspace::insert_prehashed`, replacing any multi-value duplicates with the configured single value.
- **Static Header Addition (`append_headers`)**: Appends configured static `HeaderValue` entries via `HeaderWorkspace::append` while keeping existing values intact.
- **Strict WYCIWIS Semantics**: Unconfigured headers (including RFC hop-by-hop headers like `Connection`, `TE`, or `Upgrade`) are never read, rewritten, or deleted automatically.

---

## 2. Where It Sits in Velda Edge

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `header_mutation` |
| **Phase** | `pre_upstream` |
| **Worker Affinity** | Frontline Traffic Workers (`velda-worker-{i}`) |
| **PBWW (`pbww()`)** | `0` (pure inline hot path, no background scheduler tasks) |
| **Capability** | `PluginCapability::header_only(AccessLevel::Write)` |
| **Supported Protocols** | `SupportedProtocols::ALL_HTTP \| SupportedProtocols::GRPC` |
| **Execution Order** | Strictly `remove` -> `set` -> `append`, following route plugin declaration order |

---

## 3. Requirements & Prerequisites

- At least one non-empty rule block must be declared across `remove_headers`, `set_headers`, or `append_headers`.
- Header names must follow RFC 9110 `tchar` grammar (`1..=256` bytes, no leading/trailing whitespace, no `:` pseudo-header prefix).
- Header values must contain valid RFC 9110 field characters (`<= 8192` bytes, no `\r`, `\n`, ASCII control characters `< 0x20` except `\t`, or `0x7F`).
- Unknown JSON fields are rejected (`#[serde(deny_unknown_fields)]`), and case-insensitive duplicates within the same rule block are rejected during `velda-sync` validation.

---

## 4. Quick Start Configuration

```json
{
  "id": "header_mutation",
  "config": {
    "remove_headers": [
      "x-powered-by",
      "server"
    ],
    "set_headers": {
      "x-gateway-proxy": "velda-edge",
      "x-forwarded-proto": "https"
    },
    "append_headers": {
      "x-trace-tag": "edge-ingress"
    }
  }
}
```

> [!NOTE]
> Unused rule blocks (`remove_headers`, `set_headers`, `append_headers`) use `#[serde(default)]` and may be omitted individually as long as at least one block is non-empty.

---

## 5. Documentation Directory

- **[Operational Runbook](RUNBOOK.md)**: Parameter reference table, configuration recipes, telemetry signals, and incident triage playbook.
- **[Architectural Specification](ARCHITECTURE.md)**: `#[repr(C, align(64))]` memory layout, `op_mask` dispatch, `HeaderWorkspace` SIMD `tag7` probing, and complexity bounds.
- **[Performance Benchmark Report](BENCHMARK.md)**: 5-tier diagnostic matrix and verified `x86-64-v3` release assembly audit.
- **[Test Verification Report](TEST.md)**: `cargo test` commands and verified unit test breakdown across plugin engine and validation modules.
