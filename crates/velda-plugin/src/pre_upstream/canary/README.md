# Canary Traffic Splitting Plugin (`canary`)

The `canary` plugin executes deterministic, header-only traffic splitting in the **`pre_upstream`** phase by switching the target upstream index (`u16`) inside `RequestState` (`AccessLevel::Read`) with zero heap allocations, zero header mutations, and zero 64-bit integer division.

---

## Core Design Principles

1. **Header-Only Workspace (`AccessLevel::Read`)**:
   Reads exclusively from the downstream request `HeaderWorkspace`. It never modifies, injects, or strips headers on the wire (`x-user-id`, `x-canary`, etc. arrive unmodified at the selected upstream).
2. **Dense `u16` Upstream Switch**:
   At bootstrap and hot-reload (`velda-edge`), `wire_plugin_upstreams()` resolves `"canary_upstream"` string IDs to their dense `u16` index in the compiled `upstreams` table (`AtomicU16` with `Ordering::Relaxed`). Request execution requires a single atomic register load.
3. **SWAR + SplitMix64 + Division-Free Fixed-Point Scaling**:
   Hashes extracted header bytes in 8-byte little-endian words (`slice::as_chunks::<8>()`) and maps `u64 -> 0..1000` permille buckets via 128-bit upper-half multiplication (`((hash as u128).wrapping_mul(1000)) >> 64`), compiling to a single BMI2 `mulxq` instruction without `divq` or integer counter wrap-around.

---

## Configuration Schema

Configured in `plugins.json` with canonical plugin ID `"canary"` and referenced by name in route `plugins` arrays.

| Field | Type | Required | Constraints / Allowed Values | Description |
| :--- | :--- | :---: | :--- | :--- |
| `canary_upstream` | `string` | Yes | Non-empty upstream ID present in `upstreams.json` | Target upstream ID selected when a rule matches or permille threshold is met. |
| `ratio` | `integer` (`u32`) | Yes | `0..=1000` (permille, tenths of a percent) | Fraction routed to canary (`200` = `20.0%`). `0` = rule-only override; `1000` = `100%` canary. |
| `rules` | `array<HeaderRule>` | Yes | May be empty `[]` | Ordered override rules evaluated before hashing (short-circuit OR logic). |
| `hash_strategy` | `string` | Yes | `"priority"`, `"composite"`, `"random"` | Key derivation mode when no rule matches. |
| `hash_headers` | `array<string>` | Yes | Valid HTTP header names; non-empty for `"priority"` / `"composite"` | Lowercased RFC 7230 header names used as hash keys. |

### `HeaderRule` Schema

| Field | Type | Required | Allowed Values | Description |
| :--- | :--- | :---: | :--- | :--- |
| `name` | `string` | Yes | Valid HTTP header name | Header name inspected in `HeaderWorkspace`. |
| `operator` | `string` | Yes | `"equals"`, `"prefix"`, `"suffix"`, `"contains"`, `"exists"` | Case-sensitive value matcher (`exists` ignores `value`). |
| `value` | `string` | Yes | Non-empty except when `operator` is `"exists"` | Target byte sequence matched against the header value. |

### Hash Strategies

| Strategy | Behavior | Fallback When Header Absent |
| :--- | :--- | :--- |
| **`priority`** | Iterates `hash_headers` in order and hashes the **first** non-empty header value found. | Falls back to `fast_prng_u64()` (`splitmix64` over thread-local RNG counter). |
| **`composite`** | Feeds every present header from `hash_headers` (separated by `0xFF` delimiter) into a single continuous hash state. | Falls back to `fast_prng_u64()` if all listed headers are absent. |
| **`random`** | Ignores `hash_headers` and evaluates per-request via `fast_prng_u64()`. | Always non-sticky stateless PRNG split. |

---

## Configuration Examples

### `upstreams.json`
```json
{
  "schema_version": 1,
  "upstreams": [
    {
      "id": "backend-base",
      "mode": "endpoints",
      "protocol": { "transport": "tcp", "application": "http1", "streaming": [] },
      "endpoints": [{ "address": "192.168.122.20:18081", "weight": 100 }],
      "load_balancer": { "algorithm": "round_robin" }
    },
    {
      "id": "backend-canary",
      "mode": "endpoints",
      "protocol": { "transport": "tcp", "application": "http1", "streaming": [] },
      "endpoints": [{ "address": "192.168.122.20:18091", "weight": 100 }],
      "load_balancer": { "algorithm": "round_robin" }
    }
  ]
}
```

### `plugins.json`
```json
{
  "schema_version": 1,
  "plugins": [
    {
      "id": "canary",
      "enabled": true,
      "config": {
        "canary_upstream": "backend-canary",
        "ratio": 200,
        "rules": [
          { "name": "x-canary", "operator": "equals", "value": "true" },
          { "name": "x-beta-group", "operator": "prefix", "value": "internal-" }
        ],
        "hash_strategy": "priority",
        "hash_headers": ["x-user-id", "x-real-ip"]
      }
    }
  ]
}
```

### `routes.json`
```json
{
  "schema_version": 1,
  "routes": [
    {
      "id": "http-plain",
      "kind": "l7",
      "listener": "http-in",
      "match": { "path_prefix": "/" },
      "upstream": "backend-base",
      "plugins": ["canary"]
    }
  ]
}
```

---

## Constraints & Non-Goals

- **No Header Mutation**: `canary` declares `AccessLevel::Read` and does not inject response markers or upstream canary headers. Distinguish traffic at the upstream service or via route metrics.
- **Strict Validation**: Unknown extra fields or omitted required schema keys (`canary_upstream`, `ratio`, `rules`, `hash_strategy`, `hash_headers`) are rejected by `velda-sync` before config publication.
- **No Raw Socket / IP Address Hashing**: Client IP stickiness requires downstream ingress enrichment (`x-real-ip` / `x-forwarded-for`, guaranteed present by the single ingress enrichment invariant before routing).
- **L7 Only**: Evaluates in `pre_upstream` over HTTP `HeaderWorkspace`. Raw TCP/UDP L4 listeners bypass `canary`.
