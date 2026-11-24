# Canary Traffic Splitting Plugin (`canary`)

The `canary` plugin executes deterministic, header-only traffic splitting in the **`pre_upstream`** phase by switching the target upstream index (`u16`) inside `RequestState` (`AccessLevel::Read`) with zero heap allocations, zero header mutations, zero fallback randomness, and zero 64-bit integer division.

---

## Core Design Principles

1. **Header-Only Workspace (`AccessLevel::Read`)**:
   Reads exclusively from the downstream request `HeaderWorkspace`. It never modifies, injects, or strips headers on the wire (`x-user-id`, `x-tenant-id`, `x-canary`, etc. arrive unmodified at the selected upstream).
2. **Direct Dense `u16` Upstream Index**:
   Core domains (`listeners`, `upstreams`, `etp`/`tls`, `routes`) are compiled and loaded before ecosystem plugins. `CanaryPlugin::from_config()` resolves `"canary_upstream"` directly into an immutable plain `u16` index (`canary_upstream_id: u16`) at instantiation — zero runtime string lookups, zero `AtomicU16`, and zero sentinel checks on the hot path.
3. **Strict Multi-Header Hashing with Zero Random Fallback**:
   Folds all configured `hash_headers` together in order using 8-byte SWAR chunks (`slice::as_chunks::<8>()`) and SplitMix64. If **any** configured header is missing or whitespace-only (and no override rule matches), the request **stays safely on the baseline upstream** — there is zero hidden PRNG fallback, so a user never flips between versions across requests.
4. **Division-Free Permille Scaling (`1..=999`)**:
   Maps `u64 -> 0..999` permille buckets via 128-bit upper-half multiplication (`((hash as u128).wrapping_mul(1000)) >> 64`), compiling to a single BMI2 `mulxq` instruction without `divq` or integer counter wrap-around.

---

## Configuration Schema

Configured in `plugins.json` with canonical plugin ID `"canary"` and referenced by name in route `plugins` arrays. All unknown fields are strictly rejected (`#[serde(deny_unknown_fields)]`).

| Field | Type | Required | Constraints / Allowed Values | Description |
| :--- | :--- | :---: | :--- | :--- |
| `canary_upstream` | `string` | Yes | Non-empty upstream ID present in `upstreams.json` | Target upstream ID selected when a rule matches or permille threshold is met. |
| `ratio` | `integer` (`u32`) | Yes | **`1..=999`** (permille, tenths of a percent) | Fraction routed to canary (`200` = `20.0%`). `0` and `1000` are rejected (disable plugin for `0%`, point route upstream directly for `100%`). |
| `rules` | `array<HeaderRule>` | Yes | May be empty `[]` | Ordered override rules evaluated before hashing (short-circuit OR logic; forces canary regardless of `hash_headers`). |
| `hash_headers` | `array<string>` | Yes | **Non-empty** list of valid HTTP header names | Ordered RFC 9110 header names folded jointly to compute the sticky bucket. If any header in this list is absent/empty, the request stays on baseline. |

### `HeaderRule` Schema

| Field | Type | Required | Allowed Values | Description |
| :--- | :--- | :---: | :--- | :--- |
| `name` | `string` | Yes | Valid HTTP header name | Header name inspected in `HeaderWorkspace`. |
| `operator` | `string` | Yes | `"equals"`, `"prefix"`, `"suffix"`, `"contains"`, `"exists"` | Case-insensitive ASCII value matcher (`exists` ignores `value`). |
| `value` | `string` | Yes | Non-empty except when `operator` is `"exists"` | Target byte sequence matched against the header value. |

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
          { "name": "x-beta", "operator": "prefix", "value": "vip-" }
        ],
        "hash_headers": ["x-tenant-id", "x-user-id"]
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

- **No Random / Unkeyed Fallback**: There is no thread-local PRNG or IP fallback. Every configured header in `hash_headers` must be present and non-empty (e.g., injected by `jwt_auth` or supplied by the client); missing any configured key fails safe to the baseline upstream.
- **`ratio` Strictly `1..=999`**: For `0%` canary (baseline only), disable the plugin or remove `"canary"` from `route.plugins`. For `100%` cutover, update `route.upstream` directly to `backend-canary` and remove `"canary"`.
- **No Header Mutation**: `canary` declares `AccessLevel::Read` and never injects or strips proxy headers.
- **L7 Only**: Evaluates in `pre_upstream` over HTTP `HeaderWorkspace`. Raw TCP/UDP L4 listeners bypass `canary`.
