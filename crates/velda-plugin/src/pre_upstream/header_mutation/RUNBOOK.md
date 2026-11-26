# Header Mutation Plugin (`header_mutation`) — Operational Runbook

Operational reference for configuring, validating, and diagnosing the `header_mutation` pre-upstream plugin in production.

---

## 1. Execution Context

`header_mutation` runs inline on Frontline Traffic Workers (`velda-worker-{i}`) during `pre_upstream` after route selection and before upstream forwarding. All mutations execute in memory against `L7Request.headers` (`HeaderWorkspace`) in fixed phase order:

```text
remove_headers  -->  set_headers  -->  append_headers
```

If a header name appears in both `remove_headers` and `set_headers`, the old value is removed first and the static value in `set_headers` is inserted afterward.

---

## 2. Parameter Reference Table

| Field | JSON Type | Required | Constraints / Rules | Default | Behavior |
| :--- | :--- | :---: | :--- | :---: | :--- |
| `remove_headers` | `string[]` | No* | Valid RFC 9110 `tchar` name (`1..=256` bytes), no `:`, no case-insensitive duplicates | `[]` | Deletes matching headers from `HeaderWorkspace` (including `TAG_BYTE_BYPASS_BIT` entries). |
| `set_headers` | `map<string, string>` | No* | Valid RFC 9110 name (`<= 256`B), static value (`<= 8192`B, no CRLF/control/`0x7F`), no case-insensitive key duplicates | `{}` | Overwrites existing header slots (including multi-value slots) or inserts a new slot via `insert_prehashed`. |
| `append_headers` | `map<string, string>` | No* | Valid RFC 9110 name (`<= 256`B), static value (`<= 8192`B, no CRLF/control/`0x7F`), no case-insensitive key duplicates | `{}` | Appends a new slot via `append` without clearing existing values for the same header name. |

\* **At least one** of `remove_headers`, `set_headers`, or `append_headers` must be non-empty. Unknown fields fail deserialization (`deny_unknown_fields`).

---

## 3. Configuration Recipes

### 3.1 API Edge Sanitization & Tagging

Strip internal headers and stamp static gateway identity headers:

```json
{
  "id": "header_mutation",
  "config": {
    "remove_headers": ["x-internal-secret", "x-debug-token"],
    "set_headers": {
      "x-gateway-proxy": "velda-edge",
      "x-environment": "production"
    }
  }
}
```

### 3.2 Explicit Hop/Trace Override (Including Bypass-Tagged Slots)

Delete `x-request-id` or overwrite `host` directly when an upstream contract requires it:

```json
{
  "id": "header_mutation",
  "config": {
    "remove_headers": ["x-request-id"],
    "set_headers": {
      "host": "backend.internal.svc"
    }
  }
}
```

### 3.3 Append-Only Audit Hop

Add an audit marker while preserving any pre-existing values for the same header:

```json
{
  "id": "header_mutation",
  "config": {
    "append_headers": {
      "via": "velda-edge-1"
    }
  }
}
```

---

## 4. Observability Signals

- **Request Short-Circuit Behavior**: `header_mutation` always returns `Action::Continue`. It never generates synthetic wire errors (`RejectReason` / `L7Response`) at runtime.
- **Configuration Rejection Telemetry**: Invalid configs (syntax violation, empty config, CRLF value, duplicate key, unknown field) are caught during `velda-sync` / `validate_header_mutation_config` with explicit `PluginError::Config` messages identifying `plugin_id` and the offending field before runtime publication (`LKG` remains active).

---

## 5. Incident Triage Playbook

| Symptom | Root Cause | Resolution |
| :--- | :--- | :--- |
| Config rejected with `unknown field` | Legacy fields (`strip_hop_by_hop`, `rename_headers`) are still present in `plugins.json`. | Remove deprecated fields and express rules strictly via `remove_headers`, `set_headers`, `append_headers`. |
| Config rejected with `must declare at least one rule` | All three blocks are empty or omitted `{}`. | Declare at least one entry in `remove_headers`, `set_headers`, or `append_headers`. |
| Expected header still present upstream | Either the route does not reference `header_mutation` in `plugins`, or a later plugin re-injects that header after `header_mutation`. | Verify route `plugins` ordering and move `header_mutation` after the plugin injecting the header if removal is intended. |
| Header value contains literal `$client_ip` / `$request_id` | `$` tokens are treated as ordinary static strings (no variable interpolation). | Configure exact static header values; runtime variable synthesis belongs outside `header_mutation`. |
| Config rejected with `pseudo-headers starting with ':' are forbidden` | A rule key begins with `:` (e.g. `:path`, `:authority`). | Target standard HTTP header names; pseudo-header routing belongs to `url_rewrite` or ingress/upstream codecs. |
