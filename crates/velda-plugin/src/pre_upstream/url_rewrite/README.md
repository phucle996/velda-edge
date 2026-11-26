# URL Rewrite Plugin (`url_rewrite`)

Literal HTTP URI (`path` + `query`) transformation plugin evaluated during the `pre_upstream` phase using a single Aho-Corasick + SIMD execution path.

```text
  Sync / Compile Time
    UrlRewriteConfig (#[serde(deny_unknown_fields)])
      ├─► path_rules[]  ──► 1 Aho-Corasick (LeftmostLongest) literal matcher
      └─► remove_params ──► 1 Aho-Corasick (DFA + Anchored::Yes) exact key filter

  Frontline Hot Path (PreUpstreamPlugin)
    on_upstream_request(req, _targets)
      ├─► 1. Path  : AhoCorasick::find_iter(path) [literal find -> replace, zero magic]
      ├─► 2. Query : keep (AC key filter + SIMD '&'/'=' scan + query_append) | drop | replace
      ├─► 3. Bypass: unchanged -> Action::Continue (zero heap allocation)
      └─► 4. Build : 1 BytesMut + simd_copy_nonoverlapping -> PathAndQuery -> Uri
                     (invalid URI output -> Action::Reject(RejectReason::BadRequest))
```

---

## 1. Specifications

| Property | Value |
| :--- | :--- |
| **Plugin ID** | `url_rewrite` |
| **Lifecycle Phase** | `PreUpstreamPlugin` (`pbww() == 0`) |
| **Supported Protocols** | HTTP only (`SupportedProtocols::ALL_HTTP`: `http/1.1`, `http/2`, `http/3`) |
| **Capability** | `PluginCapability::new(AccessLevel::Write, AccessLevel::None)` |
| **Scope Boundary** | Mutates `req.uri` (`path` + `query`) only. `Host` / `:authority` belongs to `header_mutation`; path traversal normalization belongs to Ingress (`normalize_path`) and `waf_lite`. |

---

## 2. Configuration Schema (`#[serde(deny_unknown_fields)]`)

| Field | Type | Required | Constraints |
| :--- | :--- | :--- | :--- |
| `path_rules` | `array<{find, replace}>` | **Yes** | Literal substrings replaced verbatim (`LeftmostLongest`). `find` must be non-empty and unique; no `?`, `#`, `\`, or control chars. |
| `query_mode` | `string` | **Yes** | `"keep"`, `"drop"`, or `"replace"`. |
| `query_replace` | `string \| null` | **Yes** | Required non-empty query string (without `?` or `#`) when `query_mode == "replace"`; `null` otherwise. |
| `remove_params` | `array<string>` | **Yes** | Exact query parameter keys stripped when `query_mode == "keep"`; must be `[]` otherwise. |
| `query_append` | `string \| null` | **Yes** | Static suffix appended when `query_mode == "keep"` (no leading/trailing `&`, no `?` or `#`); `null` otherwise. |

- No hidden slash trimming or fallback `/` insertion is applied: `find` matches literally and `replace` substitutes literally. If rewriting leaves a path without a leading `/`, the request fails cleanly with `400 Bad Request`.

---

## 3. Example

```json
{
  "id": "url_rewrite",
  "config": {
    "path_rules": [
      { "find": "/api/v1/", "replace": "/" },
      { "find": "/legacy/", "replace": "/v2/" }
    ],
    "query_mode": "keep",
    "query_replace": null,
    "remove_params": ["utm_source", "internal_token"],
    "query_append": "edge=velda"
  }
}
```
