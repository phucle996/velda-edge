# Canary Plugin Operations Runbook

## 1. Preflight Verification

Before enabling `canary` on a live route:

1. **Upstream Presence**: Confirm the target `canary_upstream` ID (for example `backend-canary`) is defined in `upstreams.json` and has active endpoints.
2. **Route Attachment**: Verify `"canary"` is listed in the target route's `plugins` array in `routes.json`, and the route's default `upstream` points to the baseline backend (for example `backend-base`).
3. **Header Stickiness Readiness**:
   - If using `priority` or `composite` on `x-user-id` / `x-tenant-id`, verify clients or upstream ingress proxies pass those headers cleanly.
   - If falling back to `x-real-ip`, rely on Velda Edge's single ingress enrichment invariant (`downstream.rs` populates `x-real-ip` and `x-forwarded-for` prior to `route` / `pre_upstream`).

---

## 2. Progressive Rollout Ladder

`velda-sync` validates configuration changes, compiles binary runtime artifacts, and atomically notifies `velda-edge` over UDS without restarting workers or dropping active TCP connections.

| Stage | `ratio` (Permille) | Effective Canary Share | Operational Purpose |
| :---: | :---: | :---: | :--- |
| **Stage 0 — Dark / Rule-Only** | `0` | `0.0%` (+ rule overrides) | Validate connectivity to `backend-canary` via QA headers (`x-canary: true`, `x-beta: vip-...`) with zero impact on general traffic. |
| **Stage 1 — Initial Smoke** | `10` | `1.0%` | Catch application panics, latency spikes, or backend dependency mismatches at minimal blast radius. |
| **Stage 2 — Partial Rollout** | `200` | `20.0%` | Evaluate saturation, cache hit ratios, and p99 stability under sustained deterministic user cohorts. |
| **Stage 3 — Majority Shift** | `500` | `50.0%` | Equal traffic distribution prior to cutover. |
| **Stage 4 — Full Canary** | `1000` | `100.0%` | Route all eligible route traffic to `canary_upstream`; afterward update default `route.upstream` and disable plugin. |

Increment `manifest.json` `revision` cleanly alongside `plugins.json` updates so `velda-sync` triggers deterministic delta compilation.

---

## 3. Verifying Live Activation & Wiring

### Inspect Edge Logs
On startup or reload of `plugins` or `upstreams`, `velda-edge` logs dense upstream index resolution:

```text
INFO Wired dense upstream index for canary plugin plugin_id=canary canary_upstream=backend-canary upstream_id=1
INFO Successfully reloaded runtime state via atomic swap revision=2 changed_domains=["plugins"] listeners_changed=false
```

Notice `upstream_id` tracks the position of `canary_upstream` in `upstreams.json`. If `upstreams.json` is reordered (`backend-canary` moved to index `0`), `velda-edge` automatically rebuilds `PluginTable` and re-wires `upstream_id=0`.

### Probe Deterministic & Rule Behavior
Send requests with QA rule headers and rotating user IDs against an inspection or health endpoint:

```bash
# 1. Verify header rule override routes directly to canary upstream
curl -s -H "x-canary: true" http://<gateway>:8080/inspect

# 2. Verify sticky routing across repeated requests for the same user ID
for i in $(seq 1 10); do
  curl -s -H "x-user-id: prod-user-00001" http://<gateway>:8080/ping
done
```

Because `canary` uses `AccessLevel::Read`, `/inspect` will reflect the client's original headers (`X-Canary`, `X-User-Id`) verbatim without any injected proxy canary markers.

---

## 4. Incident & Rollback Procedures

| Symptom / Failure Mode | Root Cause | Immediate Operator Action |
| :--- | :--- | :--- |
| **`Unknown plugin ID 'canary_main'`** in `velda-sync` | `plugins.json` used a custom `"id"` instead of canonical `"canary"`. | Change `"id"` in `plugins.json` and `route.plugins` entries to `"canary"`. |
| **`Ratio N exceeds maximum allowed 1000`** | `ratio` configured outside `0..=1000` (for example percentage confused with permille, or negative value). | Fix `ratio` to integer `0..=1000`. Note `velda-sync` preserves LKG runtime state on validation failure. |
| **Canary receives 0% traffic unexpectedly** | `canary_upstream` ID does not match any entry in `upstreams.json` (leaves `AtomicU16` at `u16::MAX` sentinel). | Verify exact string match against `upstreams[].id` and check `edge.log` for `Wired dense upstream index`. |
| **Elevated 5xx or latency from canary backend** | Regression in canary deployment. | **Instant Rollback**: Set `"ratio": 0` (and clear `rules: []`) OR remove `"canary"` from `route.plugins` / set `"enabled": false` in `plugins.json`. Existing LKG keeps traffic on `backend-base` immediately upon swap. |
| **`Invalid header rule name` or `requires non-empty value`** | Invalid RFC 7230 token in `rules[].name` / `hash_headers`, or empty `value` for `equals`/`prefix`/`suffix`/`contains`. | Supply valid lowercase HTTP header names and non-empty match strings (`value` may only be empty for `operator: "exists"`). |
