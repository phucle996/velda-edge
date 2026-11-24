# Canary Plugin Operations Runbook

## 1. Preflight Verification

Before enabling `canary` on a live route:

1. **Upstream Presence**: Confirm the target `canary_upstream` ID (for example `backend-canary`) is defined in `upstreams.json` and has active endpoints.
2. **Route Attachment**: Verify `"canary"` is listed in the target route's `plugins` array in `routes.json`, and the route's default `upstream` points to the baseline backend (for example `backend-base`).
3. **Guaranteed `hash_headers` Presence**:
   - All headers listed in `hash_headers` (for example `["x-tenant-id", "x-user-id"]`) are required at request time.
   - Any request missing one or more configured `hash_headers` safely stays on baseline unless forced by a header `rule`. Ensure upstream auth/edge plugins (e.g., `jwt_auth` claim injection) run before `canary` if internal identity headers are used.

---

## 2. Progressive Rollout Ladder

`velda-sync` validates configuration changes, compiles binary runtime artifacts, and atomically notifies `velda-edge` over UDS without restarting workers or dropping active TCP connections.

Because `ratio` is enforced within `1..=999`:

| Stage | Operational Action | Effective Canary Share | Purpose |
| :---: | :--- | :---: | :--- |
| **Stage 0 — Baseline Only** | Keep `"canary"` disabled or omitted from `route.plugins` | `0.0%` | All traffic remains on `backend-base`. (`ratio: 0` is rejected by validation.) |
| **Stage 1 — Initial Smoke** | Enable `"canary"` with `ratio: 1` or `10` | `0.1%` – `1.0%` | Validate canary stability with QA rules (`x-canary: true`) and minimal organic share. |
| **Stage 2 — Partial Rollout** | Increase `ratio` to `200` (keep same `hash_headers`) | `20.0%` | All users routed to canary at Stage 1 stay pinned to canary while cohort expands monotonically. |
| **Stage 3 — Majority Rollout** | Increase `ratio` to `500` – `999` | `50.0%` – `99.9%` | High-volume verification prior to full cutover. |
| **Stage 4 — Full Cutover** | Set `route.upstream = "backend-canary"` and remove `"canary"` from `route.plugins` | `100.0%` | Permanent cutover with zero canary plugin overhead. (`ratio: 1000` is rejected by validation.) |

Increment `manifest.json` `revision` alongside `plugins.json` updates so `velda-sync` triggers deterministic delta compilation. Keep `hash_headers` constant throughout a rollout so existing cohorts never flip between versions when increasing `ratio`.

---

## 3. Verifying Live Activation & Wiring

On startup or reload of `plugins` or `upstreams`, `velda-edge` resolves `canary_upstream` directly against the already-loaded `upstreams` core table inside `build_plugin_table()` and logs the atomic runtime swap (`successfully reloaded runtime state via atomic swap ... changed_domains=["plugins"]`). If `upstreams.json` is reordered, `velda-edge` rebuilds `PluginTable` with the updated `u16` index automatically.

Cross-domain existence of `canary_upstream` is also validated before publication inside `velda-sync` (`sync.rs` reconciles core domains `listeners -> upstreams -> tls/etp -> routes` prior to `plugins`), preserving LKG if a canary rule references an unknown upstream ID.

### Probe Deterministic, Rule & Missing-Header Safety
```bash
# 1. Rule override forces canary even when hash_headers are absent
curl -s -H "x-canary: true" http://<gateway>:8080/inspect

# 2. Missing any configured hash_header stays safely on baseline
curl -s -H "x-user-id: prod-user-00001" http://<gateway>:8080/ping   # (when ["x-tenant-id", "x-user-id"] is configured)

# 3. Repeated requests with full hash_headers always pin to the same upstream
for i in $(seq 1 10); do
  curl -s -H "x-tenant-id: acme" -H "x-user-id: prod-user-00001" http://<gateway>:8080/ping
done
```

---

## 4. Incident & Rollback Procedures

| Symptom / Failure Mode | Root Cause | Immediate Operator Action |
| :--- | :--- | :--- |
| **`ratio (0)` or `ratio (1000) must be between 1 and 999`** in `velda-sync` | `ratio` set outside `1..=999`. | Use `1..=999` for active canary, or disable plugin / remove `"canary"` from `route.plugins` for `0%`, or update `route.upstream` directly for `100%`. LKG keeps running safely. |
| **`hash_headers cannot be empty`** or **`unknown field 'hash_strategy'`** | Legacy schema or empty `hash_headers`. | Supply non-empty `hash_headers` (e.g., `["x-user-id"]`) and remove legacy `hash_strategy`. |
| **Canary fails sync/edge validation with `does not exist in upstreams`** | `canary_upstream` ID does not match any entry in `upstreams.json`. | Verify exact string match against `upstreams[].id`; `velda-sync` and `velda-edge` reject invalid references while preserving LKG. |
| **Elevated 5xx or latency from canary backend** | Regression in canary deployment. | **Instant Rollback**: Set `"enabled": false` in `plugins.json` or remove `"canary"` from `route.plugins`. Atomic LKG swap routes 100% back to baseline without dropping active connections. |
