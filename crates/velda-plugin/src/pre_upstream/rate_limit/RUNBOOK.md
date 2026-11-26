# Operations & Configuration Runbook (`rate_limit`)

## 1. Configuration Parameter Reference (`RateLimitConfig`)

`RateLimitConfig` uses `#[serde(deny_unknown_fields)]`. Legacy fields (`backend`, `redis_url`, `redis_tls_*`) are rejected at validation time; external storage connections belong exclusively in `etp.json`.

| Field | Type | Default | Constraint / Allowed Values | Purpose |
| :--- | :--- | :--- | :--- | :--- |
| `algorithm` | `string` | `"token_bucket"` | `"token_bucket"`, `"sliding_window"`, `"fixed_window"` | Local and window-bucket enforcement algorithm. |
| `etp_id` | `string \| null` | `null` | Non-blank ETP ID or `null` | `null` = local RAM only (`pbww = 0`); `Some(id)` = SoT cluster sync. |
| `etp_kind` | `string \| null` | `null` | SoT ETP kind hint | Optional validation guard checked by `velda-sync`. |
| `rate_per_second` | `u64` | `1000` | `>= 1` | Refill / quota rate per second per resolved bucket key. |
| `burst_capacity` | `u64` | `1000` | `>= 1` | Hard baseline capacity before overdraft grace applies. |
| `overdraft_percent` | `u8` | `5` | `0..=100` | Bounded soft grace percentage added above `burst_capacity`. |
| `key_strategy` | `string` | `"priority"` | `"client_ip"`, `"header"`, `"route"`, `"priority"`, `"composite"`, `"hierarchical"` | Key extraction or cascading multi-tier builder. |
| `key_headers` | `string[]` | `[]` | Non-empty for `header`, `composite`, `hierarchical` | Ordered sources: HTTP header names, `"client_ip"`, or `"route"`. |
| `fallback_behavior` | `string` | `"memory"` | `"memory"`, `"allow"`, `"deny"` | Runtime policy if Shadow sync marks SoT unhealthy. |
| `key_prefix` | `string` | `"velda:rl:"` | Trimmed non-empty string | Prefix applied to persisted keys in SoT storages. |
| `sync_interval_ms` | `u64` | `200` | `10..=60000` | Shadow Worker tick interval between delta push/reconcile cycles. |
| `max_entries` | `usize` | `65536` | `>= 64` | Bounded total slab slots distributed across 64 `CLOCK` shards. |
| `pbww` | `u32` | `20` (`0` if local) | `0..=100` | Shadow background worker scheduling weight. |

## 2. Supported State-of-Truth (SoT) ETP Kinds

`rate_limit` accepts 14 dedicated State-of-Truth storage ETP kinds:
1. `redis` — Redis OSS (`mode: "standalone" | "cluster"`, Lua-free pipelined RESP2 `SET ... EX 15 NX` + `INCRBY`, `-MOVED`/`-ASK` recovery, `db == 0` in cluster)
2. `valkey` — Valkey (`mode: "standalone" | "cluster"`, static/ACL `AUTH` + `SELECT`, Lua-free RESP2 pipelining)
3. `dragonfly` — DragonflyDB single-endpoint shared-nothing RESP2 pool (`AUTH` + `SELECT`)
4. `aws_elasticache` — AWS ElastiCache IAM-authenticated (`Action=connect` SigV4 presigned token, static password rejected, `standalone` or `cluster`)
5. `gcp_memorystore` — GCP Memorystore IAM-authenticated (GCE Metadata / Service Account RS256 JWT cached OAuth2 token, static password rejected, `standalone` or `cluster`)
6. `upstash` — Upstash Serverless REST (`POST /pipeline` JSON + `Authorization: Bearer <token>`)
7. `memcached` — ASCII `incr` / `add`
8. `dynamodb` — AWS SigV4 `DescribeTable`/`CreateTable` (`velda_rate_limit`) + `UpdateItem` (`ADD #c :d`, `ReturnValues: UPDATED_NEW`)
9. `postgres` — Wire v3 Extended Query (`Parse`/`Bind`/`Execute`/`Sync`) + CTE on auto-created `velda.velda_rate_limit`
10. `mysql` — Binary Prepared Statements (`COM_STMT_PREPARE`/`COM_STMT_EXECUTE`) + CTE on auto-created `velda.velda_rate_limit`
11. `nats` — JetStream KV read + `$KV.<bucket>.<key>` publish
12. `clickhouse` — HTTP `SummingMergeTree` `INSERT` + `SELECT sum(delta)`
13. `elasticsearch` — `_bulk` painless scripted upsert + `_mget` readback
14. `s3` — SigV4 `GET` + conditional `PUT` JSON epoch counter objects

Append-only streaming loggers such as `kafka` are intentionally excluded because `rate_limit` requires an addressable State-of-Truth counter store.

## 3. Configuration Recipes

### 3.1 Multi-Tenant Anti-Rotation Edge (Hierarchical + Redis SoT)

```json
{
  "id": "rate_limit",
  "config": {
    "algorithm": "sliding_window",
    "etp_id": "redis-sot",
    "rate_per_second": 500,
    "burst_capacity": 1000,
    "overdraft_percent": 5,
    "key_strategy": "hierarchical",
    "key_headers": ["x-tenant-id", "x-api-key", "client_ip"],
    "fallback_behavior": "memory",
    "sync_interval_ms": 150
  }
}
```

### 3.2 Zero-External Local Spike Guard

```json
{
  "id": "rate_limit",
  "config": {
    "algorithm": "token_bucket",
    "etp_id": null,
    "rate_per_second": 2000,
    "burst_capacity": 5000,
    "overdraft_percent": 10,
    "key_strategy": "client_ip"
  }
}
```

## 4. Wire Contract & Observability Signals

- **Allowed request**: Mutates request headers `x-ratelimit-limit` and `x-ratelimit-remaining` (clamped at `0` during overdraft grace), returning `Action::Continue`.
- **Rejected request**: Inserts bypass headers `retry-after`, `x-ratelimit-remaining: 0`, and `x-ratelimit-limit` into the bypass lane (`insert_bypass`) and returns `Action::Reject(RejectReason::TooManyRequests)` (HTTP `429`).

## 5. Incident Triage Playbook

| Symptom | Root Cause | Mitigation |
| :--- | :--- | :--- |
| Config rejected with `unknown field 'backend'` | Legacy config schema supplied after clean break refactor. | Remove `backend` and `redis_*` fields; configure `etp_id` referencing `etp.json` or `null`. |
| Config rejected by `velda-sync` with non-SoT kind error | `etp_id` points to `kafka`, `loki`, or auth ETP. | Point `etp_id` to one of the 14 supported SoT ETP kinds. |
| Sudden cluster-wide `429` despite low traffic | `fallback_behavior: "deny"` triggered when SoT became unreachable. | Verify ETP connectivity/health logs or switch `fallback_behavior` to `"memory"`. |
| Single tenant saturates all requests across IPs | Working as designed under `hierarchical` tier 0 saturation. | Verify tenant quota (`burst_capacity`) or separate high-volume tenants cleanly. |
