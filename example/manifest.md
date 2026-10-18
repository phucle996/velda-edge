# `manifest.json` — Configuration Manifest Specification

The `manifest.json` file serves as the root orchestration catalog for **Velda Sync** (`velda-sync`). It coordinates domain files, revision tracking, cryptographic checksum verification, incremental delta sync from the Control Plane, and atomic compilation into binary artifacts (`*.bin`).

---

## 1. Architectural Mindset & Zero-Downtime Sync

### 1.1 Incremental Synchronization & Delta Detection
Instead of re-parsing and re-compiling all configuration domains upon every update, `velda-sync` inspects `manifest.json`:
1. Compares `manifest.revision` and overall SHA-256 checksum. If identical, synchronization short-circuits in $<1\text{ms}$.
2. If the manifest has advanced, `velda-sync` compares per-domain checksums/revisions, downloading and recompiling only changed files.

### 1.2 The LKG (Last-Known-Good) Pattern
Synchronized configuration is stored in two directories:
- `config/`: Validated JSON files (`listeners.json`, `routes.json`, `upstreams.json`, `plugins.json`, `tls.json`, `manifest.json`).
- `runtime/`: Pre-compiled binary artifacts (`listeners.bin`, `routes.bin`, `upstreams.bin`, `plugins.bin`, `tls.bin`).

If the Control Plane goes offline or network connectivity drops, the Data Plane boots or reboots autonomously from the persisted LKG binary artifacts in RAM without data loss or downtime.

### 1.3 Atomic Reload via IPC
Once all modified binary files are written to disk, `velda-sync` sends a structured notification across a Unix Domain Socket (UDS) to the Data Plane. The Data Plane memory-maps the updated binary files and performs an atomic pointer swap (`arc-swap`) with zero dropped requests.

---

## 2. Schema Definition & Top-Level Fields

```json
{
  "schema_version": 1,
  "revision": 42,
  "configuration": {
    "files": [ /* array of FileEntry objects */ ]
  }
}
```

| Field | Type | Required | Description |
| :--- | :--- | :---: | :--- |
| `schema_version` | `integer` | Yes | Schema version. Must be `1`. |
| `revision` | `integer` | Yes | Monotonically increasing manifest revision number. |
| `configuration` | `object` | Yes | Container holding the file manifest. |
| `configuration.files` | `array` | Yes | List of managed domain configuration files. |

---

## 3. `FileEntry` Object Specification

| Field | Type | Required | Description |
| :--- | :--- | :---: | :--- |
| `name` | `string` | Yes | Domain name identifier: `"listeners"`, `"routes"`, `"upstreams"`, `"plugins"`, or `"tls"`. |
| `path` | `string` | Yes | Relative filename in the configuration bundle (e.g., `"listeners.json"`). |
| `required` | `boolean` | Yes | Whether this domain is strictly required for gateway operation. If `true` and missing, sync aborts. |
| `revision` | `integer` | No | Domain-specific revision number for fine-grained delta tracking. Falls back to `manifest.revision` if omitted. |
| `checksum_sha256` | `string` | No | Hex-encoded SHA-256 hash of the JSON content for pre-download validation. |

---

## 4. Managed Domain Files

| Domain Name | Config File | Binary Output | Required | Description |
| :--- | :--- | :--- | :---: | :--- |
| `listeners` | `listeners.json` | `listeners.bin` | Yes | Ingress ports, protocols, and early `pre_route` plugins. |
| `routes` | `routes.json` | `routes.bin` | Yes | Routing trie rules and phased plugin chains (`pre_upstream`, `post_response`). |
| `upstreams` | `upstreams.json` | `upstreams.bin` | Yes | Backend pools, DNS discovery, load balancing, health checks. |
| `plugins` | `plugins.json` | `plugins.bin` | Yes | Plugin definitions and typed configurations. |
| `tls` | `tls.json` | `tls.bin` | No | Server TLS certificates, private keys, and client CAs. |
