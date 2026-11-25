# Header Mutation Plugin (`header_mutation`) — Test Verification Report

Verification summary for `crates/velda-plugin/src/pre_upstream/header_mutation` covering explicit static header deletion, overwrite, append, bypass-tagged slot mutation, and validation constraints.

---

## 1. Verification Commands & Observed Results

```bash
cargo test -p velda-plugin -- pre_upstream::header_mutation
```

Observed output:

```text
test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 424 filtered out
```

Integration test in `crates/velda-plugin/src/pre_upstream/mod.rs`:

```bash
cargo test -p velda-plugin -- pre_upstream::tests::test_pre_upstream_header_mutation
```

Full crate & workspace checks run after final edits:

- `cargo fmt --check`: exited `0`
- `cargo test -p velda-plugin`: `435 passed; 0 failed`
- `cargo clippy --all-targets --all-features -- -D warnings`: exited `0`

---

## 2. Verified Test Catalog

### 2.1 Plugin Execution Tests ([plugin.rs](plugin.rs))

| Test Case | Scope Verified |
| :--- | :--- |
| `test_does_not_touch_unconfigured_headers` | Confirms unconfigured headers (`Connection`, `TE: trailers`, `Upgrade`) remain untouched when only `remove` is configured. |
| `test_can_remove_and_overwrite_bypass_tagged_headers` | Confirms `remove` deletes `TAG_BYTE_BYPASS_BIT` headers (`x-request-id`, `x-forwarded-for`), `set` overwrites `host`, `append` writes `x-trace`, and untouched bypass headers (`x-keep-bypass`) remain intact. |
| `test_set_overwrites_multi_value_and_append_adds_slot` | Confirms `remove` deletes target header, `set` collapses prior multi-value slots into 1 new static value, and `append` preserves existing slots while adding a new slot. |
| `test_from_config_complete` | Confirms JSON deserialization via `PluginConfig`, population of `remove`/`set`/`append`, and rejection of empty `{}` config. |

### 2.2 Configuration Validation Tests ([validation.rs](validation.rs))

| Test Case | Scope Verified |
| :--- | :--- |
| `test_valid_header_mutation` | Accepts explicit removal/overwrite of headers including `x-request-id` and `Host`, plus `append_headers`. |
| `test_partial_config_blocks_allowed` | Accepts configs specifying only a subset of blocks (e.g. `remove_headers` alone). |
| `test_invalid_header_name_rejected` | Rejects spaces, invalid `:` characters, leading/trailing whitespace, and `:path` pseudo-headers. |
| `test_crlf_and_control_chars_rejected` | Rejects `\r\n` injection and ASCII null/control bytes in header values. |
| `test_literal_dollar_strings_accepted_as_static_values` | Confirms `$`-prefixed strings are validated and preserved as literal static values. |
| `test_noop_configuration_rejected` | Rejects configs with no active entries in `remove_headers`, `set_headers`, or `append_headers`. |
| `test_unknown_fields_rejected` | Rejects unrecognized fields (`#[serde(deny_unknown_fields)]`). |

### 2.3 Stage Integration Test ([`pre_upstream/mod.rs`](../mod.rs))

| Test Case | Scope Verified |
| :--- | :--- |
| `pre_upstream::tests::test_pre_upstream_header_mutation` | Executes `HeaderMutationPlugin` through `run_pre_upstream_plugins` chain, verifying deletion of `x-internal-secret` and insertion of `x-forwarded-host`. |
