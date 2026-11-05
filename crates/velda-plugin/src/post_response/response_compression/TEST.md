# Response Compression Plugin — Test & Verification Suite

This document outlines the testing architecture, automated test suites, edge-case coverage, and verification recipes for the `response_compression` plugin in **Velda Edge**.

---

## 1. Testing Architecture & Test Pyramid

Testing for `response_compression` follows a strict three-tier verification pyramid:

```
         ▲
        / \     Level 3: Live Azure Distributed Benchmarking
       /   \    (wrk, fixed 32,759B JSON, wire bandwidth, mpstat profiling)
      /-----\
     /       \   Level 2: Plugin Engine & Factory Integration Tests
    /         \  (Guard conditions, MIME matching, Vary injection, status codes)
   /-----------\
  /             \ Level 1: In-Memory Unit Tests
 /_______________\ (SIMD streaming store, RFC 9110 negotiation, codec roundtrips)
```

---

## 2. Automated Test Suite Matrix

The plugin test suite is distributed across bounded module files:

| Test Name | Module | Target Component | Verifications & Invariants |
| :--- | :--- | :--- | :--- |
| `test_streaming_copy_correctness` | `simd.rs` | Hardware Streaming Copy | Tests arbitrary buffer lengths (0B to 64KB). Verifies output matches input byte-for-byte using AVX2, SSE2, and scalar copy routines. |
| `test_scalar_fallback` | `simd.rs` | Scalar Fallback | Verifies non-SIMD fallback functions accurately when advanced vector extensions are disabled. |
| `test_builtin_dictionary_integrity` | `dictionary.rs` | Built-in Dictionary | Verifies `BUILTIN_COMMON_JSON_REST_DICT` is exactly 16,384 bytes and resolves from `"common_json_rest"`, `"default"`, or `"built-in:common_json_rest"`. |
| `test_dictionary_config_parsing` | `dictionary.rs` | Dictionary Config | Validates `ZstdDictionaryConfig` parsing from `None`, empty string, `"null"`, built-in tokens, and Base64-encoded strings. |
| `test_algorithm_parsing` | `negotiation.rs` | Token Matching | Validates canonical algorithm token matching (`gzip`, `x-gzip`, `deflate`, `br`, `brotli`, `zstd`). |
| `test_parse_accept_encoding_quality_values` | `negotiation.rs` | Quality Factors | Validates zero-allocation stack parsing of complex `Accept-Encoding` headers with fractional `q=` values (e.g. `gzip, br;q=0.9, zstd;q=1.0, *;q=0.1`). |
| `test_negotiate_algorithm_priority` | `negotiation.rs` | Negotiation Logic | Tests server priority tie-breaking on equal `q`, client highest `q` priority, and explicit codec rejection (`q=0`). |
| `test_is_identity_acceptable` | `negotiation.rs` | Identity Codec | Validates RFC 9110 rules for uncompressed `identity` encoding acceptance. |
| `test_roundtrip_gzip` | `engine.rs` | Gzip Codec | Validates RFC 1952 compression and decompression integrity using `flate2`. |
| `test_roundtrip_deflate` | `engine.rs` | Deflate Codec | Validates RFC 1951 Deflate compression and decompression integrity. |
| `test_roundtrip_brotli` | `engine.rs` | Brotli Codec | Validates RFC 7932 Brotli compression and decompression integrity. |
| `test_roundtrip_zstd` | `engine.rs` | Zstandard Codec | Validates RFC 8878 Zstandard compression and decompression integrity. |
| `test_roundtrip_zstd_with_dict` | `engine.rs` | Dictionary Compression | Tests compression and decompression using pre-trained dictionary buffer. |
| `test_compression_gzip_application_json` | `plugin.rs` | PostResponse Hook | Tests end-to-end plugin execution on JSON response: verifies `content-encoding: gzip`, `vary: Accept-Encoding`, and updated `content-length`. |
| `test_compression_zstd_with_builtin_dict` | `plugin.rs` | Built-in Dict Integration | Tests end-to-end execution with `BuiltinDictionary::CommonJsonRest`: verifies roundtrip decompression using the built-in dictionary. |
| `test_from_config_strict_validation` | `plugin.rs` | Configuration Parser | Tests schema deserialization from `PluginConfig`, default values, and semantic bounds. |
| `test_guards_prevent_compression` | `plugin.rs` | Operational Guards | Verifies compression is skipped for: Status 204 No Content, already compressed payload (`Content-Encoding` present), and non-matching MIME types (`image/png`). |

---

## 3. Running Automated Tests

### 3.1 Run Dedicated Plugin Tests
```bash
cargo test -p velda-plugin --lib post_response::response_compression
```

### 3.2 Run Full Plugin Subsystem Tests
```bash
cargo test -p velda-plugin
```

### 3.3 Run Code Quality Verifications
```bash
# Check code formatting
cargo fmt --check

# Check compiler warnings and lints
cargo clippy -p velda-plugin --all-targets -- -D warnings
```

---

## 4. Live Cloud Verification Recipe (Azure)

To verify the plugin under production traffic on Azure:

```bash
# 1. Start Gateway on VM2
sudo systemctl restart velda-edge.service

# 2. Verify compression from VM1
curl -s -i -H "Accept-Encoding: zstd" http://172.16.0.5:80/api/v1/compress/json | head -n 12

# 3. Execute 15s load test with wrk
wrk -t2 -c64 -d15s --latency -H "Accept-Encoding: zstd" http://172.16.0.5:80/api/v1/compress/json
```
