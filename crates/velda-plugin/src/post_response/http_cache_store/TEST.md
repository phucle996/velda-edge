# HTTP Cache Store & Shared Cache Tests (`http_cache_store` / `http_cache`)

Báo cáo kiểm thử cho `crates/velda-plugin/src/post_response/http_cache_store`, `crates/velda-plugin/src/pre_upstream/http_cache_lookup`, `crates/velda-plugin/src/http_cache`, và cross-plugin dependency validation trong `crates/velda-plugin/src/validation.rs`.

---

## 1. Lệnh Chạy & Kết Quả Thực Tế

```bash
cargo test -p velda-plugin -- http_cache
```

Kết quả quan sát (`448` unit tests trong `velda-plugin`):

```text
test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 439 filtered out
```

Kiểm tra đồng bộ `key_headers` theo `zone` trong `validate_plugin_dependencies`:

```bash
cargo test -p velda-plugin -- validation::tests::test_same_zone_key_headers_parity_enforced
```

```text
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 447 filtered out
```

---

## 2. Danh Mục Ca Kiểm Thử Đã Xác Thực

### 2.1 Shared Cache Core (`crates/velda-plugin/src/http_cache/`)

| Test Case | Phạm Vi Kiểm Chứng |
| :--- | :--- |
| `http_cache::constants::tests::test_cache_memory_ceiling_scales_with_core_memory` | Tính trần `max_cache_memory_mb_for` bằng `50%` bộ nhớ hệ thống từ `velda_core::hardware::global_hardware_topology().memory_bytes()`, clamp tối thiểu 2 slab (4 MiB). |
| `http_cache::key::tests::test_route_id_method_and_key_headers_partitioning` | Chuẩn hóa `key_headers` không phụ thuộc thứ tự/hoa-thường, cô lập hash theo `route_id`, `HttpMethod`, giá trị header phân khúc, và phân biệt thiếu header vs giá trị rỗng. |
| `http_cache::entity::tests::test_negotiate_variant_without_accept_encoding_requires_raw` | Thiếu `Accept-Encoding` bắt buộc phục vụ `VariantKind::Raw` nếu có `raw_body`, trả `None` (MISS) nếu slot chỉ có biến thể nén. |
| `http_cache::entity::tests::test_negotiate_variant_follows_configured_priority_and_whitelist` | Chọn biến thể nén đúng thứ tự ưu tiên `preferred_encodings`, lọc bỏ encoding ngoài allowlist, fallback `Raw` hoặc `None` khi allowlist rỗng. |

### 2.2 Post-Response Store (`crates/velda-plugin/src/post_response/http_cache_store/`)

| Test Case | Phạm Vi Kiểm Chứng |
| :--- | :--- |
| `post_response::http_cache_store::validation::tests::test_store_config_validation_rules_and_deny_unknown_fields` | Hợp lệ hóa `cacheable_methods`, `cacheable_status`, `max_payload_bytes`, `max_memory_mb <= max_cache_memory_mb()`, và từ chối unknown fields (`#[serde(deny_unknown_fields)]`). |
| `post_response::http_cache_store::plugin::tests::test_upstream_no_cache_and_non_cacheable_method_bypass` | Upstream `Cache-Control: no-cache` trả `X-Cache: BYPASS` và không ghi cache; method ngoài `cacheable_methods` (`POST`) bỏ qua lưu trữ không gắn `X-Cache`. |

### 2.3 Pre-Upstream Lookup & Two-Phase Flow (`crates/velda-plugin/src/pre_upstream/http_cache_lookup/` & `validation.rs`)

| Test Case | Phạm Vi Kiểm Chứng |
| :--- | :--- |
| `pre_upstream::http_cache_lookup::plugin::tests::test_config_validation_preferred_encodings_required_and_ordered` | Bắt buộc `preferred_encodings` khác rỗng, giữ nguyên thứ tự cấu hình, từ chối trùng lặp/`deflate`/unknown fields cũ (`ignore_query_order`), từ chối vượt trần RAM 50% đơn lẻ và tổng hợp đa zone. |
| `pre_upstream::http_cache_lookup::plugin::tests::test_two_phase_shared_storage_hit_and_variant_priority` | Phối hợp `http_cache_lookup` + `http_cache_store` qua `RequestState`, trả `Action::Respond(L7Response)` với `X-Cache: HIT`, `Content-Encoding`, `Vary: Accept-Encoding`, cô lập theo `route_id` và `key_headers`. |
| `pre_upstream::http_cache_lookup::plugin::tests::test_missing_accept_encoding_misses_when_only_compressed_then_fills_raw` | Client không gửi `Accept-Encoding` sẽ MISS khi slot chỉ có `zstd`, sau đó `http_cache_store` nạp `raw_body` cho các lượt plain-text kế tiếp mà không ghi đè mất biến thể nén. |
| `validation::tests::test_same_zone_key_headers_parity_enforced` | Từ chối cấu hình lệch `key_headers` giữa `http_cache_lookup` và `http_cache_store` dùng chung `zone`; cho phép sai khác thứ tự/hoa-thường và cho phép khác `key_headers` trên các `zone` tách biệt. |

---

## 3. Lệnh Kiểm Tra Chuẩn Crate

```bash
cargo fmt --check
cargo test -p velda-plugin
cargo clippy --all-targets --all-features -- -D warnings
```
