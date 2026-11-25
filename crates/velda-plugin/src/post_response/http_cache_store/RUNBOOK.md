# Post-Response HTTP Cache Store Plugin (`http_cache_store`) — Operational Runbook

---

## 1. Tổng Quan Kiến Trúc & Bất Biến Vận Hành (Invariants)

Plugin **`http_cache_store`** thực thi ở giai đoạn `post_response` (`PostResponsePlugin::on_response(&self, req_state, req_headers, resp)`), phân tích điều kiện lưu cache của response trả về từ upstream backend theo **RFC 9111**, nạp payload vào phân vùng bộ nhớ chung **[`HttpCacheStorage`]**, và gắn header `x-cache` cùng `age`.

### 1.1 Các Bất Biến Vận Hành Cốt Lõi

1. **Pure Payload Invariant**:
   - Cache slot chỉ lưu payload thực tế (`raw_body` / `CompressedVariants`), mã HTTP (`status`), và `Content-Type` — **không lưu `HeaderMap`/`HeaderWorkspace`** của response upstream (chặn rò rỉ `Set-Cookie`, token nội bộ).
2. **Route + Method + Header Keying (Đồng Bộ Hai Pha Qua `RequestState`)**:
   - Cả `http_cache_lookup` (`pre_upstream`) và `http_cache_store` (`post_response`) cùng tính 64-bit FxHash từ `(req_state.route_id, req_state.method, compiled_key_headers)` qua `HttpCacheStorage::route_key_hash`. Không truyền header trung gian giữa hai pha.
   - `validate_plugin_dependencies` bắt buộc mọi plugin dùng chung `zone` phải có cùng tập `key_headers` (sau chuẩn hóa chữ thường và sắp xếp).
3. **Hard Cache RAM Ceiling Theo Phần Trăm Hệ Thống**:
   - Tổng ngân sách RAM của các zone không được vượt quá **50%** bộ nhớ hệ thống/cgroup đo bởi `velda_core::hardware::global_hardware_topology().memory_bytes()` (`max_cache_memory_mb()`), vừa được kiểm tra lúc sync lẫn clamp cứng khi khởi tạo `HttpCacheStorage`.
4. **DRAM Confinement (`dram.rs`)**:
   - Nhánh `x86_64 + avx2` ghi bằng `_mm256_stream_si256` (`>= 64` bytes) / `_mm_stream_si128` (`32..64`); `x86_64` SSE2 dùng `_mm_stream_si128`; nền tảng khác (`aarch64`/portable) fallback về `copy_nonoverlapping`.
5. **64-Striped Shards & CLOCK Eviction**:
   - Chia 64 shards (`#[repr(align(64))]`) chống false sharing; đọc dùng `RwLock::read()` + bit truy cập nguyên tử `AtomicBool`; thu hồi theo vòng đồng hồ CLOCK trong không gian arena slab `2 MiB` cố định.

---

## 2. Đặc Tả Cấu Hình Chuẩn Production (`plugins.json`)

> Unknown fields đều bị từ chối lúc parse (`#[serde(deny_unknown_fields)]`).

| Tham Số | Kiểu Dữ Liệu | Bắt Buộc | Ràng Buộc Hợp Lệ | Mặc Định / Ghi Chú | Ý Nghĩa Chức Năng |
| :--- | :--- | :---: | :--- | :--- | :--- |
| `zone` | `string` | **Có** | Chuỗi khác rỗng | Phải khớp `http_cache_lookup` | Tên phân vùng cache zone liên kết. |
| `cacheable_methods` | `string[]` | Không | Mảng method HTTP hợp lệ, khác rỗng | `["GET", "HEAD"]` | Các HTTP method được phép ghi vào cache. |
| `cacheable_status` | `u16[]` | **Có** | Mảng mã HTTP `200..=599`, khác rỗng | Khai báo tường minh | Các mã trạng thái HTTP cho phép lưu cache. |
| `max_payload_bytes` | `usize` | **Có** | `1 ..= 2_097_152` (`SLAB_SIZE`, 2 MiB) | `2097152` | Kích thước tối đa một payload nạp vào slab 2 MiB. |
| `default_ttl_secs` | `u64` | **Có** | `1 ..= 31_536_000` | `300` | TTL mặc định khi upstream không phát `max-age` / `s-maxage`. |
| `respect_cache_control` | `bool` | **Có** | `true` hoặc `false` | `true` | Tôn trọng `Cache-Control: no-cache`, `no-store`, `private` (trả `BYPASS`) và trích xuất `max-age`/`s-maxage`/`public`. |
| `bypass_authenticated_requests` | `bool` | **Có** | `true` hoặc `false` | `true` | Bỏ qua lưu request có `Authorization` trừ khi upstream trả `public` hoặc `authorization` nằm trong `key_headers`. |
| `key_headers` | `string[]` | **Có** | Tên header RFC 9110 (`[]` nếu không phân khúc); phải khớp `http_cache_lookup` cùng `zone` | `[]` | Headers đưa vào cache key (ví dụ `["x-tenant-id"]`, `["x-user-id"]`). |
| `async_precompression` | `bool` | **Có** | `true` hoặc `false` | `true` | Offload nén biến thể (`gzip`, `brotli`, `zstd`) qua `spawn_blocking` nếu payload raw `>= 256`B và MIME nén được. |
| `max_memory_mb` | `usize` | **Có** | `1 ..= max_cache_memory_mb()` (`50%` RAM/cgroup từ `velda-core`) | Tùy phần cứng | Dung lượng RAM tối đa cấp cho zone này. |

### Ví Dụ Cấu Hình Mẫu (`plugins.json`)

```json
{
  "id": "http_cache_store",
  "config": {
    "zone": "api_cache",
    "cacheable_methods": ["GET", "HEAD"],
    "cacheable_status": [200, 203, 204, 206, 300, 301, 404],
    "max_payload_bytes": 2097152,
    "default_ttl_secs": 300,
    "respect_cache_control": true,
    "bypass_authenticated_requests": true,
    "key_headers": ["x-tenant-id"],
    "async_precompression": true,
    "max_memory_mb": 256
  }
}
```

---

## 3. Khắc Phục Sự Cố Thường Gặp (Troubleshooting Playbooks)

### 3.1 Tỷ Lệ `X-Cache: MISS` hoặc `BYPASS` Cao Bất Thường

1. **Upstream phát chỉ thị chặn cache**: Kiểm tra header `Cache-Control` từ backend — các giá trị `no-cache`, `no-store`, `private`, hoặc `max-age=0` đều khiến `http_cache_store` đặt `X-Cache: BYPASS` và không ghi cache.
2. **Method không nằm trong `cacheable_methods`**: Ví dụ request là `POST` khi cấu hình mặc định chỉ cho phép `GET` / `HEAD` (`on_response` thoát sớm, không đặt `X-Cache`). Kiểm tra đồng bộ `cacheable_methods` giữa `http_cache_lookup` và `http_cache_store`.
3. **Request có `Authorization` nhưng không chia theo key hoặc `public`**: Nếu `bypass_authenticated_requests == true`, request mang `Authorization` sẽ `BYPASS` trừ khi response có `Cache-Control: public` hoặc `authorization` có mặt trong `key_headers`.
4. **Payload vượt quá `max_payload_bytes` / `SLAB_SIZE` (2 MiB)**: Response lớn hơn `max_payload_bytes` sẽ bị gắn `X-Cache: BYPASS`.
5. **Thiếu biến thể plain-text (`raw_body`) cho client không gửi `Accept-Encoding`**: Khi slot chỉ có biến thể nén mà client không gửi `Accept-Encoding`, `http_cache_lookup` chủ động trả MISS để forward upstream lấy bản raw bổ sung vào slot.
6. **Cardinality quá cao ở `key_headers`**: Nếu đưa header có giá trị biến thiên theo từng request (như nonce/trace-id) vào `key_headers`, mỗi request sẽ băm ra một slot mới và làm tăng CLOCK eviction.

### 3.2 Lỗi Validate Khi Sync Cấu Hình

- `HTTP cache zone '<zone>' has mismatched key_headers`: `http_cache_lookup` và `http_cache_store` dùng chung một `zone` đang khai báo `key_headers` khác nhau; đồng bộ lại danh sách ở cả hai plugin.
- `max_memory_mb (...) exceeds ... (50% of system memory ceiling)` hoặc `Aggregate HTTP cache memory budget across zones ...`: giảm `max_memory_mb` của từng zone để tổng theo các zone duy nhất không vượt trần 50% RAM vật lý/cgroup.
- `preferred_encodings cannot be empty` / `unsupported encoding`: kiểm tra `http_cache_lookup` phải khai báo ít nhất 1 encoding trong tập `zstd`, `br`, `gzip` (không trùng, không dùng `deflate`).
