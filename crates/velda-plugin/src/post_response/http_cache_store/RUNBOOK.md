# Post-Response HTTP Cache Store Plugin (`http_cache_store`) — Operational Runbook

---

## 1. Tổng Quan Kiến Trúc & Bất Biến Vận Hành (Invariants)

Plugin **`http_cache_store`** thực thi ở giai đoạn `post_response` trong vòng đời phục vụ HTTP. Plugin có nhiệm vụ phân tích tính hợp lệ để lưu trữ của HTTP response trả về từ upstream backend theo chuẩn **RFC 9111**, nạp payload vào phân vùng bộ nhớ chung **[`HttpCacheStorage`]**, và cắm các header chẩn đoán `x-cache` cùng `age` cho client.

### 1.1 Năm Bất Biến Vận Hành Tuyệt Đối (Core Invariants)

1. **Bất Biến Mô Hình Pure Payload (Pure Payload Invariant)**:
   - Bộ nhớ đệm chỉ lưu trữ byte payload thực tế (`raw_body` và `CompressedVariants`), mã trạng thái HTTP (`status`), và định dạng `Content-Type`.
   - **Tuyệt đối không lưu giữ bảng headers (`HeaderMap`)** trong RAM cache để ngăn ngừa rò rỉ token nhạy cảm (`Set-Cookie`, `Authorization`), đồng thời giảm kích thước descriptor xuống dưới 64 bytes (vừa khít 1 L1 cacheline).
2. **Bất Biến Ghi Thẳng DRAM Bằng Non-Temporal Store (DRAM Confinement Invariant)**:
   - Với mọi payload $\ge 256$ bytes, plugin sử dụng lệnh vector AVX2 `_mm256_stream_si256` (`VMOVNTDQ`) xả dữ liệu thẳng xuống RAM qua Write-Combining Buffers.
   - Ngăn chặn hoàn toàn hiện tượng ô nhiễm bộ nhớ đệm CPU L1/L2 (Cache Pollution), bảo toàn 100% không gian cache L1 cho logic routing và pipeline serving.
3. **Bất Biến Đồng Hồ Số Nguyên Coarse Clock (1-ALU Cycle Clock)**:
   - Sử dụng `velda_core::coarse_now_sec()` đọc biến nguyên tử tĩnh `AtomicU64` do luồng nền `SCHED_IDLE` cập nhật mỗi 100ms.
   - Thao tác kiểm tra hết hạn `is_fresh(&self)` thực thi trong 1 chu kỳ so sánh số nguyên trên thanh ghi CPU, **triệt tiêu 100% vDSO syscalls**.
4. **Bất Biến Phân Shard Chống Nghẽn Khóa & False Sharing (64-Striped Cache Shards)**:
   - Bộ nhớ cache được chia thành 64 phân vùng (shards) độc lập, đánh địa chỉ băm `hash % 64`.
   - Mỗi shard được đệm căn lề bộ nhớ 64 bytes (`#[repr(align(64))]`) nhằm loại bỏ hoàn toàn hiện tượng False Sharing giữa các nhân CPU của Frontline Workers.
5. **Bất Biến Thu Hồi Bộ Nhớ $O(1)$ Không Block (Second-Chance CLOCK Eviction)**:
   - Thuật toán xoay kim đồng hồ kiểm tra cờ truy cập nguyên tử `accessed: AtomicBool`. Thao tác đọc `get()` chỉ giữ `RwLock::read()`, cho phép hàng trăm luồng đọc đồng thời với chi phí contention bằng 0.

---

## 2. Đặc Tả Cấu Hình Chuẩn Production

### 2.1 Bảng Tham Số Cấu Hình (`plugins.json`)

| Tham Số | Kiểu Dữ Liệu | Bắt Buộc | Ràng Buộc Hợp Lệ | Giá Trị Khuyến Nghị | Ý Nghĩa Chức Năng |
| :--- | :--- | :---: | :--- | :--- | :--- |
| `zone` | `string` | **Có** | Chuỗi định danh duy nhất | `"default"` | Tên phân vùng cache zone liên kết (phải khớp tuyệt đối với `http_cache_lookup`). |
| `cacheable_status` | `array[integer]` | **Có** | Danh sách mã HTTP hợp lệ (`200..=599`) | `[200, 203, 204, 206, 300, 301, 404]` | Các mã HTTP được phép lưu vào cache. |
| `max_payload_bytes` | `integer` | **Có** | `1..=67108864` (tối đa 64 MB) | `2097152` (2 MB) | Kích thước payload tối đa được phép lưu vào cache RAM. |
| `default_ttl_secs` | `integer` | **Có** | `1..=31536000` (1 giây đến 1 năm) | `300` (5 phút) | Thời gian sống (TTL) mặc định khi upstream response không phát `Cache-Control: max-age`. |
| `respect_cache_control` | `boolean` | **Có** | `true` hoặc `false` | `true` | Tuân thủ nghiêm ngặt chỉ thị `no-store` / `private` và bóc tách TTL từ `max-age` / `s-maxage`. |
| `bypass_authenticated_requests` | `boolean` | **Có** | `true` hoặc `false` | `true` | Kích hoạt lá chắn RFC 9111 §3.5: bỏ qua request mang `Authorization` trừ phi được đánh dấu `public`. |
| `key_headers` | `array[string]` | **Có** | Mảng tên headers phân khúc | `[]` hoặc `["accept-language", "x-tenant-id"]` | Thêm các header tùy biến vào cache key phục vụ multi-tenant caching. |
| `async_precompression` | `boolean` | **Có** | `true` hoặc `false` | `true` | Ủy thác nén trước các biến thể (Gzip, Brotli, Zstd) cho luồng nền với 0ms độ trễ cho client. |
| `max_memory_mb` | `integer` | **Có** | `1..=65536` (tối đa 64 GB) | `256` (256 MB) | Dung lượng RAM tối đa cấp phát cho cache zone này. |

### 2.2 File Cấu Hình Khuyến Nghị Mẫu (`plugins.json`)

```json
{
  "plugins": [
    {
      "id": "edge_cache_store",
      "plugin": "http_cache_store",
      "config": {
        "zone": "api_cache_zone",
        "cacheable_status": [200, 203, 204, 206, 300, 301, 404],
        "max_payload_bytes": 2097152,
        "default_ttl_secs": 300,
        "respect_cache_control": true,
        "bypass_authenticated_requests": true,
        "key_headers": [],
        "async_precompression": true,
        "max_memory_mb": 512
      }
    }
  ]
}
```

### 2.3 Gắn Plugin Vào Tuyến Đường Tuyến Tính (`routes.json`)

```json
{
  "routes": [
    {
      "id": "route_catalog_api",
      "match": {
        "path": {
          "prefix": "/api/v1/catalog"
        }
      },
      "plugins": [
        "edge_cache_lookup",
        "edge_cache_store",
        "edge_compressor"
      ],
      "upstream": "cluster_catalog_backend"
    }
  ]
}
```

---

## 3. Hệ Thống Giám Sát & Chỉ Số Prometheus (Metrics Reference)

Bảng các metrics chuẩn mực được bóc tách từ `HttpCacheStorage` và `velda-observability`:

| Metric Tên | Loại (Type) | Nhãn (Labels) | Mô Tả Ý Nghĩa Vận Hành |
| :--- | :--- | :--- | :--- |
| `velda_cache_requests_total` | Counter | `zone`, `status` (`hit`, `miss`, `bypass`) | Tổng số request đi qua bộ lọc caching phân loại theo kết quả. |
| `velda_cache_store_operations_total` | Counter | `zone`, `mode` (`eager`, `lazy`) | Số lần lưu thành công response mới vào RAM. |
| `velda_cache_store_bytes_total` | Counter | `zone` | Tổng khối lượng payload byte được nạp vào RAM cache. |
| `velda_cache_current_bytes` | Gauge | `zone` | Dung lượng RAM hiện tại mà zone đang chiếm giữ (bytes). |
| `velda_cache_entries_count` | Gauge | `zone` | Tổng số lượng URL / cache keys đang thường trú trong zone. |
| `velda_cache_evictions_total` | Counter | `zone`, `reason` (`expired`, `capacity`) | Số lượng entry bị thu hồi bởi thuật toán Second-Chance CLOCK. |
| `velda_cache_precompression_duration_seconds` | Histogram | `codec` (`gzip`, `brotli`, `zstd`) | Thời gian xử lý nén biến thể bất đồng bộ trên luồng nền. |

---

## 4. Kịch Bản Ứng Cứu Sự Cố Vận Hành (Troubleshooting Playbooks)

### 4.1 Sự Cố 1: Tỷ Lệ Cache Miss Tăng Đột Biến (Miss Rate Spike > 70%)

#### Dấu hiệu nhận biết
- Metric `velda_cache_requests_total{status="miss"}` tăng vọt, trong khi traffic downstream không đổi.
- Băng thông mạng đẩy về Upstream Backend tăng mạnh.
- Header phản hồi client trả về `x-cache: MISS` liên tục.

#### Nguyên nhân phổ biến
1. **Upstream Backend phát chỉ thị chặn Cache**: Backend vô tình trả về `Cache-Control: no-store` hoặc `Cache-Control: private`.
2. **Khác biệt Query String**: Client gửi các param ngẫu nhiên (ví dụ `_ts=1730000000` chống cache). Nếu `ignore_query_order` không giải quyết được các dynamic query keys, mỗi request sẽ sinh ra một key băm mới.
3. **Request mang `Authorization` Header**: Client gửi token Bearer khiến tính năng RFC 9111 §3.5 Safety Shield tự động chuyển sang `BYPASS`.
4. **Vùng nhớ bị đầy**: Kích thước `max_memory_mb` quá nhỏ dẫn tới hiện tượng CLOCK Eviction liên tục đẩy các entry vừa nạp ra ngoài.

#### Quy trình xử lý
1. Kiểm tra header `Cache-Control` và `Authorization` bằng `curl`:
   ```bash
   curl -sv -o /dev/null -H "Host: api.example.com" http://172.16.0.5:8080/api/v1/catalog/items
   ```
2. Kiểm tra metric evictions:
   ```bash
   curl -s http://127.0.0.1:9090/metrics | grep velda_cache_evictions_total
   ```
   Nếu `reason="capacity"` tăng nhanh, tiến hành nâng dung lượng `max_memory_mb` trong `plugins.json` và reload cấu hình qua API Control Plane.
3. Nếu client gửi header phân khúc người dùng, bổ sung tên header vào danh sách `key_headers` để phân vùng hợp lệ.

---

### 4.2 Sự Cố 2: Tràn Bộ Nhớ RAM Cache (OOM Warning / Memory Saturation)

#### Dấu hiệu nhận biết
- Metric `velda_cache_current_bytes` tiệm cận `max_memory_mb * 1024 * 1024`.
- Tần suất gọi `evict_if_needed()` tăng cao.
- CPU của worker tăng nhẹ do phải liên tục quét vòng kim Second-Chance CLOCK.

#### Nguyên nhân
- Kích thước payload trung bình lớn hơn dự kiến nhưng vẫn nằm dưới trần `max_payload_bytes`.
- Số lượng unique URL tăng trưởng theo cấp số nhân (catalog mở rộng).

#### Quy trình xử lý
1. Xác thực dung lượng hiện thời của zone qua CLI quản trị:
   ```bash
   curl -s http://127.0.0.1:9090/metrics | grep velda_cache_current_bytes
   ```
2. Điều chỉnh hạ trần `max_payload_bytes` xuống (ví dụ từ 2 MB xuống 512 KB) để ưu tiên lưu trữ các API JSON nhẹ:
   ```json
   "max_payload_bytes": 524288
   ```
3. Nâng cấp bộ nhớ máy chủ hoặc tăng phân bổ `max_memory_mb`. Nhờ kiến trúc 64 lock-striped shards, việc mở rộng dung lượng cache không làm tăng độ trễ khóa.

---

### 4.3 Sự Cố 3: Client Nhận Dữ Liệu Cache Bị Cũ (Stale Data Issue)

#### Dấu hiệu nhận biết
- Dữ liệu trên database đã cập nhật nhưng client vẫn nhận response cũ từ edge cache.
- Header `Age` trả về có giá trị lớn hơn 0 nhưng chưa vượt quá TTL.

#### Quy trình xử lý
1. Kiểm tra thời gian tồn tại qua header `Age`:
   ```bash
   curl -I http://172.16.0.5:8080/api/v1/catalog/product/123 | grep -i age
   ```
2. Thực hiện lệnh Xóa Cache Tức Thì (Instant Cache Purge) qua Control Plane API:
   ```bash
   curl -X POST http://127.0.0.1:8001/api/v1/cache/purge \
     -H "Content-Type: application/json" \
     -d '{"zone": "api_cache_zone", "uri": "/api/v1/catalog/product/123"}'
   ```
3. Nếu muốn xả toàn bộ phân vùng:
   ```bash
   curl -X POST http://127.0.0.1:8001/api/v1/cache/clear \
     -H "Content-Type: application/json" \
     -d '{"zone": "api_cache_zone"}'
   ```

---

## 5. Quy Chuẩn Đóng Gói & Bảo Trì Hệ Thống

1. **Không Chạy Disk I/O**: Cache Zone hoàn toàn vận hành in-memory (RAM-confined). Không bao giờ cấu hình cache ghi xuống đĩa NVMe/SSD trên hot path.
2. **Theo Dõi CPU L1D Cache Misses**: Sử dụng định kỳ `perf stat -p <pid> -e L1-dcache-load-misses` để đảm bảo cơ chế AVX2 Non-Temporal Stores tiếp tục hoạt động chính xác khi có cập nhật kernel hoặc compiler toolchain mới.
