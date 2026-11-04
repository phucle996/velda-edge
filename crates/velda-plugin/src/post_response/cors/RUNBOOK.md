# Post-Response Cross-Origin Resource Sharing Plugin (`cors`) — Operator & Configuration Runbook

---

## 1. Declarative Configuration Reference

Plugin `cors` được khai báo trong `plugins.json` và gắn vào các HTTP route trong `routes.json`:

```json
{
  "id": "cors_enterprise_policy",
  "plugin": "cors",
  "phase": "post_response",
  "config": {
    "allow_origins": [
      "https://dashboard.example.com",
      "https://*.partner.io",
      "https://admin.internal.corp"
    ],
    "allow_credentials": true,
    "expose_headers": [
      "x-request-id",
      "x-trace-id",
      "x-total-count",
      "content-disposition"
    ],
    "max_age_secs": 86400
  }
}
```

### Cấu Hình Public API Mở (Không Cần Cookie/Credentials)

```json
{
  "id": "cors_public_api",
  "plugin": "cors",
  "phase": "post_response",
  "config": {
    "allow_origins": ["*"],
    "allow_credentials": false,
    "expose_headers": ["x-request-id"]
  }
}
```

### Quy Tắc Kiểm Tra Tính Hợp Lệ Của Cấu Hình (Fail-Fast Validation)
1. **Quy Tắc Bảo Mật W3C Bắt Buộc**:
   - Nếu `allow_credentials: true`, mảng `allow_origins` **tuyệt đối không được chứa wildcard `"*"`**. Hệ thống sẽ từ chối nạp cấu hình (`SyncError::Validation`) ngay lập tức để ngăn chặn rò rỉ session cookie.
2. **Quy Tắc Định Dạng Subdomain**:
   - Mẫu subdomain bắt buộc phải chứa tiền tố giao thức (ví dụ `https://*.example.com`). Các mẫu không có scheme (ví dụ `*.example.com`) sẽ bị từ chối.
3. **Giới Hạn `max_age_secs`**:
   - Giá trị `max_age_secs` nếu cấu hình phải $\ge 0$ và $\le 86,400$ (tối đa 24 giờ theo khuyến nghị của các trình duyệt hiện đại).

---

## 2. Hướng Dẫn Xử Lý Sự Cố (Troubleshooting Matrix)

### Sự Cố 1: Trình duyệt báo lỗi `No 'Access-Control-Allow-Origin' header is present on the requested resource`

- **Nguyên nhân**:
  1. Downstream request gửi header `Origin` không khớp với bất kỳ mẫu nào trong `allow_origins`.
  2. Request không phải là request CORS (không mang header `Origin`), nên gateway chủ động bypass không gắn header CORS.
- **Cách khắc phục**:
  1. Kiểm tra log của gateway hoặc bật debug xem header `Origin` từ trình duyệt gửi lên chính xác là gì (chú ý scheme `http` vs `https`, port `:3000` vs `:443`).
  2. Cập nhật `allow_origins` bổ sung domain hoặc cấu hình subdomain wildcard `https://*.domain.com`.

---

### Sự Cố 2: Trình duyệt báo lỗi `The value of the 'Access-Control-Allow-Origin' header in the response must not be the wildcard '*' when the request's credentials mode is 'include'`

- **Nguyên nhân**:
  Ứng dụng frontend sử dụng `fetch(url, { credentials: 'include' })` hoặc `axios.defaults.withCredentials = true`, nhưng gateway lại cấu hình `allow_origins: ["*"]` hoặc `allow_credentials: false`.
- **Cách khắc phục**:
  1. Đổi `allow_credentials` thành `true`.
  2. Bỏ `"*"` ra khỏi `allow_origins` và thay bằng danh sách domain cụ thể:
     ```json
     "allow_origins": ["https://app.example.com", "https://*.corp.internal"],
     "allow_credentials": true
     ```

---

### Sự Cố 3: Client JavaScript không đọc được header tùy biến (ví dụ `x-request-id` hoặc `x-total-count`)

- **Nguyên nhân**:
  Trình duyệt web áp dụng chính sách CORS mặc định chỉ cho phép JavaScript đọc các "CORS-safelisted response headers" (`Cache-Control`, `Content-Language`, `Content-Type`, `Expires`, `Last-Modified`, `Pragma`). Các header khác đều bị giấu.
- **Cách khắc phục**:
  Khai báo rõ danh sách header cần mở trong `expose_headers`:
  ```json
  "expose_headers": ["x-request-id", "x-total-count", "content-disposition"]
  ```

---

### Sự Cố 4: CDN (Cloudflare/Fastly) bị trả nhầm cache giữa các domain khác nhau (Lỗi CORS Cache Mismatch)

- **Nguyên nhân**:
  Response trả về thiếu `Vary: Origin`, khiến CDN dùng chung cache key cho tất cả client.
- **Cách khắc phục trên Velda Edge**:
  Plugin `cors` của Velda Edge **tự động bảo vệ 100%**:
  - Khi bật `allow_credentials: true` hoặc dùng exact reflection, gateway tự động kích hoạt `flags: CORS_FLAG_NEEDS_VARY_ORIGIN` và nạp `Vary: Origin`.
  - Nếu backend upstream đã có `Vary: Accept-Encoding`, gateway dùng bộ quét số nguyên 64-bit SWAR nối thêm `Origin` thành `Vary: Accept-Encoding, Origin`, bảo đảm CDN phân tách cache an toàn tuyệt đối mà không làm hỏng nén Gzip.

---

## 3. Prometheus Observability & Alerting

Khi kết hợp với plugin `prometheus_exporter`, các chỉ số CORS được theo dõi liên tục:

| Metric Name | Type | Labels | Description | Alert Condition |
| :--- | :--- | :--- | :--- | :--- |
| `cors.requests_total` | Counter | `route_id`, `status` | Tổng số response được xử lý qua plugin CORS | Đột biến rớt về 0 khi có traffic |
| `cors.untrusted_origin_total`| Counter | `route_id`, `client_ip` | Số request bị từ chối do Origin không hợp lệ | Tăng đột biến > 50 req/s (Dấu hiệu tấn công CSRF / Cross-Origin Probe) |
| `cors.vary_injected_total` | Counter | `route_id` | Số lần `Vary: Origin` được chèn vào response | Theo dõi tỷ lệ caching |

### Mẫu Prometheus Alert Rule

```yaml
groups:
  - name: velda_cors_security_alerts
    rules:
      - alert: HighUntrustedOriginSpike
        expr: rate(cors_untrusted_origin_total[1m]) > 50
        for: 2m
        labels:
          severity: warning
          subsystem: edge-cors
        annotations:
          summary: "Phát hiện đột biến request từ Origin không hợp lệ trên route {{ $labels.route_id }}"
          description: "Có hơn 50 request/s mang Origin không được phê duyệt. Kiểm tra nguy cơ tấn công Web Injection hoặc cấu hình frontend bị sai lệch domain."
```

---

## 4. Quy Trình Hot-Reload Zero-Downtime

1. Thay đổi cấu hình CORS trên giao diện Console UI hoặc qua API của Go Control Plane (`control-plane/`).
2. Go Control Plane kiểm tra schema JSON, biên dịch snapshot mới vào PostgreSQL.
3. Luồng `velda-sync` nhận thông báo IPC, biên dịch snapshot thành nhị phân `RuntimeSnapshot` atomic pointer swap.
4. Toàn bộ worker traffic của Velda Edge chuyển sang áp dụng cấu hình CORS mới trong **< 1 microsecond**, **hoàn toàn 0 downtime, không rớt kết nối TCP/H2/QUIC nào**.
