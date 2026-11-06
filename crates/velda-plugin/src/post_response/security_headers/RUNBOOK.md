# Post-Response Security Headers Plugin (`security_headers`) — Operator & Configuration Runbook

---

## 1. Declarative Configuration Reference

Plugin `security_headers` được cấu hình trong `plugins.json` và gắn vào các HTTP route trong `routes.json`. 

> [!IMPORTANT]
> **Quy Tắc Khai Báo 100% Tường Minh (100% Explicit Configuration Invariant)**:
> Mọi tham số cấu hình bắt buộc phải xuất hiện tường minh trong block `config` của `plugins.json`. Không cho phép bỏ sót bất kỳ trường nào. Để tắt hoặc bỏ qua một header, hãy gán giá trị sentinel `"none"` hoặc `null`.

### 1.1 Bảng Tham Số Cấu Hình Chi Tiết (`plugins.json`)

| Trường | Kiểu dữ liệu | Bắt buộc | Giá trị cho phép / Giới hạn | Giá trị khuyến nghị | Mô tả chức năng |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `hsts` | `object` | **Có** | Object chứa `enable`, `max_age_secs`, `include_subdomains`, `preload` | Xem bên dưới | Cấu hình HTTP Strict Transport Security (RFC 6797). |
| `hsts.enable` | `boolean` | **Có** | `true` hoặc `false` | `true` | Bật/tắt header `Strict-Transport-Security`. |
| `hsts.max_age_secs` | `integer` | **Có** | `1..=63072000` (bắt buộc $> 0$ khi bật) | `31536000` (1 năm) | Thời gian (giây) trình duyệt ép buộc HTTPS. |
| `hsts.include_subdomains` | `boolean` | **Có** | `true` hoặc `false` | `true` | Áp dụng chính sách HSTS cho toàn bộ subdomain. |
| `hsts.preload` | `boolean` | **Có** | `true` hoặc `false` | `false` | Đăng ký vào danh sách HSTS Preload của trình duyệt. |
| `frame_options` | `string` | **Có** | `"DENY"`, `"SAMEORIGIN"`, `"ALLOW-FROM"`, hoặc `"none"` | `"DENY"` | Chống Clickjacking qua `X-Frame-Options`. |
| `content_type_nosniff` | `boolean` | **Có** | `true` hoặc `false` | `true` | Khi true, tiêm `X-Content-Type-Options: nosniff`. |
| `referrer_policy` | `string` | **Có** | Chuẩn W3C hợp lệ hoặc `"none"` | `"strict-origin-when-cross-origin"` | Kiểm soát rò rỉ URL Referrer. |
| `content_security_policy` | `string \| null` | **Có** | Chuỗi CSP hợp lệ, `"none"`, hoặc `null` | `"none"` | Chỉ thị Content-Security-Policy (W3C Level 3). |
| `permissions_policy` | `string \| null` | **Có** | Chuỗi permissions policy, `"none"`, hoặc `null` | `"none"` | Vô hiệu hóa tính năng phần cứng trình duyệt (camera, mic). |
| `cross_origin_opener_policy` | `string \| null` | **Có** | `"same-origin"`, `"same-origin-allow-popups"`, `"unsafe-none"`, `"none"`, hoặc `null` | `"same-origin"` | Cô lập tiến trình duyệt web COOP chống Spectre. |
| `cross_origin_embedder_policy` | `string \| null` | **Có** | `"require-corp"`, `"credentialless"`, `"unsafe-none"`, hoặc `"none"`, hoặc `null` | `"require-corp"` | Cô lập tài nguyên nhúng COEP cho `SharedArrayBuffer`. |
| `cross_origin_resource_policy` | `string \| null` | **Có** | `"same-origin"`, `"same-site"`, `"cross-origin"`, `"none"`, hoặc `null` | `"same-origin"` | Bảo vệ tài nguyên CORP chống đọc trộm cross-origin. |
| `cross_domain_policies` | `string \| null` | **Có** | `"none"`, `"master-only"`, `"by-content-type"`, `"all"`, hoặc `null` | `"none"` | Khóa chặt Adobe Flash/PDF qua `X-Permitted-Cross-Domain-Policies`. |
| `xss_protection` | `string \| null` | **Có** | `"0"`, `"1; mode=block"`, `"none"`, hoặc `null` | `"0"` | Chuẩn OWASP: `"0"` vô hiệu hóa bộ lọc XSS lỗi thời dễ bị vũ khí hóa. |

### 1.2 Manifest Mẫu Toàn Diện Cho Web / SPA (`plugins.json`)

```json
{
  "id": "edge_security_headers",
  "plugin": "security_headers",
  "config": {
    "hsts": {
      "enable": true,
      "max_age_secs": 31536000,
      "include_subdomains": true,
      "preload": false
    },
    "frame_options": "DENY",
    "content_type_nosniff": true,
    "referrer_policy": "strict-origin-when-cross-origin",
    "content_security_policy": "default-src 'self'; frame-ancestors 'none'",
    "permissions_policy": "camera=(), microphone=(), geolocation=()",
    "cross_origin_opener_policy": "same-origin",
    "cross_origin_embedder_policy": "require-corp",
    "cross_origin_resource_policy": "same-origin",
    "cross_domain_policies": "none",
    "xss_protection": "0"
  }
}
```

### Cấu Hình Tối Giản Cho API Microservices (Không Phải HTML Frontend)

Đối với các route JSON API thuần túy, không phục vụ trang HTML trình duyệt, chỉ cần các header tối thiểu:

```json
{
  "id": "api_security_headers",
  "plugin": "security_headers",
  "config": {
    "hsts": {
      "enable": true,
      "max_age_secs": 31536000,
      "include_subdomains": true,
      "preload": false
    },
    "frame_options": "DENY",
    "content_type_nosniff": true,
    "referrer_policy": "no-referrer",
    "content_security_policy": "none",
    "permissions_policy": "none",
    "cross_origin_opener_policy": "none",
    "cross_origin_embedder_policy": "none",
    "cross_origin_resource_policy": "same-origin",
    "cross_domain_policies": "none",
    "xss_protection": "0"
  }
}
```

### 1.4 Gắn Plugin Vào Tuyến Đường (`routes.json`)

Khai báo ID của plugin (`edge_security_headers`) vào mảng `plugins` của từng route mong muốn:

```json
{
  "routes": [
    {
      "id": "route_web_frontend",
      "kind": "l7",
      "listener": "http-in",
      "match": {
        "path_prefix": "/"
      },
      "plugins": [
        "edge_security_headers"
      ],
      "upstream": "cluster_frontend"
    }
  ]
}
```

---

## 2. Quy Tắc Kiểm Tra Tính Hợp Lệ Của Cấu Hình (Fail-Fast Validation)

1. **HSTS Max Age**:
   - Khi `hsts.enable: true`, giá trị `max_age_secs` bắt buộc phải $> 0$. Cấu hình `0` khi bật sẽ bị từ chối ngay lập tức (`SyncError::Validation`).
2. **Giá Trị Hợp Lệ Của `frame_options`**:
   - Phải là `"DENY"`, `"SAMEORIGIN"`, `"ALLOW-FROM"`, hoặc `"none"`.
3. **Giá Trị Hợp Lệ Của `referrer_policy`**:
   - Phải là một trong các chuẩn W3C: `"no-referrer"`, `"no-referrer-when-downgrade"`, `"origin"`, `"origin-when-cross-origin"`, `"same-origin"`, `"strict-origin"`, `"strict-origin-when-cross-origin"`, `"unsafe-url"`, hoặc `"none"`.
4. **Giá Trị Hợp Lệ Của COOP**:
   - `"same-origin"`, `"same-origin-allow-popups"`, `"unsafe-none"`, hoặc `"none"`.
5. **Giá Trị Hợp Lệ Của COEP**:
   - `"require-corp"`, `"credentialless"`, `"unsafe-none"`, hoặc `"none"`.
6. **Giá Trị Hợp Lệ Của CORP**:
   - `"same-origin"`, `"same-site"`, `"cross-origin"`, hoặc `"none"`.

---

## 3. Hướng Dẫn Xử Lý Sự Cố (Troubleshooting Matrix)

### Sự Cố 1: Trang web không thể nhúng vào iframe trên cổng đối tác
* **Triệu chứng**: Trình duyệt đối tác thông báo lỗi `Refused to display 'https://example.com' in a frame because it set 'X-Frame-Options' to 'DENY'`.
* **Nguyên nhân**: `frame_options` đang đặt giá trị tuyệt đối `DENY`.
* **Cách xử lý**:
  - Nếu đối tác thuộc cùng tên miền gốc: Chuyển sang `"frame_options": "SAMEORIGIN"`.
  - Nếu đối tác là bên thứ ba cụ thể: Dùng CSP `frame-ancestors https://partner.example.com` và chuyển `frame_options` thành `"none"`.

### Sự Cố 2: Trình duyệt chặn tải ảnh/video từ CDN bên ngoài sau khi bật COEP
* **Triệu chứng**: Các subresource từ CDN bị chặn với lỗi `Cross-Origin-Embedder-Policy requires CORP header`.
* **Nguyên nhân**: `cross_origin_embedder_policy: "require-corp"` yêu cầu mọi tài nguyên tải từ domain khác phải có header `Cross-Origin-Resource-Policy: cross-origin` hoặc CORS.
* **Cách xử lý**:
  - Chuyển `cross_origin_embedder_policy` sang `"credentialless"` (cho phép nhúng tài nguyên công khai mà không đòi hỏi CORP), hoặc nới lỏng sang `"none"`.

### Sự Cố 3: Ứng dụng Single Page Application (SPA) không mở được cửa sổ Popup OAuth2
* **Triệu chứng**: `window.opener` bị null khi người dùng đăng nhập qua Google/GitHub Popup.
* **Nguyên nhân**: `cross_origin_opener_policy: "same-origin"` cắt đứt liên kết giữa cửa sổ chính và cửa sổ popup.
* **Cách xử lý**:
  - Chuyển `cross_origin_opener_policy` sang `"same-origin-allow-popups"`.

---

## 4. Giám Sát & Sức Khỏe Vận Hành (Operational Metrics)

- **Độ trễ chèn header**: Nhờ mảng phẳng cố định `[(HeaderName, HeaderValue); 12]` trong L1 Cache, thời gian thực thi `on_response` ổn định trong khoảng **5ns - 12ns**, không phụ thuộc vào tải hệ thống.
- **Tỷ lệ phân cấp bộ nhớ**: Zero heap allocations trên hot path. Dung lượng RSS RAM của tiến trình Velda Edge không tăng khi bật plugin `security_headers`.
