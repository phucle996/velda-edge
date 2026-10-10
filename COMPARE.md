# Velda Edge — Performance Benchmarks & Architecture Comparison

Tài liệu so sánh hiệu năng thực nghiệm toàn diện giữa **Velda Edge (Rust 2024)**, **Nginx 1.24 (C)** và **Envoy Proxy 1.31 (C++)**.

Toàn bộ hệ thống kiểm thử hiệu năng đã được quy hoạch vào thư mục [`compare/`](file:///home/phucle/Desktop/velda-edge/compare/README.md):

---

## 1. Mục Lục Bộ Đo Đối Sánh

| Bộ Benchmark | Quy Mô Kịch Bản | Trọng Tâm Đánh Giá | Chi Tiết |
| :--- | :---: | :--- | :--- |
| **[HTTP/1.1 Suite](file:///home/phucle/Desktop/velda-edge/compare/http1.md)** | **44 Kịch Bản** | • 12 Pipeline Strategies $\times$ 3 Payload Sizes (Micro, Medium, Large) = 36 Kịch Bản<br>• Dual-Upstream (No-SSL & TLS 1.3 ALPN)<br>• 8 Kịch Bản Dị Dạng & Chaos (Smuggling, Broken Chunk, Flooding, Slowloris, RST Injection, 60MB RAM OOM Containment, CPU 100% Adaptive Shedding, Atomic Reload Contention) | [Xem chi tiết](file:///home/phucle/Desktop/velda-edge/compare/http1.md) |
| **[HTTP/2 Dual-Upstream Suite](file:///home/phucle/Desktop/velda-edge/compare/http2.md)** | **7 Kịch Bản Chuyên Sâu** | • Bắn tải phân phối 50/50 giữa Cleartext `h2c` và TLS ALPN `h2`<br>• Đo lường 3 tầng: Vĩ mô (Network/L7), Vi mô (HPC/CPU `perf stat`), Vi mô (Kernel/Futex Locks) | [Xem chi tiết](file:///home/phucle/Desktop/velda-edge/compare/http2.md) |

---

## 2. Bảng Tóm Tắt Nhanh Các Điểm Nổi Bật

### 2.1 Hiệu Năng Xử Lý HTTP/1.1
- **Throughput REST 12KB**: **Velda Edge (81,722 RPS)** vượt trội hơn **Nginx (63,371 RPS, +29%)** và **Envoy (29,052 RPS, +181%)**.
- **Độ ổn định P99 Streaming 10MB**: **Velda Edge (68.72 ms)** nhanh hơn **2.5 lần** so với **Nginx (169.78 ms)** và **Envoy (150.56 ms)**.
- **Tiêu thụ Bộ Nhớ Đỉnh (Peak RAM RSS)**:
  - **Velda Edge**: **17.5 MB – 24.5 MB** ($O(1)$ Memory Footprint).
  - **Nginx**: **1,031 MB (~1.0 GB)**.
  - **Envoy**: **5,819 MB (~5.8 GB)**.

### 2.2 Khả Năng Sinh Tồn Dưới Giới Hạn Nghiệt Ngã
- **Cgroup 60MB RAM Starvation**: Velda Edge sống sót 100% trong khi Nginx (OOM kill sau 18s) và Envoy (OOM kill sau 6s) đều bị kernel `SIGKILL`.
- **CPU 100% Saturation**: Velda kích hoạt `OverloadShedder` CoDel trả 503 early drop < 10 µs, giữ P99 các request còn lại < 15ms.
- **Hot-Reload Contention**: 100 reloads/s dưới 100K RPS không làm drop kết nối nào (Zero-lock atomic runtime snapshot).
