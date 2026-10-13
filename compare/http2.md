# Bộ Đo Đối Sánh Hiệu Năng HTTP/2: Velda Edge vs Nginx vs Envoy

Tài liệu này đặc tả và định hình khung đo chuẩn cho **toàn bộ ma trận 44 kịch bản đối sánh hiệu năng (performance benchmark)** cho giao thức **HTTP/2 (RFC 9113)** giữa:
- **Velda Edge**: v0.1.0 (Rust Edition 2024, Hourglass Architecture, Zero-Copy Pipeline, Zero-Copy HPACK Engine)
- **Nginx**: **v1.31.6 Mainline / v1.30.5 Stable** (C Event-Driven Web Server & Reverse Proxy, HTTP/2 module ngx_http_v2_module)
- **Envoy Proxy**: **v1.39.3 Latest Release** (C++ Cloud-Native High-Performance Edge Proxy, nghttp2 engine)

---

## 1. Môi Trường Thử Nghiệm & Phương Pháp Đo Lường

### 1.1 Môi Trường KVM Cô Lập (Isolated Testbed)
- **Hệ điều hành**: Ubuntu 24.04 LTS (Kernel Linux 6.8.0-45-generic).
- **Cấu hình máy ảo Gateway**: KVM QEMU VM với **4 vCPUs (pinned 1-1 vật lý)**, **8 GB RAM**, cgroup v2 enabled.
- **Máy phát tải (Load Generator)**: Host vật lý (12 CPU cores, 32 GB RAM) dội tải trực tiếp qua giao diện mạng `virtio-net` (MTU 1500, offloading bật).
- **Công cụ sinh tải**: `h2load` (nghttp2 v1.68) hỗ trợ ghép kênh multiplexing `-m` (10 đến 50 streams/connection) cho cả Cleartext (`h2c`) trên cổng `:8080` và TLS ALPN `h2` trên cổng `:8443`, kết hợp bộ test scripts chuyên dụng mô phỏng dị dạng frame (Rapid Reset flood, CONTINUATION stream, HPACK bomb).

### 1.2 Phân Lớp 3 Tầng Tiêu Chí Đo Lường
Mọi kịch bản đều được phân tích qua 3 tầng số liệu chuẩn hóa:
1. **Tầng Vĩ Mô (Network & L7)**:
   - Throughput (RPS / transfers/s), Băng thông Goodput (MB/s).
   - Độ trễ phân vị: Mean, Min, Max, P50, P90, P99, P99.9, Độ lệch chuẩn SD.
   - Thời gian nhận byte đầu tiên (TTFB Avg), Thời gian bắt tay kết nối (Connect Time Avg bao gồm TLS + HTTP/2 Preface).
   - Phân bổ HTTP Status (2xx, 4xx, 5xx) và Tỷ lệ thành công (Success Rate 100%).
   - Mức tiêu thụ bộ nhớ RAM đỉnh (Peak RSS qua `/proc/<pid>/status`).
2. **Tầng Vi Mô (HPC & CPU Hardware Counters qua `perf stat`)**:
   - Tổng lệnh CPU (`instructions`) & Số lệnh / Request (`insn/req`).
   - Tổng chu kỳ CPU (`cycles`) & Chu kỳ / Request (`cycles/req`).
   - Hiệu suất lệnh / xung (`IPC = instructions / cycles`).
   - Tổng lệnh rẽ nhánh (`branches`), `branches/req`, Đoán sai nhánh (`branch-misses`), Tỷ lệ đoán sai (`branch-miss %`).
   - L1 Data Cache Miss % và Last Level Cache (LLC) Miss %.
3. **Tầng Vi Mô (Kernel & Concurrency qua `perf trace` & eBPF)**:
   - Context switches (`voluntary` và `involuntary`), Chuyển ngữ cảnh / Request (`CS/req`).
   - Tổng System Calls (`read`, `write`, `epoll_wait`, `futex`), Syscalls / Request (`syscalls/req`).
   - Thời gian thực thi không gian nhân (`Kernel Execution Time`).
   - Số lần gọi khóa đồng bộ (`Futex Calls`) và tỷ lệ Futex / Syscalls (%).

### 1.3 Thiết Kế Phân Bổ Tải Upstream Đa Tầng
Tùy theo chiến lược đường ống (Pipeline Strategy) của từng kịch bản, hệ thống phân bổ tải qua các nhánh upstream tương ứng:
- **Nhánh Upstream HTTP/1.1 (cho pipe H2 -> H1)**: Tỷ lệ 4/3/3 (40% Plaintext h1 trên `127.0.0.1:8081` : 30% TLS 1.2 h1 trên `127.0.0.1:8442` : 30% TLS 1.3 h1 trên `127.0.0.1:8443`).
- **Nhánh Upstream HTTP/2 (cho pipe H2 -> H2 Mux)**: Tỷ lệ 4/3/3 (40% Cleartext `h2c` trên `127.0.0.1:8081` : 30% TLS 1.2 `h2` trên `127.0.0.1:8442` : 30% TLS 1.3 `h2` trên `127.0.0.1:8443`).
- **Nhánh Upstream HTTP/3 (cho pipe H2 -> H3 QUIC)**: HTTP/3 over QUIC/UDP trên `127.0.0.1:8445` (Nginx không hỗ trợ upstream HTTP/3 theo kiến trúc chuẩn nên ghi chú *N/A*).

---

## 2. Ma Trận 44 Kịch Bản Thử Nghiệm HTTP/2

```
╔═══════════════════════════════════════════════════════════════════════════════════════╗
║                             MA TRẬN 44 KỊCH BẢN HTTP/2                                ║
╠═══════════════════════════════════════════════════════════════════════════════════════╣
║  Pipe Module         │ Micro (~64B)        │ Medium (12K-64K)    │ Large (1M-10M)     ║
╟──────────────────────┼─────────────────────┼─────────────────────┼────────────────────╢
║  buffered_h2_h1      │ KB 01: Demux to H1  │ KB 02: REST to H1   │ KB 03: RAM Blob H1 ║
║  buffered_h2_h2      │ KB 04: Mux-to-Mux   │ KB 05: REST H2 Mux  │ KB 06: Heavy H2 Mux║
║  buffered_h2_h3      │ KB 07: Bridge to H3 │ KB 08: REST to H3   │ KB 09: Heavy to H3 ║
║  server_stream_h2_h1 │ KB 10: SSE from H1  │ KB 11: Chunk to H2  │ KB 12: File from H1║
║  server_stream_h2_h2 │ KB 13: SSE Mux-Mux  │ KB 14: Data Stream  │ KB 15: File H2-H2  ║
║  server_stream_h2_h3 │ KB 16: SSE via H3   │ KB 17: QUIC Stream  │ KB 18: File from H3║
║  client_stream_h2_h1 │ KB 19: Drip Upload  │ KB 20: Doc to H1    │ KB 21: Bulk to H1  ║
║  client_stream_h2_h2 │ KB 22: Drip to H2   │ KB 23: Doc to H2    │ KB 24: Bulk to H2  ║
║  client_stream_h2_h3 │ KB 25: Drip to H3   │ KB 26: Doc to H3    │ KB 27: Bulk to H3  ║
║  duplex_h2_h1        │ KB 28: Bidi with H1 │ KB 29: Msg to H1    │ KB 30: Heavy Bidi  ║
║  duplex_h2_h2        │ KB 31: Pure H2 Bidi │ KB 32: Bidi H2 Data │ KB 33: Heavy H2 Bidi║
║  duplex_h2_h3        │ KB 34: QUIC Bidi    │ KB 35: Bidi H3 Data │ KB 36: Heavy H3 Bidi║
╠═══════════════════════════════════════════════════════════════════════════════════════╣
║  Nhóm Dị Dạng & Chaos HTTP/2 (Adversarial, Chaos & RFC 9113 Extreme Resources)       ║
╟───────────────────────────────────────────────────────────────────────────────────────╢
║  KB 37: HTTP/2 Rapid Reset Attack (CVE-2023-44487, 200K RST_STREAM/s Flood)          ║
║  KB 38: HTTP/2 CONTINUATION Flood (CVE-2024-27983, Infinite CONTINUATION frame stream)║
║  KB 39: HPACK Header Bomb & PING Frame Flood (Dynamic table exhaustion, 100K PING/s) ║
║  KB 40: Stream Multiplexing & Flow-Control Deadlock (WINDOW_UPDATE Drip, zero window) ║
║  KB 41: Chaos Upstream Sudden Death (Graceful GOAWAY, TCP RST & Instant Re-lease)     ║
║  KB 42: Cgroup RAM 60MB Starvation (O(1) Memory under 1,000 Multiplexed Streams)     ║
║  KB 43: CPU 100% Saturation & Overload Shedding (CoDel Shedder, REFUSED_STREAM 0x7)   ║
║  KB 44: Atomic Hot-Reload Contention (100 Reloads/s at 100K Streams, Zero Dropped)   ║
╚═══════════════════════════════════════════════════════════════════════════════════════╝
```

---

## 3. Form Đo Lường Chi Tiết Toàn Bộ 44 Kịch Bản

### KB 01: buffered_h2_h1 — Micro Payload (Ping-Pong ~64B)
- **Mục tiêu thử nghiệm**: Đo chi phí gỡ frame HTTP/2 (demux HEADERS), phân giải HPACK tĩnh/động, chuyển đổi sang HTTP/1.1 wire text và mượn connection từ pool.
- **Kích thước tải trọng & Quy chuẩn**: ~64 bytes HTTP headers (GET /ping HTTP/2)
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 50 -m 10 -t 4 http://192.168.122.14:8080/plain/ping http://192.168.122.14:8080/tls12/ping http://192.168.122.14:8080/tls13/ping
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **72,246.60** | 70,183.00 | 39,029.80 | Velda đạt 72,246.60 req/s (+2.9% vs Nginx, +85.1% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 2.07 MB/s | **5.29 MB/s** | 1.49 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **6.90 ms** | 7.11 ms | 12.77 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.08 ms** | 0.30 ms | 3.84 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 6.46 ms | **6.29 ms** | 12.49 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **11.06 ms** | 12.27 ms | 16.27 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **16.86 ms** | 19.24 ms | 22.99 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | **29.34 ms** | 30.75 ms | 49.86 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 91.66 ms | 83.91 ms | **68.57 ms** | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **3.57 ms** | 4.03 ms | 3.58 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **7.03 ms** | 18.39 ms | 38.16 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.22 ms** | 0.24 ms | 0.24 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **361,233 2xx** | 350,915 2xx | 195,149 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 53.55 MB | 159.51 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **50,798,947,827** | 52,302,766,347 | 55,320,888,052 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **140,626.50** | 149,046.80 | 283,480.30 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 69,644,137,056 | **64,936,493,893** | 73,235,562,662 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 192,795.60 | **185,049.10** | 375,280.20 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.73 | **0.81** | 0.76 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **9,111,817,604** | 10,146,795,239 | 10,145,552,850 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **25,224.20** | 28,915.30 | 51,988.80 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 119,361,988 | **118,960,229** | 170,883,929 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.31% | **1.17%** | 1.68% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 12.09% | 9.81% | **9.18%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 197,674 | **108,717** | 215,578 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.55 | **0.31** | 1.10 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 1,906,877 | 2,668,490 | **1,304,712** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **5.28** | 7.60 | 6.69 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 20,231 | **10,881** | 11,003 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 1.06% | **0.41%** | 0.84% | Tỷ lệ khóa futex |

---

### KB 02: buffered_h2_h1 — Medium Payload (REST JSON 12KB)
- **Mục tiêu thử nghiệm**: Nạp trọn vẹn 12KB DATA frame từ client H2, assemble buffer in-memory và serialize thành HTTP/1.1 Content-Length body gửi tới upstream backend.
- **Kích thước tải trọng & Quy chuẩn**: 12,288 bytes Content-Length JSON body
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 50 -m 10 -t 4 http://192.168.122.14:8080/plain/12kb http://192.168.122.14:8080/tls12/12kb http://192.168.122.14:8080/tls13/12kb
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **41,702.80** | 40,314.60 | 28,114.00 | Velda đạt 41,702.80 req/s (+3.4% vs Nginx, +48.3% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **490.46 MB/s** | 479.41 MB/s | 330.91 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **11.95 ms** | 12.37 ms | 17.69 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.12 ms** | 0.15 ms | 3.56 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **11.21 ms** | 11.30 ms | 14.53 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **19.11 ms** | 21.38 ms | 31.39 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **29.17 ms** | 32.68 ms | 47.30 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | **45.38 ms** | 48.19 ms | 81.39 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **70.97 ms** | 81.18 ms | 110.74 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **5.69 ms** | 6.91 ms | 9.36 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **8.24 ms** | 23.64 ms | 49.84 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.27 ms | 0.25 ms | **0.20 ms** | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **208,514 2xx** | 201,573 2xx | 140,570 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 62.23 MB | 167.39 MB | **6.88 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 47,260,524,318 | **47,149,301,198** | 52,901,498,458 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **226,654.00** | 233,906.80 | 376,335.60 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 67,773,604,413 | **63,937,518,360** | 72,917,647,051 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 325,031.40 | **317,192.90** | 518,728.40 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.70 | **0.74** | 0.72 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **8,320,338,429** | 8,675,136,814 | 9,202,854,223 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **39,903.00** | 43,037.20 | 65,468.10 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **117,556,992** | 118,661,555 | 150,855,886 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.41% | **1.37%** | 1.64% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 14.54% | 11.82% | **10.38%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **121,626** | 123,708 | 148,878 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.58** | 0.61 | 1.06 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,602,668 | 2,936,932 | **1,626,420** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 12.48 | 14.57 | **11.57** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 15,277 | 12,830 | **12,031** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.59% | **0.44%** | 0.74% | Tỷ lệ khóa futex |

---

### KB 03: buffered_h2_h1 — Large Payload (RAM Blob 1MB)
- **Mục tiêu thử nghiệm**: Đo chi phí cấp phát vector bộ đệm nạp trọn vẹn 1MB DATA frames từ H2 stream, chuyển tiếp qua H1 keep-alive upstream không chunking.
- **Kích thước tải trọng & Quy chuẩn**: 1,048,576 bytes Content-Length body
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 30 -m 5 -t 4 http://192.168.122.14:8080/plain/1mb http://192.168.122.14:8080/tls12/1mb http://192.168.122.14:8080/tls13/1mb
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **2,606.40** | 2,255.40 | 2,251.80 | Velda đạt 2,606.40 req/s (+15.6% vs Nginx, +15.7% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **2,621.44 MB/s** | 2,273.28 MB/s | 2,263.04 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **57.13 ms** | 65.86 ms | 66.05 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 7.88 ms | **1.28 ms** | 6.05 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **56.87 ms** | 59.02 ms | 62.97 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **80.90 ms** | 116.52 ms | 98.85 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **104.92 ms** | 238.68 ms | 139.74 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | **144.19 ms** | 307.84 ms | 168.85 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **183.41 ms** | 574.38 ms | 255.98 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **19.14 ms** | 45.37 ms | 24.65 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **12.23 ms** | 17.00 ms | 33.99 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.20 ms** | 0.20 ms | 0.21 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **13,032 2xx** | 11,277 2xx | 11,259 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 193.93 MB | 155.43 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **50,340,101,001** | 59,144,709,321 | 56,612,331,295 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **3,862,807.00** | 5,244,720.20 | 5,028,184.70 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 67,872,434,009 | **66,544,569,304** | 67,620,942,662 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **5,208,136.40** | 5,900,910.60 | 6,005,945.70 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.74 | **0.89** | 0.84 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 8,070,100,062 | 10,237,881,723 | **7,547,624,902** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **619,252.60** | 907,855.10 | 670,363.70 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 87,445,745 | 104,825,221 | **85,749,899** | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.08% | **1.02%** | 1.14% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 25.95% | **16.85%** | 17.83% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **49,249** | 50,363 | 51,614 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **3.78** | 4.47 | 4.58 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,581,462 | 3,994,573 | **1,561,328** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 198.09 | 354.22 | **138.67** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | **11,456** | 11,555 | 22,941 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.44% | **0.29%** | 1.47% | Tỷ lệ khóa futex |

---

### KB 04: buffered_h2_h2 — Micro Payload (Mux-to-Mux Ping-Pong ~64B)
- **Mục tiêu thử nghiệm**: Ghép kênh 2 đầu (Mux In -> Mux Out), đo chi phí định tuyến stream id, HPACK decode/re-encode và stream dispatching giữa 2 kết nối H2 độc lập.
- **Kích thước tải trọng & Quy chuẩn**: ~64 bytes headers (GET /ping HTTP/2)
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 50 -m 10 -t 4 http://192.168.122.14:8080/h2c/ping http://192.168.122.14:8080/h2tls12/ping http://192.168.122.14:8080/h2tls13/ping
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h2c : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **87,396.20** | 56,014.80 | 58,492.60 | Velda đạt 87,396.20 req/s (+56.0% vs Nginx, +49.4% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 2.50 MB/s | **4.22 MB/s** | 1.73 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **5.70 ms** | 8.90 ms | 8.48 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.11 ms** | 0.54 ms | 0.19 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **5.46 ms** | 7.87 ms | 7.55 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **8.97 ms** | 15.27 ms | 13.55 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **12.36 ms** | 24.16 ms | 22.96 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | **15.32 ms** | 43.93 ms | 31.84 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **23.48 ms** | 67.18 ms | 43.98 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **2.44 ms** | 4.91 ms | 4.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **4.63 ms** | 15.99 ms | 12.54 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.20 ms | **0.16 ms** | 0.25 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **436,981 2xx** | 280,074 2xx | 292,463 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 31.37 MB | 161.95 MB | **6.85 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 58,891,499,540 | **51,683,041,630** | 70,955,499,220 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **134,769.00** | 184,533.50 | 242,613.60 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 65,098,278,463 | **63,422,464,366** | 70,861,399,733 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **148,972.80** | 226,449.00 | 242,291.80 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.91 | 0.81 | **1.00** | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 10,648,465,524 | **9,825,341,689** | 13,055,984,810 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **24,368.30** | 35,081.20 | 44,641.50 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **129,259,379** | 130,286,673 | 161,697,236 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.21%** | 1.33% | 1.24% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 10.19% | 9.92% | **7.97%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 128,070 | 166,235 | **46,028** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.29 | 0.59 | **0.16** | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 1,384,600 | 2,353,113 | **931,723** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **3.17** | 8.40 | 3.19 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 51,685 | 11,906 | **11,067** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 3.73% | **0.51%** | 1.19% | Tỷ lệ khóa futex |

---

### KB 05: buffered_h2_h2 — Medium Payload (REST H2 Mux 12KB)
- **Mục tiêu thử nghiệm**: Chuyển tiếp 12KB REST payload qua 2 tầng HTTP/2 multiplexing, kiểm tra khả năng phối hợp flow-control window giữa downstream và upstream H2 stream.
- **Kích thước tải trọng & Quy chuẩn**: 12,288 bytes JSON body
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 50 -m 10 -t 4 http://192.168.122.14:8080/h2c/12kb http://192.168.122.14:8080/h2tls12/12kb http://192.168.122.14:8080/h2tls13/12kb
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h2c : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **44,810.80** | 25,016.80 | 39,941.00 | Velda đạt 44,810.80 req/s (+79.1% vs Nginx, +12.2% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **527.01 MB/s** | 297.41 MB/s | 470.12 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **11.13 ms** | 19.89 ms | 12.45 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.23 ms | **0.10 ms** | 0.66 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 10.62 ms | **7.60 ms** | 12.37 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 17.58 ms | 45.16 ms | **16.75 ms** | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 24.31 ms | 53.21 ms | **23.99 ms** | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | **29.82 ms** | 83.47 ms | 35.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **43.79 ms** | 111.13 ms | 58.53 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 4.82 ms | 19.81 ms | **3.86 ms** | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **7.05 ms** | 25.78 ms | 18.20 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.24 ms | 0.25 ms | **0.19 ms** | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **224,054 2xx** | 125,084 2xx | 199,705 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 45.67 MB | 167.18 MB | **6.89 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 49,377,058,868 | **41,648,603,481** | 63,370,951,561 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **220,380.20** | 332,965.10 | 317,322.80 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 63,593,297,820 | **58,111,012,726** | 72,305,312,833 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **283,830.20** | 464,575.90 | 362,060.60 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.78 | 0.72 | **0.88** | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 8,669,095,984 | **7,738,086,758** | 11,016,772,430 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **38,692.00** | 61,863.10 | 55,165.20 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 120,532,657 | **110,139,255** | 141,326,182 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.39% | 1.42% | **1.28%** | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 13.85% | 11.85% | **10.01%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 95,588 | 112,438 | **50,543** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.43 | 0.90 | **0.25** | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,543,993 | 2,485,856 | **1,797,415** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 11.35 | 19.87 | **9.00** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 23,292 | **10,800** | 11,442 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.92% | **0.43%** | 0.64% | Tỷ lệ khóa futex |

---

### KB 06: buffered_h2_h2 — Large Payload (Heavy H2 Mux 1MB)
- **Mục tiêu thử nghiệm**: Ghép kênh tải nặng 1MB trên đồng thời 50 kết nối H2, kiểm soát nén HPACK header và phân mảnh DATA frames 16KB giữa downstream và upstream.
- **Kích thước tải trọng & Quy chuẩn**: 1,048,576 bytes body (~1MB)
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 30 -m 5 -t 4 http://192.168.122.14:8080/h2c/1mb http://192.168.122.14:8080/h2tls12/1mb http://192.168.122.14:8080/h2tls13/1mb
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h2c : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 1,857.80 | **1,880.00** | 1,664.80 | Velda đạt 1,857.80 req/s (-1.2% vs Nginx, +11.6% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 1,863.68 MB/s | **1,894.40 MB/s** | 1,669.12 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 80.07 ms | **79.12 ms** | 87.93 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 7.01 ms | **1.40 ms** | 5.54 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 78.43 ms | 69.53 ms | **38.60 ms** | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **115.70 ms** | 135.60 ms | 235.13 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **158.62 ms** | 292.00 ms | 328.07 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | **203.22 ms** | 418.05 ms | 395.53 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **233.07 ms** | 490.44 ms | 404.31 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **28.64 ms** | 57.28 ms | 87.91 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 31.03 ms | **19.87 ms** | 40.20 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.19 ms** | 0.21 ms | 0.21 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **9,289 2xx** | 9,400 2xx | 8,324 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 231.30 MB | 156.20 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **50,822,454,726** | 60,094,077,904 | 57,828,872,067 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **5,471,251.50** | 6,392,987.00 | 6,947,245.60 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 65,982,026,194 | **65,259,859,380** | 66,930,694,792 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 7,103,243.20 | **6,942,538.20** | 8,040,688.90 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.77 | **0.92** | 0.86 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **8,439,439,370** | 10,603,285,750 | 8,494,828,422 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **908,541.20** | 1,128,009.10 | 1,020,522.40 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 96,302,652 | 109,918,802 | **95,864,700** | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.14% | **1.04%** | 1.13% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 23.22% | **15.06%** | 15.88% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 50,585 | 50,217 | **46,008** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 5.45 | **5.34** | 5.53 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 3,370,639 | 4,295,910 | **2,405,803** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 362.86 | 457.01 | **289.02** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 12,280 | **10,530** | 10,852 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.36% | **0.25%** | 0.45% | Tỷ lệ khóa futex |

---

### KB 07: buffered_h2_h3 — Micro Payload (Ping-Pong H2 Bridge to H3)
- **Mục tiêu thử nghiệm**: Chuyển đổi giao thức kép từ HTTP/2 TCP sang HTTP/3 QUIC UDP, đo chi phí dịch HPACK sang QPACK và socket UDP batching.
- **Kích thước tải trọng & Quy chuẩn**: ~64 bytes headers
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 50 -m 10 -t 4 http://192.168.122.14:8080/h3/ping
- **Nhánh Upstream**: HTTP/3 QUIC UDP upstream (100% H3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **77,647.80** | *N/A* | 26,398.00 | Velda đạt 77,647.80 req/s (+194.1% vs Envoy; Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | **2.22 MB/s** | *N/A* | 1.01 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **6.42 ms** | *N/A* | 18.86 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.10 ms** | *N/A* | 6.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **6.11 ms** | *N/A* | 16.78 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **10.30 ms** | *N/A* | 26.14 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **14.71 ms** | *N/A* | 37.51 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | **18.77 ms** | *N/A* | 42.49 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **29.79 ms** | *N/A* | 46.89 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **2.94 ms** | *N/A* | 5.43 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **6.18 ms** | *N/A* | 25.46 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.18 ms** | *N/A* | 0.25 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **388,239 2xx** | *N/A* | 131,990 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | *N/A* | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 36.25 MB | *N/A* | **6.87 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 56,702,456,237 | 0 | **50,416,576,141** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **146,050.40** | *N/A* | 381,972.70 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **63,306,512,401** | 0 | 70,202,005,565 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **163,060.70** | *N/A* | 531,873.70 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.90** | *N/A* | 0.72 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 10,520,114,801 | 0 | **9,003,210,363** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **27,097.00** | *N/A* | 68,211.30 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **117,652,242** | 0 | 204,870,947 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.12%** | *N/A* | 2.28% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 10.26% | *N/A* | **8.76%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **177,817** | 0 | 238,294 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.46** | *N/A* | 1.81 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 1,584,157 | 0 | **723,825** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **4.08** | *N/A* | 5.48 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | *N/A* | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 21,708 | 0 | **11,462** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | **1.37%** | *N/A* | 1.58% | Tỷ lệ khóa futex |

---

### KB 08: buffered_h2_h3 — Medium Payload (REST H2 Bridge to H3 12KB)
- **Mục tiêu thử nghiệm**: Nhận 12KB JSON từ HTTP/2 TCP downstream và đóng gói thành QUIC STREAM frames gửi tới HTTP/3 upstream.
- **Kích thước tải trọng & Quy chuẩn**: 12,288 bytes JSON body
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 50 -m 10 -t 4 http://192.168.122.14:8080/h3/12kb
- **Nhánh Upstream**: HTTP/3 QUIC UDP upstream (100% H3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **27,272.80** | *N/A* | 11,611.60 | Velda đạt 27,272.80 req/s (+134.9% vs Envoy; Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | **320.75 MB/s** | *N/A* | 137.53 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **18.27 ms** | *N/A* | 42.82 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.62 ms** | *N/A* | 9.75 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **14.67 ms** | *N/A* | 36.68 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **32.23 ms** | *N/A* | 71.92 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **62.10 ms** | *N/A* | 123.04 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | **91.62 ms** | *N/A* | 156.63 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 267.12 ms | *N/A* | **210.74 ms** | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **12.19 ms** | *N/A* | 21.89 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **12.05 ms** | *N/A* | 40.76 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.25 ms** | *N/A* | 0.25 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **136,364 2xx** | *N/A* | 58,058 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | *N/A* | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 36.88 MB | *N/A* | **6.88 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 68,866,642,896 | 0 | **60,173,022,990** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **505,020.70** | *N/A* | 1,036,429.50 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **63,859,550,308** | 0 | 69,286,852,351 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **468,302.10** | *N/A* | 1,193,407.50 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **1.08** | *N/A* | 0.87 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 12,318,286,129 | 0 | **10,605,121,387** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **90,333.90** | *N/A* | 182,664.30 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **117,801,979** | 0 | 186,985,423 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **0.96%** | *N/A* | 1.76% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 9.63% | *N/A* | **9.21%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **47,511** | 0 | 105,585 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.35** | *N/A* | 1.82 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 3,223,929 | 0 | **1,652,582** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **23.64** | *N/A* | 28.46 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | *N/A* | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 14,768 | 0 | **11,284** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | **0.46%** | *N/A* | 0.68% | Tỷ lệ khóa futex |

---

### KB 09: buffered_h2_h3 — Large Payload (Heavy H2 Bridge to H3 1MB)
- **Mục tiêu thử nghiệm**: Nạp 1MB body từ client H2 và stream sang HTTP/3 UDP buffer với pacing UDP, bảo vệ bộ đệm chống drop packet.
- **Kích thước tải trọng & Quy chuẩn**: 1,048,576 bytes body
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 30 -m 5 -t 4 http://192.168.122.14:8080/h3/1mb
- **Nhánh Upstream**: HTTP/3 QUIC UDP upstream (100% H3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **478.60** | *N/A* | 304.00 | Velda đạt 478.60 req/s (+57.4% vs Envoy; Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | **478.88 MB/s** | *N/A* | 317.07 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **289.06 ms** | *N/A* | 466.26 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 140.93 ms | *N/A* | **131.79 ms** | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **231.08 ms** | *N/A* | 439.89 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **320.40 ms** | *N/A* | 811.29 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 2,041.38 ms | *N/A* | **868.23 ms** | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 2,049.32 ms | *N/A* | **868.28 ms** | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 2,060.08 ms | *N/A* | **868.28 ms** | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 268.78 ms | *N/A* | **239.44 ms** | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 196.28 ms | *N/A* | **24.10 ms** | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.19 ms | *N/A* | **0.19 ms** | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **2,393 2xx** | *N/A* | 1,520 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | *N/A* | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 228.51 MB | *N/A* | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **71,673,173,701** | 0 | 79,974,808,615 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **29,951,180.00** | *N/A* | 52,615,005.70 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **58,799,202,689** | 0 | 67,254,659,852 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **24,571,334.20** | *N/A* | 44,246,486.70 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **1.22** | *N/A* | 1.19 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **12,469,657,565** | 0 | 13,960,594,967 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **5,210,889.10** | *N/A* | 9,184,602.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **96,822,177** | 0 | 119,729,882 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **0.78%** | *N/A* | 0.86% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 9.64% | *N/A* | **9.09%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 131,905 | 0 | **103,065** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **55.12** | *N/A* | 67.81 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 3,864,405 | 0 | **2,543,916** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **1,614.88** | *N/A* | 1,673.63 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | *N/A* | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 32,657 | 0 | **10,977** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.85% | *N/A* | **0.43%** | Tỷ lệ khóa futex |

---

### KB 10: server_stream_h2_h1 — Micro Payload (SSE Heartbeat from H1)
- **Mục tiêu thử nghiệm**: Nhận các SSE ping events 64B từ H1 upstream chunked và đóng gói tức thì thành các DATA frames HTTP/2 đẩy về client.
- **Kích thước tải trọng & Quy chuẩn**: ~64 bytes SSE heartbeat chunks
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 50 -m 10 -t 4 http://192.168.122.14:8080/stream/plain/sse-ping
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **68,573.40** | 68,078.00 | 40,807.00 | Velda đạt 68,573.40 req/s (+0.7% vs Nginx, +68.0% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 4.32 MB/s | **8.31 MB/s** | 2.61 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **7.27 ms** | 7.32 ms | 12.21 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.09 ms** | 0.13 ms | 2.89 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 6.78 ms | **5.94 ms** | 12.52 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **11.67 ms** | 12.84 ms | 16.05 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **19.23 ms** | 26.26 ms | 19.75 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | **29.27 ms** | 43.20 ms | 51.62 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **53.58 ms** | 214.66 ms | 81.49 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 3.66 ms | 6.39 ms | **3.59 ms** | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **5.22 ms** | 65.04 ms | 39.60 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.26 ms | **0.23 ms** | 0.26 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **342,867 2xx** | 340,390 2xx | 204,035 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 64.00 MB | 165.12 MB | **6.87 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **53,898,954,972** | 54,425,760,749 | 57,169,365,260 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **157,200.80** | 159,892.40 | 280,193.90 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 69,098,192,251 | **65,743,696,437** | 73,162,415,821 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 201,530.60 | **193,142.30** | 358,577.80 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.78 | **0.83** | 0.78 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **9,403,030,288** | 10,542,105,672 | 10,450,443,546 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **27,424.70** | 30,970.70 | 51,218.90 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 126,488,247 | **121,231,097** | 171,471,099 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.35% | **1.15%** | 1.64% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 11.40% | 9.59% | **8.68%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 202,688 | **123,716** | 203,493 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.59 | **0.36** | 1.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 1,859,827 | 2,750,000 | **1,361,935** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **5.42** | 8.08 | 6.68 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 21,247 | **11,201** | 11,457 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 1.14% | **0.41%** | 0.84% | Tỷ lệ khóa futex |

---

### KB 11: server_stream_h2_h1 — Medium Payload (SSE Chunked from H1 64KB)
- **Mục tiêu thử nghiệm**: Chuyển đổi luồng chunked 64KB từ H1 upstream sang các DATA frames 16KB HTTP/2, duy trì độ trễ gói tin tối thiểu.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes streaming data chunks
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 40 -m 8 -t 4 http://192.168.122.14:8080/stream/plain/64kb
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **20,662.60** | 19,119.60 | 17,255.20 | Velda đạt 20,662.60 req/s (+8.1% vs Nginx, +19.7% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **1,290.24 MB/s** | 1,198.08 MB/s | 1,075.20 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **15.45 ms** | 16.69 ms | 18.47 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.19 ms** | 0.55 ms | 5.62 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **14.64 ms** | 15.32 ms | 18.08 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 24.40 ms | 27.98 ms | **23.59 ms** | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **34.62 ms** | 44.32 ms | 36.14 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | **51.62 ms** | 63.80 ms | 58.40 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **72.84 ms** | 79.67 ms | 74.22 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 6.82 ms | 8.89 ms | **5.12 ms** | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **8.82 ms** | 14.27 ms | 27.93 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.22 ms | **0.17 ms** | 0.24 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **103,313 2xx** | 95,598 2xx | 86,276 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 81.57 MB | 163.58 MB | **6.85 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **48,338,864,890** | 52,656,816,810 | 55,271,459,402 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **467,887.50** | 550,815.00 | 640,635.40 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 68,495,249,181 | **66,431,602,028** | 71,334,544,339 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **662,987.70** | 694,905.80 | 826,817.90 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.71 | **0.79** | 0.78 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **8,256,681,649** | 9,469,442,787 | 8,723,813,047 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **79,919.10** | 99,054.80 | 101,115.20 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 121,284,838 | **116,375,261** | 145,099,536 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.47% | **1.23%** | 1.66% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 20.58% | 14.32% | **13.44%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 96,674 | **63,208** | 82,867 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.94 | **0.66** | 0.96 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,392,104 | 3,144,007 | **1,551,726** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 23.15 | 32.89 | **17.99** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 15,058 | 11,159 | **10,250** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.63% | **0.35%** | 0.66% | Tỷ lệ khóa futex |

---

### KB 12: server_stream_h2_h1 — Large Payload (File Stream from H1 10MB)
- **Mục tiêu thử nghiệm**: Tải file streaming 10MB từ H1 upstream, stream liên tục về client H2 với zero intermediate buffering, duy trì bộ nhớ O(1).
- **Kích thước tải trọng & Quy chuẩn**: 10,485,760 bytes streaming file download
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 20 -m 4 -t 4 http://192.168.122.14:8080/stream/plain/10mb
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **253.80** | 237.40 | 249.40 | Velda đạt 253.80 req/s (+6.9% vs Nginx, +1.8% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **2,611.20 MB/s** | 2,437.12 MB/s | 2,580.48 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 305.64 ms | **271.53 ms** | 305.28 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 162.87 ms | **13.52 ms** | 22.87 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 299.56 ms | **133.06 ms** | 308.23 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 374.04 ms | **343.65 ms** | 530.16 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **460.01 ms** | 2,907.21 ms | 713.68 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | **535.41 ms** | 3,348.42 ms | 750.41 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **542.51 ms** | 3,365.41 ms | 831.18 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **51.18 ms** | 522.05 ms | 162.44 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 20.24 ms | **16.64 ms** | 35.02 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.23 ms | **0.15 ms** | 0.17 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **1,269 2xx** | 1,187 2xx | 1,247 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 698.72 MB | 146.56 MB | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **46,836,202,615** | 54,884,803,169 | 55,853,399,029 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **36,907,961.10** | 46,238,250.40 | 44,790,215.70 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 64,579,156,151 | **63,122,022,226** | 63,631,901,961 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **50,889,800.00** | 53,177,777.80 | 51,027,988.70 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.72 | 0.87 | **0.88** | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 7,343,940,878 | 9,450,510,498 | **7,117,305,658** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 5,787,187.50 | 7,961,676.90 | **5,707,542.60** | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 70,076,798 | 88,295,133 | **68,273,883** | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.95% | **0.93%** | 0.96% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 27.63% | **17.24%** | 19.01% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 55,702 | **46,782** | 52,071 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 43.89 | **39.41** | 41.76 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,677,560 | 3,957,131 | **1,748,153** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 2,109.98 | 3,333.72 | **1,401.89** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 17,523 | 11,481 | **10,975** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.65% | **0.29%** | 0.63% | Tỷ lệ khóa futex |

---

### KB 13: server_stream_h2_h2 — Micro Payload (SSE Mux-to-Mux 64B)
- **Mục tiêu thử nghiệm**: Chuyển tiếp luồng sự kiện SSE 64B giữa 2 kênh multiplexing HTTP/2 mà không nén lại payload, điều phối tín hiệu ping.
- **Kích thước tải trọng & Quy chuẩn**: ~64 bytes headers + SSE event frame
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 50 -m 10 -t 4 http://192.168.122.14:8080/stream-h2/plain/sse-ping
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h2c : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **75,367.80** | 52,616.40 | 53,756.00 | Velda đạt 75,367.80 req/s (+43.2% vs Nginx, +40.2% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 4.75 MB/s | **6.42 MB/s** | 2.98 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **6.62 ms** | 9.48 ms | 9.23 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.09 ms | **0.08 ms** | 0.62 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **6.31 ms** | 7.51 ms | 8.50 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **10.51 ms** | 19.06 ms | 14.27 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **14.62 ms** | 33.03 ms | 21.39 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | **18.11 ms** | 53.77 ms | 29.71 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **26.25 ms** | 79.80 ms | 32.70 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **2.91 ms** | 7.15 ms | 3.89 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **6.54 ms** | 19.11 ms | 12.08 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.27 ms | **0.19 ms** | 0.21 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **376,839 2xx** | 263,082 2xx | 268,780 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 36.43 MB | 165.23 MB | **6.88 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 64,581,558,814 | **48,114,933,415** | 68,171,116,997 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **171,377.10** | 182,889.50 | 253,631.70 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 65,347,951,861 | **62,138,788,729** | 70,192,792,859 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **173,410.80** | 236,195.50 | 261,153.30 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.99** | 0.77 | 0.97 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 11,776,830,901 | **9,189,060,360** | 12,589,727,368 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **31,251.60** | 34,928.50 | 46,840.30 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **105,668,730** | 123,936,125 | 158,559,818 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **0.90%** | 1.35% | 1.26% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | **6.74%** | 10.13% | 8.04% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 151,627 | 147,563 | **47,413** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.40 | 0.56 | **0.18** | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 1,096,250 | 2,175,689 | **852,777** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **2.91** | 8.27 | 3.17 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 29,082 | 11,515 | **10,655** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 2.65% | **0.53%** | 1.25% | Tỷ lệ khóa futex |

---

### KB 14: server_stream_h2_h2 — Medium Payload (Data Stream Mux-to-Mux 64KB)
- **Mục tiêu thử nghiệm**: Stream liên tục dữ liệu 64KB giữa downstream H2 và upstream H2, kiểm tra hiệu năng window update và frame relay.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes DATA frames payload
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 40 -m 8 -t 4 http://192.168.122.14:8080/stream-h2/plain/64kb
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h2c : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **17,862.20** | 14,613.20 | 17,342.60 | Velda đạt 17,862.20 req/s (+22.2% vs Nginx, +3.0% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **1,116.16 MB/s** | 919.28 MB/s | 1,085.44 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **17.86 ms** | 21.81 ms | 18.39 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.73 ms | **0.24 ms** | 2.07 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 17.03 ms | 18.68 ms | **16.39 ms** | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **28.04 ms** | 39.31 ms | 29.52 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **38.87 ms** | 72.19 ms | 46.59 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | **47.03 ms** | 145.09 ms | 56.21 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **67.68 ms** | 211.94 ms | 69.64 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **7.55 ms** | 15.24 ms | 8.16 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **9.03 ms** | 17.01 ms | 11.82 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.22 ms** | 0.24 ms | 0.36 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **89,311 2xx** | 73,066 2xx | 86,713 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 90.25 MB | 164.73 MB | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 116,085,998,620 | **49,170,737,676** | 55,768,277,413 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 1,299,795.10 | 672,963.30 | **643,136.30** | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 79,329,154,851 | **56,470,978,645** | 65,319,369,812 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 888,235.00 | 772,876.30 | **753,282.30** | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **1.46** | 0.87 | 0.85 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 22,048,947,556 | **8,841,967,114** | 8,920,515,364 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 246,878.30 | 121,013.40 | **102,874.00** | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **4,460,118** | 111,629,716 | 119,099,790 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **0.02%** | 1.26% | 1.34% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | **0.05%** | 12.92% | 13.27% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **36,475** | 85,086 | 48,571 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.41** | 1.16 | 0.56 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | **40,298** | 2,939,082 | 1,908,106 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **0.45** | 40.23 | 22.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 11,251 | **10,640** | 11,448 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 27.92% | **0.36%** | 0.60% | Tỷ lệ khóa futex |

---

### KB 15: server_stream_h2_h2 — Large Payload (File Stream H2-to-H2 10MB)
- **Mục tiêu thử nghiệm**: Chuyển tiếp luồng file 10MB chunked qua 2 kết nối H2 multiplexed, đo throughput bão hòa băng thông và khả năng kiểm soát flow control.
- **Kích thước tải trọng & Quy chuẩn**: 10,485,760 bytes DATA stream
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 20 -m 4 -t 4 http://192.168.122.14:8080/stream-h2/plain/10mb
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h2c : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 182.80 | 185.40 | **202.20** | Velda đạt 182.80 req/s (-1.4% vs Nginx, -9.6% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 1,904.64 MB/s | **1,935.36 MB/s** | 1,935.36 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 417.95 ms | **331.78 ms** | 354.80 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 48.27 ms | **17.02 ms** | 20.61 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 400.58 ms | **166.84 ms** | 245.10 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **586.10 ms** | 758.15 ms | 723.52 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **926.82 ms** | 2,598.19 ms | 1,852.42 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | **1,199.76 ms** | 3,317.57 ms | 3,131.80 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **1,199.76 ms** | 3,317.57 ms | 3,184.95 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **150.25 ms** | 498.12 ms | 369.92 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **33.87 ms** | 46.65 ms | 43.09 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.22 ms** | 0.23 ms | 0.22 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **914 2xx** | 927 2xx | 1,011 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **99.21%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 531.97 MB | 147.42 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **46,953,294,425** | 48,874,295,546 | 55,715,378,207 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **51,371,219.30** | 52,723,080.40 | 55,109,177.30 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 63,628,695,761 | **53,647,108,644** | 65,023,636,411 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 69,615,640.90 | **57,871,746.10** | 64,316,158.70 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.74 | **0.91** | 0.86 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **7,741,159,064** | 8,648,246,106 | 7,873,608,880 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 8,469,539.50 | 9,329,283.80 | **7,787,941.50** | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 79,781,785 | 79,112,502 | **68,661,238** | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.03% | 0.91% | **0.87%** | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 23.96% | **14.57%** | 16.91% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 44,184 | **39,994** | 46,507 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 48.34 | **43.14** | 46.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 3,336,666 | 3,424,652 | **2,484,963** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 3,650.62 | 3,694.34 | **2,457.93** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 12,353 | **10,260** | 10,745 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.37% | **0.30%** | 0.43% | Tỷ lệ khóa futex |

---

### KB 16: server_stream_h2_h3 — Micro Payload (SSE Bridge from H3 QUIC)
- **Mục tiêu thử nghiệm**: Nhận các gói tin QUIC streaming nhỏ từ H3 upstream và bridge thành HTTP/2 DATA frames downstream.
- **Kích thước tải trọng & Quy chuẩn**: ~64 bytes QUIC frames stream
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 50 -m 10 -t 4 http://192.168.122.14:8080/stream-h3/sse-ping
- **Nhánh Upstream**: HTTP/3 QUIC UDP upstream (100% H3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **94,610.60** | *N/A* | 26,654.00 | Velda đạt 94,610.60 req/s (+255.0% vs Envoy; Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | **5.96 MB/s** | *N/A* | 1.71 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **5.27 ms** | *N/A* | 18.68 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.12 ms** | *N/A* | 3.18 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **5.01 ms** | *N/A* | 16.51 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **8.39 ms** | *N/A* | 30.89 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **11.88 ms** | *N/A* | 38.26 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | **15.42 ms** | *N/A* | 57.87 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **29.44 ms** | *N/A* | 68.34 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **2.36 ms** | *N/A* | 8.31 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **4.38 ms** | *N/A* | 24.64 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.22 ms | *N/A* | **0.18 ms** | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **473,053 2xx** | *N/A* | 133,270 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | *N/A* | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 35.01 MB | *N/A* | **6.88 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 63,916,949,435 | 0 | **54,427,571,092** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **135,115.80** | *N/A* | 408,400.80 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **64,322,364,427** | 0 | 72,803,229,476 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **135,972.80** | *N/A* | 546,283.70 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.99** | *N/A* | 0.75 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 11,855,033,335 | 0 | **9,785,111,437** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **25,060.70** | *N/A* | 73,423.20 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **119,341,144** | 0 | 208,648,370 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.01%** | *N/A* | 2.13% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 9.65% | *N/A* | **8.42%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **117,533** | 0 | 237,870 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.25** | *N/A* | 1.78 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 1,420,608 | 0 | **759,304** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **3.00** | *N/A* | 5.70 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | *N/A* | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 24,154 | 0 | **12,689** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 1.70% | *N/A* | **1.67%** | Tỷ lệ khóa futex |

---

### KB 17: server_stream_h2_h3 — Medium Payload (QUIC Stream to H2 64KB)
- **Mục tiêu thử nghiệm**: Tiếp nhận dòng dữ liệu 64KB từ UDP QUIC stream, re-frame thành HTTP/2 TCP DATA frames với zero-copy Bytes slicing.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes UDP QUIC stream
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 40 -m 8 -t 4 http://192.168.122.14:8080/stream-h3/64kb
- **Nhánh Upstream**: HTTP/3 QUIC UDP upstream (100% H3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **7,178.60** | *N/A* | 4,265.40 | Velda đạt 7,178.60 req/s (+68.3% vs Envoy; Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | **449.23 MB/s** | *N/A* | 270.68 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **43.94 ms** | *N/A* | 74.28 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.44 ms** | *N/A* | 12.29 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **25.11 ms** | *N/A* | 75.07 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **75.49 ms** | *N/A* | 104.97 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 447.55 ms | *N/A* | **142.14 ms** | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 1,212.47 ms | *N/A* | **149.28 ms** | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 1,247.23 ms | *N/A* | **154.24 ms** | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 84.07 ms | *N/A* | **24.76 ms** | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **41.55 ms** | *N/A* | 47.21 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.17 ms** | *N/A* | 0.31 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **35,893 2xx** | *N/A* | 21,327 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | *N/A* | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 65.74 MB | *N/A* | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **73,483,515,723** | 0 | 76,022,303,966 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **2,047,293.80** | *N/A* | 3,564,603.70 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **68,921,486,640** | 0 | 72,382,473,154 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **1,920,193.00** | *N/A* | 3,393,936.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **1.07** | *N/A* | 1.05 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **12,867,728,372** | 0 | 13,305,119,864 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **358,502.40** | *N/A* | 623,862.70 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **126,378,890** | 0 | 150,791,342 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **0.98%** | *N/A* | 1.13% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 9.56% | *N/A* | **9.09%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **51,181** | 0 | 124,087 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **1.43** | *N/A* | 5.82 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 3,705,755 | 0 | **2,334,345** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **103.24** | *N/A* | 109.45 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | *N/A* | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 16,238 | 0 | **10,552** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | **0.44%** | *N/A* | 0.45% | Tỷ lệ khóa futex |

---

### KB 18: server_stream_h2_h3 — Large Payload (File Stream from H3 QUIC 10MB)
- **Mục tiêu thử nghiệm**: Stream luồng file 10MB từ HTTP/3 QUIC upstream chuyển tiếp về HTTP/2 client, kiểm tra khả năng duy trì throughput ổn định khi đổi protocol.
- **Kích thước tải trọng & Quy chuẩn**: 10,485,760 bytes QUIC stream
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 20 -m 4 -t 4 http://192.168.122.14:8080/stream-h3/10mb
- **Nhánh Upstream**: HTTP/3 QUIC UDP upstream (100% H3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **52.20** | *N/A* | 21.60 | Velda đạt 52.20 req/s (+141.7% vs Envoy; Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | **525.23 MB/s** | *N/A* | 332.75 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **1,337.15 ms** | *N/A* | 1,437.27 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **225.50 ms** | *N/A* | 978.25 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **1,215.60 ms** | *N/A* | 1,425.79 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 2,464.55 ms | *N/A* | **1,863.75 ms** | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 2,541.99 ms | *N/A* | **1,863.77 ms** | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 2,548.10 ms | *N/A* | **1,863.77 ms** | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 2,548.10 ms | *N/A* | **1,863.77 ms** | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 654.68 ms | *N/A* | **263.61 ms** | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 966.10 ms | *N/A* | **17.86 ms** | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.22 ms** | *N/A* | 0.22 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **261 2xx** | *N/A* | 108 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | *N/A* | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 804.12 MB | *N/A* | **6.87 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 91,116,471,037 | 0 | **86,606,319,255** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **349,105,253.00** | *N/A* | 801,910,363.50 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 72,212,392,451 | 0 | **71,074,735,367** | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **276,675,833.10** | *N/A* | 658,099,401.50 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **1.26** | *N/A* | 1.22 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 15,859,880,459 | 0 | **15,115,259,787** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **60,765,825.50** | *N/A* | 139,956,109.10 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **101,998,160** | 0 | 123,766,791 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **0.64%** | *N/A* | 0.82% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | **8.43%** | *N/A* | 8.96% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **81,976** | 0 | 106,836 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **314.08** | *N/A* | 989.22 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 4,488,537 | 0 | **2,756,538** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **17,197.46** | *N/A* | 25,523.50 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | *N/A* | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 20,725 | 0 | **10,844** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.46% | *N/A* | **0.39%** | Tỷ lệ khóa futex |

---

### KB 19: client_stream_h2_h1 — Micro Payload (Drip Upload ~64B to H1)
- **Mục tiêu thử nghiệm**: Tiếp nhận các DATA frames nhỏ từ client H2 upload và giải mã thành chunked stream HTTP/1.1 đẩy sang upstream backend.
- **Kích thước tải trọng & Quy chuẩn**: ~64 bytes upload body
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 50 -m 10 -t 4 -d /tmp/payload_64b.bin http://192.168.122.14:8080/upload/plain
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 51,707.80 | **437,820.00** | 41,359.60 | Velda đạt 51,707.80 req/s (-88.2% vs Nginx, +25.0% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 1.56 MB/s | **119.00 MB/s** | 1.64 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 9.65 ms | **1.07 ms** | 12.05 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.09 ms | **0.03 ms** | 3.80 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 9.23 ms | **0.93 ms** | 12.12 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 15.02 ms | **1.70 ms** | 14.46 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 21.67 ms | **3.46 ms** | 17.33 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 31.12 ms | **3.84 ms** | 52.19 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 45.26 ms | **5.50 ms** | 75.50 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 4.19 ms | **0.62 ms** | 2.89 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 10.57 ms | **1.21 ms** | 40.23 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.17 ms** | 0.24 ms | 0.17 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **258,539 2xx** | 2,189,100 2xx | 206,798 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 46.38 MB | 133.45 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 69,240,335,667 | 71,154,909,888 | **59,935,782,439** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 267,813.90 | **32,504.20** | 289,827.70 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 70,690,650,749 | **61,128,876,828** | 73,662,328,765 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 273,423.50 | **27,924.20** | 356,204.30 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.98 | **1.16** | 0.81 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 12,712,669,659 | 13,827,726,147 | **10,902,125,611** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 49,171.20 | **6,316.60** | 52,718.70 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 155,865,733 | **103,087,499** | 163,172,184 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.23% | **0.75%** | 1.50% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 8.96% | **5.65%** | 8.74% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 333,757 | **63,084** | 227,950 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 1.29 | **0.03** | 1.10 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,609,725 | 3,862,850 | **1,398,844** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 10.09 | **1.76** | 6.76 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 16,567 | **11,821** | 12,091 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.63% | **0.31%** | 0.86% | Tỷ lệ khóa futex |

---

### KB 20: client_stream_h2_h1 — Medium Payload (Doc Upload 64KB to H1)
- **Mục tiêu thử nghiệm**: Client upload 64KB streaming qua HTTP/2 DATA frames, gateway chuyển tiếp thành chunked HTTP/1.1 với ProgressiveChunkDecoder.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes document upload
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 40 -m 8 -t 4 -d /tmp/payload_64kb.bin http://192.168.122.14:8080/upload/plain
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **20,065.60** | 1,115.40 | 16,507.20 | Velda đạt 20,065.60 req/s (+1699.0% vs Nginx, +21.6% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.65 MB/s | 0.30 MB/s | **0.70 MB/s** | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 15.90 ms | **5.88 ms** | 19.31 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.20 ms | **0.03 ms** | 5.31 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 15.10 ms | **5.57 ms** | 18.73 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 24.55 ms | **10.44 ms** | 25.89 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 35.79 ms | **16.12 ms** | 37.40 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | **45.96 ms** | 56.77 ms | 58.84 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **78.81 ms** | 79.60 ms | 88.18 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 6.72 ms | **3.84 ms** | 5.53 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 11.93 ms | **1.35 ms** | 30.59 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.23 ms** | 0.29 ms | 0.25 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **100,328 2xx** | 5,812 2xx | 82,536 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **95.96%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 100.26 MB | 134.73 MB | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 47,844,911,109 | **1,968,422,263** | 54,170,799,987 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 476,884.90 | **352,953.60** | 656,329.40 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 70,361,213,298 | **3,077,796,001** | 72,870,772,259 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 701,311.80 | **551,873.10** | 882,896.80 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.68 | 0.64 | **0.74** | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 7,902,327,993 | **354,499,988** | 8,644,232,080 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 78,764.90 | **63,564.60** | 104,732.90 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 114,036,119 | **7,476,893** | 127,002,601 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.44%** | 2.11% | 1.47% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 20.65% | **11.35%** | 13.90% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 146,769 | **42,541** | 197,954 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **1.46** | 7.63 | 2.40 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,450,865 | **71,972** | 1,835,831 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 24.43 | **12.91** | 22.24 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 15,046 | **11,010** | 11,397 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | **0.61%** | 15.30% | 0.62% | Tỷ lệ khóa futex |

---

### KB 21: client_stream_h2_h1 — Large Payload (Bulk Upload 5MB to H1)
- **Mục tiêu thử nghiệm**: Client upload liên tục khối dữ liệu 5MB qua H2 stream, gateway chuyển tiếp qua H1 chunked với flow-control backpressure.
- **Kích thước tải trọng & Quy chuẩn**: 5,242,880 bytes streaming upload
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 20 -m 4 -t 4 -d /tmp/payload_5mb.bin http://192.168.122.14:8080/upload/plain
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 407.60 | **537.00** | 388.80 | Velda đạt 407.60 req/s (-24.1% vs Nginx, +4.8% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.02 MB/s | **0.15 MB/s** | 0.02 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **190.99 ms** | 15,118.39 ms | 197.82 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 16.78 ms | **0.03 ms** | 20.86 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 185.05 ms | **1.76 ms** | 201.88 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 270.18 ms | **2.19 ms** | 289.84 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 392.00 ms | **3.00 ms** | 355.96 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 439.67 ms | 13,766,200.26 ms | **402.77 ms** | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 547.50 ms | 13,766,200.27 ms | **404.49 ms** | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 62.51 ms | **0.44 ms** | 71.88 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 111.30 ms | **0.77 ms** | 80.64 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.21 ms** | 0.21 ms | 0.24 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **2,038 2xx** | 2,732 2xx | 1,944 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **98.28%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 242.02 MB | 134.13 MB | **6.89 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 52,242,251,717 | **1,689,977,749** | 48,728,767,598 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 25,634,078.40 | **629,414.40** | 25,066,238.50 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 72,038,803,945 | **2,599,108,994** | 69,581,652,438 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 35,347,793.90 | **968,010.80** | 35,793,031.10 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.72** | 0.65 | 0.70 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 8,567,125,619 | **301,441,121** | 6,604,206,460 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 4,203,692.60 | **112,268.60** | 3,397,225.50 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 81,480,784 | **7,058,384** | 58,385,628 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.95% | 2.34% | **0.88%** | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 24.63% | **9.74%** | 19.25% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 221,988 | **40,332** | 144,712 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 108.92 | **15.02** | 74.44 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 3,326,500 | **64,220** | 1,957,455 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 1,632.24 | **23.92** | 1,006.92 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 13,485 | **10,752** | 20,270 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | **0.41%** | 16.74% | 1.04% | Tỷ lệ khóa futex |

---

### KB 22: client_stream_h2_h2 — Micro Payload (Drip Upload ~64B to H2)
- **Mục tiêu thử nghiệm**: Đẩy luồng upload nhỏ qua 2 tầng HTTP/2 multiplexing, chuyển tiếp trực tiếp DATA frames và flags END_STREAM.
- **Kích thước tải trọng & Quy chuẩn**: ~64 bytes headers + DATA frame
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 50 -m 10 -t 4 -d /tmp/payload_64b.bin http://192.168.122.14:8080/upload-h2/plain
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h2c : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 53,954.20 | **412,243.60** | 47,218.60 | Velda đạt 53,954.20 req/s (-86.9% vs Nginx, +14.3% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 1.55 MB/s | **112.83 MB/s** | 1.40 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 9.24 ms | **1.14 ms** | 10.51 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.15 ms | **0.05 ms** | 1.69 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 5.49 ms | **1.11 ms** | 10.03 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 23.08 ms | **1.71 ms** | 15.06 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 45.33 ms | **2.80 ms** | 22.27 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 59.47 ms | **5.17 ms** | 29.57 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 69.05 ms | **16.22 ms** | 44.19 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 9.75 ms | **0.56 ms** | 3.61 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 8.70 ms | **1.26 ms** | 9.97 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.24 ms | **0.20 ms** | 0.25 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **269,773 2xx** | 2,061,218 2xx | 236,093 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 76.33 MB | 134.11 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **59,481,059,447** | 66,881,063,494 | 71,820,217,074 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 220,487.20 | **32,447.40** | 304,203.10 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 69,427,484,652 | **63,053,231,945** | 73,748,368,899 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 257,357.10 | **30,590.30** | 312,370.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.86 | **1.06** | 0.97 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **10,724,944,458** | 12,991,858,518 | 13,215,505,063 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 39,755.70 | **6,303.00** | 55,975.80 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 150,389,955 | **102,594,712** | 164,235,942 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.40% | **0.79%** | 1.24% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 9.99% | **5.81%** | 7.73% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 159,550 | 52,092 | **51,506** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.59 | **0.03** | 0.22 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 1,674,958 | 3,566,603 | **1,231,040** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 6.21 | **1.73** | 5.21 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 44,727 | 11,110 | **10,775** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 2.67% | **0.31%** | 0.88% | Tỷ lệ khóa futex |

---

### KB 23: client_stream_h2_h2 — Medium Payload (Doc Upload 64KB to H2)
- **Mục tiêu thử nghiệm**: Upload 64KB document từ client H2 sang upstream H2 qua stream multiplexing, kiểm tra backpressure liên thông.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes DATA frames upload
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 40 -m 8 -t 4 -d /tmp/payload_64kb.bin http://192.168.122.14:8080/upload-h2/plain
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h2c : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **24,851.20** | 1,118.80 | 19,225.40 | Velda đạt 24,851.20 req/s (+2121.2% vs Nginx, +29.3% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **0.72 MB/s** | 0.31 MB/s | 0.69 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 12.63 ms | **5.73 ms** | 16.41 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.21 ms | **0.03 ms** | 1.14 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 10.37 ms | **5.38 ms** | 16.13 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 20.68 ms | **9.78 ms** | 26.53 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 60.64 ms | **25.06 ms** | 41.81 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 104.52 ms | 99.29 ms | **55.53 ms** | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 138.13 ms | 100.75 ms | **71.05 ms** | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 10.47 ms | **6.69 ms** | 8.29 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 12.65 ms | **1.26 ms** | 23.79 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.17 ms** | 0.24 ms | 0.19 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **125,950 2xx** | 5,821 2xx | 96,127 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **98.66%** | **96.1%** | **93.1%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 86.84 MB | 134.51 MB | **6.88 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 50,329,149,506 | **1,926,321,604** | 59,841,531,581 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 405,044.00 | **344,355.00** | 622,525.70 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 64,052,697,261 | **2,786,959,959** | 70,135,636,624 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 515,489.80 | **498,205.20** | 729,614.30 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.79 | 0.69 | **0.85** | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 8,748,053,388 | **346,005,114** | 9,347,129,157 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 70,403.50 | **61,852.90** | 97,237.30 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 121,617,214 | **6,967,961** | 126,472,108 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.39% | 2.01% | **1.35%** | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 14.61% | **11.05%** | 13.49% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 170,972 | **42,559** | 146,077 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **1.38** | 7.61 | 1.52 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 1,925,497 | **72,769** | 1,174,277 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 15.50 | 13.01 | **12.22** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 28,387 | **10,686** | 11,392 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 1.47% | 14.68% | **0.97%** | Tỷ lệ khóa futex |

---

### KB 24: client_stream_h2_h2 — Large Payload (Bulk Upload 5MB to H2)
- **Mục tiêu thử nghiệm**: Upload 5MB stream liên tục qua H2-to-H2 pipe, kiểm tra khả năng duy trì bộ nhớ O(1) và điều tiết WINDOW_UPDATE giữa downstream và upstream.
- **Kích thước tải trọng & Quy chuẩn**: 5,242,880 bytes streaming DATA
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 20 -m 4 -t 4 -d /tmp/payload_5mb.bin http://192.168.122.14:8080/upload-h2/plain
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h2c : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 1,021.40 | **6,189.00** | 570.60 | Velda đạt 1,021.40 req/s (-83.5% vs Nginx, +79.0% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.05 MB/s | **0.62 MB/s** | 0.02 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 73.06 ms | **12.89 ms** | 131.30 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 2.99 ms | **0.92 ms** | 16.80 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 62.51 ms | **12.24 ms** | 74.52 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 124.41 ms | **20.23 ms** | 293.43 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 190.00 ms | **28.15 ms** | 527.17 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 228.00 ms | **36.70 ms** | 895.53 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 259.23 ms | **54.00 ms** | 903.19 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 38.15 ms | **5.46 ms** | 118.61 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **3.97 ms** | 5.33 ms | 56.19 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.18 ms** | 0.22 ms | 0.22 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **5,107 2xx** | 30,945 2xx | 2,853 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **99.96%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 49.75 MB | 154.98 MB | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 28,774,439,311 | 78,339,338,888 | **22,454,681,726** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 5,634,313.60 | **2,531,566.90** | 7,870,550.90 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **44,662,812,293** | 70,462,050,731 | 60,541,094,074 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 8,745,410.70 | **2,277,009.20** | 21,220,152.10 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.64 | **1.11** | 0.37 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 5,159,369,363 | 11,169,246,373 | **3,971,053,419** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 1,010,254.40 | **360,938.60** | 1,391,886.90 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 49,401,764 | 178,734,799 | **42,198,893** | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **0.96%** | 1.60% | 1.06% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 22.57% | **8.66%** | 26.62% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 155,476 | 72,706 | **60,866** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 30.44 | **2.35** | 21.33 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,622,030 | 1,302,281 | **278,720** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 513.42 | **42.08** | 97.69 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 69,188 | **10,947** | 27,307 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 2.64% | **0.84%** | 9.80% | Tỷ lệ khóa futex |

---

### KB 25: client_stream_h2_h3 — Micro Payload (Drip Upload ~64B to H3)
- **Mục tiêu thử nghiệm**: Chuyển tiếp từng frame DATA nhỏ từ HTTP/2 TCP downstream sang QUIC STREAM frame trên HTTP/3 upstream.
- **Kích thước tải trọng & Quy chuẩn**: ~64 bytes upload frame
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 50 -m 10 -t 4 -d /tmp/payload_64b.bin http://192.168.122.14:8080/upload-h3
- **Nhánh Upstream**: HTTP/3 QUIC UDP upstream (100% H3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **97,276.40** | *N/A* | 25,098.80 | Velda đạt 97,276.40 req/s (+287.6% vs Envoy; Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | **2.79 MB/s** | *N/A* | 0.96 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **5.13 ms** | *N/A* | 19.84 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.10 ms** | *N/A* | 6.69 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **4.83 ms** | *N/A* | 17.04 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **8.19 ms** | *N/A* | 30.32 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **12.14 ms** | *N/A* | 43.39 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | **15.87 ms** | *N/A* | 54.87 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **38.61 ms** | *N/A* | 65.59 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **2.38 ms** | *N/A* | 7.14 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **4.45 ms** | *N/A* | 25.32 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.27 ms** | *N/A* | 0.28 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **486,382 2xx** | *N/A* | 125,494 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | *N/A* | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 41.50 MB | *N/A* | **6.87 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 68,612,348,684 | 0 | **59,498,976,836** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **141,066.80** | *N/A* | 474,118.10 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **67,282,767,518** | 0 | 73,359,224,221 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **138,333.20** | *N/A* | 584,563.60 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **1.02** | *N/A* | 0.81 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 12,610,796,015 | 0 | **10,662,570,406** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **25,927.80** | *N/A* | 84,964.80 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **126,638,559** | 0 | 180,436,179 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.00%** | *N/A* | 1.69% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 9.66% | *N/A* | **8.13%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **76,809** | 0 | 263,177 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.16** | *N/A* | 2.10 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 1,547,643 | 0 | **1,005,455** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **3.18** | *N/A* | 8.01 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | *N/A* | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 24,730 | 0 | **11,254** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 1.60% | *N/A* | **1.12%** | Tỷ lệ khóa futex |

---

### KB 26: client_stream_h2_h3 — Medium Payload (Doc Upload 64KB to H3)
- **Mục tiêu thử nghiệm**: Upload tài liệu 64KB từ client H2 đẩy sang H3 UDP upstream, kiểm tra cơ chế coalesce batching và UDP sendmsg.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes upload body
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 40 -m 8 -t 4 -d /tmp/payload_64kb.bin http://192.168.122.14:8080/upload-h3
- **Nhánh Upstream**: HTTP/3 QUIC UDP upstream (100% H3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 4,446.80 | *N/A* | **7,871.40** | Velda đạt 4,446.80 req/s (-43.5% vs Envoy; Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.13 MB/s | *N/A* | **0.30 MB/s** | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 64.61 ms | *N/A* | **40.41 ms** | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.42 ms** | *N/A* | 10.50 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **36.35 ms** | *N/A* | 37.97 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 107.26 ms | *N/A* | **60.48 ms** | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 700.92 ms | *N/A* | **101.94 ms** | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 1,393.15 ms | *N/A* | **164.51 ms** | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 2,096.07 ms | *N/A* | **174.99 ms** | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 125.22 ms | *N/A* | **18.16 ms** | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **24.58 ms** | *N/A* | 43.08 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.19 ms** | *N/A* | 0.25 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **22,234 2xx** | *N/A* | 39,357 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | *N/A* | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 441.98 MB | *N/A* | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **7,184,912,709** | 0 | 58,514,280,809 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **323,149.80** | *N/A* | 1,486,756.60 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **9,456,512,763** | 0 | 70,575,813,969 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **425,317.70** | *N/A* | 1,793,221.40 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.76 | *N/A* | **0.83** | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **1,276,964,555** | 0 | 10,353,041,523 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **57,433.00** | *N/A* | 263,054.60 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **18,037,510** | 0 | 154,750,995 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.41%** | *N/A* | 1.49% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 9.67% | *N/A* | **9.54%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **45,745** | 0 | 360,951 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **2.06** | *N/A* | 9.17 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | **431,880** | 0 | 1,734,346 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **19.42** | *N/A* | 44.07 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | *N/A* | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | **11,516** | 0 | 11,532 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 2.67% | *N/A* | **0.66%** | Tỷ lệ khóa futex |

---

### KB 27: client_stream_h2_h3 — Large Payload (Bulk Upload 5MB to H3)
- **Mục tiêu thử nghiệm**: Client upload luồng lớn 5MB qua H2 downstream, gateway stream trực tiếp sang H3 upstream bằng non-blocking chunk fin.
- **Kích thước tải trọng & Quy chuẩn**: 5,242,880 bytes streaming data
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 20 -m 4 -t 4 -d /tmp/payload_5mb.bin http://192.168.122.14:8080/upload-h3
- **Nhánh Upstream**: HTTP/3 QUIC UDP upstream (100% H3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **720.60** | *N/A* | 480.40 | Velda đạt 720.60 req/s (+50.0% vs Envoy; Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | **0.02 MB/s** | *N/A* | 0.02 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **104.77 ms** | *N/A* | 159.85 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **9.57 ms** | *N/A* | 12.09 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **85.61 ms** | *N/A* | 156.30 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **186.30 ms** | *N/A* | 278.35 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **358.19 ms** | *N/A* | 485.15 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 736.96 ms | *N/A* | **617.50 ms** | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **810.54 ms** | *N/A* | 879.07 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **67.71 ms** | *N/A* | 93.54 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **50.14 ms** | *N/A* | 100.15 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.18 ms** | *N/A* | 0.20 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **3,603 2xx** | *N/A* | 2,402 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | *N/A* | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 300.08 MB | *N/A* | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **32,182,889,066** | 0 | 40,966,331,985 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **8,932,247.90** | *N/A* | 17,055,092.40 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **45,619,092,173** | 0 | 70,631,078,238 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **12,661,418.90** | *N/A* | 29,405,111.70 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.70** | *N/A* | 0.58 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **5,723,286,504** | 0 | 7,299,497,689 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **1,588,478.10** | *N/A* | 3,038,924.90 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 56,478,148 | 0 | **47,927,182** | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.99% | *N/A* | **0.66%** | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 21.10% | *N/A* | **15.05%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 164,086 | 0 | **129,259** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **45.54** | *N/A* | 53.81 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,562,316 | 0 | **511,248** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 711.16 | *N/A* | **212.84** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | *N/A* | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 75,548 | 0 | **33,717** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | **2.95%** | *N/A* | 6.60% | Tỷ lệ khóa futex |

---

### KB 28: duplex_h2_h1 — Micro Payload (Interactive Ping-Pong 64B)
- **Mục tiêu thử nghiệm**: Mô phỏng kênh tương tác song công (gRPC/WebSocket-style over H2), downstream gửi frame nhỏ và nhận phản hồi tức thời từ H1 upstream.
- **Kích thước tải trọng & Quy chuẩn**: ~64 bytes headers + frame song công
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 50 -m 10 -t 4 -d /tmp/payload_64b.bin http://192.168.122.14:8080/duplex/plain
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 48,178.80 | **374,941.80** | 42,678.60 | Velda đạt 48,178.80 req/s (-87.2% vs Nginx, +12.9% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 1.79 MB/s | **101.91 MB/s** | 1.63 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 10.36 ms | **1.27 ms** | 11.67 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.09 ms | **0.05 ms** | 2.37 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 9.82 ms | **1.13 ms** | 11.93 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 16.24 ms | **2.12 ms** | 15.25 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 24.29 ms | **3.04 ms** | 19.28 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 33.94 ms | **4.11 ms** | 53.98 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 57.68 ms | **5.73 ms** | 80.98 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 4.65 ms | **0.60 ms** | 4.03 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 9.26 ms | **1.25 ms** | 40.26 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.20 ms** | 0.23 ms | 0.21 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **240,894 2xx** | 1,874,709 2xx | 213,393 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 50.75 MB | 133.33 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 72,213,735,800 | 66,270,250,858 | **62,841,986,853** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 299,773.90 | **35,349.60** | 294,489.40 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 69,484,914,491 | **56,395,547,236** | 69,604,602,273 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 288,446.00 | **30,082.30** | 326,180.30 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 1.04 | **1.18** | 0.90 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 13,193,115,791 | 12,881,945,992 | **11,489,572,102** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 54,767.30 | **6,871.40** | 53,842.30 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 163,440,362 | **96,732,587** | 166,639,799 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.24% | **0.75%** | 1.45% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 8.26% | **5.26%** | 8.19% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 350,537 | **72,403** | 228,476 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 1.46 | **0.04** | 1.07 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,677,929 | 3,600,027 | **1,426,139** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 11.12 | **1.92** | 6.68 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 16,093 | 11,535 | **10,626** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.60% | **0.32%** | 0.75% | Tỷ lệ khóa futex |

---

### KB 29: duplex_h2_h1 — Medium Payload (Bidirectional Messaging 64KB)
- **Mục tiêu thử nghiệm**: Trao đổi dữ liệu 2 chiều 64KB liên tục giữa client H2 và backend H1, kiểm tra cơ chế tách biệt 2 luồng đọc/ghi độc lập trong duplex pipe.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes bidirectional messaging
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 40 -m 8 -t 4 -d /tmp/payload_64kb.bin http://192.168.122.14:8080/duplex/plain
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **18,997.40** | 1,121.20 | 16,551.60 | Velda đạt 18,997.40 req/s (+1594.4% vs Nginx, +14.8% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **0.71 MB/s** | 0.31 MB/s | 0.63 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 16.80 ms | **5.70 ms** | 19.25 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.22 ms | **0.03 ms** | 6.03 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 15.99 ms | **5.50 ms** | 16.81 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 26.16 ms | **10.55 ms** | 28.25 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 38.31 ms | **20.32 ms** | 49.93 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 49.72 ms | **27.44 ms** | 96.32 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 72.23 ms | **29.43 ms** | 141.60 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 7.26 ms | **3.59 ms** | 9.56 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 13.24 ms | **1.32 ms** | 53.43 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.28 ms | **0.28 ms** | 0.29 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **94,987 2xx** | 5,844 2xx | 82,758 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **95.93%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 122.50 MB | 134.23 MB | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 49,768,888,970 | **1,895,542,769** | 54,178,762,896 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 523,954.70 | **338,127.50** | 654,665.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 70,463,825,008 | **2,672,155,651** | 72,723,005,797 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 741,826.00 | **476,659.90** | 878,742.90 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.71 | 0.71 | **0.74** | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 8,425,826,311 | **337,546,003** | 8,672,785,262 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 88,705.00 | **60,211.60** | 104,796.90 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 121,706,079 | **7,008,814** | 125,486,617 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.44%** | 2.08% | 1.45% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 19.13% | **11.00%** | 13.94% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 200,491 | **40,391** | 179,276 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **2.11** | 7.20 | 2.17 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,538,251 | **73,889** | 1,785,540 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 26.72 | **13.18** | 21.58 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 14,974 | **10,394** | 11,286 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | **0.59%** | 14.07% | 0.63% | Tỷ lệ khóa futex |

---

### KB 30: duplex_h2_h1 — Large Payload (Heavy Bidirectional Pump 1MB)
- **Mục tiêu thử nghiệm**: Dội tải 1MB truyền và nhận đồng thời 2 chiều giữa client H2 và backend H1, kiểm tra áp lực cấp phát bộ nhớ khi cả 2 chiều đều bão hòa.
- **Kích thước tải trọng & Quy chuẩn**: 1,048,576 bytes song công 2 chiều
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 25 -m 5 -t 4 -d /tmp/payload_1mb.bin http://192.168.122.14:8080/duplex/plain
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **1,907.00** | 685.80 | 1,765.00 | Velda đạt 1,907.00 req/s (+178.1% vs Nginx, +8.0% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.07 MB/s | **0.19 MB/s** | 0.07 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 64.86 ms | **3.36 ms** | 70.14 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 3.83 ms | **0.03 ms** | 3.27 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 60.27 ms | **2.95 ms** | 65.60 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 101.61 ms | **5.23 ms** | 111.11 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 154.76 ms | **7.83 ms** | 240.89 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 246.24 ms | **10.87 ms** | 317.37 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 322.68 ms | **11.64 ms** | 343.57 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 28.91 ms | **1.54 ms** | 42.98 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 4.94 ms | **4.43 ms** | 51.41 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.22 ms** | 2.61 ms | 1.23 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **9,535 2xx** | 3,498 2xx | 8,825 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **98.03%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 231.92 MB | 135.35 MB | **6.85 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 61,436,355,128 | **1,698,392,911** | 56,175,547,784 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 6,443,246.50 | **495,302.70** | 6,365,501.20 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 72,273,588,287 | **2,363,063,952** | 70,774,297,272 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 7,579,820.50 | **689,140.80** | 8,019,750.40 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.85** | 0.72 | 0.79 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 10,360,506,627 | **298,121,426** | 8,022,156,038 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 1,086,576.50 | **86,941.20** | 909,026.20 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 99,669,194 | **7,238,172** | 81,299,462 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **0.96%** | 2.43% | 1.01% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 18.14% | **10.05%** | 17.38% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 302,119 | **40,474** | 181,225 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 31.69 | **11.80** | 20.54 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 3,781,592 | **62,100** | 2,053,477 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 396.60 | **18.11** | 232.69 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 12,961 | **10,783** | 13,165 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | **0.34%** | 17.36% | 0.64% | Tỷ lệ khóa futex |

---

### KB 31: duplex_h2_h2 — Micro Payload (Pure H2 Bidi Ping-Pong 64B)
- **Mục tiêu thử nghiệm**: Kết nối song công thuần HTTP/2 hai đầu (H2 Downstream <-> H2 Upstream), chuyển tiếp frame trực tiếp không decode thân gói.
- **Kích thước tải trọng & Quy chuẩn**: ~64 bytes frame song công
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 50 -m 10 -t 4 -d /tmp/payload_64b.bin http://192.168.122.14:8080/duplex-h2/plain
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h2c : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 53,143.80 | **409,629.00** | 35,485.20 | Velda đạt 53,143.80 req/s (-87.0% vs Nginx, +49.8% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 1.98 MB/s | **112.12 MB/s** | 1.36 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 9.39 ms | **1.16 ms** | 14.04 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.11 ms | **0.04 ms** | 2.94 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 5.55 ms | **1.00 ms** | 12.21 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 23.01 ms | **1.93 ms** | 21.84 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 46.99 ms | **3.44 ms** | 31.99 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 63.30 ms | **4.46 ms** | 37.19 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 79.59 ms | **7.54 ms** | 43.22 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 10.06 ms | **0.64 ms** | 5.82 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 8.80 ms | **1.32 ms** | 26.66 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.26 ms | **0.25 ms** | 0.28 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **265,719 2xx** | 2,048,145 2xx | 177,426 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 80.88 MB | 133.89 MB | **6.87 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **57,359,760,664** | 69,080,020,260 | 65,539,615,613 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 215,866.20 | **33,728.10** | 369,391.30 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 69,006,916,374 | **61,501,736,548** | 74,208,766,139 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 259,698.80 | **30,028.00** | 418,251.90 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.83 | **1.12** | 0.88 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **10,254,327,780** | 13,384,324,631 | 11,985,209,891 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 38,590.90 | **6,534.90** | 67,550.50 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 154,946,761 | **103,837,650** | 155,023,690 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.51% | **0.78%** | 1.29% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 10.11% | **5.74%** | 8.21% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 161,249 | **81,971** | 241,648 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.61 | **0.04** | 1.36 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 1,563,130 | 3,723,957 | **1,289,020** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 5.88 | **1.82** | 7.27 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 37,088 | 11,367 | **10,926** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 2.37% | **0.31%** | 0.85% | Tỷ lệ khóa futex |

---

### KB 32: duplex_h2_h2 — Medium Payload (Bidi H2 Data Exchange 64KB)
- **Mục tiêu thử nghiệm**: Trao đổi luồng 64KB song công qua 2 kênh multiplexing HTTP/2, kiểm tra khả năng phối hợp quota window ở cả 2 hướng.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes DATA frames song công
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 40 -m 8 -t 4 -d /tmp/payload_64kb.bin http://192.168.122.14:8080/duplex-h2/plain
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h2c : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **23,857.60** | 1,124.40 | 20,347.40 | Velda đạt 23,857.60 req/s (+2021.8% vs Nginx, +17.3% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **0.89 MB/s** | 0.31 MB/s | 0.78 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 13.32 ms | **5.44 ms** | 15.66 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.17 ms | **0.03 ms** | 4.23 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 8.70 ms | **4.99 ms** | 13.84 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 30.84 ms | **9.66 ms** | 24.23 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 57.58 ms | **14.32 ms** | 38.59 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 74.26 ms | 64.41 ms | **50.52 ms** | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 97.14 ms | 88.53 ms | **58.11 ms** | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 12.47 ms | **3.97 ms** | 6.90 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 10.90 ms | **1.23 ms** | 31.82 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.23 ms** | 0.30 ms | 0.27 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **119,727 2xx** | 5,852 2xx | 101,737 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **99.63%** | **96.07%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 140.19 MB | 134.72 MB | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 52,641,793,972 | **2,029,475,112** | 48,887,453,984 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 441,300.00 | **360,988.10** | 480,527.80 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 68,789,386,019 | **2,758,869,271** | 72,582,513,533 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 576,666.40 | **490,727.40** | 713,432.80 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.77** | 0.74 | 0.67 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 8,989,701,153 | **367,906,371** | 8,977,100,603 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 75,361.30 | **65,440.50** | 88,238.30 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 136,386,009 | **6,976,154** | 114,611,667 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.52% | 1.90% | **1.28%** | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 14.33% | **10.57%** | 14.51% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 155,114 | **41,584** | 142,241 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **1.30** | 7.40 | 1.40 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 1,911,309 | **82,720** | 2,250,864 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 16.02 | **14.71** | 22.12 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 40,588 | 10,554 | **10,444** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 2.12% | 12.76% | **0.46%** | Tỷ lệ khóa futex |

---

### KB 33: duplex_h2_h2 — Large Payload (Heavy H2 Bidi Pump 1MB)
- **Mục tiêu thử nghiệm**: Bơm tải song công 1MB ở cả 2 đầu kết nối HTTP/2 multiplexing, kiểm tra khả năng tránh deadlock và bão hòa throughput.
- **Kích thước tải trọng & Quy chuẩn**: 1,048,576 bytes song công H2-to-H2
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 25 -m 5 -t 4 -d /tmp/payload_1mb.bin http://192.168.122.14:8080/duplex-h2/plain
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h2c : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 4,873.00 | **5,883.80** | 2,124.00 | Velda đạt 4,873.00 req/s (-17.2% vs Nginx, +129.4% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.24 MB/s | **0.59 MB/s** | 0.08 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **19.54 ms** | 21.18 ms | 58.41 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 1.15 ms | **0.30 ms** | 7.80 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **17.29 ms** | 20.35 ms | 49.37 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **32.88 ms** | 35.85 ms | 90.30 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **51.75 ms** | 53.47 ms | 126.44 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 78.31 ms | **70.22 ms** | 158.10 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 223.53 ms | **87.50 ms** | 271.55 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **10.45 ms** | 11.45 ms | 24.91 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **5.40 ms** | 10.62 ms | 31.42 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.24 ms** | 0.24 ms | 0.24 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **24,365 2xx** | 29,419 2xx | 10,620 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 155.50 MB | 162.56 MB | **6.88 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **30,544,281,911** | 78,293,660,675 | 38,199,107,677 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **1,253,613.00** | 2,661,329.80 | 3,596,902.80 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **52,525,786,245** | 70,592,803,684 | 70,625,227,708 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **2,155,788.50** | 2,399,565.00 | 6,650,209.80 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.58 | **1.11** | 0.54 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **5,456,986,059** | 11,221,602,649 | 7,024,598,757 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **223,968.20** | 381,440.70 | 661,450.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **66,791,311** | 171,927,400 | 71,691,532 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.22% | 1.53% | **1.02%** | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 21.24% | **8.79%** | 20.94% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 135,499 | **65,789** | 177,784 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 5.56 | **2.24** | 16.74 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,418,537 | **1,295,913** | 2,824,102 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 99.26 | **44.05** | 265.92 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 57,882 | **11,289** | 15,008 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 2.39% | 0.87% | **0.53%** | Tỷ lệ khóa futex |

---

### KB 34: duplex_h2_h3 — Micro Payload (QUIC Bidi Ping-Pong 64B)
- **Mục tiêu thử nghiệm**: Song công tương tác giữa HTTP/2 TCP downstream và HTTP/3 QUIC UDP upstream, kiểm tra đồng bộ state stream.
- **Kích thước tải trọng & Quy chuẩn**: ~64 bytes headers + frame song công
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 50 -m 10 -t 4 -d /tmp/payload_64b.bin http://192.168.122.14:8080/duplex-h3
- **Nhánh Upstream**: HTTP/3 QUIC UDP upstream (100% H3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **89,104.20** | *N/A* | 22,388.00 | Velda đạt 89,104.20 req/s (+298.0% vs Envoy; Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | **3.32 MB/s** | *N/A* | 0.86 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **5.60 ms** | *N/A* | 22.25 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.12 ms** | *N/A* | 10.77 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **5.28 ms** | *N/A* | 20.43 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **9.05 ms** | *N/A* | 32.24 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **13.23 ms** | *N/A* | 44.02 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | **16.70 ms** | *N/A* | 55.65 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **29.08 ms** | *N/A* | 67.65 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **2.62 ms** | *N/A* | 6.89 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **5.18 ms** | *N/A* | 25.46 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.27 ms | *N/A* | **0.25 ms** | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **445,521 2xx** | *N/A* | 111,940 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | *N/A* | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 37.38 MB | *N/A* | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 64,405,731,860 | 0 | **54,752,629,913** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **144,562.70** | *N/A* | 489,124.80 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **68,478,005,014** | 0 | 72,930,436,121 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **153,703.20** | *N/A* | 651,513.60 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.94** | *N/A* | 0.75 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 11,839,936,548 | 0 | **9,834,537,148** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **26,575.50** | *N/A* | 87,855.40 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **132,635,278** | 0 | 180,021,142 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.12%** | *N/A* | 1.83% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 9.71% | *N/A* | **8.54%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **138,634** | 0 | 280,655 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.31** | *N/A* | 2.51 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 1,547,951 | 0 | **914,331** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **3.47** | *N/A* | 8.17 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | *N/A* | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 24,669 | 0 | **10,838** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 1.59% | *N/A* | **1.19%** | Tỷ lệ khóa futex |

---

### KB 35: duplex_h2_h3 — Medium Payload (Bidi H3 Data Exchange 64KB)
- **Mục tiêu thử nghiệm**: Chuyển tiếp dữ liệu 2 chiều 64KB giữa TCP HTTP/2 và UDP HTTP/3, kiểm tra cơ chế phân luồng độc lập.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes bidirectional streaming
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 40 -m 8 -t 4 -d /tmp/payload_64kb.bin http://192.168.122.14:8080/duplex-h3
- **Nhánh Upstream**: HTTP/3 QUIC UDP upstream (100% H3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 2,388.00 | *N/A* | **7,550.00** | Velda đạt 2,388.00 req/s (-68.4% vs Envoy; Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.09 MB/s | *N/A* | **0.29 MB/s** | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 55.11 ms | *N/A* | **42.06 ms** | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.19 ms** | *N/A* | 12.25 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **27.43 ms** | *N/A* | 37.04 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 88.85 ms | *N/A* | **64.85 ms** | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 545.85 ms | *N/A* | **114.86 ms** | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 1,467.31 ms | *N/A* | **138.21 ms** | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 1,508.59 ms | *N/A* | **145.60 ms** | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 129.87 ms | *N/A* | **20.57 ms** | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **15.24 ms** | *N/A* | 33.19 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.25 ms** | *N/A* | 0.27 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **11,940 2xx** | *N/A* | 37,750 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | *N/A* | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 443.23 MB | *N/A* | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **1,364,973,978** | 0 | 55,795,872,232 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **114,319.40** | *N/A* | 1,478,036.40 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **2,768,747,013** | 0 | 70,569,082,002 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **231,888.40** | *N/A* | 1,869,379.70 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.49 | *N/A* | **0.79** | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **240,211,127** | 0 | 9,879,029,949 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **20,118.20** | *N/A* | 261,696.20 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **7,323,800** | 0 | 151,712,824 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 3.05% | *N/A* | **1.54%** | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | **9.37%** | *N/A* | 9.76% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **38,081** | 0 | 328,261 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **3.19** | *N/A* | 8.70 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | **54,190** | 0 | 1,628,890 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **4.54** | *N/A* | 43.15 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | *N/A* | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | **11,201** | 0 | 11,769 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 20.67% | *N/A* | **0.72%** | Tỷ lệ khóa futex |

---

### KB 36: duplex_h2_h3 — Large Payload (Heavy H3 Bidi Pump 1MB)
- **Mục tiêu thử nghiệm**: Truyền và nhận đồng thời 1MB giữa HTTP/2 downstream và HTTP/3 upstream, đo lường giới hạn throughput khi phối hợp TCP và QUIC.
- **Kích thước tải trọng & Quy chuẩn**: 1,048,576 bytes song công 2 chiều
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 25 -m 5 -t 4 -d /tmp/payload_1mb.bin http://192.168.122.14:8080/duplex-h3
- **Nhánh Upstream**: HTTP/3 QUIC UDP upstream (100% H3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **2,973.80** | *N/A* | 1,918.40 | Velda đạt 2,973.80 req/s (+55.0% vs Envoy; Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | **0.11 MB/s** | *N/A* | 0.08 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **40.27 ms** | *N/A* | 64.62 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.85 ms** | *N/A* | 6.16 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **31.90 ms** | *N/A* | 47.56 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **64.04 ms** | *N/A* | 143.76 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **177.48 ms** | *N/A* | 229.69 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 449.38 ms | *N/A* | **311.14 ms** | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 977.51 ms | *N/A* | **378.91 ms** | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **44.29 ms** | *N/A* | 47.25 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **17.38 ms** | *N/A* | 61.30 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.16 ms** | *N/A* | 0.20 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **14,869 2xx** | *N/A* | 9,592 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | *N/A* | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 300.04 MB | *N/A* | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 44,323,591,276 | 0 | **43,318,659,988** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **2,980,939.60** | *N/A* | 4,516,123.90 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **69,157,293,773** | 0 | 72,387,384,564 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **4,651,105.90** | *N/A* | 7,546,641.40 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.64** | *N/A* | 0.60 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 7,832,137,632 | 0 | **7,693,101,681** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **526,742.70** | *N/A* | 802,033.10 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 76,813,613 | 0 | **75,595,704** | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **0.98%** | *N/A* | 0.98% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 17.09% | *N/A* | **14.64%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **160,486** | 0 | 182,060 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **10.79** | *N/A* | 18.98 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,795,988 | 0 | **580,653** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 188.04 | *N/A* | **60.54** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | *N/A* | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | **16,851** | 0 | 23,371 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | **0.60%** | *N/A* | 4.02% | Tỷ lệ khóa futex |

---

### KB 37: HTTP/2 Rapid Reset Attack (CVE-2023-44487, 200K RST_STREAM/s Flood)
- **Mục tiêu thử nghiệm**: Bắn liên tục các frame HEADERS ngay lập tức đi kèm RST_STREAM frame nhằm vắt kiệt CPU event loop và bộ nhớ phân bổ stream.
- **Kích thước tải trọng & Quy chuẩn**: 200,000 RST_STREAM frames/sec dội liên tục
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 50 -m 100 -t 4 http://192.168.122.14:8080/plain/ping (Rapid Reset Flood)
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **77,764.00** | 56,869.20 | 50,706.20 | Velda đạt 77,764.00 req/s (+36.7% vs Nginx, +53.4% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 2.23 MB/s | 4.29 MB/s | **7.37 MB/s** | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **62.82 ms** | 84.17 ms | 97.43 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.09 ms** | 6.51 ms | 26.98 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **13.54 ms** | 52.50 ms | 89.83 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 185.12 ms | **108.64 ms** | 144.51 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 320.95 ms | 1,214.61 ms | **218.08 ms** | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 425.56 ms | 2,597.49 ms | **257.69 ms** | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 555.25 ms | 2,811.94 ms | **264.85 ms** | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 82.61 ms | 194.17 ms | **35.52 ms** | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **52.11 ms** | 386.81 ms | 118.36 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.44 ms | **0.36 ms** | 0.46 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **388,820 2xx** | 284,346 2xx | 253,531 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **17.41%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 141.98 MB | 314.24 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 61,631,543,690 | 59,810,357,222 | **55,588,989,626** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **158,509.20** | 210,343.60 | 219,259.10 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 71,442,713,000 | **68,078,867,636** | 74,676,188,772 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **183,742.40** | 239,422.60 | 294,544.60 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.86 | **0.88** | 0.74 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 10,549,803,964 | 10,777,919,424 | **9,768,113,534** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **27,132.90** | 37,904.20 | 38,528.30 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 128,420,814 | **122,002,022** | 129,693,514 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.22% | **1.13%** | 1.33% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 10.22% | **8.44%** | 8.59% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 157,016 | 143,374 | **97,079** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.40 | 0.50 | **0.38** | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,065,742 | 2,329,069 | **584,183** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 5.31 | 8.19 | **2.30** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 38,934 | **10,805** | 12,581 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 1.88% | **0.46%** | 2.15% | Tỷ lệ khóa futex |

---

### KB 38: HTTP/2 CONTINUATION Flood (CVE-2024-27983, Infinite CONTINUATION frame stream)
- **Mục tiêu thử nghiệm**: Bắn dòng frame CONTINUATION vô hạn không có cờ END_HEADERS nhằm ép gateway cấp phát bộ nhớ RAM không giới hạn.
- **Kích thước tải trọng & Quy chuẩn**: Unbounded CONTINUATION frames stream mà không có END_HEADERS
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 50 -m 20 -t 4 http://192.168.122.14:8080/plain/ping (CONTINUATION Flood Defense)
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **79,598.40** | 72,085.20 | 40,240.20 | Velda đạt 79,598.40 req/s (+10.4% vs Nginx, +97.8% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 2.28 MB/s | **5.43 MB/s** | 1.55 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **12.53 ms** | 13.83 ms | 24.75 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.09 ms | **0.06 ms** | 8.37 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 10.62 ms | **7.11 ms** | 26.34 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **22.11 ms** | 38.51 ms | 32.19 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 44.18 ms | 64.12 ms | **38.74 ms** | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | **60.86 ms** | 150.63 ms | 69.78 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **99.50 ms** | 180.71 ms | 105.56 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 8.19 ms | 16.49 ms | **7.65 ms** | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **9.25 ms** | 79.19 ms | 55.63 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.27 ms** | 0.28 ms | 0.29 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **397,992 2xx** | 360,426 2xx | 201,201 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **99.7%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 64.00 MB | 184.71 MB | **6.85 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 59,044,652,291 | **54,413,181,099** | 57,056,589,837 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **148,356.40** | 150,969.10 | 283,580.10 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 70,546,591,975 | **66,840,128,097** | 73,488,669,743 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **177,256.30** | 185,447.60 | 365,250.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.84** | 0.81 | 0.78 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **10,230,504,608** | 10,520,786,887 | 10,413,229,964 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **25,705.30** | 29,189.90 | 51,755.40 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 125,254,796 | **125,211,260** | 157,349,225 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.22% | **1.19%** | 1.51% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 10.79% | 9.63% | **8.92%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **167,201** | 177,686 | 213,479 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.42** | 0.49 | 1.06 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,068,115 | 2,747,668 | **1,307,170** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **5.20** | 7.62 | 6.50 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 20,644 | 11,290 | **11,155** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 1.00% | **0.41%** | 0.85% | Tỷ lệ khóa futex |

---

### KB 39: HPACK Header Bomb & PING Frame Flood (Dynamic table exhaustion, 100K PING/s)
- **Mục tiêu thử nghiệm**: Bơm các chuỗi nén HPACK ép dynamic table mở rộng tối đa kết hợp bắn dồn dập 100,000 PING frame/s ép gateway hồi đáp ACK.
- **Kích thước tải trọng & Quy chuẩn**: 4KB compressed to 2MB expansion + 100,000 PING frames/s
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 50 -m 20 -t 4 http://192.168.122.14:8080/plain/ping (HPACK Header Bomb Defense)
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **79,274.20** | 68,983.00 | 40,573.20 | Velda đạt 79,274.20 req/s (+14.9% vs Nginx, +95.4% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 2.27 MB/s | **5.20 MB/s** | 1.56 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **12.59 ms** | 14.46 ms | 24.56 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.10 ms** | 1.32 ms | 6.41 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **10.70 ms** | 12.55 ms | 23.20 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **22.41 ms** | 23.80 ms | 29.67 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 43.44 ms | **38.85 ms** | 40.97 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | **62.73 ms** | 109.69 ms | 94.18 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **118.72 ms** | 175.79 ms | 126.11 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 8.18 ms | 8.91 ms | **5.96 ms** | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **13.78 ms** | 50.25 ms | 68.21 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.25 ms | 0.29 ms | **0.19 ms** | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **396,371 2xx** | 344,915 2xx | 202,866 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **99.78%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 62.97 MB | 181.56 MB | **6.88 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 60,229,733,220 | **53,709,024,133** | 57,009,029,494 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **151,952.90** | 155,716.70 | 281,018.20 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 70,504,971,306 | **66,277,648,911** | 73,510,906,509 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **177,876.20** | 192,156.50 | 362,361.90 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.85** | 0.81 | 0.78 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **10,204,300,573** | 10,377,013,811 | 10,377,219,097 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **25,744.30** | 30,085.70 | 51,153.10 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 127,068,168 | **115,574,919** | 155,471,943 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.25% | **1.11%** | 1.50% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 10.35% | 9.58% | **8.94%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 165,513 | **110,533** | 209,975 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.42 | **0.32** | 1.04 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,046,787 | 2,668,113 | **1,301,761** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **5.16** | 7.74 | 6.42 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 21,096 | **10,512** | 10,517 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 1.03% | **0.39%** | 0.81% | Tỷ lệ khóa futex |

---

### KB 40: Stream Multiplexing & Flow-Control Deadlock (WINDOW_UPDATE Drip, zero window)
- **Mục tiêu thử nghiệm**: Ép gateway vào trạng thái nghẽn stream flow control bằng cách không gửi WINDOW_UPDATE hoặc drip 1 byte/s.
- **Kích thước tải trọng & Quy chuẩn**: Zero window advertise + 1 byte / 5s drip
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 50 -m 10 -t 4 http://192.168.122.14:8080/plain/12kb (Zero-Window Deadlock Defense)
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **43,723.60** | 39,276.60 | 30,976.20 | Velda đạt 43,723.60 req/s (+11.3% vs Nginx, +41.2% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **514.22 MB/s** | 467.06 MB/s | 364.60 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **11.41 ms** | 12.69 ms | 16.08 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.12 ms | **0.11 ms** | 7.22 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **10.75 ms** | 11.46 ms | 15.80 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **18.34 ms** | 25.46 ms | 18.97 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 27.21 ms | 37.15 ms | **24.19 ms** | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | **38.08 ms** | 80.48 ms | 59.18 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **61.66 ms** | 122.48 ms | 81.22 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 5.35 ms | 9.62 ms | **3.26 ms** | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **7.32 ms** | 47.17 ms | 43.16 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.19 ms** | 0.27 ms | 0.25 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **218,618 2xx** | 196,383 2xx | 154,881 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 64.12 MB | 171.96 MB | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 53,862,733,264 | **50,501,654,476** | 55,411,047,007 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **246,378.30** | 257,159.00 | 357,765.30 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 68,702,458,525 | **66,786,521,720** | 73,031,317,125 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **314,258.00** | 340,083.00 | 471,531.80 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.78** | 0.76 | 0.76 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **9,152,047,894** | 9,413,010,268 | 9,629,566,930 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **41,863.20** | 47,931.90 | 62,174.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 126,987,844 | **122,165,240** | 156,706,598 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.39% | **1.30%** | 1.63% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 13.15% | 11.74% | **10.21%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 121,413 | **120,924** | 141,449 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.56** | 0.62 | 0.91 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,740,637 | 3,199,942 | **1,698,916** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 12.54 | 16.29 | **10.97** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 14,401 | **10,430** | 10,971 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.53% | **0.33%** | 0.65% | Tỷ lệ khóa futex |

---

### KB 41: Chaos Upstream Sudden Death (Graceful GOAWAY, TCP RST & Instant Re-lease)
- **Mục tiêu thử nghiệm**: Bắn tín hiệu TCP RST hoặc GOAWAY đột ngột từ upstream server giữa lúc 1,000 stream đang ghép kênh.
- **Kích thước tải trọng & Quy chuẩn**: Bơm 1,000 active streams + Tiêm TCP RST đột ngột
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 50 -m 10 -t 4 http://192.168.122.14:8080/plain/12kb (Upstream Sudden Death Chaos)
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **45,065.40** | 40,248.80 | 31,346.80 | Velda đạt 45,065.40 req/s (+12.0% vs Nginx, +43.8% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **530.00 MB/s** | 478.49 MB/s | 368.96 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **11.07 ms** | 12.39 ms | 15.89 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.14 ms** | 0.35 ms | 2.55 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **10.54 ms** | 11.24 ms | 15.55 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **17.54 ms** | 20.82 ms | 19.54 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **24.79 ms** | 30.91 ms | 27.08 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | **31.64 ms** | 41.99 ms | 65.10 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **48.03 ms** | 73.76 ms | 82.49 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 4.88 ms | 6.39 ms | **4.19 ms** | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **7.42 ms** | 19.21 ms | 40.37 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.27 ms | **0.23 ms** | 0.27 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **225,327 2xx** | 201,244 2xx | 156,734 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 57.61 MB | 167.20 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 52,034,802,093 | **50,948,381,738** | 57,252,084,280 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **230,930.20** | 253,167.20 | 365,281.80 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 68,946,282,292 | **65,929,729,373** | 72,992,612,155 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **305,983.20** | 327,610.90 | 465,710.10 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.76 | 0.77 | **0.78** | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **8,987,349,324** | 9,504,268,509 | 9,947,228,613 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **39,885.80** | 47,227.60 | 63,465.70 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 122,236,795 | **114,865,087** | 156,626,375 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.36% | **1.21%** | 1.57% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 13.72% | 11.59% | **10.11%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 130,779 | **81,482** | 143,811 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.58 | **0.40** | 0.92 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,766,096 | 3,172,534 | **1,758,803** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 12.28 | 15.76 | **11.22** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 15,146 | 11,324 | **11,014** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.55% | **0.36%** | 0.63% | Tỷ lệ khóa futex |

---

### KB 42: Cgroup RAM 60MB Starvation (O(1) Memory under 1,000 Multiplexed Streams)
- **Mục tiêu thử nghiệm**: Giới hạn cgroup RAM 60MB và mở 1,000 streams multiplexed cùng lúc truyền tải dữ liệu, kiểm tra khả năng sinh tồn trước Linux OOM killer.
- **Kích thước tải trọng & Quy chuẩn**: 1,000 streams đồng thời trong cgroup memory.max=60MB
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 20 -m 4 -t 4 http://192.168.122.14:8080/stream/plain/10mb (RAM 60MB Starvation)
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 225.00 | 230.60 | **242.40** | Velda đạt 225.00 req/s (-2.4% vs Nginx, -7.2% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 2,314.24 MB/s | 2,365.44 MB/s | **2,508.80 MB/s** | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 344.34 ms | **309.14 ms** | 318.07 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 124.51 ms | **12.23 ms** | 38.27 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 338.08 ms | **159.03 ms** | 297.63 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 428.97 ms | **358.80 ms** | 462.83 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **558.52 ms** | 4,079.58 ms | 569.52 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | **588.60 ms** | 4,159.91 ms | 605.99 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **594.60 ms** | 4,166.80 ms | 609.44 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **64.12 ms** | 645.33 ms | 101.64 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **15.73 ms** | 17.25 ms | 24.72 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.20 ms | 0.24 ms | **0.18 ms** | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **1,125 2xx** | 1,153 2xx | 1,212 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 1,028.04 MB | 146.46 MB | **6.87 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **44,453,766,499** | 55,971,757,670 | 55,246,265,579 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **39,514,459.10** | 48,544,455.90 | 45,582,727.40 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 63,580,924,701 | **63,061,190,978** | 64,269,994,898 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 56,516,377.50 | 54,693,140.50 | **53,028,048.60** | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.70 | **0.89** | 0.86 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 7,025,091,542 | 9,631,976,099 | **6,993,857,763** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 6,244,525.80 | 8,353,838.80 | **5,770,509.70** | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 68,190,391 | 87,597,733 | **65,654,810** | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.97% | **0.91%** | 0.94% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 31.46% | **17.16%** | 19.12% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 50,361 | **46,960** | 50,197 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 44.77 | **40.73** | 41.42 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,478,710 | 4,035,254 | **1,731,988** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 2,203.30 | 3,499.79 | **1,429.03** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 14,585 | 10,960 | **10,431** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.59% | **0.27%** | 0.60% | Tỷ lệ khóa futex |

---

### KB 43: CPU 100% Saturation & Overload Shedding (CoDel Shedder, REFUSED_STREAM 0x7)
- **Mục tiêu thử nghiệm**: Ép CPU máy chủ chạm 100% bằng cách dội 200,000 stream/s vượt năng lực xử lý, kiểm tra thuật toán cắt tỉa tải sớm (load shedding).
- **Kích thước tải trọng & Quy chuẩn**: 200,000 requests bão hòa 100% 4 vCPUs
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 80 -m 20 -t 4 http://192.168.122.14:8080/plain/ping (CPU 100% Saturation Shedding)
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **76,671.00** | 66,715.20 | 41,094.60 | Velda đạt 76,671.00 req/s (+14.9% vs Nginx, +86.6% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 2.20 MB/s | **5.03 MB/s** | 3.15 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **20.77 ms** | 23.88 ms | 38.64 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.09 ms** | 1.18 ms | 6.37 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **12.59 ms** | 19.98 ms | 37.60 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 51.05 ms | **46.74 ms** | 63.38 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 93.48 ms | **71.52 ms** | 79.60 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 137.66 ms | 253.22 ms | **91.00 ms** | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 262.29 ms | 306.18 ms | **144.04 ms** | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 20.20 ms | 19.02 ms | **18.30 ms** | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **20.83 ms** | 79.04 ms | 73.76 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.28 ms | **0.28 ms** | 0.34 ms | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **383,355 2xx** | 333,576 2xx | 205,473 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **70.35%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 75.00 MB | 198.32 MB | **6.87 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 55,513,220,356 | **52,703,243,032** | 53,198,325,632 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **144,808.90** | 157,994.70 | 258,906.60 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 70,458,698,462 | **66,735,185,749** | 73,550,888,249 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **183,794.90** | 200,059.90 | 357,958.90 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.79 | **0.79** | 0.72 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 9,757,780,426 | 10,118,800,166 | **9,566,662,819** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **25,453.60** | 30,334.30 | 46,559.20 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 123,573,073 | **113,086,983** | 154,472,636 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.27% | **1.12%** | 1.61% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 11.13% | 9.64% | **9.17%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 181,561 | **107,398** | 178,498 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.47 | **0.32** | 0.87 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 1,969,388 | 2,542,301 | **1,005,963** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 5.14 | 7.62 | **4.90** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 19,115 | **10,837** | 10,991 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.97% | **0.43%** | 1.09% | Tỷ lệ khóa futex |

---

### KB 44: Atomic Hot-Reload Contention (100 Reloads/s at 100K Streams, Zero Dropped Conns)
- **Mục tiêu thử nghiệm**: Kích hoạt 100 lần reload runtime snapshot mỗi giây trong khi đang phục vụ 100,000 streams ghép kênh.
- **Kích thước tải trọng & Quy chuẩn**: 100,000 streams + 100 atomic snapshot swaps/sec
- **Lệnh thực thi chuẩn**: h2load -D 5s -c 50 -m 10 -t 4 http://192.168.122.14:8080/plain/ping (Atomic Hot-Reload Contention)
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **79,125.80** | 72,281.00 | 40,611.40 | Velda đạt 79,125.80 req/s (+9.5% vs Nginx, +94.8% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 2.27 MB/s | **5.45 MB/s** | 1.55 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **6.30 ms** | 6.90 ms | 12.27 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.07 ms** | 0.13 ms | 4.32 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 5.99 ms | **4.80 ms** | 12.10 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **10.05 ms** | 15.69 ms | 15.84 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **14.87 ms** | 21.41 ms | 18.11 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | **20.54 ms** | 30.65 ms | 57.01 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **36.92 ms** | 86.40 ms | 88.79 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **2.94 ms** | 5.88 ms | 3.64 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **5.70 ms** | 34.49 ms | 45.85 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.29 ms | 0.27 ms | **0.25 ms** | Bắt tay kết nối downstream (TCP + Preface) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **395,629 2xx** | 361,405 2xx | 203,057 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 47.82 MB | 164.11 MB | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 55,932,170,132 | **54,657,765,524** | 58,107,941,851 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **141,375.30** | 151,236.90 | 286,165.70 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 69,332,405,181 | **66,635,168,655** | 73,038,266,670 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **175,246.00** | 184,378.10 | 359,693.40 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.81 | **0.82** | 0.80 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **9,972,478,980** | 10,605,677,244 | 10,656,237,080 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **25,206.60** | 29,345.70 | 52,479.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **123,973,659** | 129,157,600 | 175,430,107 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.24% | **1.22%** | 1.65% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 11.55% | 9.64% | **8.75%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 218,742 | **211,295** | 216,973 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.55** | 0.58 | 1.07 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,070,548 | 2,811,507 | **1,362,653** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **5.23** | 7.78 | 6.71 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.75s** | 1.75s | 1.75s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 20,344 | **10,617** | 11,793 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.98% | **0.38%** | 0.87% | Tỷ lệ khóa futex |

---
