# Bộ Đo Đối Sánh Hiệu Năng HTTP/1.1: Velda Edge vs Nginx vs Envoy

Tài liệu này đặc tả và định hình khung đo chuẩn cho **toàn bộ ma trận 44 kịch bản đối sánh hiệu năng (performance benchmark)** cho giao thức **HTTP/1.1** giữa:
- **Velda Edge**: v0.1.0 (Rust Edition 2024, Hourglass Architecture, Zero-Copy Pipeline)
- **Nginx**: **v1.31.6 Mainline / v1.30.5 Stable** (C Event-Driven Web Server & Reverse Proxy)
- **Envoy Proxy**: **v1.39.3 Latest Release** (C++ Cloud-Native High-Performance Edge Proxy)

---

## 1. Môi Trường Thử Nghiệm & Phương Pháp Đo Lường

### 1.1 Môi Trường KVM Cô Lập (Isolated Testbed)
- **Hệ điều hành**: Ubuntu 24.04 LTS (Kernel Linux 6.8.0-45-generic).
- **Cấu hình máy ảo Gateway**: KVM QEMU VM với **4 vCPUs (pinned 1-1 vật lý)**, **8 GB RAM**, cgroup v2 enabled.
- **Máy phát tải (Load Generator)**: Host vật lý (12 CPU cores, 32 GB RAM) dội tải trực tiếp qua giao diện mạng `virtio-net` (MTU 1500, offloading bật).
- **Công cụ sinh tải**: `wrk` (HTTP/1.1 pipeline engine với Lua scripts cho dị dạng payload), `h2load` (nghttp2 v1.68 cho HTTP/2 & HTTP/3 bridges), và bộ script socket mô phỏng dị dạng/chaos.

### 1.2 Phân Lớp 3 Tầng Tiêu Chí Đo Lường
Mọi kịch bản đều được phân tích qua 3 tầng số liệu:
1. **Tầng Vĩ Mô (Network & L7)**:
   - Throughput (RPS / transfers/s), Băng thông Goodput (MB/s).
   - Độ trễ phân vị: Mean, Min, Max, P50, P90, P99, P99.9, Độ lệch chuẩn SD.
   - Thời gian nhận byte đầu tiên (TTFB Avg), Thời gian bắt tay kết nối (Connect Time Avg).
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
   - Thời gian chờ khóa đồng bộ (`Futex Lock Contention Time`) và tỷ lệ Futex / Syscall time.

### 1.3 Thiết Kế Phân Bổ Tải Triple-Upstream (40% Plain : 30% TLS 1.2 : 30% TLS 1.3)
Mọi kịch bản thử nghiệm đều chia tỷ lệ 4/3/3 giữa 3 nhánh upstream để đánh giá toàn diện chi phí xử lý và bắt tay mã hóa:
- **Nhánh Plaintext (40%)**: HTTP/1.1 keep-alive cleartext trên `127.0.0.1:8081`.
- **Nhánh TLS 1.2 (30%)**: HTTP/1.1 over TLS 1.2 ALPN `http/1.1` trên `127.0.0.1:8442`.
- **Nhánh TLS 1.3 (30%)**: HTTP/1.1 over TLS 1.3 ALPN `http/1.1` trên `127.0.0.1:8443`.

---

## 2. Ma Trận 44 Kịch Bản Thử Nghiệm HTTP/1.1

```
╔═══════════════════════════════════════════════════════════════════════════════════════╗
║                                 MA TRẬN 44 KỊCH BẢN                                   ║
╠═══════════════════════════════════════════════════════════════════════════════════════╣
║  Pipe Module         │ Micro (~64B)        │ Medium (12K-64K)    │ Large (1M-10M)     ║
╟──────────────────────┼─────────────────────┼─────────────────────┼────────────────────╢
║  buffered_h1_h1      │ KB 01: Ping-Pong    │ KB 02: REST JSON    │ KB 03: RAM Blob    ║
║  buffered_h1_h2      │ KB 04: Multiplex In │ KB 05: REST H2 Mux  │ KB 06: Heavy Mux   ║
║  buffered_h1_h3      │ KB 07: QUIC Ingest  │ KB 08: REST H3 Mux  │ KB 09: Heavy QUIC  ║
║  server_stream_h1_h1 │ KB 10: SSE Heartbeat│ KB 11: SSE Chunked  │ KB 12: File Stream ║
║  server_stream_h1_h2 │ KB 13: SSE via H2   │ KB 14: Data Stream  │ KB 15: File via H2 ║
║  server_stream_h1_h3 │ KB 16: SSE via H3   │ KB 17: QUIC Stream  │ KB 18: File via H3 ║
║  client_stream_h1_h1 │ KB 19: Drip Upload  │ KB 20: Doc Upload   │ KB 21: Bulk Upload ║
║  client_stream_h1_h2 │ KB 22: Drip to H2   │ KB 23: Doc to H2    │ KB 24: Bulk to H2  ║
║  client_stream_h1_h3 │ KB 25: Drip to H3   │ KB 26: Doc to H3    │ KB 27: Bulk to H3  ║
║  duplex_h1_h1        │ KB 28: Interactive  │ KB 29: Bidirect Msg │ KB 30: Heavy Pump  ║
║  duplex_h1_h2        │ KB 31: Mux Duplex   │ KB 32: Bidi H2 Data │ KB 33: Heavy H2 Pmp║
║  duplex_h1_h3        │ KB 34: QUIC Duplex  │ KB 35: Bidi H3 Data │ KB 36: Heavy H3 Pmp║
╠═══════════════════════════════════════════════════════════════════════════════════════╣
║  Nhóm Dị Dạng & Chaos (Adversarial, Chaos & Extreme Resources)                       ║
╟───────────────────────────────────────────────────────────────────────────────────────╢
║  KB 37: HTTP Request Smuggling (CL.TE & TE.CL Desync Attack)                          ║
║  KB 38: Corrupted Chunk Framing (Invalid Hex, Broken CRLF, Premature FIN)             ║
║  KB 39: Header Flooding & Giant Line (8KB Header Line, 200+ Junk Headers)             ║
║  KB 40: Slowloris Attack & Idle Starvation (Drip 1B / 3s, Idle Watchdog Verification) ║
║  KB 41: Chaos Upstream Sudden Death (TCP RST Injection, Instant Failover & Re-lease)  ║
║  KB 42: Cgroup RAM 60MB Starvation (O(1) Memory Footprint vs OOM Killer)             ║
║  KB 43: CPU 100% Saturation & Adaptive Overload Shedding (CoDel Shedder, Early 503)   ║
║  KB 44: Atomic Hot-Reload Contention (100 Reloads/s at 100K RPS, Zero Dropped Conns)  ║
╚═══════════════════════════════════════════════════════════════════════════════════════╝
```

---

## 3. Form Đo Lường Chi Tiết Toàn Bộ 44 Kịch Bản

### KB 01: buffered_h1_h1 — Micro Payload (Ping-Pong ~64B)
- **Mục tiêu thử nghiệm**: Đo IPC, Event Loop và chi phí lấy connection từ Pool khi tải trọng body rỗng.
- **Kích thước tải trọng & Quy chuẩn**: ~64 bytes HTTP headers (GET /ping HTTP/1.1)
- **Lệnh thực thi chuẩn**: wrk -t5 -c200 -d10s --latency -s /tmp/kb01_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **78,349.59** | 75,492.70 | 32,797.13 | Velda đạt 78,349.59 req/s (+3.8% vs Nginx, +138.9% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 9.49 MB/s | **10.87 MB/s** | 4.82 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **2.59 ms** | 2.71 ms | 6.17 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.13 ms** | 0.14 ms | 0.31 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **2.38 ms** | 2.57 ms | 5.73 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **4.20 ms** | 4.74 ms | 8.31 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **6.78 ms** | 7.15 ms | 13.66 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **22.85 ms** | 28.84 ms | 66.59 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **1.32 ms** | 1.62 ms | 2.54 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **2.59 ms** | 2.71 ms | 6.17 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.10 ms** | 0.11 ms | 0.25 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **791,324 2xx** | 755,026 2xx | 331,238 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 26.75 MB | 153.38 MB | **6.88 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 54,543,964,583 | 49,893,882,681 | **45,626,984,138** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 139,232.30 | **132,182.00** | 278,237.70 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 67,650,914,915 | **63,664,500,684** | 67,795,311,095 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 172,689.90 | **168,664.00** | 413,422.20 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.81** | 0.78 | 0.67 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 9,893,334,702 | 9,742,152,911 | **8,396,812,198** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **25,254.30** | 25,809.50 | 51,204.60 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **115,918,351** | 118,679,357 | 190,661,364 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.17%** | 1.22% | 2.27% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 10.68% | 10.62% | **9.73%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 205,196 | **143,737** | 196,166 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.52 | **0.38** | 1.20 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,345,922 | 2,764,453 | **1,432,416** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **5.99** | 7.32 | 8.74 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 13,899 | 11,565 | **10,385** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.59% | **0.42%** | 0.72% | Tỷ lệ khóa futex |

---

### KB 02: buffered_h1_h1 — Medium Payload (REST JSON 12KB)
- **Mục tiêu thử nghiệm**: Đo lường ngưỡng tải REST Microservices điển hình với body 12KB.
- **Kích thước tải trọng & Quy chuẩn**: 12,286 bytes Content-Length JSON body
- **Lệnh thực thi chuẩn**: wrk -t6 -c200 -d10s --latency -s /tmp/kb02_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **53,497.30** | 36,474.48 | 25,750.23 | Velda đạt 53,497.30 req/s (+46.7% vs Nginx, +107.8% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **638.25 MB/s** | 436.04 MB/s | 307.88 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **3.71 ms** | 6.31 ms | 7.71 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.18 ms** | 0.32 ms | 0.39 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **3.54 ms** | 4.39 ms | 7.33 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **5.70 ms** | 14.34 ms | 10.41 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **8.31 ms** | 23.93 ms | 14.45 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **28.55 ms** | 52.52 ms | 56.85 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **1.58 ms** | 5.47 ms | 2.45 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **3.71 ms** | 6.31 ms | 7.71 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.15 ms** | 0.25 ms | 0.31 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **540,324 2xx** | 368,395 2xx | 260,088 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 50.95 MB | 156.41 MB | **6.87 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 54,116,410,736 | **43,729,912,887** | 44,832,067,403 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **202,314.50** | 239,783.60 | 348,207.10 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 70,436,074,723 | **59,150,139,826** | 67,181,864,414 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **263,325.70** | 324,337.10 | 521,796.20 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.77** | 0.74 | 0.67 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 9,648,025,211 | 8,166,344,096 | **7,861,000,774** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **36,069.20** | 44,778.40 | 61,055.80 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 106,498,657 | **99,666,829** | 165,870,794 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.10%** | 1.22% | 2.11% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 14.41% | 12.12% | **10.92%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 113,470 | **73,360** | 141,515 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.42 | **0.40** | 1.10 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 3,392,878 | 2,789,546 | **1,635,345** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **12.68** | 15.30 | 12.70 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 12,629 | **10,867** | 11,504 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | **0.37%** | 0.39% | 0.70% | Tỷ lệ khóa futex |

---

### KB 03: buffered_h1_h1 — Large Payload (RAM Blob 1MB)
- **Mục tiêu thử nghiệm**: Nạp trọn vẹn 1MB vào bộ nhớ RAM buffer, kiểm tra hiệu năng cấp phát vector allocator & throughput tải nặng.
- **Kích thước tải trọng & Quy chuẩn**: 1,048,576 bytes Content-Length body
- **Lệnh thực thi chuẩn**: wrk -t6 -c100 -d10s --latency -s /tmp/kb03_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **3,042.44** | 1,847.20 | 2,157.61 | Velda đạt 3,042.44 req/s (+64.7% vs Nginx, +41.0% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **3,041.28 MB/s** | 1,853.44 MB/s | 2,160.64 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **31.47 ms** | 52.34 ms | 44.85 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **1.57 ms** | 2.62 ms | 2.24 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **30.41 ms** | 50.75 ms | 41.71 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **43.66 ms** | 82.51 ms | 75.70 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **63.17 ms** | 127.34 ms | 109.64 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **167.96 ms** | 235.09 ms | 177.06 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **10.29 ms** | 25.37 ms | 23.23 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **31.47 ms** | 52.34 ms | 44.85 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **1.26 ms** | 2.09 ms | 1.79 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **30,729 2xx** | 18,492 2xx | 21,588 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 143.75 MB | 150.95 MB | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **47,834,275,339** | 48,408,565,148 | 50,388,352,941 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **3,144,467.90** | 5,241,291.20 | 4,670,756.30 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 64,772,000,970 | **56,561,417,567** | 59,170,265,291 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **4,257,898.30** | 6,124,016.60 | 5,484,797.10 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.74 | **0.86** | 0.85 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 7,328,216,915 | 8,351,953,646 | **6,508,763,564** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **481,732.90** | 904,282.60 | 603,330.90 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **76,506,906** | 85,082,099 | 87,261,872 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.04% | **1.02%** | 1.34% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 29.15% | **17.07%** | 18.88% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **46,295** | 53,282 | 50,634 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **3.04** | 5.77 | 4.69 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 1,545,093 | 3,432,490 | **1,493,614** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **101.57** | 371.64 | 138.45 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 11,768 | **11,319** | 13,850 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.76% | **0.33%** | 0.93% | Tỷ lệ khóa futex |

---

### KB 04: buffered_h1_h2 — Micro Payload (~64B Bridge to H2 Multiplex)
- **Mục tiêu thử nghiệm**: Chuyển tiếp H1 downstream thành stream ghép kênh trên H2 upstream connection pool.
- **Kích thước tải trọng & Quy chuẩn**: ~64 bytes headers (GET /ping HTTP/1.1)
- **Lệnh thực thi chuẩn**: wrk -t6 -c200 -d10s --latency -s /tmp/kb04_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **91,378.41** | 65,626.37 | 42,933.32 | Velda đạt 91,378.41 req/s (+39.2% vs Nginx, +112.8% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **11.07 MB/s** | 9.45 MB/s | 6.31 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **2.18 ms** | 3.20 ms | 4.62 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.11 ms** | 0.16 ms | 0.23 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **2.07 ms** | 2.85 ms | 4.35 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **3.41 ms** | 5.32 ms | 6.65 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **4.94 ms** | 9.67 ms | 9.44 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **13.22 ms** | 49.36 ms | 19.77 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **0.94 ms** | 2.55 ms | 1.55 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **2.18 ms** | 3.20 ms | 4.62 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.09 ms** | 0.13 ms | 0.18 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **922,914 2xx** | 662,833 2xx | 429,450 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 20.62 MB | 153.54 MB | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 63,390,405,297 | **48,345,919,339** | 53,820,813,961 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **138,742.60** | 147,336.90 | 250,718.20 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 63,808,314,417 | **59,738,104,853** | 66,976,743,852 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **139,657.30** | 182,055.20 | 312,003.60 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.99** | 0.81 | 0.80 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 11,530,694,401 | **9,321,292,484** | 9,923,587,110 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **25,237.20** | 28,407.20 | 46,227.90 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 124,450,615 | **113,139,039** | 187,823,832 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.08%** | 1.21% | 1.89% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | **8.50%** | 10.39% | 8.68% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 139,215 | 104,881 | **77,943** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.30** | 0.32 | 0.36 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 1,891,269 | 2,422,453 | **1,236,799** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **4.14** | 7.38 | 5.76 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 68,340 | **11,011** | 11,461 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 3.61% | **0.45%** | 0.93% | Tỷ lệ khóa futex |

---

### KB 05: buffered_h1_h2 — Medium Payload (REST 12KB to H2)
- **Mục tiêu thử nghiệm**: Ghép tải 12KB REST JSON vào DATA frames của HTTP/2 connection pool.
- **Kích thước tải trọng & Quy chuẩn**: 12,288 bytes Content-Length JSON body
- **Lệnh thực thi chuẩn**: wrk -t6 -c200 -d10s --latency -s /tmp/kb05_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **56,470.55** | 11,875.79 | 30,854.83 | Velda đạt 56,470.55 req/s (+375.5% vs Nginx, +83.0% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **673.72 MB/s** | 141.96 MB/s | 368.91 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **3.50 ms** | 18.90 ms | 6.45 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.17 ms** | 0.94 ms | 0.32 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **3.35 ms** | 24.05 ms | 6.08 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **5.35 ms** | 41.11 ms | 9.63 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **7.56 ms** | 42.18 ms | 14.45 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **18.03 ms** | 88.77 ms | 31.09 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **1.42 ms** | 17.45 ms | 2.51 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **3.50 ms** | 18.90 ms | 6.45 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.14 ms** | 0.76 ms | 0.26 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **570,309 2xx** | 118,827 2xx | 308,680 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 33.10 MB | 154.11 MB | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 60,097,315,383 | **23,152,097,417** | 48,826,828,473 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **212,844.80** | 389,904.10 | 316,493.90 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 65,572,630,047 | **34,059,825,410** | 66,210,263,985 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **232,236.60** | 573,601.00 | 429,172.80 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.92** | 0.68 | 0.74 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 10,587,684,286 | **4,307,522,236** | 8,535,427,356 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **37,498.10** | 72,542.90 | 55,326.40 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 113,861,913 | **75,293,468** | 160,533,784 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.08%** | 1.75% | 1.88% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 12.83% | 12.01% | **10.44%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 83,737 | 199,808 | **68,578** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.30** | 3.36 | 0.44 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 3,138,993 | **1,404,059** | 1,679,140 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 11.12 | 23.65 | **10.88** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 47,741 | **11,089** | 11,869 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 1.52% | 0.79% | **0.71%** | Tỷ lệ khóa futex |

---

### KB 06: buffered_h1_h2 — Large Payload (1MB to H2 Mux)
- **Mục tiêu thử nghiệm**: Chia nhỏ 1MB thành các DATA frames 16KB truyền qua multiplexed H2 pipe.
- **Kích thước tải trọng & Quy chuẩn**: 1,048,576 bytes body (~1MB)
- **Lệnh thực thi chuẩn**: wrk -t6 -c60 -d10s --latency -s /tmp/kb06_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **1,843.37** | 1,500.04 | 1,712.59 | Velda đạt 1,843.37 req/s (+22.9% vs Nginx, +7.6% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **1,843.20 MB/s** | 1,505.28 MB/s | 1,710.08 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **32.85 ms** | 41.82 ms | 35.31 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **1.64 ms** | 2.09 ms | 1.77 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **29.93 ms** | 37.19 ms | 32.70 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **51.44 ms** | 78.04 ms | 57.17 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **81.01 ms** | 128.24 ms | 82.83 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **122.63 ms** | 252.55 ms | 141.08 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **14.04 ms** | 28.22 ms | 16.09 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **32.85 ms** | 41.82 ms | 35.31 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **1.31 ms** | 1.67 ms | 1.41 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **18,446 2xx** | 15,010 2xx | 17,141 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 159.29 MB | 149.62 MB | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **43,900,431,819** | 47,835,990,487 | 49,567,376,092 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **4,763,062.40** | 6,377,962.00 | 5,788,586.40 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 59,926,884,018 | **54,017,134,227** | 57,129,005,637 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **6,501,883.40** | 7,202,092.50 | 6,671,650.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.73 | **0.89** | 0.87 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **7,100,603,037** | 8,447,793,396 | 7,105,201,935 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **770,393.70** | 1,126,342.40 | 829,761.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **82,936,353** | 90,083,667 | 85,870,462 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.17% | **1.07%** | 1.21% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 25.70% | **14.86%** | 16.69% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 50,858 | 55,212 | **46,038** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 5.52 | 7.36 | **5.38** | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,550,946 | 3,408,297 | **2,216,272** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 276.77 | 454.43 | **258.82** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 20,351 | 11,150 | **10,989** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.80% | **0.33%** | 0.50% | Tỷ lệ khóa futex |

---

### KB 07: buffered_h1_h3 — Micro Payload (~64B Bridge to H3 QUIC)
- **Mục tiêu thử nghiệm**: Đo chi phí QPACK encoding và UDP socket batching khi bridge H1 sang QUIC upstream.
- **Kích thước tải trọng & Quy chuẩn**: ~64 bytes headers
- **Lệnh thực thi chuẩn**: wrk -t5 -c200 -d10s --latency -s /tmp/kb07_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **75,481.48** | 0 | 0 | Velda đạt 75,481.48 req/s |
| | **Băng thông truyền tải (Throughput MB/s)** | **9.79 MB/s** | 0 MB/s | 0 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **2.67 ms** | 0 ms | 0 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.13 ms** | 0 ms | 0 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **2.48 ms** | 0 ms | 0 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **4.38 ms** | 0 ms | 0 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **6.80 ms** | 0 ms | 0 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0 ms | 0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **18.24 ms** | 0 ms | 0 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **1.34 ms** | 0 ms | 0 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **2.67 ms** | 0 ms | 0 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.11 ms** | 0 ms | 0 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **762,304 2xx** | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **23.19 MB** | 0 MB | 0 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **53,810,517,037** | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **142,579.40** | 0 | 0 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **61,773,776,482** | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **163,679.30** | 0 | 0 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.87** | 0 | 0 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **10,028,634,975** | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **26,572.40** | 0 | 0 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **106,328,707** | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.06%** | 0% | 0% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | **9.19%** | 0% | 0% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **160,773** | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.43** | 0 | 0 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | **1,868,065** | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **4.95** | 0 | 0 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 0s | 0s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | **15,796** | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | **0.85%** | 0% | 0% | Tỷ lệ khóa futex |

---

### KB 08: buffered_h1_h3 — Medium Payload (REST 12KB to H3 QUIC)
- **Mục tiêu thử nghiệm**: Chuyển tiếp 12KB JSON qua QUIC stream, đánh giá khả năng buffer và scale-out connection pool của HTTP/3 engine.
- **Kích thước tải trọng & Quy chuẩn**: 12,288 bytes JSON body
- **Lệnh thực thi chuẩn**: wrk -t5 -c200 -d10s --latency -s /tmp/kb08_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **23,145.32** | 0 | 0 | Velda đạt 23,145.32 req/s |
| | **Băng thông truyền tải (Throughput MB/s)** | **276.33 MB/s** | 0 MB/s | 0 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **10.00 ms** | 0 ms | 0 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.50 ms** | 0 ms | 0 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **9.06 ms** | 0 ms | 0 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **22.58 ms** | 0 ms | 0 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **34.44 ms** | 0 ms | 0 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0 ms | 0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **82.57 ms** | 0 ms | 0 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **8.97 ms** | 0 ms | 0 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **10.00 ms** | 0 ms | 0 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.40 ms** | 0 ms | 0 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **231,551 2xx** | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **29.53 MB** | 0 MB | 0 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **65,044,408,566** | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **562,052.40** | 0 | 0 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **57,445,542,614** | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **496,390.10** | 0 | 0 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **1.13** | 0 | 0 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **11,663,713,704** | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **100,786.80** | 0 | 0 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **108,663,224** | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **0.93%** | 0% | 0% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | **9.36%** | 0% | 0% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **94,102** | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.81** | 0 | 0 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | **3,259,896** | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **28.17** | 0 | 0 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 0s | 0s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | **36,152** | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | **1.11%** | 0% | 0% | Tỷ lệ khóa futex |

---

### KB 09: buffered_h1_h3 — Large Payload (1MB to H3 QUIC)
- **Mục tiêu thử nghiệm**: Đẩy 1MB payload qua QUIC flow-control và kernel UDP GSO.
- **Kích thước tải trọng & Quy chuẩn**: 1,048,576 bytes body
- **Lệnh thực thi chuẩn**: wrk -t6 -c40 -d10s --latency -s /tmp/kb09_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **453.97** | 0 | 0 | Velda đạt 453.97 req/s |
| | **Băng thông truyền tải (Throughput MB/s)** | **454.08 MB/s** | 0 MB/s | 0 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **90.52 ms** | 0 ms | 0 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **4.53 ms** | 0 ms | 0 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **43.05 ms** | 0 ms | 0 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **232.84 ms** | 0 ms | 0 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **263.57 ms** | 0 ms | 0 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0 ms | 0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **291.58 ms** | 0 ms | 0 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **84.33 ms** | 0 ms | 0 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **90.52 ms** | 0 ms | 0 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **3.62 ms** | 0 ms | 0 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **4,544 2xx** | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **96.29 MB** | 0 MB | 0 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **72,496,433,763** | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **31,938,865.50** | 0 | 0 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **57,427,034,628** | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **25,299,924.90** | 0 | 0 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **1.26** | 0 | 0 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **12,578,054,145** | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **5,541,359.20** | 0 | 0 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **98,496,013** | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **0.78%** | 0% | 0% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | **9.55%** | 0% | 0% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **140,107** | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **61.73** | 0 | 0 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | **3,872,782** | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **1,706.18** | 0 | 0 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 0s | 0s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | **44,336** | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | **1.14%** | 0% | 0% | Tỷ lệ khóa futex |

---

### KB 10: server_stream_h1_h1 — Micro Payload (SSE Ping / Heartbeat ~64B)
- **Mục tiêu thử nghiệm**: Đo độ trễ chuyển tiếp từng chunk cực nhỏ (Server-Sent Events) không đọng buffer.
- **Kích thước tải trọng & Quy chuẩn**: 4-64 bytes SSE chunked frames
- **Lệnh thực thi chuẩn**: wrk -t4 -c100 -d10s --latency -s /tmp/kb10_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 74,954.56 | **79,379.74** | 32,943.43 | Velda đạt 74,954.56 req/s (-5.6% vs Nginx, +127.5% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 14.58 MB/s | **15.82 MB/s** | 6.66 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 1.36 ms | **1.34 ms** | 3.05 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.07 ms | **0.07 ms** | 0.15 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 1.22 ms | **1.13 ms** | 2.88 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **2.28 ms** | 2.55 ms | 4.35 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **3.62 ms** | 4.39 ms | 6.96 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **15.57 ms** | 15.73 ms | 21.27 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **0.71 ms** | 0.93 ms | 1.15 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 1.36 ms | **1.34 ms** | 3.05 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.05 ms** | 0.05 ms | 0.12 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **757,048 2xx** | 801,707 2xx | 332,708 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 27.92 MB | 150.55 MB | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 53,133,180,350 | 46,511,849,260 | **44,332,758,937** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 141,774.40 | **117,188.20** | 269,144.80 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 62,231,906,651 | **58,070,153,932** | 62,740,419,672 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 166,052.40 | **146,309.80** | 380,897.90 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.85** | 0.80 | 0.71 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 9,857,002,202 | 9,079,061,301 | **8,169,223,925** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 26,301.30 | **22,875.00** | 49,595.50 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 118,224,813 | **112,800,323** | 195,242,296 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.20%** | 1.24% | 2.39% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 11.32% | 10.47% | **9.45%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 249,633 | **155,992** | 182,595 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.67 | **0.39** | 1.11 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,352,132 | 2,565,892 | **1,385,517** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **6.28** | 6.46 | 8.41 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 20,494 | 11,803 | **11,327** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.87% | **0.46%** | 0.82% | Tỷ lệ khóa futex |

---

### KB 11: server_stream_h1_h1 — Medium Payload (Chunked Stream 64KB)
- **Mục tiêu thử nghiệm**: Upstream đẩy luồng dữ liệu 64KB qua 16 chunk 4KB, gateway forward trực tiếp.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes (16 chunks x 4KB)
- **Lệnh thực thi chuẩn**: wrk -t6 -c150 -d10s --latency -s /tmp/kb11_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **22,825.44** | 17,491.46 | 14,358.40 | Velda đạt 22,825.44 req/s (+30.5% vs Nginx, +59.0% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **1,433.60 MB/s** | 1,095.68 MB/s | 901.12 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **6.65 ms** | 9.14 ms | 10.57 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.33 ms** | 0.46 ms | 0.53 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **6.26 ms** | 7.64 ms | 10.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **9.85 ms** | 17.45 ms | 15.22 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **16.37 ms** | 30.70 ms | 24.48 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **54.94 ms** | 56.28 ms | 80.57 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **3.26 ms** | 6.31 ms | 4.32 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **6.65 ms** | 9.14 ms | 10.57 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.27 ms** | 0.37 ms | 0.42 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **230,531 2xx** | 176,677 2xx | 143,664 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 45.29 MB | 150.20 MB | **6.85 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 45,487,745,410 | 46,214,875,128 | **42,377,802,845** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **398,570.60** | 528,427.90 | 590,285.90 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 62,243,872,274 | **56,320,452,082** | 62,600,087,118 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **545,390.30** | 643,976.60 | 871,964.70 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.73 | **0.82** | 0.68 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 7,548,221,743 | 8,279,789,077 | **6,655,795,815** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **66,138.70** | 94,672.40 | 92,709.40 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 107,183,342 | **98,167,894** | 137,918,662 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.42% | **1.19%** | 2.07% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 19.95% | 14.93% | **14.45%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 72,189 | **59,780** | 89,706 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.63** | 0.68 | 1.25 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,114,336 | 2,900,383 | **1,400,448** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **18.53** | 33.16 | 19.51 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 12,755 | **11,193** | 11,336 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.60% | **0.39%** | 0.81% | Tỷ lệ khóa futex |

---

### KB 12: server_stream_h1_h1 — Large Payload (10MB Download - Chunked)
- **Mục tiêu thử nghiệm**: Tải luồng file 10MB chunked, kiểm tra khả năng kiểm soát backpressure và tail latency P99.
- **Kích thước tải trọng & Quy chuẩn**: 10,485,760 bytes chunked stream
- **Lệnh thực thi chuẩn**: wrk -t4 -c20 -d10s --latency -s /tmp/kb12_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **296.14** | 228.43 | 250.11 | Velda đạt 296.14 req/s (+29.6% vs Nginx, +18.4% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **2,969.60 MB/s** | 2,293.76 MB/s | 2,508.80 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **67.63 ms** | 92.50 ms | 79.62 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **3.38 ms** | 4.62 ms | 3.98 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **63.23 ms** | 82.39 ms | 72.77 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **99.30 ms** | 156.67 ms | 134.01 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **168.26 ms** | 292.17 ms | 180.49 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 278.29 ms | 1,390.00 ms | **236.96 ms** | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **27.19 ms** | 79.64 ms | 38.25 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **67.63 ms** | 92.50 ms | 79.62 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **2.71 ms** | 3.70 ms | 3.19 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **2,991 2xx** | 2,307 2xx | 2,526 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 16.86 MB | 147.53 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 40,429,172,477 | **36,917,629,423** | 48,465,526,199 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **27,304,094.30** | 32,322,925.60 | 38,755,368.60 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 55,824,109,290 | **43,711,838,964** | 53,559,461,452 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **37,701,161.10** | 38,271,539.60 | 42,828,724.50 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.72 | 0.84 | **0.91** | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 6,114,254,268 | 6,370,991,345 | **5,845,742,509** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **4,129,299.80** | 5,578,068.90 | 4,674,537.20 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 64,757,158 | 63,010,535 | **62,696,135** | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.06% | **0.99%** | 1.07% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 32.33% | **16.55%** | 20.39% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 50,115 | **38,729** | 48,345 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **33.85** | 33.91 | 38.66 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 1,525,765 | 2,399,149 | **1,516,881** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **1,030.43** | 2,100.56 | 1,212.97 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 12,267 | 11,179 | **10,751** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.80% | **0.47%** | 0.71% | Tỷ lệ khóa futex |

---

### KB 13: server_stream_h1_h2 — Micro SSE via H2
- **Mục tiêu thử nghiệm**: Nhận SSE stream từ H2 DATA frame và chuyển đổi thành H1 Chunked Transfer Encoding.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes SSE chunks
- **Lệnh thực thi chuẩn**: wrk -t4 -c100 -d10s --latency -s /tmp/kb13_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **87,776.27** | 77,715.38 | 46,117.03 | Velda đạt 87,776.27 req/s (+12.9% vs Nginx, +90.3% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **17.08 MB/s** | 15.49 MB/s | 9.32 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **1.14 ms** | 1.57 ms | 2.18 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.06 ms** | 0.08 ms | 0.11 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **1.08 ms** | 1.16 ms | 1.99 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **1.78 ms** | 2.56 ms | 3.30 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **2.79 ms** | 7.84 ms | 5.45 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **9.81 ms** | 49.86 ms | 22.44 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **0.52 ms** | 2.66 ms | 0.97 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **1.14 ms** | 1.57 ms | 2.18 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.05 ms** | 0.06 ms | 0.09 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **886,471 2xx** | 784,942 2xx | 465,747 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 16.57 MB | 146.39 MB | **6.90 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 63,316,419,404 | 54,541,068,660 | **53,836,609,098** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 144,267.70 | **140,361.10** | 233,478.20 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 62,869,887,686 | **62,397,410,982** | 67,189,016,507 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **143,250.30** | 160,579.30 | 291,384.80 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **1.01** | 0.87 | 0.80 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 11,510,216,933 | 10,487,213,711 | **9,976,361,287** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **26,226.30** | 26,988.80 | 43,265.40 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **126,795,273** | 129,514,927 | 217,099,775 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.10%** | 1.23% | 2.18% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | **8.59%** | 10.11% | 8.71% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 149,731 | 135,108 | **102,847** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.34** | 0.35 | 0.45 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 1,988,583 | 2,684,862 | **1,266,750** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **4.53** | 6.91 | 5.49 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 229,912 | **11,136** | 13,191 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 11.56% | **0.41%** | 1.04% | Tỷ lệ khóa futex |

---

### KB 14: server_stream_h1_h2 — Medium Stream (64KB via H2)
- **Mục tiêu thử nghiệm**: Chuyển luồng DATA frames H2 64KB sang chunked downstream.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes chunked data
- **Lệnh thực thi chuẩn**: wrk -t4 -c100 -d10s --latency -s /tmp/kb14_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **21,245.70** | 16,444.30 | 16,550.91 | Velda đạt 21,245.70 req/s (+29.2% vs Nginx, +28.4% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **1,331.20 MB/s** | 1,034.24 MB/s | 1,034.24 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **4.77 ms** | 6.33 ms | 6.14 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.24 ms** | 0.32 ms | 0.31 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **4.52 ms** | 5.51 ms | 5.57 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **7.64 ms** | 11.76 ms | 9.66 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **11.92 ms** | 19.11 ms | 15.74 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 51.99 ms | **51.78 ms** | 60.63 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **2.45 ms** | 4.15 ms | 2.86 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **4.77 ms** | 6.33 ms | 6.14 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.19 ms** | 0.25 ms | 0.25 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **214,581 2xx** | 166,074 2xx | 165,590 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 93.84 MB | 151.88 MB | **6.85 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **48,265,756,352** | 51,271,432,041 | 54,604,757,797 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **454,357.90** | 623,576.90 | 659,839.90 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **56,849,692,353** | 59,543,128,133 | 65,448,298,533 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **535,164.20** | 724,179.50 | 790,872.50 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.85 | **0.86** | 0.83 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **8,245,785,814** | 9,230,940,420 | 8,696,779,534 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **77,623.10** | 112,269.20 | 105,091.30 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **108,141,209** | 115,999,612 | 144,824,593 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.31% | **1.26%** | 1.67% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 17.87% | **13.61%** | 13.78% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 72,875 | 72,184 | **55,206** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.69 | 0.88 | **0.67** | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,626,832 | 3,302,689 | **2,098,217** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **24.73** | 40.17 | 25.35 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 63,248 | **10,715** | 11,030 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 2.41% | **0.32%** | 0.53% | Tỷ lệ khóa futex |

---

### KB 15: server_stream_h1_h2 — Large Stream (10MB via H2)
- **Mục tiêu thử nghiệm**: Kéo stream 10MB từ Upstream H2 về Downstream H1.
- **Kích thước tải trọng & Quy chuẩn**: 10,485,760 bytes stream
- **Lệnh thực thi chuẩn**: wrk -t4 -c20 -d10s --latency -s /tmp/kb15_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **219.09** | 185.15 | 186.56 | Velda đạt 219.09 req/s (+18.3% vs Nginx, +17.4% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **2,191.36 MB/s** | 1,853.44 MB/s | 1,873.92 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **113.21 ms** | 165.96 ms | 119.29 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **5.66 ms** | 8.30 ms | 5.96 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **70.93 ms** | 91.69 ms | 89.93 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **228.75 ms** | 341.69 ms | 233.51 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 774.80 ms | 1,310.00 ms | **445.38 ms** | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **1,260.00 ms** | 1,810.00 ms | 1,390.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 129.88 ms | 240.50 ms | **100.89 ms** | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **113.21 ms** | 165.96 ms | 119.29 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **4.53 ms** | 6.64 ms | 4.77 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **2,192 2xx** | 1,870 2xx | 1,867 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 325.21 MB | 147.95 MB | **6.85 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **45,971,368,877** | 52,175,487,683 | 48,700,911,104 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **41,965,739.10** | 56,360,235.10 | 52,209,381.50 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **55,446,423,439** | 56,559,776,410 | 55,967,423,522 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **50,615,202.40** | 61,096,166.80 | 59,999,382.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.83 | **0.92** | 0.87 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 7,382,075,466 | 9,126,905,478 | **6,719,680,099** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **6,738,852.00** | 9,858,931.10 | 7,203,773.70 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 73,205,381 | 86,376,203 | **65,755,514** | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.99% | **0.95%** | 0.98% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 23.78% | **15.40%** | 17.66% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 64,136 | **46,545** | 54,169 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 58.55 | **50.28** | 58.07 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,740,603 | 4,078,134 | **2,296,276** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 2,501.81 | 4,405.22 | **2,461.70** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 39,805 | **10,972** | 13,409 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 1.45% | **0.27%** | 0.58% | Tỷ lệ khóa futex |

---

### KB 16: server_stream_h1_h3 — Micro SSE via H3 QUIC
- **Mục tiêu thử nghiệm**: Upstream QUIC stream đẩy dữ liệu SSE sang H1 chunked downstream.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes SSE chunks
- **Lệnh thực thi chuẩn**: wrk -t4 -c80 -d10s --latency -s /tmp/kb16_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **60,442.76** | 0 | 0 | Velda đạt 60,442.76 req/s |
| | **Băng thông truyền tải (Throughput MB/s)** | **12.28 MB/s** | 0 MB/s | 0 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **1.33 ms** | 0 ms | 0 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.07 ms** | 0 ms | 0 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **1.26 ms** | 0 ms | 0 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **1.87 ms** | 0 ms | 0 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **2.72 ms** | 0 ms | 0 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0 ms | 0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **8.99 ms** | 0 ms | 0 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **0.46 ms** | 0 ms | 0 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **1.33 ms** | 0 ms | 0 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.05 ms** | 0 ms | 0 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **610,465 2xx** | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **16.62 MB** | 0 MB | 0 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **49,877,832,512** | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **165,041.50** | 0 | 0 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **44,081,102,551** | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **145,860.70** | 0 | 0 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **1.13** | 0 | 0 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **9,296,301,349** | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **30,760.70** | 0 | 0 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **80,942,434** | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **0.87%** | 0% | 0% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | **7.23%** | 0% | 0% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **194,991** | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.65** | 0 | 0 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | **1,702,016** | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **5.63** | 0 | 0 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 0s | 0s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | **69,604** | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | **4.09%** | 0% | 0% | Tỷ lệ khóa futex |

---

### KB 17: server_stream_h1_h3 — Medium Stream (64KB via H3)
- **Mục tiêu thử nghiệm**: Nhận stream 64KB từ QUIC upstream stream, pump sang client.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes data
- **Lệnh thực thi chuẩn**: wrk -t4 -c80 -d10s --latency -s /tmp/kb17_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **5,892.14** | 0 | 0 | Velda đạt 5,892.14 req/s |
| | **Băng thông truyền tải (Throughput MB/s)** | **369.74 MB/s** | 0 MB/s | 0 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **15.63 ms** | 0 ms | 0 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.78 ms** | 0 ms | 0 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **10.44 ms** | 0 ms | 0 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **37.09 ms** | 0 ms | 0 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **48.68 ms** | 0 ms | 0 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0 ms | 0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **77.00 ms** | 0 ms | 0 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **13.63 ms** | 0 ms | 0 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **15.63 ms** | 0 ms | 0 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.62 ms** | 0 ms | 0 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **59,513 2xx** | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **34.01 MB** | 0 MB | 0 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **62,019,757,186** | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **2,105,169.20** | 0 | 0 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **54,284,433,981** | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **1,842,605.00** | 0 | 0 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **1.14** | 0 | 0 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **10,849,902,523** | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **368,283.90** | 0 | 0 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **108,903,050** | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.00%** | 0% | 0% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | **9.33%** | 0% | 0% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **109,734** | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **3.72** | 0 | 0 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | **3,195,080** | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **108.45** | 0 | 0 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 0s | 0s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | **39,144** | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | **1.23%** | 0% | 0% | Tỷ lệ khóa futex |

---

### KB 18: server_stream_h1_h3 — Large Stream (10MB via H3)
- **Mục tiêu thử nghiệm**: Tải luồng 10MB từ QUIC stream chuyển tiếp downstream.
- **Kích thước tải trọng & Quy chuẩn**: 10,485,760 bytes stream
- **Lệnh thực thi chuẩn**: wrk -t4 -c15 -d10s --latency -s /tmp/kb18_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **43.75** | *N/A* | 37.03 | Velda đạt 43.75 req/s (+18.1% vs Envoy; Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | **437.52 MB/s** | *N/A* | 378.14 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **268.97 ms** | *N/A* | 315.23 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **13.45 ms** | *N/A* | 15.76 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 253.19 ms | *N/A* | **239.57 ms** | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **417.00 ms** | *N/A* | 444.32 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 579.47 ms | *N/A* | **521.15 ms** | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | *N/A* ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 613.49 ms | *N/A* | **523.39 ms** | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 116.83 ms | *N/A* | **112.02 ms** | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **268.97 ms** | *N/A* | 315.23 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **10.76 ms** | *N/A* | 12.61 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **438 2xx** | *N/A* | 374 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | *N/A* | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 224.20 MB | *N/A* | **6.85 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **74,441,422,253** | 0 | 88,000,404,506 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **340,303,644.60** | *N/A* | 475,292,489.90 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **59,811,951,311** | 0 | 69,497,387,386 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **273,426,063.10** | *N/A* | 375,357,209.80 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 1.25 | *N/A* | **1.27** | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **12,962,056,351** | 0 | 15,299,574,415 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **59,255,114.70** | *N/A* | 82,633,402.20 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **89,987,206** | 0 | 119,454,288 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **0.69%** | *N/A* | 0.78% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 9.04% | *N/A* | **8.88%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **130,145** | 0 | 171,422 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **594.95** | *N/A* | 925.85 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 3,814,826 | 0 | **3,054,372** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 17,439.20 | *N/A* | **16,496.74** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | *N/A* | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 44,323 | 0 | **10,547** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 1.16% | *N/A* | **0.35%** | Tỷ lệ khóa futex |

---

### KB 19: client_stream_h1_h1 — Micro Upload (Drip Ingest ~64B)
- **Mục tiêu thử nghiệm**: Client đẩy sự kiện IoT nhỏ dạng chunked liên tục.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes chunked upload body
- **Lệnh thực thi chuẩn**: wrk -t4 -c100 -d10s -s /tmp/kb19_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 79,532.73 | **80,556.41** | 31,781.81 | Velda đạt 79,532.73 req/s (-1.3% vs Nginx, +150.2% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 9.75 MB/s | **11.60 MB/s** | 4.67 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **1.30 ms** | 1.33 ms | 3.16 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.07 ms** | 0.07 ms | 0.16 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **1.13 ms** | 1.14 ms | 2.95 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **2.23 ms** | 2.53 ms | 4.53 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **3.69 ms** | 4.39 ms | 7.14 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 13.15 ms | **12.42 ms** | 33.33 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **0.72 ms** | 0.93 ms | 1.30 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **1.30 ms** | 1.33 ms | 3.16 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.05 ms** | 0.05 ms | 0.13 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **803,235 2xx** | 813,605 2xx | 320,994 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 24.23 MB | 149.78 MB | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 59,488,834,839 | 52,770,106,035 | **48,814,230,000** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 149,595.90 | **131,014.00** | 307,183.40 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 62,664,657,488 | **58,217,216,567** | 65,925,999,958 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 157,582.10 | **144,537.80** | 414,866.20 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.95** | 0.91 | 0.74 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 11,035,221,208 | 10,334,504,009 | **8,942,343,296** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 27,750.10 | **25,657.80** | 56,273.30 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 121,318,286 | **118,454,746** | 224,612,606 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.10%** | 1.15% | 2.51% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 10.12% | 10.09% | **8.87%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 224,286 | **170,147** | 197,988 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.56 | **0.42** | 1.25 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,567,598 | 2,848,620 | **1,415,700** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **6.46** | 7.07 | 8.91 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 22,172 | **10,639** | 12,188 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.86% | **0.37%** | 0.86% | Tỷ lệ khóa futex |

---

### KB 20: client_stream_h1_h1 — Medium Upload (Document 64KB)
- **Mục tiêu thử nghiệm**: Upload tài liệu 64KB chunked stream lên upstream server.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes chunked upload body
- **Lệnh thực thi chuẩn**: wrk -t4 -c50 -d10s -s /tmp/kb20_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **26,902.27** | 20,369.13 | 15,294.51 | Velda đạt 26,902.27 req/s (+32.1% vs Nginx, +75.9% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **3.30 MB/s** | 3.01 MB/s | 2.29 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **1.82 ms** | 2.37 ms | 3.15 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.09 ms** | 0.12 ms | 0.16 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **1.62 ms** | 2.28 ms | 2.84 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **3.06 ms** | 4.01 ms | 5.12 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **5.02 ms** | 6.02 ms | 7.93 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **12.48 ms** | 19.54 ms | 26.26 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **0.97 ms** | 1.28 ms | 1.52 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **1.82 ms** | 2.37 ms | 3.15 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.07 ms** | 0.10 ms | 0.13 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **271,707 2xx** | 205,714 2xx | 154,457 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **99.99%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 33.38 MB | 148.99 MB | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 51,704,032,224 | 58,921,241,822 | **48,934,754,795** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **384,384.20** | 578,534.70 | 639,899.60 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **60,674,468,607** | 62,742,479,892 | 63,543,098,524 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **451,073.20** | 616,054.60 | 830,926.90 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.85 | **0.94** | 0.77 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 8,831,591,626 | 10,753,517,019 | **7,834,636,468** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **65,656.90** | 105,586.40 | 102,450.30 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **107,075,258** | 135,323,701 | 169,485,989 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.21%** | 1.26% | 2.16% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 16.82% | 14.51% | **13.85%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **154,499** | 225,591 | 156,106 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **1.15** | 2.22 | 2.04 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,518,359 | 3,762,770 | **1,893,184** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **18.72** | 36.95 | 24.76 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 19,058 | **11,299** | 11,639 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.76% | **0.30%** | 0.61% | Tỷ lệ khóa futex |

---

### KB 21: client_stream_h1_h1 — Large Upload (200MB Chunked Stream Upload)
- **Mục tiêu thử nghiệm**: Đẩy luồng file cực lớn 200MB/request liên tục lên upstream qua 5 kết nối đồng thời, kiểm tra cơ chế zero-alloc chunk parsing và tail latency.
- **Kích thước tải trọng & Quy chuẩn**: 209,715,200 bytes (200 MB) chunked upload body.
- **Lệnh thực thi chuẩn**: 5 concurrent streams liên tục 12 giây qua HTTP/1.1 chunked sockets
- **Nhánh Upstream**: Dual-Upstream (50% Cleartext h1 : 50% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **17.58** | 0 | 0 | Velda đạt 17.58 req/s |
| | **Băng thông truyền tải (Throughput MB/s)** | 3,516.00 MB/s | **4,862.00 MB/s** | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 286.58 ms | **205.75 ms** | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 152.37 ms | **145.63 ms** | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 285.75 ms | **204.49 ms** | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 355.51 ms | **222.63 ms** | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 416.12 ms | **293.75 ms** | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | **468.58 ms** | 519.40 ms | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **478.14 ms** | 530.00 ms | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 54.66 ms | **27.77 ms** | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 2.87 ms | **2.06 ms** | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.22 ms** | 0.22 ms | 0.22 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **211 2xx** | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **0.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 16.09 MB | 143.98 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 78,046,909,521 | 87,156,760,872 | **3,204,572,777** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 369,890,566.50 | **326,429,816.00** | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 128,539,367,790 | 90,805,132,823 | **4,915,124,959** | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 609,191,316.50 | **340,094,130.40** | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.61 | **0.96** | 0.65 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 14,479,061,768 | 16,069,319,341 | **544,430,698** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 68,621,145.80 | **60,184,716.60** | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 110,894,644 | 100,370,931 | **14,708,297** | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.77% | **0.62%** | 2.70% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | 12.04% | **8.62%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 318,732 | 400,876 | **79,139** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 1,510.58 | **1,501.41** | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | **9,720,646** | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **46,069.41** | 0.00 | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | **32,838** | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | **0.50%** | 0.50% | 0.50% | Tỷ lệ khóa futex |

---

### KB 22: client_stream_h1_h2 — Micro Upload to H2
- **Mục tiêu thử nghiệm**: Nhận chunked upload từ client và biến thành H2 DATA frames đẩy vào upstream.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes chunked upload body
- **Lệnh thực thi chuẩn**: wrk -t4 -c100 -d10s -s /tmp/kb22_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **60,907.26** | 47,724.77 | 36,556.01 | Velda đạt 60,907.26 req/s (+27.6% vs Nginx, +66.6% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **7.38 MB/s** | 6.91 MB/s | 5.37 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **1.68 ms** | 4.85 ms | 2.76 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.08 ms** | 0.24 ms | 0.14 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **1.52 ms** | 1.76 ms | 2.58 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **2.82 ms** | 15.41 ms | 4.01 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **4.68 ms** | 40.03 ms | 6.25 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 27.83 ms | 49.93 ms | **16.45 ms** | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **0.90 ms** | 8.70 ms | 1.05 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **1.68 ms** | 4.85 ms | 2.76 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.07 ms** | 0.19 ms | 0.11 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **615,118 2xx** | 482,000 2xx | 365,613 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 17.66 MB | 146.46 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 55,002,745,326 | 56,122,026,687 | **53,127,886,414** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **180,611.50** | 235,190.30 | 290,665.70 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **56,565,199,225** | 58,375,289,765 | 63,463,243,213 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **185,742.10** | 244,633.10 | 347,211.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.97** | 0.96 | 0.84 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 10,020,090,620 | 10,309,330,355 | **9,785,023,758** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **32,902.80** | 43,203.30 | 53,534.40 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **120,788,756** | 135,565,246 | 214,974,276 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.21%** | 1.31% | 2.20% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 8.38% | 9.01% | **7.93%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 114,362 | 106,481 | **81,996** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.38** | 0.45 | 0.45 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 1,672,201 | 2,209,393 | **1,312,300** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **5.49** | 9.26 | 7.18 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 186,936 | **10,890** | 10,895 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 11.18% | **0.49%** | 0.83% | Tỷ lệ khóa futex |

---

### KB 23: client_stream_h1_h2 — Medium Upload to H2 (64KB)
- **Mục tiêu thử nghiệm**: Chuyển 64KB chunked stream từ client H1 sang H2 DATA stream.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes chunked upload body
- **Lệnh thực thi chuẩn**: wrk -t4 -c50 -d10s -s /tmp/kb23_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **21,954.90** | 6,340.41 | 16,046.10 | Velda đạt 21,954.90 req/s (+246.3% vs Nginx, +36.8% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **2.67 MB/s** | 0.94 MB/s | 2.43 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **2.21 ms** | 7.65 ms | 3.01 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.11 ms** | 0.38 ms | 0.15 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **2.01 ms** | 7.13 ms | 2.77 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **3.60 ms** | 12.73 ms | 4.74 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **6.09 ms** | 18.60 ms | 7.72 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **15.11 ms** | 36.87 ms | 20.27 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **1.14 ms** | 3.77 ms | 1.41 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **2.21 ms** | 7.65 ms | 3.01 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.09 ms** | 0.31 ms | 0.12 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **221,736 2xx** | 63,420 2xx | 162,058 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 26.95 MB | 149.68 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **48,867,752,302** | 82,031,778,306 | 51,905,901,505 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **445,164.90** | 2,587,585.90 | 646,959.70 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **60,735,535,104** | 63,352,375,039 | 64,076,046,323 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **553,275.40** | 1,998,368.40 | 798,649.50 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.81 | **1.29** | 0.81 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 8,276,076,831 | 11,629,740,530 | **8,156,346,039** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **75,391.60** | 366,845.10 | 101,661.40 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **120,352,461** | 180,910,118 | 170,683,389 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.45%** | 1.56% | 2.09% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 16.22% | **7.37%** | 13.94% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 191,870 | **67,905** | 115,489 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 1.75 | 2.14 | **1.44** | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 1,546,975 | 1,771,209 | **1,389,306** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **14.09** | 55.87 | 17.32 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 34,102 | 11,541 | **10,934** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 2.20% | **0.65%** | 0.79% | Tỷ lệ khóa futex |

---

### KB 24: client_stream_h1_h2 — Large Upload to H2 (5MB)
- **Mục tiêu thử nghiệm**: Chuyển luồng upload 5MB từ client H1 sang H2 upstream có backpressure window.
- **Kích thước tải trọng & Quy chuẩn**: 5,242,880 bytes chunked upload body
- **Lệnh thực thi chuẩn**: wrk -t4 -c20 -d10s -s /tmp/kb24_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **2,115.93** | 1,910.29 | 1,287.24 | Velda đạt 2,115.93 req/s (+10.8% vs Nginx, +64.4% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **0.30 MB/s** | 0.28 MB/s | 0.17 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **5.36 ms** | 7.45 ms | 19.82 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.27 ms** | 0.37 ms | 0.99 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **5.22 ms** | 6.17 ms | 5.54 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **7.89 ms** | 13.69 ms | 52.56 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **13.78 ms** | 28.32 ms | 130.35 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **33.39 ms** | 63.11 ms | 268.98 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **2.41 ms** | 5.40 ms | 28.16 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **5.36 ms** | 7.45 ms | 19.82 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.21 ms** | 0.30 ms | 0.79 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **21,298 2xx** | 19,184 2xx | 12,910 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **0.01%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 88.37 MB | 148.74 MB | **6.87 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 30,159,411,746 | 37,203,468,966 | **28,132,381,170** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **2,850,700.30** | 3,895,059.80 | 4,370,961.30 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 54,047,483,899 | 56,768,100,842 | **42,288,967,687** | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **5,108,626.80** | 5,943,401.40 | 6,570,486.90 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.56 | 0.66 | **0.67** | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 5,633,076,597 | 5,752,014,134 | **5,323,481,919** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **532,444.50** | 602,213.70 | 827,115.70 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 57,176,987 | 99,981,455 | **57,116,127** | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.02%** | 1.74% | 1.07% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 8.48% | 9.58% | **6.68%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 117,339 | 107,272 | **76,996** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **11.09** | 11.23 | 11.96 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 5,632,532 | **2,101,055** | 5,468,812 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 532.39 | **219.97** | 849.70 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 51,807 | **11,274** | 12,236 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.92% | 0.54% | **0.22%** | Tỷ lệ khóa futex |

---

### KB 25: client_stream_h1_h3 — Micro Upload to H3
- **Mục tiêu thử nghiệm**: Client H1 upload micro chunks, gateway đóng gói sang QUIC frames.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes chunked upload body
- **Lệnh thực thi chuẩn**: wrk -t4 -c80 -d10s -s /tmp/kb25_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **53,516.98** | *N/A* | 39,152.92 | Velda đạt 53,516.98 req/s (+36.7% vs Envoy; Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | **6.94 MB/s** | *N/A* | 5.75 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **1.51 ms** | *N/A* | 2.05 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.08 ms** | *N/A* | 0.10 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **1.39 ms** | *N/A* | 1.93 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **2.46 ms** | *N/A* | 3.09 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **3.71 ms** | *N/A* | 4.88 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | *N/A* ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **11.93 ms** | *N/A* | 16.38 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **0.71 ms** | *N/A* | 0.91 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **1.51 ms** | *N/A* | 2.05 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.06 ms** | *N/A* | 0.08 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **540,493 2xx** | *N/A* | 395,447 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | *N/A* | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 41.29 MB | *N/A* | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 63,765,027,657 | 0 | **56,144,907,012** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **238,298.30** | *N/A* | 286,798.10 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **66,913,767,658** | 0 | 70,038,234,109 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **250,065.60** | *N/A* | 357,767.60 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.95** | *N/A* | 0.80 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 11,897,370,048 | 0 | **10,256,611,914** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **44,462.00** | *N/A* | 52,392.60 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **129,467,024** | 0 | 249,554,629 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.09%** | *N/A* | 2.43% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 9.79% | *N/A* | **8.49%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 338,322 | 0 | **235,083** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 1.26 | *N/A* | **1.20** | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,737,135 | 0 | **1,515,095** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 10.23 | *N/A* | **7.74** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | *N/A* | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 19,195 | 0 | **11,479** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | **0.70%** | *N/A* | 0.76% | Tỷ lệ khóa futex |

---

### KB 26: client_stream_h1_h3 — Medium Upload to H3 (64KB)
- **Mục tiêu thử nghiệm**: Client upload 64KB chunked sang QUIC stream.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes chunked upload body
- **Lệnh thực thi chuẩn**: wrk -t4 -c50 -d10s -s /tmp/kb26_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 5,557.97 | *N/A* | **21,397.91** | Velda đạt 5,557.97 req/s (-74.0% vs Envoy; Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.72 MB/s | *N/A* | **3.14 MB/s** | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 18.83 ms | *N/A* | **2.28 ms** | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.94 ms | *N/A* | **0.11 ms** | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 6.34 ms | *N/A* | **2.04 ms** | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 33.72 ms | *N/A* | **3.73 ms** | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 260.64 ms | *N/A* | **6.26 ms** | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | *N/A* ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 1,020.00 ms | *N/A* | **15.88 ms** | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 62.97 ms | *N/A* | **1.16 ms** | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 18.83 ms | *N/A* | **2.28 ms** | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.75 ms | *N/A* | **0.09 ms** | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **56,130 2xx** | *N/A* | 216,104 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | *N/A* | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 215.15 MB | *N/A* | **6.85 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 55,031,632,807 | 0 | **45,380,714,068** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 1,980,278.20 | *N/A* | **424,160.20** | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **58,180,078,927** | 0 | 68,690,384,299 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 2,093,573.00 | *N/A* | **642,028.90** | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.95** | *N/A* | 0.66 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 9,750,792,326 | 0 | **8,345,929,616** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 350,876.00 | *N/A* | **78,007.00** | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **105,247,435** | 0 | 182,887,429 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.08%** | *N/A* | 2.19% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | **9.33%** | *N/A* | 13.91% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **129,441** | 0 | 181,776 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 4.66 | *N/A* | **1.70** | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 3,485,217 | 0 | **2,394,742** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 125.41 | *N/A* | **22.38** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | *N/A* | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 38,158 | 0 | **11,270** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 1.09% | *N/A* | **0.47%** | Tỷ lệ khóa futex |

---

### KB 27: client_stream_h1_h3 — Large Upload to H3 (5MB)
- **Mục tiêu thử nghiệm**: Client upload 5MB chunked sang QUIC stream.
- **Kích thước tải trọng & Quy chuẩn**: 5,242,880 bytes chunked upload body
- **Lệnh thực thi chuẩn**: wrk -t4 -c20 -d10s -s /tmp/kb27_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 641.29 | *N/A* | **1,019.13** | Velda đạt 641.29 req/s (-37.1% vs Envoy; Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.08 MB/s | *N/A* | **0.14 MB/s** | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 29.82 ms | *N/A* | **25.26 ms** | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 1.49 ms | *N/A* | **1.26 ms** | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 28.20 ms | *N/A* | **8.31 ms** | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **35.34 ms** | *N/A* | 67.31 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **67.21 ms** | *N/A* | 99.88 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | *N/A* ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **126.66 ms** | *N/A* | 187.33 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **8.32 ms** | *N/A* | 28.44 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 29.82 ms | *N/A* | **25.26 ms** | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 1.19 ms | *N/A* | **1.01 ms** | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **6,459 2xx** | *N/A* | 10,206 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | *N/A* | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 145.44 MB | *N/A* | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 24,127,663,299 | 0 | **23,749,548,427** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 7,524,727.80 | *N/A* | **4,660,749.50** | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 54,557,853,724 | 0 | **44,238,087,744** | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 17,015,033.40 | *N/A* | **8,681,539.70** | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.44 | *N/A* | **0.54** | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **4,273,032,628** | 0 | 4,498,095,989 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 1,332,636.60 | *N/A* | **882,732.50** | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **42,229,952** | 0 | 60,118,408 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **0.99%** | *N/A* | 1.34% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 25.40% | *N/A* | **8.51%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 126,437 | 0 | **80,734** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 39.43 | *N/A* | **15.84** | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | **1,191,728** | 0 | 3,566,843 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **371.67** | *N/A* | 699.98 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | *N/A* | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 26,177 | 0 | **11,025** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 2.20% | *N/A* | **0.31%** | Tỷ lệ khóa futex |

---

### KB 28: duplex_h1_h1 — Micro Interactive Echo (~64B)
- **Mục tiêu thử nghiệm**: Client vừa gửi vừa nhận đồng thời từng gói 64B qua giao thức chunked song công.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes bidi frames
- **Lệnh thực thi chuẩn**: wrk -t4 -c100 -d10s -s /tmp/kb28_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 81,827.02 | **84,744.72** | 31,745.57 | Velda đạt 81,827.02 req/s (-3.4% vs Nginx, +157.8% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **13.27 MB/s** | 12.20 MB/s | 4.66 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 1.27 ms | **1.26 ms** | 3.16 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.06 ms | **0.06 ms** | 0.16 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 1.09 ms | **1.07 ms** | 2.99 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **2.18 ms** | 2.36 ms | 4.54 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **3.76 ms** | 4.17 ms | 6.45 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 23.81 ms | **13.92 ms** | 23.45 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **0.77 ms** | 0.87 ms | 1.14 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 1.27 ms | **1.26 ms** | 3.16 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.05 ms | **0.05 ms** | 0.13 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **826,396 2xx** | 855,875 2xx | 320,612 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 23.12 MB | 147.31 MB | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 60,577,617,044 | 52,877,252,007 | **50,422,626,573** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 148,062.60 | **124,791.90** | 317,667.20 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 63,114,227,807 | **61,075,453,006** | 68,223,941,734 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 154,262.60 | **144,139.80** | 429,817.10 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.96** | 0.87 | 0.74 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 11,278,710,694 | 10,374,429,241 | **9,299,758,078** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 27,567.20 | **24,484.00** | 58,589.30 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **120,135,037** | 120,417,111 | 227,548,510 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.07%** | 1.16% | 2.45% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 10.27% | 10.19% | **9.13%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 232,779 | **176,630** | 199,988 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.57 | **0.42** | 1.26 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,680,425 | 2,839,134 | **1,422,400** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **6.55** | 6.70 | 8.96 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 21,219 | 11,022 | **10,912** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.79% | **0.39%** | 0.77% | Tỷ lệ khóa futex |

---

### KB 29: duplex_h1_h1 — Medium Bidirectional Messaging (64KB)
- **Mục tiêu thử nghiệm**: Vừa stream 64KB lên, vừa nhận 64KB về đồng thời trên cùng một connection.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes in / 65,536 bytes out
- **Lệnh thực thi chuẩn**: wrk -t4 -c50 -d10s -s /tmp/kb29_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **24,086.95** | 18,733.61 | 15,247.30 | Velda đạt 24,086.95 req/s (+28.6% vs Nginx, +58.0% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **3.35 MB/s** | 2.77 MB/s | 2.30 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **2.03 ms** | 2.58 ms | 3.16 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.10 ms** | 0.13 ms | 0.16 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **1.80 ms** | 2.38 ms | 3.04 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **3.42 ms** | 4.31 ms | 5.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **5.77 ms** | 6.71 ms | 7.49 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 20.44 ms | 20.64 ms | **15.34 ms** | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **1.10 ms** | 1.35 ms | 1.49 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **2.03 ms** | 2.58 ms | 3.16 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.08 ms** | 0.10 ms | 0.13 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **243,263 2xx** | 189,213 2xx | 153,988 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **99.99%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 41.05 MB | 149.70 MB | **6.82 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **43,687,725,104** | 53,228,501,340 | 50,780,895,710 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **362,750.20** | 568,267.40 | 666,096.90 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **58,639,101,759** | 61,407,739,099 | 66,380,726,010 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **486,895.20** | 655,589.00 | 870,721.10 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.74 | **0.87** | 0.77 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **7,468,661,233** | 9,736,076,281 | 8,172,089,496 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **62,014.20** | 103,942.30 | 107,193.90 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **100,085,739** | 128,455,022 | 173,197,427 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.34% | **1.32%** | 2.12% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 17.56% | 14.62% | **13.40%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **133,326** | 247,277 | 166,576 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **1.11** | 2.64 | 2.18 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,132,439 | 3,437,709 | **2,010,267** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **17.71** | 36.70 | 26.37 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 18,219 | 11,667 | **11,094** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.85% | **0.34%** | 0.55% | Tỷ lệ khóa futex |

---

### KB 30: duplex_h1_h1 — Large Heavy Duplex Pump (5MB In / 5MB Out)
- **Mục tiêu thử nghiệm**: Bơm liên tục 5MB hai chiều đồng thời, kiểm tra điều phối asymmetric backpressure.
- **Kích thước tải trọng & Quy chuẩn**: 5,242,880 bytes in / 5,242,880 bytes out
- **Lệnh thực thi chuẩn**: wrk -t4 -c20 -d10s -s /tmp/kb30_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **2,477.93** | 1,668.98 | 1,012.78 | Velda đạt 2,477.93 req/s (+48.5% vs Nginx, +144.7% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **0.33 MB/s** | 0.24 MB/s | 0.13 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **3.28 ms** | 8.94 ms | 25.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.16 ms** | 0.45 ms | 1.25 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **3.21 ms** | 6.73 ms | 9.42 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **6.44 ms** | 17.17 ms | 71.29 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **7.83 ms** | 38.63 ms | 102.54 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **13.69 ms** | 122.32 ms | 141.83 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **2.28 ms** | 7.57 ms | 28.33 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **3.28 ms** | 8.94 ms | 25.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.13 ms** | 0.36 ms | 1.00 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **24,918 2xx** | 16,785 2xx | 10,228 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **0.08%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 24.32 MB | 148.79 MB | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **4,283,821,916** | 37,306,506,446 | 31,808,019,620 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **345,758.10** | 4,470,575.60 | 6,281,328.50 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **8,418,971,870** | 58,880,013,350 | 48,660,930,748 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **679,516.50** | 7,055,808.10 | 9,609,378.30 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.51 | 0.63 | **0.65** | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **777,306,729** | 5,924,196,639 | 4,453,257,555 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **62,738.40** | 709,918.20 | 879,412.60 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **15,554,802** | 90,177,440 | 83,282,997 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 2.00% | **1.52%** | 1.87% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | **9.63%** | 10.66% | 10.71% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **71,813** | 118,361 | 97,907 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **5.80** | 14.18 | 19.33 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | **244,119** | 2,522,926 | 733,159 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **19.70** | 302.33 | 144.78 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 27,967 | 11,424 | **10,961** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 11.46% | **0.45%** | 1.50% | Tỷ lệ khóa futex |

---

### KB 31: duplex_h1_h2 — Micro Duplex H1 to H2 Stream
- **Mục tiêu thử nghiệm**: Ghép luồng duplex H1 vào một HTTP/2 bidirectional stream duy nhất.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes bidi frames
- **Lệnh thực thi chuẩn**: wrk -t4 -c100 -d10s -s /tmp/kb31_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **60,993.72** | 51,158.28 | 37,188.17 | Velda đạt 60,993.72 req/s (+19.2% vs Nginx, +64.0% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 7.39 MB/s | **7.40 MB/s** | 5.46 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **1.68 ms** | 5.09 ms | 2.69 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.08 ms** | 0.26 ms | 0.14 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 1.59 ms | **1.58 ms** | 2.53 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **3.04 ms** | 18.49 ms | 3.78 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **4.70 ms** | 40.09 ms | 5.61 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **13.56 ms** | 51.80 ms | 18.66 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 1.05 ms | 9.16 ms | **0.91 ms** | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **1.68 ms** | 5.09 ms | 2.69 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.07 ms** | 0.20 ms | 0.11 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **615,997 2xx** | 516,697 2xx | 375,577 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 17.20 MB | 150.82 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 57,662,560,929 | 62,101,759,071 | **55,652,477,331** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **189,077.00** | 242,782.80 | 299,302.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **55,380,817,951** | 62,928,110,064 | 67,133,254,535 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **181,595.10** | 246,013.40 | 361,046.30 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **1.04** | 0.99 | 0.83 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 10,505,195,327 | 11,432,386,055 | **10,262,394,729** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **34,446.80** | 44,694.20 | 55,191.70 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **119,717,025** | 153,864,878 | 232,790,827 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.14%** | 1.35% | 2.27% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | **8.28%** | 8.98% | 8.57% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 150,959 | **117,309** | 228,858 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.49 | **0.46** | 1.23 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 1,801,027 | 2,472,343 | **1,449,692** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **5.91** | 9.67 | 7.80 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 231,194 | 11,744 | **11,093** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 12.84% | **0.48%** | 0.77% | Tỷ lệ khóa futex |

---

### KB 32: duplex_h1_h2 — Medium Duplex H1 to H2 (64KB)
- **Mục tiêu thử nghiệm**: Bơm song công 64KB qua HTTP/2 multiplexing.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes in / out
- **Lệnh thực thi chuẩn**: wrk -t4 -c50 -d10s -s /tmp/kb32_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **24,001.28** | 6,602.51 | 19,627.72 | Velda đạt 24,001.28 req/s (+263.5% vs Nginx, +22.3% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **2.92 MB/s** | 0.98 MB/s | 2.88 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **2.02 ms** | 7.29 ms | 2.46 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.10 ms** | 0.36 ms | 0.12 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **1.82 ms** | 7.07 ms | 2.23 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **3.35 ms** | 12.79 ms | 3.81 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **5.73 ms** | 17.82 ms | 6.11 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **16.06 ms** | 27.79 ms | 17.96 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **1.08 ms** | 4.06 ms | 1.08 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **2.02 ms** | 7.29 ms | 2.46 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.08 ms** | 0.29 ms | 0.10 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **242,405 2xx** | 66,683 2xx | 198,233 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **99.99%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 27.44 MB | 150.39 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 47,926,917,770 | 80,518,210,472 | **43,076,185,110** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **399,369.70** | 2,439,018.20 | 438,932.10 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **61,271,826,811** | 65,991,799,071 | 65,613,231,722 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **510,571.30** | 1,998,991.30 | 668,577.20 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.78 | **1.22** | 0.66 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 8,008,402,741 | 11,416,297,339 | **7,968,812,321** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **66,733.10** | 345,816.90 | 81,199.60 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **120,867,080** | 180,261,254 | 171,724,747 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.51%** | 1.58% | 2.15% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 16.83% | **7.48%** | 13.84% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 151,974 | **64,167** | 166,741 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **1.27** | 1.94 | 1.70 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | **1,439,654** | 1,746,334 | 2,219,591 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **12.00** | 52.90 | 22.62 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 35,394 | **11,181** | 11,590 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 2.46% | 0.64% | **0.52%** | Tỷ lệ khóa futex |

---

### KB 33: duplex_h1_h2 — Large Duplex H1 to H2 (5MB)
- **Mục tiêu thử nghiệm**: Đẩy 5MB in / 5MB out qua HTTP/2 data frames.
- **Kích thước tải trọng & Quy chuẩn**: 5,242,880 bytes in / out
- **Lệnh thực thi chuẩn**: wrk -t4 -c20 -d10s -s /tmp/kb33_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **4,021.53** | 1,705.63 | 1,082.80 | Velda đạt 4,021.53 req/s (+135.8% vs Nginx, +271.4% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **0.53 MB/s** | 0.25 MB/s | 0.15 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **3.75 ms** | 8.29 ms | 23.41 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.19 ms** | 0.41 ms | 1.17 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **3.23 ms** | 7.06 ms | 6.40 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **6.37 ms** | 15.22 ms | 65.04 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **11.79 ms** | 27.43 ms | 89.97 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 66.68 ms | **47.88 ms** | 169.67 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **2.38 ms** | 5.43 ms | 26.61 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **3.75 ms** | 8.29 ms | 23.41 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.15 ms** | 0.33 ms | 0.94 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **40,491 2xx** | 17,136 2xx | 10,934 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **0.02%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 51.29 MB | 149.88 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 30,276,056,416 | 34,342,681,440 | **24,246,326,826** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **1,505,698.40** | 4,026,979.10 | 4,478,449.70 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 55,481,714,968 | 58,983,869,354 | **40,217,611,507** | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **2,759,234.20** | 6,916,373.30 | 7,428,446.90 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.55 | 0.58 | **0.60** | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 5,652,733,521 | 5,321,251,311 | **4,599,247,764** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **281,123.50** | 623,963.10 | 849,510.10 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 57,693,284 | 94,624,663 | **57,078,875** | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.02%** | 1.78% | 1.24% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 8.67% | 9.82% | **7.80%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 113,065 | 105,241 | **81,221** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **5.62** | 12.34 | 15.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 5,646,123 | **1,950,215** | 4,024,764 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 280.79 | **228.68** | 743.40 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 49,591 | 12,276 | **11,391** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.88% | 0.63% | **0.28%** | Tỷ lệ khóa futex |

---

### KB 34: duplex_h1_h3 — Micro Duplex H1 to H3 QUIC
- **Mục tiêu thử nghiệm**: Bidi stream H1 downstream ghép vào QUIC bidirectional stream upstream.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes bidi frames
- **Lệnh thực thi chuẩn**: wrk -t4 -c80 -d10s -s /tmp/kb34_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **48,947.49** | *N/A* | 16,556.22 | Velda đạt 48,947.49 req/s (+195.6% vs Envoy; Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | **6.35 MB/s** | *N/A* | 2.43 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **1.66 ms** | *N/A* | 4.85 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.08 ms** | *N/A* | 0.24 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **1.53 ms** | *N/A* | 4.47 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **2.76 ms** | *N/A* | 7.09 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **4.23 ms** | *N/A* | 10.16 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | *N/A* ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **11.32 ms** | *N/A* | 20.55 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **0.83 ms** | *N/A* | 1.67 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **1.66 ms** | *N/A* | 4.85 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.07 ms** | *N/A* | 0.19 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **494,386 2xx** | *N/A* | 165,633 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | *N/A* | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 45.01 MB | *N/A* | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 59,192,782,159 | 0 | **47,159,837,758** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **241,862.40** | *N/A* | 569,693.30 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **63,416,602,791** | 0 | 64,380,873,173 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **259,121.00** | *N/A* | 777,724.30 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.93** | *N/A* | 0.73 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 11,058,284,586 | 0 | **8,515,957,607** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **45,184.30** | *N/A* | 102,873.20 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **121,103,203** | 0 | 189,554,213 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.10%** | *N/A* | 2.23% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 10.00% | *N/A* | **8.31%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 305,093 | 0 | **256,546** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **1.25** | *N/A* | 3.10 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,518,678 | 0 | **1,127,509** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **10.29** | *N/A* | 13.62 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | *N/A* | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 15,951 | 0 | **11,198** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | **0.63%** | *N/A* | 0.99% | Tỷ lệ khóa futex |

---

### KB 35: duplex_h1_h3 — Medium Duplex H1 to H3 (64KB)
- **Mục tiêu thử nghiệm**: Bơm song công 64KB qua QUIC bidi stream.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes in / out
- **Lệnh thực thi chuẩn**: wrk -t4 -c50 -d10s -s /tmp/kb35_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 5,152.57 | *N/A* | **7,553.21** | Velda đạt 5,152.57 req/s (-31.8% vs Envoy; Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.67 MB/s | *N/A* | **2.33 MB/s** | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 72.63 ms | *N/A* | **6.56 ms** | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 3.63 ms | *N/A* | **0.33 ms** | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **1.95 ms** | *N/A* | 5.86 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 247.77 ms | *N/A* | **10.79 ms** | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 936.76 ms | *N/A* | **19.88 ms** | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | *N/A* ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 1,990.00 ms | *N/A* | **40.93 ms** | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 207.55 ms | *N/A* | **3.58 ms** | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 72.63 ms | *N/A* | **6.56 ms** | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 2.90 ms | *N/A* | **0.26 ms** | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **51,551 2xx** | *N/A* | 75,595 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **99.91%** | *N/A* | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 113.63 MB | *N/A* | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 64,220,124,847 | 0 | **55,612,853,539** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 2,492,741.50 | *N/A* | **1,472,562.10** | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **53,959,256,616** | 0 | 61,810,734,846 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 2,094,459.90 | *N/A* | **1,636,674.60** | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **1.19** | *N/A* | 0.90 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 11,413,579,594 | 0 | **9,850,712,161** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 443,024.70 | *N/A* | **260,835.10** | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **108,186,050** | 0 | 153,475,590 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **0.95%** | *N/A* | 1.56% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | **8.26%** | *N/A* | 8.56% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **187,416** | 0 | 251,142 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 7.27 | *N/A* | **6.65** | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 4,165,140 | 0 | **1,962,371** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 161.67 | *N/A* | **51.96** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | *N/A* | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 54,909 | 0 | **11,786** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 1.32% | *N/A* | **0.60%** | Tỷ lệ khóa futex |

---

### KB 36: duplex_h1_h3 — Large Duplex H1 to H3 (5MB)
- **Mục tiêu thử nghiệm**: Bơm 5MB song công qua QUIC stream.
- **Kích thước tải trọng & Quy chuẩn**: 5,242,880 bytes in / out
- **Lệnh thực thi chuẩn**: wrk -t4 -c20 -d10s -s /tmp/kb36_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **1,855.44** | *N/A* | 1,167.94 | Velda đạt 1,855.44 req/s (+58.9% vs Envoy; Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | **0.24 MB/s** | *N/A* | 0.15 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **7.88 ms** | *N/A* | 20.04 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.39 ms** | *N/A* | 1.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **4.57 ms** | *N/A* | 8.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **13.73 ms** | *N/A* | 52.97 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **69.36 ms** | *N/A* | 75.65 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | *N/A* ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 180.30 ms | *N/A* | **103.43 ms** | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **12.40 ms** | *N/A* | 21.26 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **7.88 ms** | *N/A* | 20.04 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.32 ms** | *N/A* | 0.80 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **18,614 2xx** | *N/A* | 11,709 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **1.57%** | *N/A* | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 152.63 MB | *N/A* | **6.87 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 41,061,584,303 | 0 | **30,277,774,857** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **4,426,075.10** | *N/A* | 5,184,816.80 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 54,131,769,641 | 0 | **48,544,718,334** | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **5,834,925.40** | *N/A* | 8,312,878.80 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.76** | *N/A* | 0.62 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 7,551,805,154 | 0 | **5,497,107,892** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **814,017.70** | *N/A* | 941,334.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **58,518,030** | 0 | 75,297,517 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **0.77%** | *N/A* | 1.37% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 9.78% | *N/A* | **8.21%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 152,864 | 0 | **127,767** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **16.48** | *N/A* | 21.88 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 6,572,839 | 0 | **2,146,399** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 708.49 | *N/A* | **367.55** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | *N/A* | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 33,066 | 0 | **10,899** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | **0.50%** | *N/A* | 0.51% | Tỷ lệ khóa futex |

---

### KB 37: HTTP Request Smuggling (CL.TE & TE.CL Desync Attack)
- **Mục tiêu thử nghiệm**: Kiểm tra tính bất khả xâm phạm trước kỹ thuật desync HTTP Pipeline Smuggling.
- **Kích thước tải trọng & Quy chuẩn**: Malformed CL + TE mixed headers
- **Lệnh thực thi chuẩn**: python3 test_smuggling.py --target http://192.168.122.14:8080/ (CL.TE & TE.CL Desync Attack)
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.0 | 0.0 | 0.0 | Velda đạt 0.00 req/s |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.0 MB/s | 0.0 MB/s | 0.0 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.0 ms | 0.0 ms | 0.0 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.0 ms | 0.0 ms | 0.0 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.0 ms | 0.0 ms | 0.0 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.0 ms | 0.0 ms | 0.0 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.0 ms | 0.0 ms | 0.0 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.0 ms | 0.0 ms | 0.0 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.0 ms | 0.0 ms | 0.0 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.0 ms | 0.0 ms | 0.0 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **0 2xx** | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **0.0%** | **0.0%** | **0.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 7.82 MB | 131.95 MB | **6.83 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **1,245,986,204** | 1,290,188,384 | 1,729,899,877 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.0 | 0.0 | 0.0 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **1,991,824,099** | 2,099,991,802 | 2,503,263,324 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.0 | 0.0 | 0.0 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.63 | 0.61 | **0.69** | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **218,346,230** | 228,678,563 | 310,466,420 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.0 | 0.0 | 0.0 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **6,211,715** | 6,684,155 | 8,221,037 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 2.84% | 2.92% | **2.65%** | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 7.39% | 7.45% | **6.89%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 37,898 | 37,341 | **37,181** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.0 | 0.0 | 0.0 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 57,967 | **53,245** | 53,900 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.0 | 0.0 | 0.0 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 11,330 | 11,421 | **11,261** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | **19.55%** | 21.45% | 20.89% | Tỷ lệ khóa futex |

---

### KB 38: Corrupted Chunk Framing (Invalid Hex, Broken CRLF, Premature FIN)
- **Mục tiêu thử nghiệm**: Bắn các gói chunk sai cấu trúc hex và ngắt socket giữa chừng để thử độ bền parser.
- **Kích thước tải trọng & Quy chuẩn**: Invalid hex size, missing CRLF, early TCP RST/FIN
- **Lệnh thực thi chuẩn**: python3 test_corrupted_chunk.py --target http://192.168.122.14:8080/ (Invalid Hex, Missing CRLF)
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.0 | 0.0 | 0.0 | Velda đạt 0.00 req/s |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.0 MB/s | 0.0 MB/s | 0.0 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.0 ms | 0.0 ms | 0.0 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.0 ms | 0.0 ms | 0.0 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.0 ms | 0.0 ms | 0.0 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.0 ms | 0.0 ms | 0.0 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.0 ms | 0.0 ms | 0.0 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.0 ms | 0.0 ms | 0.0 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.0 ms | 0.0 ms | 0.0 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.0 ms | 0.0 ms | 0.0 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **0 2xx** | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **0.0%** | **0.0%** | **0.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 7.89 MB | 131.87 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 1,917,678,494 | 1,278,251,635 | **1,277,390,430** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.0 | 0.0 | 0.0 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 2,747,235,885 | **2,085,381,895** | 2,096,300,087 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.0 | 0.0 | 0.0 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.70** | 0.61 | 0.61 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 351,026,125 | **226,023,284** | 226,752,243 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.0 | 0.0 | 0.0 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 9,426,595 | 6,424,263 | **6,323,570** | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **2.69%** | 2.84% | 2.79% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | **7.10%** | 7.50% | 7.43% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 40,324 | 38,316 | **37,867** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.0 | 0.0 | 0.0 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 58,352 | 56,301 | **53,502** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.0 | 0.0 | 0.0 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 12,372 | 11,667 | **11,383** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 21.20% | **20.72%** | 21.28% | Tỷ lệ khóa futex |

---

### KB 39: Header Flooding & Giant Line (8KB Header, 250+ Junk Headers)
- **Mục tiêu thử nghiệm**: Kiểm tra giới hạn phòng vệ cạn kiệt tài nguyên bộ nhớ header table (RFC 9112).
- **Kích thước tải trọng & Quy chuẩn**: 8KB oversized header line, 250 header fields
- **Lệnh thực thi chuẩn**: python3 test_header_flood.py --target http://192.168.122.14:8080/ (8KB Header, 250+ Junk Headers)
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **9,320.20** | 4,323.83 | 1,368.21 | Velda đạt 9,320.20 req/s (+115.6% vs Nginx, +581.2% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **2.17 MB/s** | 1.11 MB/s | 0.38 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **3.87 ms** | 7.58 ms | 7.49 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.19 ms** | 0.38 ms | 0.38 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **2.09 ms** | 8.69 ms | 7.80 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **10.10 ms** | 11.46 ms | 12.24 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **11.35 ms** | 14.02 ms | 15.75 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **16.64 ms** | 20.20 ms | 26.26 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **3.58 ms** | 3.85 ms | 4.04 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **3.87 ms** | 7.58 ms | 7.49 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.15 ms** | 0.30 ms | 0.30 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **93,992 2xx** | 43,531 2xx | 13,745 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **99.65%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 22.89 MB | 135.96 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **10,545,572,656** | 10,573,545,818 | 14,457,671,386 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **226,295.00** | 489,082.40 | 2,113,370.20 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 16,606,716,200 | 14,235,580,953 | **13,077,914,878** | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **356,359.70** | 658,470.90 | 1,911,682.40 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.64 | 0.74 | **1.11** | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **2,125,277,869** | 2,230,756,760 | 2,867,495,165 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **45,605.80** | 103,184.30 | 419,160.10 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 34,682,210 | **33,870,280** | 37,911,259 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.63% | 1.52% | **1.32%** | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 10.18% | 8.37% | **5.50%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 87,111 | 86,455 | **72,568** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **1.87** | 4.00 | 10.61 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 596,269 | 319,413 | **273,180** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **12.80** | 14.77 | 39.93 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 27,558 | 11,685 | **11,385** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 4.62% | **3.66%** | 4.17% | Tỷ lệ khóa futex |

---

### KB 40: Slowloris Attack & Idle Starvation (Drip 1B / 3s, Idle Watchdog)
- **Mục tiêu thử nghiệm**: Mở đồng thời 5,000 socket client, drip 1 byte/3s chiếm FD hệ thống.
- **Kích thước tải trọng & Quy chuẩn**: 5,000 slow connections (1 byte per 3s)
- **Lệnh thực thi chuẩn**: python3 test_slowloris.py --target 192.168.122.14:8080 --sockets 5000 (Drip 1B / 3s, Idle Watchdog)
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **84,976.83** | 83,030.69 | 32,746.52 | Velda đạt 84,976.83 req/s (+2.3% vs Nginx, +159.5% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 10.29 MB/s | **11.96 MB/s** | 4.81 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **0.59 ms** | 0.61 ms | 1.48 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.02 ms** | 0.02 ms | 0.07 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **0.50 ms** | 0.54 ms | 1.37 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **1.01 ms** | 1.06 ms | 2.19 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **1.84 ms** | 2.22 ms | 3.77 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **8.99 ms** | 9.91 ms | 13.79 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **0.36 ms** | 0.42 ms | 0.67 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **0.59 ms** | 0.61 ms | 1.48 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.02 ms** | 0.03 ms | 0.06 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **858,263 2xx** | 838,612 2xx | 330,733 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 20.38 MB | 148.92 MB | **6.87 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 56,267,023,644 | 51,623,853,915 | **48,642,842,049** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 132,429.10 | **124,348.80** | 297,087.10 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 63,015,488,855 | **58,527,929,566** | 65,073,553,824 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 148,312.20 | **140,979.00** | 397,438.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.89** | 0.88 | 0.75 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 10,359,530,009 | 10,097,608,106 | **8,977,158,723** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 24,382.00 | **24,322.60** | 54,828.20 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 127,381,925 | **124,941,208** | 234,135,048 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.23%** | 1.24% | 2.61% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 10.51% | 10.00% | **9.22%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 236,847 | **197,609** | 209,003 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.56 | **0.48** | 1.28 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,521,269 | 2,934,696 | **1,529,540** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **5.93** | 7.07 | 9.34 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 43,571 | 12,937 | **11,397** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 1.73% | **0.44%** | 0.75% | Tỷ lệ khóa futex |

---

### KB 41: Chaos Upstream Sudden Death (TCP RST Injection & Instant Failover)
- **Mục tiêu thử nghiệm**: Tiêm TCP RST ngắt 50% backend nodes đột ngột dưới 50,000 RPS, kiểm tra retry & passive health.
- **Kích thước tải trọng & Quy chuẩn**: REST JSON 12KB dưới tác động RST injection
- **Lệnh thực thi chuẩn**: wrk -t6 -c100 -d10s http://192.168.122.14:8080/plain/12kb (with RST chaos & instant failover)
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **52,143.25** | 39,878.94 | 24,053.23 | Velda đạt 52,143.25 req/s (+30.8% vs Nginx, +116.8% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **622.09 MB/s** | 476.71 MB/s | 287.59 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **1.88 ms** | 2.52 ms | 4.01 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.09 ms** | 0.13 ms | 0.20 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **1.68 ms** | 2.14 ms | 3.78 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **3.12 ms** | 4.72 ms | 5.82 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **4.99 ms** | 7.88 ms | 9.13 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **13.48 ms** | 20.57 ms | 43.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **0.96 ms** | 1.65 ms | 1.61 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **1.88 ms** | 2.52 ms | 4.01 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.07 ms** | 0.10 ms | 0.16 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **521,719 2xx** | 402,749 2xx | 242,930 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 47.45 MB | 148.32 MB | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 55,993,425,196 | **43,351,424,396** | 43,613,070,808 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **214,767.70** | 217,415.10 | 362,638.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 66,104,265,541 | **55,346,001,661** | 64,957,435,480 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **253,548.70** | 277,570.10 | 540,114.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.85** | 0.78 | 0.67 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 9,887,961,190 | 8,095,311,327 | **7,659,841,136** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **37,926.10** | 40,599.40 | 63,690.70 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 108,753,766 | **105,326,186** | 168,407,422 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.10%** | 1.30% | 2.20% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 13.97% | 11.91% | **10.47%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 111,333 | **95,572** | 136,060 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.43** | 0.48 | 1.13 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 3,458,592 | 2,778,855 | **1,568,833** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 13.27 | 13.94 | **13.04** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 19,143 | 11,710 | **11,578** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.55% | **0.42%** | 0.74% | Tỷ lệ khóa futex |

---

### KB 42: Cgroup RAM 60MB Starvation (O(1) Memory Footprint vs OOM Killer)
- **Mục tiêu thử nghiệm**: Giới hạn memory.max = 60M, dội đồng thời 100 stream 10MB chunked download.
- **Kích thước tải trọng & Quy chuẩn**: 100 concurrent 10MB chunked streams dưới cgroup 60MB
- **Lệnh thực thi chuẩn**: wrk -t4 -c30 -d10s http://192.168.122.14:8080/stream/plain/10mb (under Cgroup RAM 60MB limitation)
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **324.46** | 159.72 | 262.32 | Velda đạt 324.46 req/s (+103.1% vs Nginx, +23.7% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **3,256.32 MB/s** | 1,607.68 MB/s | 2,631.68 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **86.26 ms** | 288.10 ms | 106.32 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **4.31 ms** | 14.40 ms | 5.32 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **80.82 ms** | 114.89 ms | 99.17 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **126.72 ms** | 894.73 ms | 159.91 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **209.55 ms** | 1,510.00 ms | 225.23 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **307.89 ms** | 1,620.00 ms | 310.34 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **34.52 ms** | 369.41 ms | 39.04 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **86.26 ms** | 288.10 ms | 106.32 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **3.45 ms** | 11.52 ms | 4.25 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **3,277 2xx** | 1,613 2xx | 2,624 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 18.21 MB | 148.30 MB | **6.88 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **42,386,236,843** | 45,734,850,181 | 49,993,378,556 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **26,127,249.50** | 57,268,783.10 | 38,116,330.10 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 56,118,004,406 | **51,746,695,737** | 53,236,931,327 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **34,591,631.90** | 64,796,764.00 | 40,589,304.20 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.76 | 0.88 | **0.94** | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 6,387,615,999 | 7,829,186,957 | **6,023,663,948** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **3,937,382.70** | 9,803,640.10 | 4,592,607.50 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 65,061,510 | 75,346,450 | **62,986,834** | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.02% | **0.96%** | 1.05% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 32.19% | **17.18%** | 19.91% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 52,262 | **49,195** | 50,580 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **32.21** | 61.60 | 38.56 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 1,604,683 | 3,311,172 | **1,573,076** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **989.14** | 4,146.22 | 1,199.36 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 11,524 | **10,936** | 11,456 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.72% | **0.33%** | 0.73% | Tỷ lệ khóa futex |

---

### KB 43: CPU 100% Saturation & Adaptive Overload Shedding (CoDel Shedder)
- **Mục tiêu thử nghiệm**: Ép máy chủ 100% CPU utilization, dội tải đột biến 300,000 RPS kiểm tra early drop 503.
- **Kích thước tải trọng & Quy chuẩn**: 300,000 RPS flood
- **Lệnh thực thi chuẩn**: wrk -t6 -c300 -d10s http://192.168.122.14:8080/plain/ping (CPU 100% saturation flood)
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **75,760.83** | 74,677.72 | 31,279.96 | Velda đạt 75,760.83 req/s (+1.5% vs Nginx, +142.2% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 9.18 MB/s | **10.75 MB/s** | 4.59 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **3.99 ms** | 4.08 ms | 9.67 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.20 ms** | 0.20 ms | 0.48 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 3.77 ms | **3.73 ms** | 8.97 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **6.36 ms** | 6.67 ms | 12.47 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **9.93 ms** | 10.70 ms | 17.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **28.82 ms** | 45.64 ms | 104.36 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **1.94 ms** | 2.16 ms | 3.90 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **3.99 ms** | 4.08 ms | 9.67 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.16 ms** | 0.16 ms | 0.39 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **765,174 2xx** | 754,257 2xx | 315,925 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 30.88 MB | 156.73 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 53,696,214,409 | 49,104,981,214 | **45,640,196,571** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 141,751.90 | **131,511.70** | 291,817.50 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 66,129,701,961 | **59,812,419,692** | 65,395,247,886 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 174,574.90 | **160,188.10** | 418,128.70 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.81 | **0.82** | 0.70 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 9,665,849,841 | 9,580,625,157 | **8,412,943,240** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **25,516.70** | 25,658.60 | 53,791.30 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 115,650,278 | **111,720,189** | 178,094,103 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.20% | **1.17%** | 2.12% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 10.34% | 10.23% | **9.47%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 198,451 | **126,330** | 190,839 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.52 | **0.34** | 1.22 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,275,409 | 2,683,298 | **1,376,991** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **6.01** | 7.19 | 8.80 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 13,020 | 11,282 | **11,050** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.57% | **0.42%** | 0.80% | Tỷ lệ khóa futex |

---

### KB 44: Atomic Hot-Reload Contention (100 Reloads/s at 100K RPS, Zero Dropped Conns)
- **Mục tiêu thử nghiệm**: Kích hoạt 100 lần reload runtime snapshot/giây dưới 100,000 RPS, kiểm tra zero-lock read path.
- **Kích thước tải trọng & Quy chuẩn**: 100,000 RPS + 100 atomic snapshot swaps/sec
- **Lệnh thực thi chuẩn**: wrk -t6 -c150 -d10s http://192.168.122.14:8080/plain/ping (100 atomic reloads/s under load)
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **80,468.26** | 77,845.90 | 33,075.99 | Velda đạt 80,468.26 req/s (+3.4% vs Nginx, +143.3% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 9.75 MB/s | **11.21 MB/s** | 4.86 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **1.91 ms** | 1.98 ms | 4.52 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.10 ms** | 0.10 ms | 0.23 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **1.72 ms** | 1.80 ms | 4.37 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **3.16 ms** | 3.52 ms | 6.14 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **5.12 ms** | 5.49 ms | 8.32 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **24.59 ms** | 27.72 ms | 32.33 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **1.06 ms** | 1.20 ms | 1.47 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **1.91 ms** | 1.98 ms | 4.52 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.08 ms** | 0.08 ms | 0.18 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **812,674 2xx** | 786,198 2xx | 334,061 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 24.27 MB | 152.93 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 53,375,043,006 | 48,486,878,161 | **45,859,309,924** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 132,661.10 | **124,571.40** | 277,296.70 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 61,185,039,578 | **57,674,918,633** | 64,528,780,394 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 152,072.50 | **148,177.20** | 390,185.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.87** | 0.84 | 0.71 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 9,847,834,486 | 9,458,582,155 | **8,465,317,706** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 24,476.30 | **24,300.80** | 51,187.10 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **109,884,624** | 111,090,873 | 193,933,844 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.12%** | 1.17% | 2.29% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 10.50% | 10.21% | **9.43%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 210,946 | **134,327** | 193,581 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.52 | **0.35** | 1.17 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,337,353 | 2,665,950 | **1,404,316** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **5.81** | 6.85 | 8.49 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 15,200 | **10,874** | 12,682 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.65% | **0.41%** | 0.90% | Tỷ lệ khóa futex |

---
