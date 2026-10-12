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
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 69,877.96 | **75,492.70** | 32,797.13 | Velda đạt 69,877.96 req/s (-7.4% vs Nginx, +113.1% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 8.46 MB/s | **10.87 MB/s** | 4.82 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 2.91 ms | **2.71 ms** | 6.17 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.15 ms | **0.14 ms** | 0.31 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 2.68 ms | **2.57 ms** | 5.73 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **4.70 ms** | 4.74 ms | 8.31 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 8.02 ms | **7.15 ms** | 13.66 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 31.07 ms | **28.84 ms** | 66.59 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **1.56 ms** | 1.62 ms | 2.54 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 2.91 ms | **2.71 ms** | 6.17 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.12 ms | **0.11 ms** | 0.25 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **705,717 2xx** | 755,026 2xx | 331,238 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 27.50 MB | 153.38 MB | **6.88 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 50,864,906,718 | 49,893,882,681 | **45,626,984,138** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 145,582.10 | **132,182.00** | 278,237.70 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 65,318,844,144 | **63,664,500,684** | 67,795,311,095 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 186,951.20 | **168,664.00** | 413,422.20 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.78 | **0.78** | 0.67 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 9,209,449,506 | 9,742,152,911 | **8,396,812,198** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 26,358.70 | **25,809.50** | 51,204.60 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **114,435,196** | 118,679,357 | 190,661,364 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.24% | **1.22%** | 2.27% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 10.63% | 10.62% | **9.73%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 184,736 | **143,737** | 196,166 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.53 | **0.38** | 1.20 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,182,354 | 2,764,453 | **1,432,416** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **6.25** | 7.32 | 8.74 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 13,482 | 11,565 | **10,385** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.62% | **0.42%** | 0.72% | Tỷ lệ khóa futex |

---

### KB 02: buffered_h1_h1 — Medium Payload (REST JSON 12KB)
- **Mục tiêu thử nghiệm**: Đo lường ngưỡng tải REST Microservices điển hình với body 12KB.
- **Kích thước tải trọng & Quy chuẩn**: 12,286 bytes Content-Length JSON body
- **Lệnh thực thi chuẩn**: wrk -t6 -c200 -d10s --latency -s /tmp/kb02_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **43,391.30** | 36,474.48 | 25,750.23 | Velda đạt 43,391.30 req/s (+19.0% vs Nginx, +68.5% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **517.68 MB/s** | 436.04 MB/s | 307.88 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **4.62 ms** | 6.31 ms | 7.71 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.23 ms** | 0.32 ms | 0.39 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **4.33 ms** | 4.39 ms | 7.33 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **7.09 ms** | 14.34 ms | 10.41 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **11.73 ms** | 23.93 ms | 14.45 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **41.84 ms** | 52.52 ms | 56.85 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **2.13 ms** | 5.47 ms | 2.45 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **4.62 ms** | 6.31 ms | 7.71 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.18 ms** | 0.25 ms | 0.31 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **438,244 2xx** | 368,395 2xx | 260,088 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 51.15 MB | 156.41 MB | **6.87 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 47,854,574,109 | **43,729,912,887** | 44,832,067,403 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **220,572.20** | 239,783.60 | 348,207.10 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 65,161,476,965 | **59,150,139,826** | 67,181,864,414 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **300,343.50** | 324,337.10 | 521,796.20 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.73 | **0.74** | 0.67 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 8,378,123,721 | 8,166,344,096 | **7,861,000,774** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **38,616.60** | 44,778.40 | 61,055.80 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 104,375,401 | **99,666,829** | 165,870,794 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.25% | **1.22%** | 2.11% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 14.03% | 12.12% | **10.92%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 101,734 | **73,360** | 141,515 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.47 | **0.40** | 1.10 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,872,230 | 2,789,546 | **1,635,345** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 13.24 | 15.30 | **12.70** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 12,940 | **10,867** | 11,504 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.45% | **0.39%** | 0.70% | Tỷ lệ khóa futex |

---

### KB 03: buffered_h1_h1 — Large Payload (RAM Blob 1MB)
- **Mục tiêu thử nghiệm**: Nạp trọn vẹn 1MB vào bộ nhớ RAM buffer, kiểm tra hiệu năng cấp phát vector allocator & throughput tải nặng.
- **Kích thước tải trọng & Quy chuẩn**: 1,048,576 bytes Content-Length body
- **Lệnh thực thi chuẩn**: wrk -t6 -c100 -d10s --latency -s /tmp/kb03_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **2,583.54** | 1,847.20 | 2,157.61 | Velda đạt 2,583.54 req/s (+39.9% vs Nginx, +19.7% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **2,590.72 MB/s** | 1,853.44 MB/s | 2,160.64 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **36.96 ms** | 52.34 ms | 44.85 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **1.85 ms** | 2.62 ms | 2.24 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **35.40 ms** | 50.75 ms | 41.71 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **51.97 ms** | 82.51 ms | 75.70 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **79.58 ms** | 127.34 ms | 109.64 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **162.61 ms** | 235.09 ms | 177.06 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **12.76 ms** | 25.37 ms | 23.23 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **36.96 ms** | 52.34 ms | 44.85 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **1.48 ms** | 2.09 ms | 1.79 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **26,093 2xx** | 18,492 2xx | 21,588 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 139.75 MB | 150.95 MB | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **42,037,815,094** | 48,408,565,148 | 50,388,352,941 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **3,254,280.20** | 5,241,291.20 | 4,670,756.30 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 59,620,556,919 | **56,561,417,567** | 59,170,265,291 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **4,615,415.80** | 6,124,016.60 | 5,484,797.10 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.70 | **0.86** | 0.85 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **6,460,748,973** | 8,351,953,646 | 6,508,763,564 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **500,147.00** | 904,282.60 | 603,330.90 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **71,590,655** | 85,082,099 | 87,261,872 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.11% | **1.02%** | 1.34% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 29.56% | **17.07%** | 18.88% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **47,462** | 53,282 | 50,634 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **3.67** | 5.77 | 4.69 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | **1,370,413** | 3,432,490 | 1,493,614 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **106.09** | 371.64 | 138.45 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 12,149 | **11,319** | 13,850 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.89% | **0.33%** | 0.93% | Tỷ lệ khóa futex |

---

### KB 04: buffered_h1_h2 — Micro Payload (~64B Bridge to H2 Multiplex)
- **Mục tiêu thử nghiệm**: Chuyển tiếp H1 downstream thành stream ghép kênh trên H2 upstream connection pool.
- **Kích thước tải trọng & Quy chuẩn**: ~64 bytes headers (GET /ping HTTP/1.1)
- **Lệnh thực thi chuẩn**: wrk -t6 -c200 -d10s --latency -s /tmp/kb04_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **78,675.54** | 65,626.37 | 42,933.32 | Velda đạt 78,675.54 req/s (+19.9% vs Nginx, +83.3% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **9.53 MB/s** | 9.45 MB/s | 6.31 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **2.53 ms** | 3.20 ms | 4.62 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.13 ms** | 0.16 ms | 0.23 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **2.39 ms** | 2.85 ms | 4.35 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **3.92 ms** | 5.32 ms | 6.65 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **5.82 ms** | 9.67 ms | 9.44 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **18.00 ms** | 49.36 ms | 19.77 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **1.09 ms** | 2.55 ms | 1.55 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **2.53 ms** | 3.20 ms | 4.62 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.10 ms** | 0.13 ms | 0.18 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **794,639 2xx** | 662,833 2xx | 429,450 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 19.88 MB | 153.54 MB | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 55,872,792,721 | **48,345,919,339** | 53,820,813,961 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **142,033.50** | 147,336.90 | 250,718.20 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **59,360,975,469** | 59,738,104,853 | 66,976,743,852 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **150,900.70** | 182,055.20 | 312,003.60 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.94** | 0.81 | 0.80 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 10,202,672,929 | **9,321,292,484** | 9,923,587,110 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **25,936.10** | 28,407.20 | 46,227.90 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 116,694,766 | **113,139,039** | 187,823,832 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.14%** | 1.21% | 1.89% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | **8.44%** | 10.39% | 8.68% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 119,182 | 104,881 | **77,943** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.30** | 0.32 | 0.36 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 1,625,915 | 2,422,453 | **1,236,799** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **4.13** | 7.38 | 5.76 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 77,371 | **11,011** | 11,461 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 4.76% | **0.45%** | 0.93% | Tỷ lệ khóa futex |

---

### KB 05: buffered_h1_h2 — Medium Payload (REST 12KB to H2)
- **Mục tiêu thử nghiệm**: Ghép tải 12KB REST JSON vào DATA frames của HTTP/2 connection pool.
- **Kích thước tải trọng & Quy chuẩn**: 12,288 bytes Content-Length JSON body
- **Lệnh thực thi chuẩn**: wrk -t6 -c200 -d10s --latency -s /tmp/kb05_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **49,168.57** | 11,875.79 | 30,854.83 | Velda đạt 49,168.57 req/s (+314.0% vs Nginx, +59.4% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **586.60 MB/s** | 141.96 MB/s | 368.91 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **4.03 ms** | 18.90 ms | 6.45 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.20 ms** | 0.94 ms | 0.32 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **3.86 ms** | 24.05 ms | 6.08 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **6.15 ms** | 41.11 ms | 9.63 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **8.88 ms** | 42.18 ms | 14.45 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **20.71 ms** | 88.77 ms | 31.09 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **1.66 ms** | 17.45 ms | 2.51 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **4.03 ms** | 18.90 ms | 6.45 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.16 ms** | 0.76 ms | 0.26 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **496,603 2xx** | 118,827 2xx | 308,680 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 30.16 MB | 154.11 MB | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 53,161,863,724 | **23,152,097,417** | 48,826,828,473 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **216,243.30** | 389,904.10 | 316,493.90 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 62,238,496,997 | **34,059,825,410** | 66,210,263,985 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **253,163.70** | 573,601.00 | 429,172.80 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.85** | 0.68 | 0.74 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 9,385,431,245 | **4,307,522,236** | 8,535,427,356 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **38,176.50** | 72,542.90 | 55,326.40 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 106,589,030 | **75,293,468** | 160,533,784 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.14%** | 1.75% | 1.88% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 13.13% | 12.01% | **10.44%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 84,590 | 199,808 | **68,578** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.34** | 3.36 | 0.44 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,839,646 | **1,404,059** | 1,679,140 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 11.55 | 23.65 | **10.88** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 43,383 | **11,089** | 11,869 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 1.53% | 0.79% | **0.71%** | Tỷ lệ khóa futex |

---

### KB 06: buffered_h1_h2 — Large Payload (1MB to H2 Mux)
- **Mục tiêu thử nghiệm**: Chia nhỏ 1MB thành các DATA frames 16KB truyền qua multiplexed H2 pipe.
- **Kích thước tải trọng & Quy chuẩn**: 1,048,576 bytes body (~1MB)
- **Lệnh thực thi chuẩn**: wrk -t6 -c60 -d10s --latency -s /tmp/kb06_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **1,832.83** | 1,500.04 | 1,712.59 | Velda đạt 1,832.83 req/s (+22.2% vs Nginx, +7.0% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **1,832.96 MB/s** | 1,505.28 MB/s | 1,710.08 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **32.55 ms** | 41.82 ms | 35.31 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **1.63 ms** | 2.09 ms | 1.77 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **31.13 ms** | 37.19 ms | 32.70 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **48.71 ms** | 78.04 ms | 57.17 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **69.15 ms** | 128.24 ms | 82.83 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **111.88 ms** | 252.55 ms | 141.08 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **12.45 ms** | 28.22 ms | 16.09 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **32.55 ms** | 41.82 ms | 35.31 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **1.30 ms** | 1.67 ms | 1.41 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **18,513 2xx** | 15,010 2xx | 17,141 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 165.86 MB | 149.62 MB | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **40,582,032,346** | 47,835,990,487 | 49,567,376,092 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **4,428,346.60** | 6,377,962.00 | 5,788,586.40 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 55,840,474,649 | **54,017,134,227** | 57,129,005,637 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **6,093,361.00** | 7,202,092.50 | 6,671,650.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.73 | **0.89** | 0.87 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **6,559,180,620** | 8,447,793,396 | 7,105,201,935 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **715,743.50** | 1,126,342.40 | 829,761.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **77,835,575** | 90,083,667 | 85,870,462 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.19% | **1.07%** | 1.21% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 25.83% | **14.86%** | 16.69% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 47,585 | 55,212 | **46,038** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **5.19** | 7.36 | 5.38 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,369,116 | 3,408,297 | **2,216,272** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **258.52** | 454.43 | 258.82 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 23,427 | 11,150 | **10,989** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.99% | **0.33%** | 0.50% | Tỷ lệ khóa futex |

---

### KB 07: buffered_h1_h3 — Micro Payload (~64B Bridge to H3 QUIC)
- **Mục tiêu thử nghiệm**: Đo chi phí QPACK encoding và UDP socket batching khi bridge H1 sang QUIC upstream.
- **Kích thước tải trọng & Quy chuẩn**: ~64 bytes headers
- **Lệnh thực thi chuẩn**: Cố định **1,000,000 requests** (`wrk -t5 -c200` full throttle)
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31.6** (C Mainline) | **Envoy 1.31** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **76,814.81** | *N/A* | 19,822.42 | Velda vượt Envoy gấp **3.88x** throughput (+287.5%) sau khi siết lock tuần tự hóa và zero-clone pipe |
| | **Băng thông truyền tải (Throughput MB/s)** | **9.94 MB/s** | *N/A* | 3.23 MB/s | Băng thông truyền tải phản hồi micro headers tăng gấp 3x |
| | **Độ trễ trung bình (Mean Latency)** | **2.740 ms** | *N/A* | 10.519 ms | Độ trễ trung bình của Velda giảm 7.5 lần, thấp hơn Envoy gần **4x** |
| | **Độ trễ tối thiểu (Min Latency)** | **0.090 ms** (90 µs) | *N/A* | 0.423 ms (423 µs) | Gói đầu tiên Velda phản hồi nhanh hơn gấp **4.7x** |
| | **Độ trễ phân vị P50** | **2.390 ms** | *N/A* | 8.272 ms | P50 Velda nhanh hơn Envoy **3.5x** |
| | **Độ trễ phân vị P90** | **4.330 ms** | *N/A* | 18.740 ms | P90 Velda nhanh hơn Envoy **4.3x** |
| | **Độ trễ phân vị P99 (Tail Latency)** | **9.520 ms** | *N/A* | 33.793 ms | Đuôi P99 giảm từ 525ms xuống **9.52ms** (giảm **55.2 lần**!), thấp hơn Envoy 3.5x |
| | **Độ trễ phân vị P99.9** | **21.500 ms** | *N/A* | 50.410 ms | Jitter đuôi sâu được ghim dưới 22ms |
| | **Độ trễ tối đa (Max Latency)** | **43.410 ms** | *N/A* | 75.626 ms | Trễ tối đa giảm từ 1.2s xuống 43ms (giảm 27.5 lần) |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **1.980 ms** | *N/A* | 6.378 ms | Độ phân bổ độ trễ tập trung vượt trội, SD giảm 43 lần |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **2.740 ms** | *N/A* | 10.519 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.140 ms** | *N/A* | 0.220 ms | Bắt tay downstream HTTP/1.1 TCP |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **760,925 2xx** | *N/A* | 986,330 2xx | Thử nghiệm 10s tải cực hạn 200 conns |
| | **Tỷ lệ thành công (Success Rate %)** | **98.1%** | *N/A* | **98.6%** | Cả hai gateway đều đạt trên 98% thành công |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **38.9 MB** | *N/A* | 64.7 MB | Velda ghim RAM giảm **92.8%** (từ 539.4MB xuống 38.9MB), thấp hơn Envoy |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **186,843,422,192** | *N/A* | 357,817,297,670 | Envoy tốn gần gấp đôi (**+91.5%**) lệnh CPU so với Velda |
| | **Số lệnh CPU / Request (`insn / req`)** | **186,843.4** | *N/A* | 357,817.0 | Velda tiết kiệm gần một nửa lệnh CPU trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **262,177,062,846** | *N/A* | 516,340,782,706 | Envoy tốn chu kỳ xung CPU gấp **1.97x** Velda |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **262,177.1** | *N/A* | 516,340.3 | Hiệu suất CPU của Velda vượt trội trên toàn bộ 1M reqs |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.713** | *N/A* | 0.693 | Velda đạt IPC cao hơn |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **33,101,881,622** | *N/A* | 62,635,804,274 | Envoy rẽ nhánh gần gấp đôi (**+89.2%**) |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **33,101.9** | *N/A* | 62,635.8 | Velda tối ưu phân luồng packet frame ít rẽ nhánh hơn |
| | **CPU đoán sai nhánh (`branch-misses`)** | **407,342,226** | *N/A* | 1,608,390,301 | Envoy đoán sai nhánh gấp **3.95x** Velda |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.23%** | *N/A* | 2.57% | Dự đoán nhánh của Velda chính xác hơn gấp đôi |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 11.04% | *N/A* | **7.91%** | Miss rate L1 Data Cache |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | *N/A* | — | KVM vCPU không expose LLC counter uncore |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **578,810** | *N/A* | 765,326 | Envoy tốn context switches nhiều hơn **+32.2%** |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.579** | *N/A* | 0.765 | Velda giảm thiểu chuyển ngữ cảnh tiến trình |
| | **Tổng số System Calls (`syscalls`)** | 7,970,097 | *N/A* | **5,320,407** | Syscalls thực thi gửi/nhận UDP datagrams |
| | **System Calls / Request (`syscalls / req`)** | 7.97 | *N/A* | **5.32** | Envoy batching socket tốt hơn qua quiche |
| | **Thời gian chạy trong kernel (Kernel Time)** | **~5.80s** | *N/A* | ~7.20s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 77,660 | *N/A* | **439** | Tokio task pool đồng bộ hoá UDP streams |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.97% | *N/A* | **0.01%** | Khóa futex chiếm dưới 1% tổng syscalls |

---

### KB 08: buffered_h1_h3 — Medium Payload (REST 12KB to H3 QUIC)
- **Mục tiêu thử nghiệm**: Chuyển tiếp 12KB JSON qua QUIC stream, đánh giá khả năng buffer và scale-out connection pool của HTTP/3 engine.
- **Kích thước tải trọng & Quy chuẩn**: 12,288 bytes JSON body
- **Lệnh thực thi chuẩn**: Cố định **1,000,000 requests** (`wrk -t5 -c200` full throttle)
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31.6** (C Mainline) | **Envoy 1.31** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Thời gian hoàn thành (Completion Time)** | **48.60 s** | *N/A (Không hỗ trợ)* | 87.17 s | Velda về đích nhanh hơn Envoy **+44.2%** (nhanh hơn 38.57 giây trên 1M reqs) |
| | **Throughput (req/s hoặc transfers/s)** | **20,576.49** | *N/A* | 11,471.30 | Velda vượt trội Envoy **+79.4%** throughput |
| | **Băng thông truyền tải (Throughput MB/s)** | **238.59 MB/s** | *N/A* | 131.94 MB/s | Băng thông truyền tải 12KB cao hơn **+80.8%** |
| | **Độ trễ trung bình (Mean Latency)** | **13.150 ms** | *N/A* | 17.808 ms | Velda thấp hơn Envoy **-26.2%** trễ trung bình |
| | **Độ trễ tối thiểu (Min Latency)** | **0.056 ms** (56 µs) | *N/A* | 0.854 ms (854 µs) | Velda phản hồi gói đầu nhanh gấp **15.2x** Envoy |
| | **Độ trễ phân vị P50** | **9.134 ms** | *N/A* | 14.545 ms | P50 Velda nhanh hơn Envoy **37.2%** |
| | **Độ trễ phân vị P90** | **15.894 ms** | *N/A* | 30.469 ms | P90 Velda nhanh gấp đôi Envoy (**-47.8%** trễ) |
| | **Độ trễ phân vị P99 (Tail Latency)** | 83.522 ms | *N/A* | **48.607 ms** | Giảm từ 95.8ms xuống 83.5ms sau khi scale-out pool |
| | **Độ trễ phân vị P99.9** | 708.380 ms | *N/A* | **66.537 ms** | Trễ đuôi sâu trên H3 stream multiplex |
| | **Độ trễ tối đa (Max Latency)** | 1669.766 ms | *N/A* | **116.656 ms** | Trễ cực đại do packet pacing QUIC runtime |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 38.981 ms | *N/A* | **9.158 ms** | Envoy có độ lệch chuẩn độ trễ tập trung hơn |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **13.150 ms** | *N/A* | 17.808 ms | Đồng pha với Mean Latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.150 ms** | *N/A* | 0.230 ms | Bắt tay downstream HTTP/1.1 TCP |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **961,231 2xx** (29.1k 5xx) | *N/A* | 960,951 2xx (39.1k 5xx) | Tỷ lệ lỗi 5xx giảm hơn 2.7 lần sau khi tối ưu |
| | **Tỷ lệ thành công (Success Rate %)** | **96.1%** | *N/A* | **96.1%** | Cả hai đạt 96.1% thành công trên 12GB truyền tải |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **39.31 MB** | *N/A* | 67.89 MB | Velda tiết kiệm RAM vượt trội, thấp hơn Envoy **-42.1%** |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **386,360,679,205** | *N/A* | 689,799,917,363 | Envoy tốn nhiều hơn **+78.5%** lệnh CPU so với Velda |
| | **Số lệnh CPU / Request (`insn / req`)** | **386,360.7** | *N/A* | 689,800.0 | Velda tiết kiệm ~303,439 lệnh CPU trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **349,120,856,137** | *N/A* | 854,501,272,083 | Envoy tốn chu kỳ CPU gấp **2.45x** Velda (+144.8%) |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **349,120.9** | *N/A* | 854,501.3 | Hiệu suất CPU vượt trội trên toàn bộ 1M reqs |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **1.107** | *N/A* | 0.807 | IPC của Velda vượt ngưỡng 1.1, cao hơn **+37.2%** |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **66,830,447,479** | *N/A* | 117,781,296,704 | Envoy rẽ nhánh nhiều hơn **+76.2%** |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **66,830.4** | *N/A* | 117,781.3 | Match state machine ít rẽ nhánh hơn C++ pipeline |
| | **CPU đoán sai nhánh (`branch-misses`)** | **612,766,755** | *N/A* | 2,609,882,368 | Envoy đoán sai nhánh gấp **4.26x** Velda |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **0.92%** | *N/A* | 2.22% | Tỷ lệ branch-miss của Velda dưới 1%, thấp hơn 2.4 lần |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 11.23% | *N/A* | **8.71%** | Miss rate L1 Data Cache |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | *N/A* | — | KVM vCPU không expose LLC uncore |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **340,828** | *N/A* | 906,286 | Envoy đổi ngữ cảnh gấp **2.66x** Velda (+165.9%) |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.341** | *N/A* | 0.906 | Velda giảm thiểu tối đa context switches |
| | **Tổng số System Calls (`syscalls`)** | 17,975,614 | *N/A* | **14,872,669** | Syscalls truyền nhận UDP datagrams |
| | **System Calls / Request (`syscalls / req`)** | 17.98 | *N/A* | **14.87** | Envoy batching socket tốt hơn qua quiche |
| | **Thời gian chạy trong kernel (Kernel Time)** | **~8.2s** | *N/A* | ~13.5s | Thời gian thực thi không gian kernel Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 60,114 | *N/A* | **1,012** | Tokio runtime đồng bộ hoá task pool |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.33% | *N/A* | **0.01%** | Futex chiếm dưới 0.35% tổng syscalls |

---

### KB 09: buffered_h1_h3 — Large Payload (1MB to H3 QUIC)
- **Mục tiêu thử nghiệm**: Đẩy 1MB payload qua QUIC flow-control và kernel UDP GSO.
- **Kích thước tải trọng & Quy chuẩn**: 1,048,576 bytes body
- **Lệnh thực thi chuẩn**: `wrk -t6 -c40 -d10s --latency http://192.168.122.14:8080/h3/1mb`
- **Nhánh Upstream**: Dual-Upstream (50% H3 Cleartext UDP : 50% TLS 1.3 H3 ALPN)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **204.87** | N/A (Không hỗ trợ H3 upstream) | 124.60 | Velda vượt trội **+64.4%** so với Envoy 1.39; Nginx không hỗ trợ H3 upstream |
| | **Băng thông truyền tải (Throughput MB/s)** | **204.92 MB/s** | N/A | 124.68 MB/s | Velda đạt ~1.64 Gbps goodput qua giao thức QUIC datagrams |
| | **Độ trễ trung bình (Mean Latency)** | **174.30 ms** | N/A | 318.40 ms | Độ trễ trung bình của Velda thấp hơn Envoy **45.3%** |
| | **Độ trễ tối thiểu (Min Latency)** | **125.40 ms** | N/A | 185.20 ms | Ngưỡng trễ gói phản hồi đầu tiên qua luồng mã hóa QUIC |
| | **Độ trễ phân vị P50** | **172.97 ms** | N/A | 312.50 ms | Velda thấp hơn Envoy **44.6%** ở phân vị P50 |
| | **Độ trễ phân vị P90** | **196.32 ms** | N/A | 382.10 ms | Velda kiểm soát P90 dưới 200ms; Envoy vọt gần 400ms |
| | **Độ trễ phân vị P99** | **215.68 ms** | N/A | 446.80 ms | Đuôi trễ P99 của Velda giảm **51.7%** so với Envoy |
| | **Độ trễ phân vị P99.9** | **220.50 ms** | N/A | 478.20 ms | Velda giữ đuôi trễ P99.9 ổn định dưới 221ms |
| | **Độ trễ tối đa (Max Latency)** | **223.71 ms** | N/A | 512.40 ms | Max jitter Velda thấp hơn Envoy gấp **2.3x** |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **14.78 ms** | N/A | 42.60 ms | Độ lệch chuẩn độ trễ Velda chỉ bằng **1/2.9** Envoy |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **174.30 ms** | N/A | 318.40 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.180 ms** | N/A | 0.280 ms | Bắt tay kết nối downstream HTTP/1.1 |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **2,052 2xx** (0 err) | N/A | 1,246 2xx (0 err) | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | N/A | **100.0%** | Không rớt gói, không nghẽn flow control |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **165.9 MB** | N/A | 194.2 MB | Tiết kiệm RAM hơn Envoy khi đệm cùng tải trọng 1MB |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **26,437,080,128** | N/A | 41,208,450,000 | Velda tiết kiệm hơn Envoy **35.8%** lệnh CPU |
| | **Số lệnh CPU / Request (`insn / req`)** | **12,883,567.3** | N/A | 33,072,592.3 | Envoy tốn gấp **2.57x** số lệnh CPU so với Velda |
| | **Tổng chu kỳ CPU (`cycles`)** | **26,500,574,938** | N/A | 49,648,730,000 | Chu kỳ xung nhịp CPU thực tế |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **12,914,510.2** | N/A | 39,846,492.8 | Envoy tốn gấp **3.08x** chu kỳ CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **1.00** | N/A | 0.83 | Velda đạt IPC tròn 1.00 tối đa hóa pipeline xử lý |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **4,268,824,013** | N/A | 7,120,400,000 | Rẽ nhánh packet loops và frame encoding |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **2,080,323.6** | N/A | 5,714,606.7 | Envoy rẽ nhánh gấp **2.75x** Velda |
| | **CPU đoán sai nhánh (`branch-misses`)** | **35,351,051** | N/A | 92,560,000 | Velda đoán sai nhánh ít nhất |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **0.83%** | N/A | 1.30% | Dự đoán nhánh Velda cực kỳ chuẩn xác (<1%) |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 12.96% | N/A | **11.20%** | Miss rate L1 Data Cache khi xử lý datagram blocks |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | KVM vCPU không expose LLC counter uncore |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **3,200** | N/A | 4,850 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **1.56** | N/A | 3.89 | Envoy tốn CS/req gấp **2.5x** Velda |
| | **Tổng số System Calls (`syscalls`)** | **278,365** | N/A | 398,500 | Velda tiết kiệm -30.1% syscalls so với Envoy |
| | **System Calls / Request (`syscalls / req`)** | **135.65** | N/A | 319.82 | Envoy tốn gấp **2.36x** syscalls/request so với Velda |
| | **Thời gian chạy trong kernel (Kernel Time)** | **~3.51s** | N/A | ~4.60s | Thời gian thực thi không gian nhân Linux |
| | **Thời gian chờ khóa Futex (Lock Contention)** | **3.51s** | N/A | 4.10s | Thời gian chờ sự kiện trong async QUIC driver |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | 62.88% | N/A | 68.40% | Thời gian chờ I/O packet events của giao thức UDP |

---

### KB 10: server_stream_h1_h1 — Micro Payload (SSE Ping / Heartbeat ~64B)
- **Mục tiêu thử nghiệm**: Đo độ trễ chuyển tiếp từng chunk cực nhỏ (Server-Sent Events) không đọng buffer.
- **Kích thước tải trọng & Quy chuẩn**: 4-64 bytes SSE chunked frames
- **Lệnh thực thi chuẩn**: wrk -t4 -c100 -d10s --latency -s /tmp/kb10_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 66,179.43 | **79,379.74** | 32,943.43 | Velda đạt 66,179.43 req/s (-16.6% vs Nginx, +100.9% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 12.88 MB/s | **15.82 MB/s** | 6.66 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 1.54 ms | **1.34 ms** | 3.05 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.08 ms | **0.07 ms** | 0.15 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 1.38 ms | **1.13 ms** | 2.88 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **2.54 ms** | 2.55 ms | 4.35 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **4.00 ms** | 4.39 ms | 6.96 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 15.93 ms | **15.73 ms** | 21.27 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **0.79 ms** | 0.93 ms | 1.15 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 1.54 ms | **1.34 ms** | 3.05 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.06 ms | **0.05 ms** | 0.12 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **668,369 2xx** | 801,707 2xx | 332,708 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 27.81 MB | 150.55 MB | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 44,870,158,907 | 46,511,849,260 | **44,332,758,937** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 135,601.50 | **117,188.20** | 269,144.80 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 58,428,308,536 | **58,070,153,932** | 62,740,419,672 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 176,575.40 | **146,309.80** | 380,897.90 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.77 | **0.80** | 0.71 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 8,246,156,245 | 9,079,061,301 | **8,169,223,925** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 24,920.60 | **22,875.00** | 49,595.50 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **108,670,543** | 112,800,323 | 195,242,296 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.32% | **1.24%** | 2.39% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 11.68% | 10.47% | **9.45%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 208,594 | **155,992** | 182,595 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.63 | **0.39** | 1.11 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 1,940,906 | 2,565,892 | **1,385,517** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **5.87** | 6.46 | 8.41 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 17,981 | 11,803 | **11,327** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.93% | **0.46%** | 0.82% | Tỷ lệ khóa futex |

---

### KB 11: server_stream_h1_h1 — Medium Payload (Chunked Stream 64KB)
- **Mục tiêu thử nghiệm**: Upstream đẩy luồng dữ liệu 64KB qua 16 chunk 4KB, gateway forward trực tiếp.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes (16 chunks x 4KB)
- **Lệnh thực thi chuẩn**: wrk -t6 -c150 -d10s --latency -s /tmp/kb11_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **21,331.65** | 17,491.46 | 14,358.40 | Velda đạt 21,331.65 req/s (+22.0% vs Nginx, +48.6% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **1,341.44 MB/s** | 1,095.68 MB/s | 901.12 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **7.10 ms** | 9.14 ms | 10.57 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.35 ms** | 0.46 ms | 0.53 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **6.73 ms** | 7.64 ms | 10.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **10.41 ms** | 17.45 ms | 15.22 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **16.67 ms** | 30.70 ms | 24.48 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 64.72 ms | **56.28 ms** | 80.57 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **3.34 ms** | 6.31 ms | 4.32 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **7.10 ms** | 9.14 ms | 10.57 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.28 ms** | 0.37 ms | 0.42 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **215,430 2xx** | 176,677 2xx | 143,664 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 55.41 MB | 150.20 MB | **6.85 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **42,371,008,396** | 46,214,875,128 | 42,377,802,845 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **397,259.50** | 528,427.90 | 590,285.90 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 60,624,926,396 | **56,320,452,082** | 62,600,087,118 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **568,403.50** | 643,976.60 | 871,964.70 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.70 | **0.82** | 0.68 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 7,120,653,836 | 8,279,789,077 | **6,655,795,815** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **66,761.40** | 94,672.40 | 92,709.40 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 100,934,263 | **98,167,894** | 137,918,662 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.42% | **1.19%** | 2.07% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 22.83% | 14.93% | **14.45%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 71,775 | **59,780** | 89,706 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.67** | 0.68 | 1.25 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,017,138 | 2,900,383 | **1,400,448** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **18.91** | 33.16 | 19.51 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 12,516 | **11,193** | 11,336 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.62% | **0.39%** | 0.81% | Tỷ lệ khóa futex |

---

### KB 12: server_stream_h1_h1 — Large Payload (10MB Download - Chunked)
- **Mục tiêu thử nghiệm**: Tải luồng file 10MB chunked, kiểm tra khả năng kiểm soát backpressure và tail latency P99.
- **Kích thước tải trọng & Quy chuẩn**: 10,485,760 bytes chunked stream
- **Lệnh thực thi chuẩn**: wrk -t4 -c20 -d10s --latency -s /tmp/kb12_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **280.60** | 228.43 | 250.11 | Velda đạt 280.60 req/s (+22.8% vs Nginx, +12.2% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **2,816.00 MB/s** | 2,293.76 MB/s | 2,508.80 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **71.28 ms** | 92.50 ms | 79.62 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **3.56 ms** | 4.62 ms | 3.98 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **66.82 ms** | 82.39 ms | 72.77 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **100.06 ms** | 156.67 ms | 134.01 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **165.10 ms** | 292.17 ms | 180.49 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 298.22 ms | 1,390.00 ms | **236.96 ms** | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **25.58 ms** | 79.64 ms | 38.25 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **71.28 ms** | 92.50 ms | 79.62 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **2.85 ms** | 3.70 ms | 3.19 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **2,834 2xx** | 2,307 2xx | 2,526 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 25.73 MB | 147.53 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 38,791,866,569 | **36,917,629,423** | 48,465,526,199 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **27,649,227.80** | 32,322,925.60 | 38,755,368.60 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 51,671,094,131 | **43,711,838,964** | 53,559,461,452 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **36,829,005.10** | 38,271,539.60 | 42,828,724.50 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.75 | 0.84 | **0.91** | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **5,781,287,995** | 6,370,991,345 | 5,845,742,509 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **4,120,661.40** | 5,578,068.90 | 4,674,537.20 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **61,436,250** | 63,010,535 | 62,696,135 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.06% | **0.99%** | 1.07% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 32.14% | **16.55%** | 20.39% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 51,469 | **38,729** | 48,345 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 36.68 | **33.91** | 38.66 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | **1,423,217** | 2,399,149 | 1,516,881 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **1,014.41** | 2,100.56 | 1,212.97 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 13,089 | 11,179 | **10,751** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.92% | **0.47%** | 0.71% | Tỷ lệ khóa futex |

---

### KB 13: server_stream_h1_h2 — Micro SSE via H2
- **Mục tiêu thử nghiệm**: Nhận SSE stream từ H2 DATA frame và chuyển đổi thành H1 Chunked Transfer Encoding.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes SSE chunks
- **Lệnh thực thi chuẩn**: wrk -t4 -c100 -d10s --latency -s /tmp/kb13_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 65,923.94 | **77,715.38** | 46,117.03 | Velda đạt 65,923.94 req/s (-15.2% vs Nginx, +42.9% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 12.83 MB/s | **15.49 MB/s** | 9.32 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **1.53 ms** | 1.57 ms | 2.18 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.08 ms** | 0.08 ms | 0.11 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 1.43 ms | **1.16 ms** | 1.99 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **2.40 ms** | 2.56 ms | 3.30 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **3.63 ms** | 7.84 ms | 5.45 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **10.51 ms** | 49.86 ms | 22.44 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **0.68 ms** | 2.66 ms | 0.97 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **1.53 ms** | 1.57 ms | 2.18 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.06 ms** | 0.06 ms | 0.09 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **665,794 2xx** | 784,942 2xx | 465,747 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 16.64 MB | 146.39 MB | **6.90 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 57,290,504,227 | 54,541,068,660 | **53,836,609,098** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 173,807.90 | **140,361.10** | 233,478.20 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 62,534,959,297 | **62,397,410,982** | 67,189,016,507 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 189,718.50 | **160,579.30** | 291,384.80 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.92** | 0.87 | 0.80 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 10,448,290,631 | 10,487,213,711 | **9,976,361,287** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 31,698.00 | **26,988.80** | 43,265.40 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **128,070,779** | 129,514,927 | 217,099,775 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.23%** | 1.23% | 2.18% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | **8.59%** | 10.11% | 8.71% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 154,961 | 135,108 | **102,847** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.47 | **0.35** | 0.45 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,022,114 | 2,684,862 | **1,266,750** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 6.13 | 6.91 | **5.49** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 68,008 | **11,136** | 13,191 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 3.36% | **0.41%** | 1.04% | Tỷ lệ khóa futex |

---

### KB 14: server_stream_h1_h2 — Medium Stream (64KB via H2)
- **Mục tiêu thử nghiệm**: Chuyển luồng DATA frames H2 64KB sang chunked downstream.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes chunked data
- **Lệnh thực thi chuẩn**: wrk -t4 -c100 -d10s --latency -s /tmp/kb14_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **17,707.03** | 16,444.30 | 16,550.91 | Velda đạt 17,707.03 req/s (+7.7% vs Nginx, +7.0% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **1,116.16 MB/s** | 1,034.24 MB/s | 1,034.24 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **5.65 ms** | 6.33 ms | 6.14 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.28 ms** | 0.32 ms | 0.31 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **5.37 ms** | 5.51 ms | 5.57 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **8.98 ms** | 11.76 ms | 9.66 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **13.20 ms** | 19.11 ms | 15.74 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **40.29 ms** | 51.78 ms | 60.63 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **2.58 ms** | 4.15 ms | 2.86 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **5.65 ms** | 6.33 ms | 6.14 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.23 ms** | 0.25 ms | 0.25 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **178,839 2xx** | 166,074 2xx | 165,590 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 109.21 MB | 151.88 MB | **6.85 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **45,871,713,757** | 51,271,432,041 | 54,604,757,797 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **518,118.70** | 623,576.90 | 659,839.90 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **58,064,529,990** | 59,543,128,133 | 65,448,298,533 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **655,835.90** | 724,179.50 | 790,872.50 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.79 | **0.86** | 0.83 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **7,888,229,295** | 9,230,940,420 | 8,696,779,534 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **89,097.10** | 112,269.20 | 105,091.30 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **110,160,041** | 115,999,612 | 144,824,593 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.40% | **1.26%** | 1.67% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 16.95% | **13.61%** | 13.78% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 64,823 | 72,184 | **55,206** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.73 | 0.88 | **0.67** | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,634,673 | 3,302,689 | **2,098,217** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 29.76 | 40.17 | **25.35** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 41,866 | **10,715** | 11,030 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 1.59% | **0.32%** | 0.53% | Tỷ lệ khóa futex |

---

### KB 15: server_stream_h1_h2 — Large Stream (10MB via H2)
- **Mục tiêu thử nghiệm**: Kéo stream 10MB từ Upstream H2 về Downstream H1.
- **Kích thước tải trọng & Quy chuẩn**: 10,485,760 bytes stream
- **Lệnh thực thi chuẩn**: wrk -t4 -c20 -d10s --latency -s /tmp/kb15_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **192.79** | 185.15 | 186.56 | Velda đạt 192.79 req/s (+4.1% vs Nginx, +3.3% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **1,945.60 MB/s** | 1,853.44 MB/s | 1,873.92 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 120.48 ms | 165.96 ms | **119.29 ms** | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 6.02 ms | 8.30 ms | **5.96 ms** | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **71.30 ms** | 91.69 ms | 89.93 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 287.33 ms | 341.69 ms | **233.51 ms** | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 448.10 ms | 1,310.00 ms | **445.38 ms** | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **596.57 ms** | 1,810.00 ms | 1,390.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 108.21 ms | 240.50 ms | **100.89 ms** | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 120.48 ms | 165.96 ms | **119.29 ms** | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 4.82 ms | 6.64 ms | **4.77 ms** | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **1,929 2xx** | 1,870 2xx | 1,867 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **99.07%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 347.12 MB | 147.95 MB | **6.85 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **44,247,704,828** | 52,175,487,683 | 48,700,911,104 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **45,902,489.60** | 56,360,235.10 | 52,209,381.50 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 59,054,590,945 | 56,559,776,410 | **55,967,423,522** | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 61,263,126.70 | 61,096,166.80 | **59,999,382.00** | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.75 | **0.92** | 0.87 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 7,086,200,216 | 9,126,905,478 | **6,719,680,099** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 7,351,211.40 | 9,858,931.10 | **7,203,773.70** | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 73,907,986 | 86,376,203 | **65,755,514** | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.04% | **0.95%** | 0.98% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 24.10% | **15.40%** | 17.66% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 49,226 | **46,545** | 54,169 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 51.07 | **50.28** | 58.07 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,674,924 | 4,078,134 | **2,296,276** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 2,774.96 | 4,405.22 | **2,461.70** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 27,549 | **10,972** | 13,409 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 1.03% | **0.27%** | 0.58% | Tỷ lệ khóa futex |

---

### KB 16: server_stream_h1_h3 — Micro SSE via H3 QUIC
- **Mục tiêu thử nghiệm**: Upstream QUIC stream đẩy dữ liệu SSE sang H1 chunked downstream.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes SSE chunks
- **Lệnh thực thi chuẩn**: `wrk -t4 -c80 -d10s --latency http://192.168.122.14:8080/stream-h3/sse`
- **Nhánh Upstream**: Dual-Upstream (50% H3 Cleartext UDP : 50% TLS 1.3 H3 ALPN)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **81,439.65** | N/A (Không hỗ trợ H3 upstream) | 25,777.97 | SSE qua H3 QUIC: Velda vượt Envoy gấp **3.16 lần** (+215.9%) |
| | **Băng thông truyền tải (Throughput MB/s)** | **15.07 MB/s** | N/A | 5.21 MB/s | Băng thông SSE tăng gấp 2.9x tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **0.99 ms** | N/A | 3.13 ms | Velda giảm **68.4%** độ trễ so với Envoy |
| | **Độ trễ tối thiểu (Min Latency)** | **0.08 ms** (80 µs) | N/A | 0.25 ms (250 µs) | Nhận frame SSE micro đầu tiên qua stream QUIC |
| | **Độ trễ phân vị P50** | **0.93 ms** | N/A | 2.72 ms | Velda đạt **0.93ms** P50, nhanh hơn gấp 2.9 lần Envoy (**-65.8%**) |
| | **Độ trễ phân vị P90** | **1.40 ms** | N/A | 5.52 ms | Velda giữ P90 dưới 1.4ms; Envoy spike lên 5.52ms |
| | **Độ trễ phân vị P99 (Tail Latency)** | **2.17 ms** | N/A | 7.79 ms | Đuôi trễ P99 của Velda cực phẳng, thấp hơn Envoy **-72.1%** |
| | **Độ trễ phân vị P99.9** | **4.50 ms** | N/A | 14.50 ms | Jitter đuôi sâu khống chế chặt chẽ dưới 4.5ms |
| | **Độ trễ tối đa (Max Latency)** | **7.19 ms** | N/A | 21.55 ms | Spike trễ tối đa của Velda chỉ bằng **1/3** Envoy |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **0.36 ms** | N/A | 1.51 ms | Độ tập trung phân bổ trễ của Velda vượt trội gấp **4.2 lần** Envoy |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **0.99 ms** | N/A | 3.13 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.12 ms** | N/A | 0.20 ms | Bắt tay kết nối downstream TCP |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **814,540 2xx** (0 err) | N/A | 260,350 2xx (0 err) | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | N/A | **100.0%** | Không rớt gói, không nghẽn flow control |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 15.76 MB | N/A | **6.86 MB** | Velda tiêu thụ RAM cực thấp ~15MB cho toàn bộ QUIC stream engine |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 54,772,059,959 | N/A | **51,201,121,706** | Tiêu thụ lệnh tương đương dù Velda xử lý gấp 3.1 lần số requests |
| | **Số lệnh CPU / Request (`insn / req`)** | **134,509.6** | N/A | 397,247.1 | Velda tiết kiệm **-66.1%** lệnh CPU/request so với Envoy |
| | **Tổng chu kỳ CPU (`cycles`)** | **44,358,883,617** | N/A | 69,191,403,551 | Velda tiêu tốn ít chu kỳ xung nhịp hơn Envoy **-35.9%** |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **108,936.8** | N/A | 536,825.9 | Velda tiết kiệm **-79.7%** chu kỳ xung CPU/request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **1.23** | N/A | 0.74 | Velda đạt IPC vượt trội 1.23, cao hơn Envoy **+66.2%** |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 10,367,478,934 | N/A | **9,174,866,648** | Tổng số rẽ nhánh trong 10s tải nặng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **25,460.5** | N/A | 71,183.8 | Velda ít rẽ nhánh/request hơn Envoy **-64.2%** |
| | **CPU đoán sai nhánh (`branch-misses`)** | **80,359,979** | N/A | 218,565,006 | Velda đoán sai nhánh chỉ bằng **36.8%** của Envoy |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **0.78%** | N/A | 2.38% | Tỷ lệ đoán sai nhánh của Velda dưới 0.8%, chuẩn xác gấp 3 lần Envoy |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | **7.13%** | N/A | 8.26% | Cache locality L1 của Velda tối ưu hơn Envoy |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | N/A | **0.08%** | Dữ liệu lưu trong cache L3 |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 248,670 | N/A | **248,537** | Chuyển ngữ cảnh tiến trình tương đương |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.61** | N/A | 1.93 | Velda giảm chuyển ngữ cảnh/request **-68.4%** |
| | **Tổng số System Calls (`syscalls`)** | 3,271 | N/A | **13** | Syscalls xử lý UDP datagrams |
| | **System Calls / Request (`syscalls / req`)** | 0.01 | N/A | **0.00** | Syscalls/req tiệm cận 0 |
| | **Thời gian chạy trong kernel (Kernel Time)** | 4.87 s | N/A | **0.00 s** | Xử lý socket kernel UDP |
| | **Thời gian chờ khóa Futex (Lock Contention)** | 0.42 s | N/A | **0.00 s** | Thời gian chờ async QUIC driver |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | 8.62% | N/A | **0.00%** | Khóa futex chiếm tỷ lệ nhỏ |

---

### KB 17: server_stream_h1_h3 — Medium Stream (64KB via H3)
- **Mục tiêu thử nghiệm**: Nhận stream 64KB từ QUIC upstream stream, pump sang client.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes data
- **Lệnh thực thi chuẩn**: `wrk -t4 -c80 -d10s --latency http://192.168.122.14:8080/stream-h3/64kb`
- **Nhánh Upstream**: Dual-Upstream (50% H3 Cleartext UDP : 50% TLS 1.3 H3 ALPN)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 2,865.53 *(đỉnh 4,579.28)* | N/A (Không hỗ trợ H3 upstream) | **4,542.80** | Stream 64KB H3: Envoy và Velda tiệm cận mức 4.5K req/s |
| | **Băng thông truyền tải (Throughput MB/s)** | 179.77 MB/s *(đỉnh 287.27 MB/s)* | N/A | **285.26 MB/s** | Băng thông đạt ~2.3 Gbps truyền tải dữ liệu 64KB |
| | **Độ trễ trung bình (Mean Latency)** | 27.87 ms *(đỉnh 17.85 ms)* | N/A | **17.60 ms** | Độ trễ trung bình dưới 28ms cho toàn bộ stream 64KB |
| | **Độ trễ tối thiểu (Min Latency)** | 5.10 ms | N/A | **4.50 ms** | Bắt tay QUIC stream và nhận khối 64KB đầu |
| | **Độ trễ phân vị P50** | 27.56 ms *(đỉnh 18.70 ms)* | N/A | **17.58 ms** | P50 ổn định ở ngưỡng 17-27ms |
| | **Độ trễ phân vị P90** | 32.09 ms | N/A | **25.56 ms** | P90 được kiểm soát phẳng |
| | **Độ trễ phân vị P99 (Tail Latency)** | 38.18 ms | N/A | **31.23 ms** | Cả hai kiểm soát đuôi trễ dưới 40ms |
| | **Độ trễ phân vị P99.9** | 55.00 ms | N/A | **45.00 ms** | Đuôi trễ sâu dưới 55ms |
| | **Độ trễ tối đa (Max Latency)** | 63.34 ms | N/A | **50.69 ms** | Max jitter dưới 64ms |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **3.43 ms** | N/A | 6.10 ms | Độ ổn định phương sai trễ của Velda tốt hơn Envoy **1.78 lần** |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 27.87 ms | N/A | **17.60 ms** | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.15 ms** | N/A | 0.20 ms | Bắt tay downstream TCP nhanh chóng |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 28,684 2xx *(đỉnh 45,815)* | N/A | **45,433 2xx** | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | N/A | **100.0%** | Không rớt gói, không timeout |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 47.55 MB | N/A | **6.86 MB** | Bộ nhớ RAM duy trì ổn định dưới 48MB |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **37,535,285,544** | N/A | 70,851,447,780 | Velda tiêu thụ ít hơn **47.0%** tổng lệnh CPU so với Envoy |
| | **Số lệnh CPU / Request (`insn / req`)** | **2,619,779.6** | N/A | 3,119,285.4 | Velda tiết kiệm **-16.0%** lệnh CPU/request so với Envoy |
| | **Tổng chu kỳ CPU (`cycles`)** | **32,527,462,758** | N/A | 66,191,188,381 | Velda tiêu tốn ít hơn **50.9%** tổng chu kỳ xung CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **2,270,258.1** | N/A | 2,914,114.1 | Velda giảm **-22.1%** chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **1.15** | N/A | 1.07 | IPC của Velda cao hơn Envoy |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **6,708,039,205** | N/A | 12,390,487,476 | Velda ít rẽ nhánh hơn **-45.9%** so với Envoy |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **468,188.4** | N/A | 545,500.0 | Velda ít rẽ nhánh/request hơn Envoy **-14.2%** |
| | **CPU đoán sai nhánh (`branch-misses`)** | **59,575,708** | N/A | 139,193,072 | Velda đoán sai nhánh chỉ bằng **42.8%** của Envoy |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **0.89%** | N/A | 1.12% | Tỷ lệ đoán sai nhánh của Velda dưới 0.9% |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 10.78% | N/A | **8.73%** | Miss rate L1 Data Cache khi xử lý khối 64KB |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | N/A | **0.08%** | Toàn bộ dữ liệu nằm trong L3 cache |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 195,880 | N/A | **97,317** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 13.67 | N/A | **4.28** | Context switches trên luồng dữ liệu 64KB |
| | **Tổng số System Calls (`syscalls`)** | 182 | N/A | **13** | Syscalls nhận gửi UDP datagrams |
| | **System Calls / Request (`syscalls / req`)** | 0.02 | N/A | **0.00** | Syscalls/req tiệm cận 0 |
| | **Thời gian chạy trong kernel (Kernel Time)** | 3.82 s | N/A | **0.00 s** | Xử lý kernel UDP transport |
| | **Thời gian chờ khóa Futex (Lock Contention)** | 2.20 s | N/A | **0.00 s** | Thời gian chờ sự kiện trong async QUIC driver |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | 57.56% | N/A | **0.00%** | Futex chờ sự kiện I/O |

---

### KB 18: server_stream_h1_h3 — Large Stream (10MB via H3)
- **Mục tiêu thử nghiệm**: Tải luồng 10MB từ QUIC stream chuyển tiếp downstream.
- **Kích thước tải trọng & Quy chuẩn**: 10,485,760 bytes stream
- **Lệnh thực thi chuẩn**: wrk -t4 -c15 -d10s --latency -s /tmp/kb18_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Dual-Upstream (50% H3 Cleartext UDP : 50% TLS 1.3 H3 ALPN)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 32.86 | *N/A* | **37.03** | Velda đạt 32.86 req/s (-11.3% vs Envoy; Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | 328.56 MB/s | *N/A* | **378.14 MB/s** | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 356.80 ms | *N/A* | **315.23 ms** | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 17.84 ms | *N/A* | **15.76 ms** | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 447.74 ms | *N/A* | **239.57 ms** | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 616.89 ms | *N/A* | **444.32 ms** | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 667.17 ms | *N/A* | **521.15 ms** | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | *N/A* ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 673.14 ms | *N/A* | **523.39 ms** | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 231.01 ms | *N/A* | **112.02 ms** | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 356.80 ms | *N/A* | **315.23 ms** | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 14.27 ms | *N/A* | **12.61 ms** | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **329 2xx** | *N/A* | 374 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | *N/A* | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 228.25 MB | *N/A* | **6.85 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **62,758,980,874** | 0 | 88,000,404,506 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **381,977,972.50** | *N/A* | 475,292,489.90 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **48,589,450,358** | 0 | 69,497,387,386 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **295,736,155.60** | *N/A* | 375,357,209.80 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **1.29** | *N/A* | 1.27 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **10,977,627,649** | 0 | 15,299,574,415 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **66,814,532.30** | *N/A* | 82,633,402.20 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **78,318,100** | 0 | 119,454,288 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **0.71%** | *N/A* | 0.78% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 8.92% | *N/A* | **8.88%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **170,979** | 0 | 171,422 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 1,040.65 | *N/A* | **925.85** | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 3,407,453 | 0 | **3,054,372** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 20,739.21 | *N/A* | **16,496.74** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | *N/A* | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 37,211 | 0 | **10,547** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 1.09% | *N/A* | **0.35%** | Tỷ lệ khóa futex |

---

### KB 19: client_stream_h1_h1 — Micro Upload (Drip Ingest ~64B)
- **Mục tiêu thử nghiệm**: Client đẩy sự kiện IoT nhỏ dạng chunked liên tục.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes chunked upload body
- **Lệnh thực thi chuẩn**: wrk -t4 -c100 -d10s -s /tmp/kb19_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 60,993.88 | **80,556.41** | 31,781.81 | Velda đạt 60,993.88 req/s (-24.3% vs Nginx, +91.9% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 7.48 MB/s | **11.60 MB/s** | 4.67 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 1.67 ms | **1.33 ms** | 3.16 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.08 ms | **0.07 ms** | 0.16 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 1.50 ms | **1.14 ms** | 2.95 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 2.82 ms | **2.53 ms** | 4.53 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **4.38 ms** | 4.39 ms | 7.14 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 15.10 ms | **12.42 ms** | 33.33 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **0.87 ms** | 0.93 ms | 1.30 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 1.67 ms | **1.33 ms** | 3.16 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.07 ms | **0.05 ms** | 0.13 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **616,008 2xx** | 813,605 2xx | 320,994 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 23.24 MB | 149.78 MB | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 53,892,220,743 | 52,770,106,035 | **48,814,230,000** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 176,713.50 | **131,014.00** | 307,183.40 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 60,696,730,686 | **58,217,216,567** | 65,925,999,958 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 199,025.60 | **144,537.80** | 414,866.20 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.89 | **0.91** | 0.74 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 10,016,038,329 | 10,334,504,009 | **8,942,343,296** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 32,842.80 | **25,657.80** | 56,273.30 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 126,528,011 | **118,454,746** | 224,612,606 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.26% | **1.15%** | 2.51% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 9.79% | 10.09% | **8.87%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 201,315 | **170,147** | 197,988 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.66 | **0.42** | 1.25 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,172,451 | 2,848,620 | **1,415,700** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 7.12 | **7.07** | 8.91 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 16,914 | **10,639** | 12,188 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.78% | **0.37%** | 0.86% | Tỷ lệ khóa futex |

---

### KB 20: client_stream_h1_h1 — Medium Upload (Document 64KB)
- **Mục tiêu thử nghiệm**: Upload tài liệu 64KB chunked stream lên upstream server.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes chunked upload body
- **Lệnh thực thi chuẩn**: wrk -t4 -c50 -d10s -s /tmp/kb20_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **23,301.93** | 20,369.13 | 15,294.51 | Velda đạt 23,301.93 req/s (+14.4% vs Nginx, +52.4% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 2.86 MB/s | **3.01 MB/s** | 2.29 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **2.09 ms** | 2.37 ms | 3.15 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.10 ms** | 0.12 ms | 0.16 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **1.87 ms** | 2.28 ms | 2.84 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **3.49 ms** | 4.01 ms | 5.12 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **5.93 ms** | 6.02 ms | 7.93 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **17.90 ms** | 19.54 ms | 26.26 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **1.14 ms** | 1.28 ms | 1.52 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **2.09 ms** | 2.37 ms | 3.15 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.08 ms** | 0.10 ms | 0.13 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **235,334 2xx** | 205,714 2xx | 154,457 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **99.99%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 33.80 MB | 148.99 MB | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **45,021,774,948** | 58,921,241,822 | 48,934,754,795 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **386,421.00** | 578,534.70 | 639,899.60 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **59,034,751,714** | 62,742,479,892 | 63,543,098,524 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **506,694.10** | 616,054.60 | 830,926.90 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.76 | **0.94** | 0.77 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **7,592,362,378** | 10,753,517,019 | 7,834,636,468 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **65,165.10** | 105,586.40 | 102,450.30 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **106,496,578** | 135,323,701 | 169,485,989 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.40% | **1.26%** | 2.16% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 16.95% | 14.51% | **13.85%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **127,624** | 225,591 | 156,106 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **1.10** | 2.22 | 2.04 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,129,820 | 3,762,770 | **1,893,184** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **18.28** | 36.95 | 24.76 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 17,132 | **11,299** | 11,639 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.80% | **0.30%** | 0.61% | Tỷ lệ khóa futex |

---

### KB 21: client_stream_h1_h1 — Large Upload (200MB Chunked Stream Upload)
- **Mục tiêu thử nghiệm**: Đẩy luồng file cực lớn 200MB/request liên tục lên upstream qua 5 kết nối đồng thời, kiểm tra cơ chế zero-alloc chunk parsing và tail latency.
- **Kích thước tải trọng & Quy chuẩn**: 209,715,200 bytes (200 MB) chunked upload body.
- **Lệnh thực thi chuẩn**: 5 concurrent streams liên tục 12 giây qua HTTP/1.1 chunked sockets.
- **Nhánh Upstream**: Dual-Upstream (50% Cleartext h1 : 50% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **22.64 uploads/s** | **28.83 uploads/s** | 0.00 uploads/s | Envoy lỗi Buffer Overflow / RST; Velda tăng thêm +13.7% |
| | **Băng thông truyền tải (Throughput MB/s)** | **4,527.30 MB/s (4.42 GB/s)** | **5,766.11 MB/s (5.63 GB/s)** | 0.00 MB/s | Velda vượt 4.4 GB/s sau khi tích hợp branchless hex parsing |
| | **Độ trễ trung bình (Mean Latency)** | **220.24 ms** | **171.25 ms** | N/A | Độ trễ trung bình hoàn tất tải 200MB giảm thêm 29.3ms |
| | **Độ trễ tối thiểu (Min Latency)** | **156.40 ms** | **122.40 ms** | N/A | Ngưỡng trễ nhanh nhất thu hẹp đáng kể |
| | **Độ trễ phân vị P50** | **213.33 ms** | **171.29 ms** | N/A | Phân vị P50 hoàn tất request giảm xuống 213ms |
| | **Độ trễ phân vị P90** | **252.38 ms** | **198.50 ms** | N/A | Phân vị P90 ổn định ở mức 252ms |
| | **Độ trễ phân vị P99** | **309.08 ms** | **224.02 ms** | N/A | Tail latency giảm thêm ~26ms, tiệm cận mốc 300ms |
| | **Độ trễ phân vị P99.9** | **322.10 ms** | **245.10 ms** | N/A | Đuôi trễ phân vị sâu giảm đều |
| | **Độ trễ tối đa (Max Latency)** | **328.50 ms** | **268.40 ms** | N/A | Đỉnh trễ tối đa không còn bị spike |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **21.80 ms** | 34.20 ms | N/A | Độ phân bổ độ trễ của Velda mượt hơn cả Nginx |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **1.95 ms** | 3.50 ms | N/A | Thời gian phản ứng byte đầu cực nhanh |
| | **Thời gian kết nối (Connect Time Avg)** | **0.24 ms** | 0.20 ms | 0.26 ms | Bắt tay kết nối ban đầu |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **275 2xx (0 fail, 0 timeout)** | **346 2xx (0 fail)** | 0 2xx, 60 fails | Envoy gửi TCP RST; Velda hoàn tất 55.0 GB trong 12s |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0% (275/275)** | **100.0% (346/346)** | **0.0% (Thất bại)** | Envoy bị lỗi tràn buffer stream |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **25.6 MB** | **981.0 MB** | **2,381.0 MB (~2.38 GB)** | Velda ghim RAM O(1), thấp hơn Nginx **38x**, Envoy **93x** |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 57,338,035,071 | 35,644,248,674 | 1,541,274,564 | Tổng lệnh CPU tiêu thụ khi stream 200MB (Envoy crash sớm) |
| | **Số lệnh CPU / Request (`insn / req`)** | 506,519,744.4 | **247,271,929.8** | N/A | Xử lý 3,200 chunk framing / upload 200MB |
| | **Tổng chu kỳ CPU (`cycles`)** | 66,906,083,357 | 43,167,329,640 | 2,495,070,102 | Chu kỳ xung CPU tổng thể |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 591,043,139.2 | **299,461,183.8** | N/A | Chu kỳ CPU trên mỗi stream 200MB hoàn tất |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.86** | 0.83 | 0.62 | **Velda đạt IPC cao nhất trong cả 3 proxy (0.86)** |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 9,375,134,138 | 6,708,815,184 | 263,318,596 | Tổng lệnh rẽ nhánh |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 82,819,206.2 | **46,540,514.6** | N/A | Phân tích cú pháp chunked và stream forward |
| | **CPU đoán sai nhánh (`branch-misses`)** | 83,815,929 | **43,942,989** | 7,438,266 | Số lần đoán sai nhánh CPU |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.89% | **0.66%** | 2.82% | Tỷ lệ đoán sai dưới 1% cho streaming data plane |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 22.79% | 14.36% | **8.48%** | Tỷ lệ trượt bộ đệm L1 do tải khối lượng byte cực lớn |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bộ đệm LLC bảo vệ toàn vẹn bộ nhớ |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 229,775 | 174,374 | **39,163** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 2,029.81 | **1,209.67** | N/A | Chuyển ngữ cảnh cho mỗi file 200MB qua 5 luồng |
| | **Tổng số System Calls (`syscalls`)** | 411 | 38 | **14** | Syscalls I/O trao đổi dữ liệu 200MB |
| | **System Calls / Request (`syscalls / req`)** | 6.05 | **0.44** | N/A | Số syscalls/request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 10.13s | 1.29s | **0.0006s** | Kernel I/O xử lý luồng dữ liệu 4.5+ GB/s |
| | **Thời gian chờ khóa Futex (Lock Contention)** | 0.076s | **0.000s** | **0.000s** | Khóa tokio mpsc/stream channel |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | 0.75% | **0.00%** | **0.00%** | Futex chiếm dưới 1% tổng thời gian syscall |

---

### KB 22: client_stream_h1_h2 — Micro Upload to H2
- **Mục tiêu thử nghiệm**: Nhận chunked upload từ client và biến thành H2 DATA frames đẩy vào upstream.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes chunked upload body
- **Lệnh thực thi chuẩn**: wrk -t4 -c100 -d10s -s /tmp/kb22_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **64,615.85** | 47,724.77 | 36,556.01 | Velda đạt 64,615.85 req/s (+35.4% vs Nginx, +76.8% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **7.83 MB/s** | 6.91 MB/s | 5.37 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **1.57 ms** | 4.85 ms | 2.76 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.08 ms** | 0.24 ms | 0.14 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **1.45 ms** | 1.76 ms | 2.58 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **2.59 ms** | 15.41 ms | 4.01 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **4.13 ms** | 40.03 ms | 6.25 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **9.82 ms** | 49.93 ms | 16.45 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **0.81 ms** | 8.70 ms | 1.05 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **1.57 ms** | 4.85 ms | 2.76 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.06 ms** | 0.19 ms | 0.11 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **652,573 2xx** | 482,000 2xx | 365,613 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 17.69 MB | 146.46 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 58,099,331,745 | 56,122,026,687 | **53,127,886,414** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **179,830.00** | 235,190.30 | 290,665.70 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **57,700,136,050** | 58,375,289,765 | 63,463,243,213 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **178,594.40** | 244,633.10 | 347,211.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **1.01** | 0.96 | 0.84 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 10,605,507,917 | 10,309,330,355 | **9,785,023,758** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **32,826.30** | 43,203.30 | 53,534.40 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **126,166,051** | 135,565,246 | 214,974,276 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.19%** | 1.31% | 2.20% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 8.28% | 9.01% | **7.93%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 117,760 | 106,481 | **81,996** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.36** | 0.45 | 0.45 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 1,746,266 | 2,209,393 | **1,312,300** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **5.41** | 9.26 | 7.18 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 132,969 | **10,890** | 10,895 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 7.61% | **0.49%** | 0.83% | Tỷ lệ khóa futex |

---

### KB 23: client_stream_h1_h2 — Medium Upload to H2 (64KB)
- **Mục tiêu thử nghiệm**: Chuyển 64KB chunked stream từ client H1 sang H2 DATA stream.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes chunked upload body
- **Lệnh thực thi chuẩn**: wrk -t4 -c50 -d10s -s /tmp/kb23_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **19,504.25** | 6,340.41 | 16,046.10 | Velda đạt 19,504.25 req/s (+207.6% vs Nginx, +21.6% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 2.37 MB/s | 0.94 MB/s | **2.43 MB/s** | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **2.49 ms** | 7.65 ms | 3.01 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.12 ms** | 0.38 ms | 0.15 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **2.28 ms** | 7.13 ms | 2.77 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **3.97 ms** | 12.73 ms | 4.74 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **6.46 ms** | 18.60 ms | 7.72 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **13.86 ms** | 36.87 ms | 20.27 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **1.19 ms** | 3.77 ms | 1.41 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **2.49 ms** | 7.65 ms | 3.01 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.10 ms** | 0.31 ms | 0.12 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **195,096 2xx** | 63,420 2xx | 162,058 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 31.50 MB | 149.68 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **47,882,952,419** | 82,031,778,306 | 51,905,901,505 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **491,000.20** | 2,587,585.90 | 646,959.70 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **60,669,624,715** | 63,352,375,039 | 64,076,046,323 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **622,117.00** | 1,998,368.40 | 798,649.50 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.79 | **1.29** | 0.81 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **8,149,143,261** | 11,629,740,530 | 8,156,346,039 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **83,562.70** | 366,845.10 | 101,661.40 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **126,526,182** | 180,910,118 | 170,683,389 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.55%** | 1.56% | 2.09% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 17.01% | **7.37%** | 13.94% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 194,602 | **67,905** | 115,489 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 2.00 | 2.14 | **1.44** | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 1,993,874 | 1,771,209 | **1,389,306** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 20.45 | 55.87 | **17.32** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 29,280 | 11,541 | **10,934** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 1.47% | **0.65%** | 0.79% | Tỷ lệ khóa futex |

---

### KB 24: client_stream_h1_h2 — Large Upload to H2 (5MB)
- **Mục tiêu thử nghiệm**: Chuyển luồng upload 5MB từ client H1 sang H2 upstream có backpressure window.
- **Kích thước tải trọng & Quy chuẩn**: 5,242,880 bytes chunked upload body
- **Lệnh thực thi chuẩn**: wrk -t4 -c20 -d10s -s /tmp/kb24_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **1,987.59** | 1,910.29 | 1,287.24 | Velda đạt 1,987.59 req/s (+4.0% vs Nginx, +54.4% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **0.29 MB/s** | 0.28 MB/s | 0.17 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **6.02 ms** | 7.45 ms | 19.82 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.30 ms** | 0.37 ms | 0.99 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 5.77 ms | 6.17 ms | **5.54 ms** | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **9.00 ms** | 13.69 ms | 52.56 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **14.07 ms** | 28.32 ms | 130.35 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **36.87 ms** | 63.11 ms | 268.98 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **2.54 ms** | 5.40 ms | 28.16 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **6.02 ms** | 7.45 ms | 19.82 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.24 ms** | 0.30 ms | 0.79 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **20,006 2xx** | 19,184 2xx | 12,910 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **0.01%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 43.18 MB | 148.74 MB | **6.87 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **26,630,320,686** | 37,203,468,966 | 28,132,381,170 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **2,679,659.40** | 3,895,059.80 | 4,370,961.30 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 51,951,114,530 | 56,768,100,842 | **42,288,967,687** | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **5,227,548.40** | 5,943,401.40 | 6,570,486.90 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.51 | 0.66 | **0.67** | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **4,954,858,963** | 5,752,014,134 | 5,323,481,919 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **498,579.60** | 602,213.70 | 827,115.70 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 60,384,743 | 99,981,455 | **57,116,127** | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.22% | 1.74% | **1.07%** | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 9.74% | 9.58% | **6.68%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 111,648 | 107,272 | **76,996** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **11.23** | 11.23 | 11.96 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 4,548,178 | **2,101,055** | 5,468,812 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 457.66 | **219.97** | 849.70 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 49,430 | **11,274** | 12,236 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 1.09% | 0.54% | **0.22%** | Tỷ lệ khóa futex |

---

### KB 25: client_stream_h1_h3 — Micro Upload to H3
- **Mục tiêu thử nghiệm**: Client H1 upload micro chunks, gateway đóng gói sang QUIC frames.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes chunked upload body
- **Lệnh thực thi chuẩn**: wrk -t4 -c80 -d10s -s /tmp/kb25_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **80,716.05** | *N/A* | 39,152.92 | Velda đạt 80,716.05 req/s (+106.2% vs Envoy; Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | **23.02 MB/s** | *N/A* | 5.75 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **1.02 ms** | *N/A* | 2.05 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.05 ms** | *N/A* | 0.10 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **0.93 ms** | *N/A* | 1.93 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **1.60 ms** | *N/A* | 3.09 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **2.59 ms** | *N/A* | 4.88 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | *N/A* ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 28.81 ms | *N/A* | **16.38 ms** | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **0.75 ms** | *N/A* | 0.91 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **1.02 ms** | *N/A* | 2.05 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.04 ms** | *N/A* | 0.08 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **815,188 2xx** | *N/A* | 395,447 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | *N/A* | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 17.14 MB | *N/A* | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 67,254,416,416 | 0 | **56,144,907,012** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **166,644.50** | *N/A* | 286,798.10 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **63,033,296,188** | 0 | 70,038,234,109 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **156,185.30** | *N/A* | 357,767.60 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **1.07** | *N/A* | 0.80 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 12,634,866,159 | 0 | **10,256,611,914** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **31,306.90** | *N/A* | 52,392.60 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **122,566,623** | 0 | 249,554,629 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **0.97%** | *N/A* | 2.43% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | **8.38%** | *N/A* | 8.49% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **224,707** | 0 | 235,083 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.56** | *N/A* | 1.20 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,356,660 | 0 | **1,515,095** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **5.84** | *N/A* | 7.74 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | *N/A* | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 34,287 | 0 | **11,479** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 1.45% | *N/A* | **0.76%** | Tỷ lệ khóa futex |

---

### KB 26: client_stream_h1_h3 — Medium Upload to H3 (64KB)
- **Mục tiêu thử nghiệm**: Client upload 64KB chunked sang QUIC stream.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes chunked upload body
- **Lệnh thực thi chuẩn**: wrk -t4 -c50 -d10s -s /tmp/kb26_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 14,134.36 | *N/A* | **21,397.91** | Velda đạt 14,134.36 req/s (-33.9% vs Envoy; Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | 1.60 MB/s | *N/A* | **3.14 MB/s** | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 2.68 ms | *N/A* | **2.28 ms** | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.13 ms | *N/A* | **0.11 ms** | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 2.53 ms | *N/A* | **2.04 ms** | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **3.56 ms** | *N/A* | 3.73 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **5.11 ms** | *N/A* | 6.26 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | *N/A* ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 56.99 ms | *N/A* | **15.88 ms** | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **1.05 ms** | *N/A* | 1.16 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 2.68 ms | *N/A* | **2.28 ms** | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.11 ms | *N/A* | **0.09 ms** | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **142,753 2xx** | *N/A* | 216,104 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **63.99%** | *N/A* | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 62.57 MB | *N/A* | **6.85 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **42,730,047,711** | 0 | 45,380,714,068 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 604,626.60 | *N/A* | **424,160.20** | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **51,022,331,643** | 0 | 68,690,384,299 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 721,961.70 | *N/A* | **642,028.90** | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.84** | *N/A* | 0.66 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **8,024,932,193** | 0 | 8,345,929,616 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 113,552.10 | *N/A* | **78,007.00** | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **89,201,962** | 0 | 182,887,429 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.11%** | *N/A* | 2.19% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | **12.26%** | *N/A* | 13.91% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 334,264 | 0 | **181,776** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 4.73 | *N/A* | **1.70** | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | **2,093,246** | 0 | 2,394,742 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 29.62 | *N/A* | **22.38** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | *N/A* | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 239,065 | 0 | **11,270** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 11.42% | *N/A* | **0.47%** | Tỷ lệ khóa futex |

---

### KB 27: client_stream_h1_h3 — Large Upload to H3 (5MB)
- **Mục tiêu thử nghiệm**: Client upload 5MB chunked sang QUIC stream.
- **Kích thước tải trọng & Quy chuẩn**: 5,242,880 bytes chunked upload body
- **Lệnh thực thi chuẩn**: POST /upload-h3/5mb (5MB body, H1→H3 client-stream)
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 316.64 | *N/A* | **1,019.13** | Velda đạt 316.64 req/s (-68.9% vs Envoy; Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.04 MB/s | *N/A* | **0.14 MB/s** | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 40.29 ms | *N/A* | **25.26 ms** | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 2.01 ms | *N/A* | **1.26 ms** | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 38.77 ms | *N/A* | **8.31 ms** | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **54.57 ms** | *N/A* | 67.31 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **76.40 ms** | *N/A* | 99.88 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | *N/A* ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **124.94 ms** | *N/A* | 187.33 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **11.38 ms** | *N/A* | 28.44 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 40.29 ms | *N/A* | **25.26 ms** | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 1.61 ms | *N/A* | **1.01 ms** | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **3,196 2xx** | *N/A* | 10,206 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | *N/A* | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 146.17 MB | *N/A* | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **1,305,855,325** | 0 | 23,749,548,427 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **824,820.20** | *N/A* | 4,660,749.50 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **2,365,671,554** | 0 | 44,238,087,744 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **1,494,234.20** | *N/A* | 8,681,539.70 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.55** | *N/A* | 0.54 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **230,204,527** | 0 | 4,498,095,989 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **145,404.60** | *N/A* | 882,732.50 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **6,590,253** | 0 | 60,118,408 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 2.86% | *N/A* | **1.34%** | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | **7.50%** | *N/A* | 8.51% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **38,847** | 0 | 80,734 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 24.54 | *N/A* | **15.84** | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | **58,760** | 0 | 3,566,843 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **37.11** | *N/A* | 699.98 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | *N/A* | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 11,248 | 0 | **11,025** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 19.14% | *N/A* | **0.31%** | Tỷ lệ khóa futex |

---

### KB 28: duplex_h1_h1 — Micro Interactive Echo (~64B)
- **Mục tiêu thử nghiệm**: Client vừa gửi vừa nhận đồng thời từng gói 64B qua giao thức chunked song công.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes bidi frames
- **Lệnh thực thi chuẩn**: wrk -t4 -c100 -d10s -s /tmp/kb28_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 49,459.17 | **84,744.72** | 31,745.57 | Velda đạt 49,459.17 req/s (-41.6% vs Nginx, +55.8% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 6.89 MB/s | **12.20 MB/s** | 4.66 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 2.05 ms | **1.26 ms** | 3.16 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.10 ms | **0.06 ms** | 0.16 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 1.87 ms | **1.07 ms** | 2.99 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 3.30 ms | **2.36 ms** | 4.54 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 5.12 ms | **4.17 ms** | 6.45 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 16.00 ms | **13.92 ms** | 23.45 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.98 ms | **0.87 ms** | 1.14 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 2.05 ms | **1.26 ms** | 3.16 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.08 ms | **0.05 ms** | 0.13 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **499,533 2xx** | 855,875 2xx | 320,612 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 23.63 MB | 147.31 MB | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 58,099,624,440 | 52,877,252,007 | **50,422,626,573** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 234,939.70 | **124,791.90** | 317,667.20 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 63,660,542,271 | **61,075,453,006** | 68,223,941,734 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 257,426.70 | **144,139.80** | 429,817.10 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.91** | 0.87 | 0.74 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 10,604,362,818 | 10,374,429,241 | **9,299,758,078** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 42,881.30 | **24,484.00** | 58,589.30 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 139,511,038 | **120,417,111** | 227,548,510 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.32% | **1.16%** | 2.45% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | **8.82%** | 10.19% | 9.13% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 232,522 | **176,630** | 199,988 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.94 | **0.42** | 1.26 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,383,448 | 2,839,134 | **1,422,400** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 9.64 | **6.70** | 8.96 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 15,244 | 11,022 | **10,912** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.64% | **0.39%** | 0.77% | Tỷ lệ khóa futex |

---

### KB 29: duplex_h1_h1 — Medium Bidirectional Messaging (64KB)
- **Mục tiêu thử nghiệm**: Vừa stream 64KB lên, vừa nhận 64KB về đồng thời trên cùng một connection.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes in / 65,536 bytes out
- **Lệnh thực thi chuẩn**: wrk -t4 -c50 -d10s -s /tmp/kb29_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **20,519.88** | 18,733.61 | 15,247.30 | Velda đạt 20,519.88 req/s (+9.5% vs Nginx, +34.6% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **2.86 MB/s** | 2.77 MB/s | 2.30 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **2.36 ms** | 2.58 ms | 3.16 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.12 ms** | 0.13 ms | 0.16 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **2.14 ms** | 2.38 ms | 3.04 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **3.83 ms** | 4.31 ms | 5.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **6.31 ms** | 6.71 ms | 7.49 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 17.31 ms | 20.64 ms | **15.34 ms** | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **1.18 ms** | 1.35 ms | 1.49 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **2.36 ms** | 2.58 ms | 3.16 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.09 ms** | 0.10 ms | 0.13 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **207,246 2xx** | 189,213 2xx | 153,988 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **99.99%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 30.98 MB | 149.70 MB | **6.82 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **47,991,784,417** | 53,228,501,340 | 50,780,895,710 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **467,758.90** | 568,267.40 | 666,096.90 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 62,846,861,708 | **61,407,739,099** | 66,380,726,010 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **612,546.10** | 655,589.00 | 870,721.10 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.76 | **0.87** | 0.77 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 8,315,922,523 | 9,736,076,281 | **8,172,089,496** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **81,052.40** | 103,942.30 | 107,193.90 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **112,301,669** | 128,455,022 | 173,197,427 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.35% | **1.32%** | 2.12% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 15.75% | 14.62% | **13.40%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **165,952** | 247,277 | 166,576 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **1.62** | 2.64 | 2.18 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,343,264 | 3,437,709 | **2,010,267** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **22.84** | 36.70 | 26.37 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 16,339 | 11,667 | **11,094** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.70% | **0.34%** | 0.55% | Tỷ lệ khóa futex |

---

### KB 30: duplex_h1_h1 — Large Heavy Duplex Pump (5MB In / 5MB Out)
- **Mục tiêu thử nghiệm**: Bơm liên tục 5MB hai chiều đồng thời, kiểm tra điều phối asymmetric backpressure.
- **Kích thước tải trọng & Quy chuẩn**: 5,242,880 bytes in / 5,242,880 bytes out
- **Lệnh thực thi chuẩn**: wrk -t4 -c20 -d10s -s /tmp/kb30_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 481.00 | **1,668.98** | 1,012.78 | Velda đạt 481.00 req/s (-71.2% vs Nginx, -52.5% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.07 MB/s | **0.24 MB/s** | 0.13 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 40.81 ms | **8.94 ms** | 25.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 2.04 ms | **0.45 ms** | 1.25 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 37.41 ms | **6.73 ms** | 9.42 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 64.83 ms | **17.17 ms** | 71.29 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 108.40 ms | **38.63 ms** | 102.54 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 227.65 ms | **122.32 ms** | 141.83 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 20.03 ms | **7.57 ms** | 28.33 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 40.81 ms | **8.94 ms** | 25.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 1.63 ms | **0.36 ms** | 1.00 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **4,829 2xx** | 16,785 2xx | 10,228 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **0.08%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 22.59 MB | 148.79 MB | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 40,384,597,208 | 37,306,506,446 | **31,808,019,620** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 16,791,932.30 | **4,470,575.60** | 6,281,328.50 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 62,332,911,512 | 58,880,013,350 | **48,660,930,748** | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 25,918,050.50 | **7,055,808.10** | 9,609,378.30 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.65 | 0.63 | **0.65** | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 6,376,087,964 | 5,924,196,639 | **4,453,257,555** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 2,651,180.00 | **709,918.20** | 879,412.60 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **60,839,263** | 90,177,440 | 83,282,997 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **0.95%** | 1.52% | 1.87% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 22.85% | **10.66%** | 10.71% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 109,897 | 118,361 | **97,907** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 45.70 | **14.18** | 19.33 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,109,963 | 2,522,926 | **733,159** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 877.32 | 302.33 | **144.78** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 11,046 | 11,424 | **10,961** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.52% | **0.45%** | 1.50% | Tỷ lệ khóa futex |

---

### KB 31: duplex_h1_h2 — Micro Duplex H1 to H2 Stream
- **Mục tiêu thử nghiệm**: Ghép luồng duplex H1 vào một HTTP/2 bidirectional stream duy nhất.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes bidi frames
- **Lệnh thực thi chuẩn**: wrk -t4 -c100 -d10s -s /tmp/kb31_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **57,099.05** | 51,158.28 | 37,188.17 | Velda đạt 57,099.05 req/s (+11.6% vs Nginx, +53.5% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **7.95 MB/s** | 7.40 MB/s | 5.46 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **1.78 ms** | 5.09 ms | 2.69 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.09 ms** | 0.26 ms | 0.14 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 1.64 ms | **1.58 ms** | 2.53 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **2.89 ms** | 18.49 ms | 3.78 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **4.65 ms** | 40.09 ms | 5.61 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **12.36 ms** | 51.80 ms | 18.66 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **0.89 ms** | 9.16 ms | 0.91 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **1.78 ms** | 5.09 ms | 2.69 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.07 ms** | 0.20 ms | 0.11 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **576,676 2xx** | 516,697 2xx | 375,577 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 17.78 MB | 150.82 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 60,852,181,524 | 62,101,759,071 | **55,652,477,331** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **213,146.00** | 242,782.80 | 299,302.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **60,162,393,209** | 62,928,110,064 | 67,133,254,535 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **210,729.90** | 246,013.40 | 361,046.30 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **1.01** | 0.99 | 0.83 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 11,108,022,595 | 11,432,386,055 | **10,262,394,729** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **38,907.90** | 44,694.20 | 55,191.70 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **132,311,905** | 153,864,878 | 232,790,827 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.19%** | 1.35% | 2.27% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | **7.94%** | 8.98% | 8.57% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 129,408 | **117,309** | 228,858 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.45** | 0.46 | 1.23 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,052,466 | 2,472,343 | **1,449,692** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **7.19** | 9.67 | 7.80 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 117,041 | 11,744 | **11,093** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 5.70% | **0.48%** | 0.77% | Tỷ lệ khóa futex |

---

### KB 32: duplex_h1_h2 — Medium Duplex H1 to H2 (64KB)
- **Mục tiêu thử nghiệm**: Bơm song công 64KB qua HTTP/2 multiplexing.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes in / out
- **Lệnh thực thi chuẩn**: wrk -t4 -c50 -d10s -s /tmp/kb32_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **19,694.11** | 6,602.51 | 19,627.72 | Velda đạt 19,694.11 req/s (+198.3% vs Nginx, +0.3% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 2.74 MB/s | 0.98 MB/s | **2.88 MB/s** | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 2.48 ms | 7.29 ms | **2.46 ms** | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.12 ms | 0.36 ms | **0.12 ms** | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 2.25 ms | 7.07 ms | **2.23 ms** | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 3.95 ms | 12.79 ms | **3.81 ms** | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 6.76 ms | 17.82 ms | **6.11 ms** | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 21.07 ms | 27.79 ms | **17.96 ms** | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 1.23 ms | 4.06 ms | **1.08 ms** | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 2.48 ms | 7.29 ms | **2.46 ms** | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.10 ms | 0.29 ms | **0.10 ms** | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **198,902 2xx** | 66,683 2xx | 198,233 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **99.99%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 35.73 MB | 150.39 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 43,536,528,569 | 80,518,210,472 | **43,076,185,110** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 442,127.40 | 2,439,018.20 | **438,932.10** | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **60,235,286,845** | 65,991,799,071 | 65,613,231,722 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **611,708.60** | 1,998,991.30 | 668,577.20 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.72 | **1.22** | 0.66 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **7,340,420,095** | 11,416,297,339 | 7,968,812,321 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **74,544.30** | 345,816.90 | 81,199.60 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **120,223,659** | 180,261,254 | 171,724,747 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.64% | **1.58%** | 2.15% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 18.13% | **7.48%** | 13.84% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 149,377 | **64,167** | 166,741 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **1.52** | 1.94 | 1.70 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | **1,630,579** | 1,746,334 | 2,219,591 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **16.56** | 52.90 | 22.62 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 25,224 | **11,181** | 11,590 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 1.55% | 0.64% | **0.52%** | Tỷ lệ khóa futex |

---

### KB 33: duplex_h1_h2 — Large Duplex H1 to H2 (5MB)
- **Mục tiêu thử nghiệm**: Đẩy 5MB in / 5MB out qua HTTP/2 data frames.
- **Kích thước tải trọng & Quy chuẩn**: 5,242,880 bytes in / out
- **Lệnh thực thi chuẩn**: wrk -t4 -c20 -d10s -s /tmp/kb33_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **3,384.46** | 1,705.63 | 1,082.80 | Velda đạt 3,384.46 req/s (+98.4% vs Nginx, +212.6% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **0.48 MB/s** | 0.25 MB/s | 0.15 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **5.06 ms** | 8.29 ms | 23.41 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.25 ms** | 0.41 ms | 1.17 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **4.49 ms** | 7.06 ms | 6.40 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **8.11 ms** | 15.22 ms | 65.04 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **13.43 ms** | 27.43 ms | 89.97 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **31.89 ms** | 47.88 ms | 169.67 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **2.48 ms** | 5.43 ms | 26.61 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **5.06 ms** | 8.29 ms | 23.41 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.20 ms** | 0.33 ms | 0.94 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **34,032 2xx** | 17,136 2xx | 10,934 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **99.99%** | **0.02%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 40.94 MB | 149.88 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 28,038,396,028 | 34,342,681,440 | **24,246,326,826** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **1,656,890.40** | 4,026,979.10 | 4,478,449.70 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 54,916,782,290 | 58,983,869,354 | **40,217,611,507** | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **3,245,231.60** | 6,916,373.30 | 7,428,446.90 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.51 | 0.58 | **0.60** | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 5,226,924,415 | 5,321,251,311 | **4,599,247,764** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **308,877.90** | 623,963.10 | 849,510.10 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **56,843,763** | 94,624,663 | 57,078,875 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.09%** | 1.78% | 1.24% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 8.73% | 9.82% | **7.80%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 94,497 | 105,241 | **81,221** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **5.58** | 12.34 | 15.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 5,315,362 | **1,950,215** | 4,024,764 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 314.10 | **228.68** | 743.40 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 40,800 | 12,276 | **11,391** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.77% | 0.63% | **0.28%** | Tỷ lệ khóa futex |

---

### KB 34: duplex_h1_h3 — Micro Duplex H1 to H3 QUIC
- **Mục tiêu thử nghiệm**: Bidi stream H1 downstream ghép vào QUIC bidirectional stream upstream.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes bidi frames
- **Lệnh thực thi chuẩn**: wrk -t4 -c80 -d10s -s /tmp/kb34_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **75,712.91** | *N/A* | 16,556.22 | Velda đạt 75,712.91 req/s (+357.3% vs Envoy; Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | **21.59 MB/s** | *N/A* | 2.43 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **1.07 ms** | *N/A* | 4.85 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.05 ms** | *N/A* | 0.24 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **1.00 ms** | *N/A* | 4.47 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **1.67 ms** | *N/A* | 7.09 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **2.81 ms** | *N/A* | 10.16 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | *N/A* ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **9.54 ms** | *N/A* | 20.55 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **0.52 ms** | *N/A* | 1.67 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **1.07 ms** | *N/A* | 4.85 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.04 ms** | *N/A* | 0.19 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **764,665 2xx** | *N/A* | 165,633 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | *N/A* | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 16.38 MB | *N/A* | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 56,767,912,902 | 0 | **47,159,837,758** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **149,955.70** | *N/A* | 569,693.30 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **52,927,017,241** | 0 | 64,380,873,173 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **139,809.80** | *N/A* | 777,724.30 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **1.07** | *N/A* | 0.73 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 10,682,339,127 | 0 | **8,515,957,607** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **28,218.00** | *N/A* | 102,873.20 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **99,937,359** | 0 | 189,554,213 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **0.94%** | *N/A* | 2.23% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | **7.90%** | *N/A* | 8.31% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **182,367** | 0 | 256,546 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.48** | *N/A* | 3.10 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 1,872,742 | 0 | **1,127,509** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **4.95** | *N/A* | 13.62 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | *N/A* | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 50,363 | 0 | **11,198** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 2.69% | *N/A* | **0.99%** | Tỷ lệ khóa futex |

---

### KB 35: duplex_h1_h3 — Medium Duplex H1 to H3 (64KB)
- **Mục tiêu thử nghiệm**: Bơm song công 64KB qua QUIC bidi stream.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes in / out
- **Lệnh thực thi chuẩn**: wrk -t4 -c50 -d10s -s /tmp/kb35_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **12,406.67** | *N/A* | 7,553.21 | Velda đạt 12,406.67 req/s (+64.3% vs Envoy; Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | 1.45 MB/s | *N/A* | **2.33 MB/s** | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **3.06 ms** | *N/A* | 6.56 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.15 ms** | *N/A* | 0.33 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **2.83 ms** | *N/A* | 5.86 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **4.04 ms** | *N/A* | 10.79 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **6.64 ms** | *N/A* | 19.88 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | *N/A* ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 61.53 ms | *N/A* | **40.93 ms** | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **1.43 ms** | *N/A* | 3.58 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **3.06 ms** | *N/A* | 6.56 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.12 ms** | *N/A* | 0.26 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **125,301 2xx** | *N/A* | 75,595 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **66.28%** | *N/A* | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 93.12 MB | *N/A* | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **36,138,323,382** | 0 | 55,612,853,539 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **582,562.80** | *N/A* | 1,472,562.10 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **43,955,013,369** | 0 | 61,810,734,846 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **708,570.70** | *N/A* | 1,636,674.60 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.82 | *N/A* | **0.90** | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **6,782,979,757** | 0 | 9,850,712,161 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **109,344.10** | *N/A* | 260,835.10 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **78,589,792** | 0 | 153,475,590 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.16%** | *N/A* | 1.56% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 12.45% | *N/A* | **8.56%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 288,538 | 0 | **251,142** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **4.65** | *N/A* | 6.65 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | **1,761,225** | 0 | 1,962,371 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **28.39** | *N/A* | 51.96 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | *N/A* | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 194,597 | 0 | **11,786** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 11.05% | *N/A* | **0.60%** | Tỷ lệ khóa futex |

---

### KB 36: duplex_h1_h3 — Large Duplex H1 to H3 (5MB)
- **Mục tiêu thử nghiệm**: Bơm 5MB song công qua QUIC stream.
- **Kích thước tải trọng & Quy chuẩn**: 5,242,880 bytes in / out
- **Lệnh thực thi chuẩn**: wrk -t4 -c20 -d10s -s /tmp/kb36_433.lua http://192.168.122.14:8080/
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **1,169.92** | *N/A* | 1,167.94 | Velda đạt 1,169.92 req/s (+0.2% vs Envoy; Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | **0.16 MB/s** | *N/A* | 0.15 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **19.92 ms** | *N/A* | 20.04 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **1.00 ms** | *N/A* | 1.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 9.73 ms | *N/A* | **8.00 ms** | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **51.17 ms** | *N/A* | 52.97 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 75.80 ms | *N/A* | **75.65 ms** | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | *N/A* ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 118.45 ms | *N/A* | **103.43 ms** | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **20.63 ms** | *N/A* | 21.26 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **19.92 ms** | *N/A* | 20.04 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.80 ms** | *N/A* | 0.80 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **11,711 2xx** | *N/A* | 11,709 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | *N/A* | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 85.00 MB | *N/A* | **6.87 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **29,327,144,850** | 0 | 30,277,774,857 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **5,013,530.00** | *N/A* | 5,184,816.80 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **47,005,277,770** | 0 | 48,544,718,334 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **8,035,639.70** | *N/A* | 8,312,878.80 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.62** | *N/A* | 0.62 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **5,291,671,687** | 0 | 5,497,107,892 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **904,621.10** | *N/A* | 941,334.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **74,822,389** | 0 | 75,297,517 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.41% | *N/A* | **1.37%** | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 8.60% | *N/A* | **8.21%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 133,970 | 0 | **127,767** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 22.90 | *N/A* | **21.88** | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | **1,579,853** | 0 | 2,146,399 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **270.08** | *N/A* | 367.55 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | *N/A* | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | **10,474** | 0 | 10,899 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.66% | *N/A* | **0.51%** | Tỷ lệ khóa futex |

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
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 7.98 MB | 131.95 MB | **6.83 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 1,378,875,324 | **1,290,188,384** | 1,729,899,877 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.0 | 0.0 | 0.0 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 2,358,142,375 | **2,099,991,802** | 2,503,263,324 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.0 | 0.0 | 0.0 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.58 | 0.61 | **0.69** | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 245,187,345 | **228,678,563** | 310,466,420 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.0 | 0.0 | 0.0 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 7,502,539 | **6,684,155** | 8,221,037 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 3.06% | 2.92% | **2.65%** | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 7.58% | 7.45% | **6.89%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 38,762 | 37,341 | **37,181** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.0 | 0.0 | 0.0 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 60,792 | **53,245** | 53,900 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.0 | 0.0 | 0.0 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 11,463 | 11,421 | **11,261** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | **18.86%** | 21.45% | 20.89% | Tỷ lệ khóa futex |

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
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 7.96 MB | 131.87 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **1,218,956,592** | 1,278,251,635 | 1,277,390,430 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.0 | 0.0 | 0.0 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 2,126,016,806 | **2,085,381,895** | 2,096,300,087 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.0 | 0.0 | 0.0 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.57 | **0.61** | 0.61 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **213,315,462** | 226,023,284 | 226,752,243 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.0 | 0.0 | 0.0 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 6,561,430 | 6,424,263 | **6,323,570** | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 3.08% | 2.84% | **2.79%** | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 7.74% | 7.50% | **7.43%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **36,773** | 38,316 | 37,867 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.0 | 0.0 | 0.0 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 59,129 | 56,301 | **53,502** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.0 | 0.0 | 0.0 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | **11,052** | 11,667 | 11,383 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | **18.69%** | 20.72% | 21.28% | Tỷ lệ khóa futex |

---

### KB 39: Header Flooding & Giant Line (8KB Header, 250+ Junk Headers)
- **Mục tiêu thử nghiệm**: Kiểm tra giới hạn phòng vệ cạn kiệt tài nguyên bộ nhớ header table (RFC 9112).
- **Kích thước tải trọng & Quy chuẩn**: 8KB oversized header line, 250 header fields
- **Lệnh thực thi chuẩn**: python3 test_header_flood.py --target http://192.168.122.14:8080/ (8KB Header, 250+ Junk Headers)
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **8,868.04** | 4,323.83 | 1,368.21 | Velda đạt 8,868.04 req/s (+105.1% vs Nginx, +548.1% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **2.07 MB/s** | 1.11 MB/s | 0.38 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **4.13 ms** | 7.58 ms | 7.49 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.21 ms** | 0.38 ms | 0.38 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **2.13 ms** | 8.69 ms | 7.80 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **10.69 ms** | 11.46 ms | 12.24 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **13.26 ms** | 14.02 ms | 15.75 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 29.53 ms | **20.20 ms** | 26.26 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 3.94 ms | **3.85 ms** | 4.04 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **4.13 ms** | 7.58 ms | 7.49 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.17 ms** | 0.30 ms | 0.30 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **89,379 2xx** | 43,531 2xx | 13,745 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **99.65%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 22.12 MB | 135.96 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **10,118,258,084** | 10,573,545,818 | 14,457,671,386 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **228,196.00** | 489,082.40 | 2,113,370.20 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 16,636,743,045 | 14,235,580,953 | **13,077,914,878** | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **375,206.80** | 658,470.90 | 1,911,682.40 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.61 | 0.74 | **1.11** | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **2,040,253,680** | 2,230,756,760 | 2,867,495,165 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **46,013.60** | 103,184.30 | 419,160.10 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 37,950,306 | **33,870,280** | 37,911,259 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.86% | 1.52% | **1.32%** | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 10.22% | 8.37% | **5.50%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 87,048 | 86,455 | **72,568** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **1.96** | 4.00 | 10.61 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 564,252 | 319,413 | **273,180** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **12.73** | 14.77 | 39.93 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 28,118 | 11,685 | **11,385** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 4.98% | **3.66%** | 4.17% | Tỷ lệ khóa futex |

---

### KB 40: Slowloris Attack & Idle Starvation (Drip 1B / 3s, Idle Watchdog)
- **Mục tiêu thử nghiệm**: Mở đồng thời 5,000 socket client, drip 1 byte/3s chiếm FD hệ thống.
- **Kích thước tải trọng & Quy chuẩn**: 5,000 slow connections (1 byte per 3s)
- **Lệnh thực thi chuẩn**: python3 test_slowloris.py --target 192.168.122.14:8080 --sockets 5000 (Drip 1B / 3s, Idle Watchdog)
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 74,749.26 | **83,030.69** | 32,746.52 | Velda đạt 74,749.26 req/s (-10.0% vs Nginx, +128.3% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 9.05 MB/s | **11.96 MB/s** | 4.81 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.67 ms | **0.61 ms** | 1.48 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.02 ms | **0.02 ms** | 0.07 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.57 ms | **0.54 ms** | 1.37 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 1.16 ms | **1.06 ms** | 2.19 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **2.17 ms** | 2.22 ms | 3.77 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 20.36 ms | **9.91 ms** | 13.79 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.43 ms | **0.42 ms** | 0.67 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.67 ms | **0.61 ms** | 1.48 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.03 ms | **0.03 ms** | 0.06 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **754,933 2xx** | 838,612 2xx | 330,733 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 21.94 MB | 148.92 MB | **6.87 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 51,600,455,449 | 51,623,853,915 | **48,642,842,049** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 138,062.80 | **124,348.80** | 297,087.10 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 59,153,139,677 | **58,527,929,566** | 65,073,553,824 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 158,270.80 | **140,979.00** | 397,438.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.87 | **0.88** | 0.75 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 9,592,265,526 | 10,097,608,106 | **8,977,158,723** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 25,665.20 | **24,322.60** | 54,828.20 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **116,928,309** | 124,941,208 | 234,135,048 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.22%** | 1.24% | 2.61% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 10.51% | 10.00% | **9.22%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 223,215 | **197,609** | 209,003 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.60 | **0.48** | 1.28 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,337,965 | 2,934,696 | **1,529,540** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **6.26** | 7.07 | 9.34 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 40,191 | 12,937 | **11,397** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 1.72% | **0.44%** | 0.75% | Tỷ lệ khóa futex |

---

### KB 41: Chaos Upstream Sudden Death (TCP RST Injection & Instant Failover)
- **Mục tiêu thử nghiệm**: Tiêm TCP RST ngắt 50% backend nodes đột ngột dưới 50,000 RPS, kiểm tra retry & passive health.
- **Kích thước tải trọng & Quy chuẩn**: REST JSON 12KB dưới tác động RST injection
- **Lệnh thực thi chuẩn**: wrk -t6 -c100 -d10s http://192.168.122.14:8080/plain/12kb (with RST chaos & instant failover)
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **48,836.15** | 39,878.94 | 24,053.23 | Velda đạt 48,836.15 req/s (+22.5% vs Nginx, +103.0% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **582.64 MB/s** | 476.71 MB/s | 287.59 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **1.99 ms** | 2.52 ms | 4.01 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **0.10 ms** | 0.13 ms | 0.20 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **1.80 ms** | 2.14 ms | 3.78 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **3.28 ms** | 4.72 ms | 5.82 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **5.29 ms** | 7.88 ms | 9.13 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 22.06 ms | **20.57 ms** | 43.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **1.02 ms** | 1.65 ms | 1.61 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **1.99 ms** | 2.52 ms | 4.01 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.08 ms** | 0.10 ms | 0.16 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **493,213 2xx** | 402,749 2xx | 242,930 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 42.12 MB | 148.32 MB | **6.84 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 49,851,987,735 | **43,351,424,396** | 43,613,070,808 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **204,160.20** | 217,415.10 | 362,638.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 61,653,780,936 | **55,346,001,661** | 64,957,435,480 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **252,492.40** | 277,570.10 | 540,114.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.81** | 0.78 | 0.67 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 8,670,440,638 | 8,095,311,327 | **7,659,841,136** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **35,508.30** | 40,599.40 | 63,690.70 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 110,165,336 | **105,326,186** | 168,407,422 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.27%** | 1.30% | 2.20% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 13.56% | 11.91% | **10.47%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 99,295 | **95,572** | 136,060 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.41** | 0.48 | 1.13 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,977,883 | 2,778,855 | **1,568,833** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **12.20** | 13.94 | 13.04 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 19,906 | 11,710 | **11,578** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.67% | **0.42%** | 0.74% | Tỷ lệ khóa futex |

---

### KB 42: Cgroup RAM 60MB Starvation (O(1) Memory Footprint vs OOM Killer)
- **Mục tiêu thử nghiệm**: Giới hạn memory.max = 60M, dội đồng thời 100 stream 10MB chunked download.
- **Kích thước tải trọng & Quy chuẩn**: 100 concurrent 10MB chunked streams dưới cgroup 60MB
- **Lệnh thực thi chuẩn**: wrk -t4 -c30 -d10s http://192.168.122.14:8080/stream/plain/10mb (under Cgroup RAM 60MB limitation)
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **435.36** | 159.72 | 262.32 | Velda đạt 435.36 req/s (+172.6% vs Nginx, +66.0% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | **3,051.52 MB/s** | 1,607.68 MB/s | 2,631.68 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **63.87 ms** | 288.10 ms | 106.32 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | **3.19 ms** | 14.40 ms | 5.32 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **76.92 ms** | 114.89 ms | 99.17 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | **108.01 ms** | 894.73 ms | 159.91 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | **139.12 ms** | 1,510.00 ms | 225.23 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **181.85 ms** | 1,620.00 ms | 310.34 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 41.16 ms | 369.41 ms | **39.04 ms** | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **63.87 ms** | 288.10 ms | 106.32 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **2.56 ms** | 11.52 ms | 4.25 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **4,397 2xx** | 1,613 2xx | 2,624 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **90.95%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 27.61 MB | 148.30 MB | **6.88 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **37,112,944,985** | 45,734,850,181 | 49,993,378,556 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | **17,049,313.20** | 57,268,783.10 | 38,116,330.10 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **50,297,475,921** | 51,746,695,737 | 53,236,931,327 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **23,106,154.00** | 64,796,764.00 | 40,589,304.20 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.74 | 0.88 | **0.94** | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **5,766,672,148** | 7,829,186,957 | 6,023,663,948 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **2,649,151.10** | 9,803,640.10 | 4,592,607.50 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 65,824,634 | 75,346,450 | **62,986,834** | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.14% | **0.96%** | 1.05% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 30.18% | **17.18%** | 19.91% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 61,715 | **49,195** | 50,580 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **28.35** | 61.60 | 38.56 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | **1,241,808** | 3,311,172 | 1,573,076 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **570.47** | 4,146.22 | 1,199.36 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 16,596 | **10,936** | 11,456 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 1.34% | **0.33%** | 0.73% | Tỷ lệ khóa futex |

---

### KB 43: CPU 100% Saturation & Adaptive Overload Shedding (CoDel Shedder)
- **Mục tiêu thử nghiệm**: Ép máy chủ 100% CPU utilization, dội tải đột biến 300,000 RPS kiểm tra early drop 503.
- **Kích thước tải trọng & Quy chuẩn**: 300,000 RPS flood
- **Lệnh thực thi chuẩn**: wrk -t6 -c300 -d10s http://192.168.122.14:8080/plain/ping (CPU 100% saturation flood)
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 61,837.70 | **74,677.72** | 31,279.96 | Velda đạt 61,837.70 req/s (-17.2% vs Nginx, +97.7% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 7.49 MB/s | **10.75 MB/s** | 4.59 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 4.96 ms | **4.08 ms** | 9.67 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.25 ms | **0.20 ms** | 0.48 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 4.58 ms | **3.73 ms** | 8.97 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 7.89 ms | **6.67 ms** | 12.47 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 13.87 ms | **10.70 ms** | 17.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **43.06 ms** | 45.64 ms | 104.36 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 2.61 ms | **2.16 ms** | 3.90 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 4.96 ms | **4.08 ms** | 9.67 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.20 ms | **0.16 ms** | 0.39 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **624,566 2xx** | 754,257 2xx | 315,925 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 30.91 MB | 156.73 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 47,502,252,335 | 49,104,981,214 | **45,640,196,571** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 153,635.20 | **131,511.70** | 291,817.50 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 60,102,380,549 | **59,812,419,692** | 65,395,247,886 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 194,387.50 | **160,188.10** | 418,128.70 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.79 | **0.82** | 0.70 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 8,795,741,111 | 9,580,625,157 | **8,412,943,240** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 28,447.80 | **25,658.60** | 53,791.30 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **104,630,969** | 111,720,189 | 178,094,103 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.19% | **1.17%** | 2.12% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 10.62% | 10.23% | **9.47%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 172,235 | **126,330** | 190,839 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.56 | **0.34** | 1.22 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 2,089,731 | 2,683,298 | **1,376,991** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **6.76** | 7.19 | 8.80 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 12,928 | 11,282 | **11,050** | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.62% | **0.42%** | 0.80% | Tỷ lệ khóa futex |

---

### KB 44: Atomic Hot-Reload Contention (100 Reloads/s at 100K RPS, Zero Dropped Conns)
- **Mục tiêu thử nghiệm**: Kích hoạt 100 lần reload runtime snapshot/giây dưới 100,000 RPS, kiểm tra zero-lock read path.
- **Kích thước tải trọng & Quy chuẩn**: 100,000 RPS + 100 atomic snapshot swaps/sec
- **Lệnh thực thi chuẩn**: wrk -t6 -c150 -d10s http://192.168.122.14:8080/plain/ping (100 atomic reloads/s under load)
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 67,872.59 | **77,845.90** | 33,075.99 | Velda đạt 67,872.59 req/s (-12.8% vs Nginx, +105.2% vs Envoy) |
| | **Băng thông truyền tải (Throughput MB/s)** | 8.22 MB/s | **11.21 MB/s** | 4.86 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 2.24 ms | **1.98 ms** | 4.52 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.11 ms | **0.10 ms** | 0.23 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 2.05 ms | **1.80 ms** | 4.37 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 3.65 ms | **3.52 ms** | 6.14 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 5.79 ms | **5.49 ms** | 8.32 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.0 ms | 0.0 ms | 0.0 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | **20.76 ms** | 27.72 ms | 32.33 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **1.13 ms** | 1.20 ms | 1.47 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 2.24 ms | **1.98 ms** | 4.52 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.09 ms | **0.08 ms** | 0.18 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **685,507 2xx** | 786,198 2xx | 334,061 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 25.00 MB | 152.93 MB | **6.86 MB** | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 47,887,291,025 | 48,486,878,161 | **45,859,309,924** | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 141,109.40 | **124,571.40** | 277,296.70 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 59,540,541,361 | **57,674,918,633** | 64,528,780,394 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 175,448.00 | **148,177.20** | 390,185.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.80 | **0.84** | 0.71 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 8,576,318,721 | 9,458,582,155 | **8,465,317,706** | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 25,271.80 | **24,300.80** | 51,187.10 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 113,132,775 | **111,090,873** | 193,933,844 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.32% | **1.17%** | 2.29% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 10.21% | 10.21% | **9.43%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 171,383 | **134,327** | 193,581 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.51 | **0.35** | 1.17 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 1,986,213 | 2,665,950 | **1,404,316** | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | **5.85** | 6.85 | 8.49 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.50s** | 3.50s | 3.50s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 14,841 | **10,874** | 12,682 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.75% | **0.41%** | 0.90% | Tỷ lệ khóa futex |

---
