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
- **Lệnh thực thi chuẩn**: Cố định **1,000,000 requests** (`wrk -t5 -c200` xả hết công suất)
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31.6** (C Mainline) | **Envoy 1.31** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Thời gian hoàn thành (Completion Time)** | **16.94 s** | 17.11 s | 34.32 s | Velda về đích nhanh nhất (-1.0% vs Nginx, nhanh hơn **2.0x** Envoy) |
| | **Throughput (req/s hoặc transfers/s)** | **59,040.33** | 58,452.72 | 29,135.23 | Velda vượt Nginx **+1.0%**; vượt Envoy **+102.6%** |
| | **Băng thông truyền tải (Throughput MB/s)** | 7.15 MB/s | **8.42 MB/s** | 4.28 MB/s | Nginx trả thêm banner header nên byte/req lớn hơn |
| | **Độ trễ trung bình (Mean Latency)** | 3.409 ms | **3.120 ms** | 6.717 ms | Nginx nhỉnh hơn ở mean do multi-process không context switch |
| | **Độ trễ tối thiểu (Min Latency)** | 0.073 ms (73 µs) | **0.044 ms** (44 µs) | 0.196 ms (196 µs) | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 3.264 ms | **2.056 ms** | 6.556 ms | P50 Velda nhanh hơn Envoy gấp **2.0x** |
| | **Độ trễ phân vị P90** | **5.105 ms** | 7.781 ms | 10.072 ms | Velda nhanh hơn Nginx **34.4%**, nhanh hơn Envoy **49.3%** |
| | **Độ trễ phân vị P99 (Tail Latency)** | **7.346 ms** | 12.331 ms | 14.147 ms | Tail latency Velda giảm **40.4%** vs Nginx, **48.1%** vs Envoy |
| | **Độ trễ phân vị P99.9** | **13.148 ms** | 17.266 ms | 17.998 ms | Velda kiểm soát đuôi trễ dưới 13.2ms; Nginx lên 17.3ms |
| | **Độ trễ tối đa (Max Latency)** | **29.516 ms** | 48.841 ms | 45.155 ms | Velda kiểm soát jitter tối đa tốt nhất (-39.6% vs Nginx) |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **1.411 ms** | 3.076 ms | 2.649 ms | Velda ổn định nhất: SD chỉ bằng **45.9%** Nginx |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 3.409 ms | **3.120 ms** | 6.717 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.150 ms | **0.140 ms** | 0.220 ms | Bắt tay kết nối HTTP/1.1 keepalive pool |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **1,000,000 2xx** (0 err) | **1,000,000 2xx** (0 err) | **1,000,000 2xx** (0 err) | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói hay timeout |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **27.0 MB** | 156.2 MB | 63.5 MB | Velda tiêu tốn RAM chỉ bằng **1/5.8** Nginx, **1/2.4** Envoy |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 97,931,018,183 | **91,176,911,521** | 248,454,440,963 | Envoy tốn nhiều lệnh nhất dù throughput thấp nhất |
| | **Số lệnh CPU / Request (`insn / req`)** | 97,931.0 | **91,176.9** | 248,454.4 | Envoy tốn gấp **2.54x** số lệnh CPU trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 112,897,929,667 | **102,405,439,704** | 335,100,462,482 | Chu kỳ xung nhịp CPU thực tế trên 1M request |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 112,897.9 | **102,405.4** | 335,100.5 | Envoy tốn gấp **2.97x** chu kỳ CPU so với Velda |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.867 | **0.890** | 0.741 | Velda & Nginx đạt IPC cao nhất (+17.0% vs Envoy) |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **17,384,050,160** | 17,899,826,951 | 44,640,761,973 | Velda tối ưu luồng thực thi ít phân nhánh nhất |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **17,384.1** | 17,899.8 | 44,640.8 | Velda ít rẽ nhánh nhất: Envoy gấp **2.57x** |
| | **CPU đoán sai nhánh (`branch-misses`)** | **196,282,430** | 221,038,701 | 1,063,510,124 | Velda đoán sai nhánh ít nhất |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.13%** | 1.23% | 2.38% | Dự đoán nhánh Velda chính xác nhất: 1.13% vs 2.38% (Envoy) |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 10.91% | 10.79% | **8.74%** | Miss rate L1 Data Cache |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | KVM vCPU không expose LLC counter uncore |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 281,436 | **246,537** | 517,117 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.281 | **0.247** | 0.517 | Envoy tốn CS/req gấp **1.8x** Velda |
| | **Tổng số System Calls (`syscalls`)** | **4,142,659** | 5,833,876 | 6,702,987 | Velda tiết kiệm syscalls nhất (-29.0% vs Nginx, -38.2% vs Envoy) |
| | **System Calls / Request (`syscalls / req`)** | **4.14** | 5.83 | 6.70 | Velda chỉ tốn **4.14** syscalls/req (Nginx 5.83, Envoy 6.70) |
| | **Thời gian chạy trong kernel (Kernel Time)** | **~3.95s** | ~4.50s | ~8.20s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 9,065 | **0** | 446 | Nginx multi-process không futex; Velda dùng Tokio runtime |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | **0.22%** | 0.00% | 0.01% | Contention lock trong runtime Tokio của Velda dưới 0.22% |

---

### KB 02: buffered_h1_h1 — Medium Payload (REST JSON 12KB)
- **Mục tiêu thử nghiệm**: Đo lường ngưỡng tải REST Microservices điển hình với body 12KB.
- **Kích thước tải trọng & Quy chuẩn**: 12,286 bytes Content-Length JSON body
- **Lệnh thực thi chuẩn**: `wrk -t6 -c200 -d10s --latency -s compare/wrk_433_12kb.lua http://192.168.122.14:8080/`
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31.6** (C Mainline) | **Envoy 1.31** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **45,879.67** | 35,661.44 | 23,933.86 | Velda vượt Nginx 1.31.6 **+28.7%**, vượt Envoy **+91.7%** |
| | **Băng thông truyền tải (Throughput MB/s)** | **547.37 MB/s** | 426.33 MB/s | 286.16 MB/s | Velda đạt ~4.38 Gbps goodput (+121.04 MB/s vs Nginx) |
| | **Độ trễ trung bình (Mean Latency)** | **4.305 ms** | 6.164 ms | 8.254 ms | Velda thấp hơn Nginx **30.2%**, thấp hơn Envoy **47.8%** |
| | **Độ trễ tối thiểu (Min Latency)** | 0.112 ms (112 µs) | **0.091 ms** (91 µs) | 0.157 ms (157 µs) | Ngưỡng trễ gói phản hồi đầu tiên |
| | **Độ trễ phân vị P50** | **4.148 ms** | 5.324 ms | 7.989 ms | Velda thấp hơn Nginx 1.18ms, thấp hơn Envoy 3.84ms |
| | **Độ trễ phân vị P90** | **6.327 ms** | 13.132 ms | 11.437 ms | Velda tốt hơn Nginx **51.8%**, tốt hơn Envoy **44.7%** |
| | **Độ trễ phân vị P99 (Tail Latency)** | **9.046 ms** | 21.215 ms | 16.279 ms | Tail latency Velda giảm **57.4%** vs Nginx, **44.4%** vs Envoy |
| | **Độ trễ phân vị P99.9** | **13.593 ms** | 27.126 ms | 28.727 ms | Velda kiểm soát đuôi trễ dưới 13.6ms; Nginx/Envoy vọt ~27-29ms |
| | **Độ trễ tối đa (Max Latency)** | **21.969 ms** | 53.610 ms | 64.746 ms | Jitter tối đa của Velda thấp hơn một nửa so với Nginx & Envoy |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **1.622 ms** | 4.999 ms | 2.807 ms | Velda ổn định nhất: SD chỉ bằng ~1/3 Nginx 1.31.6 |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **4.305 ms** | 6.164 ms | 8.254 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.170 ms** | 0.160 ms | 0.260 ms | Bắt tay kết nối HTTP/1.1 keepalive pool |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **463,375 2xx** (0 err) | 360,165 2xx (0 err) | 241,733 2xx (0 err) | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói hay timeout |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **53.5 MB** | 158.1 MB | 66.4 MB | Velda tiêu tốn RAM chỉ bằng **1/3.0** Nginx 1.31.6, ít hơn Envoy |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 56,025,518,529 | 60,416,112,127 | 67,920,730,434 | Velda ít tốn lệnh CPU nhất dù xử lý nhiều requests nhất |
| | **Số lệnh CPU / Request (`insn / req`)** | **120,907.5** | 167,745.6 | 280,974.2 | Nginx tốn hơn **38.7%**, Envoy tốn gấp **2.32x** Velda |
| | **Tổng chu kỳ CPU (`cycles`)** | 70,897,336,285 | 71,299,124,943 | 92,734,533,550 | Chu kỳ xung nhịp CPU thực tế |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **153,002.1** | 197,962.4 | 383,623.8 | Nginx tốn hơn **29.4%**, Envoy tốn gấp **2.51x** Velda |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.790 | **0.847** | 0.732 | IPC của Nginx và Velda trong khoảng tối ưu L1/L2 |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 9,594,254,411 | 11,393,338,976 | 11,500,708,791 | Velda tối ưu luồng thực thi ít phân nhánh nhất |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **20,705.2** | 31,633.7 | 47,576.1 | Nginx rẽ nhánh nhiều hơn 52.8%, Envoy gấp **2.30x** |
| | **CPU đoán sai nhánh (`branch-misses`)** | **113,458,390** | 142,005,699 | 262,144,143 | Velda đoán sai nhánh ít nhất |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.18%** | 1.25% | 2.28% | Velda đoán nhánh chính xác nhất: 1.18% vs 2.28% (Envoy) |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 15.31% | 11.69% | **9.26%** | L1 Data Cache Miss rate khi đệm 12KB chunk payload |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | KVM vCPU không expose LLC counter uncore |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 88,290 | 108,974 | 117,964 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.191** | 0.303 | 0.488 | Velda ít context-switches nhất: Envoy gấp **2.55x** |
| | **Tổng số System Calls (`syscalls`)** | 2,985,151 | 3,959,888 | **1,551,253** | Velda giảm **-24.6%** tổng syscalls so với Nginx 1.31.6 |
| | **System Calls / Request (`syscalls / req`)** | **6.44** | 10.99 | 6.42 | Nginx tốn gấp **1.71x** syscalls/request so với Velda |
| | **Thời gian chạy trong kernel (Kernel Time)** | **~2.85s** | ~3.15s | ~3.40s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 4,730 | **0** | 110 | Nginx multi-process không futex; Velda dùng Tokio runtime |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | **0.16%** | 0.00% | 0.01% | Contention lock trong runtime Tokio của Velda dưới 0.16% |

---

### KB 03: buffered_h1_h1 — Large Payload (RAM Blob 1MB)
- **Mục tiêu thử nghiệm**: Nạp trọn vẹn 1MB vào bộ nhớ RAM buffer, kiểm tra hiệu năng cấp phát vector allocator & throughput tải nặng.
- **Kích thước tải trọng & Quy chuẩn**: 1,048,576 bytes Content-Length body
- **Lệnh thực thi chuẩn**: `wrk -t6 -c60 -d10s --latency -s compare/wrk_433_1mb.lua http://192.168.122.14:8080/`
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31.6** (C Mainline) | **Envoy 1.31** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **2,969.44** | 1,994.46 | 2,386.35 | Velda vượt Nginx 1.31.6 **+48.9%**, vượt Envoy **+24.4%** |
| | **Băng thông truyền tải (Throughput MB/s)** | **2,969.60 MB/s** | 1,996.80 MB/s | 2,385.92 MB/s | Velda đạt ~23.76 Gbps (+972.80 MB/s vs Nginx, +583.68 MB/s vs Envoy) |
| | **Độ trễ trung bình (Mean Latency)** | **20.238 ms** | 47.336 ms | 25.269 ms | Velda thấp hơn Nginx **57.2%**, thấp hơn Envoy **19.9%** |
| | **Độ trễ tối thiểu (Min Latency)** | 3.578 ms | **0.823 ms** | 0.948 ms | Gói phản hồi đầu tiên trong buffer pipelining |
| | **Độ trễ phân vị P50** | 19.201 ms | **14.852 ms** | 24.195 ms | P50 Nginx thấp do buffer trả từng đợt, nhưng nghẽn đuôi P90/P99 |
| | **Độ trễ phân vị P90** | **29.192 ms** | 133.040 ms | 39.425 ms | Velda tốt hơn Nginx **78.1%** (Nginx trễ vọt gấp 4.5x), tốt hơn Envoy **26.0%** |
| | **Độ trễ phân vị P99 (Tail Latency)** | **41.893 ms** | 226.205 ms | 56.373 ms | Đuôi trễ Velda giảm **81.5%** vs Nginx (Nginx gấp **5.4x**), giảm **25.7%** vs Envoy |
| | **Độ trễ phân vị P99.9** | **58.332 ms** | 296.613 ms | 75.011 ms | Velda giữ đuôi trễ dưới 58ms; Nginx vọt gần 300ms do backpressure |
| | **Độ trễ tối đa (Max Latency)** | **76.210 ms** | 352.385 ms | 94.816 ms | Max jitter Velda thấp hơn Nginx gấp **4.6x** |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **6.981 ms** | 55.746 ms | 10.884 ms | Velda ổn định vượt trội: SD chỉ bằng **1/8.0** Nginx |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **20.238 ms** | 47.336 ms | 25.269 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.180 ms** | 0.170 ms | 0.260 ms | Bắt tay kết nối HTTP/1.1 keepalive pool |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **29,717 2xx** (0 err) | 20,144 2xx (0 err) | 23,871 2xx (0 err) | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói hay timeout |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **87.6 MB** | 150.8 MB | 113.7 MB | Velda tiêu tốn RAM chỉ bằng **58.1%** Nginx, **77.0%** Envoy |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 40,918,035,812 | 86,580,079,335 | 74,606,730,869 | Velda tốn ít lệnh CPU nhất dù đẩy throughput cao nhất |
| | **Số lệnh CPU / Request (`insn / req`)** | **1,376,923.5** | 4,298,057.9 | 3,125,413.1 | Nginx tốn gấp **3.12x**, Envoy tốn gấp **2.27x** Velda |
| | **Tổng chu kỳ CPU (`cycles`)** | 75,370,581,142 | 85,167,109,202 | 86,101,162,058 | Chu kỳ xung nhịp CPU thực tế |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **2,536,278.3** | 4,227,914.5 | 3,606,935.7 | Nginx tốn hơn 66.7%, Envoy tốn hơn 42.2% trên mỗi req |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.543 | **1.017** | 0.866 | Nginx IPC cao do vòng lặp copy bộ đệm nhỏ; Velda zero-copy |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 5,628,347,102 | 15,264,501,317 | 7,703,263,303 | Velda tối ưu rẽ nhánh thấp nhất |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **189,401.6** | 757,769.1 | 322,693.3 | Nginx rẽ nhánh gấp **4.0x**, Envoy gấp **1.7x** Velda |
| | **CPU đoán sai nhánh (`branch-misses`)** | **57,001,840** | 131,815,330 | 105,673,513 | Velda đoán sai nhánh ít nhất |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.01% | **0.86%** | 1.37% | Tỷ lệ đoán sai nhánh của cả 3 trong ngưỡng rất tốt (<1.4%) |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 41.20% | 13.87% | 16.49% | L1 miss Velda cao do buffer 1MB đọc trực tiếp socket-to-socket |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | KVM vCPU không expose LLC counter uncore |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 22,589 | **20,657** | 28,015 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.760** | 1.025 | 1.174 | Velda ít context switches trên mỗi request nhất |
| | **Tổng số System Calls (`syscalls`)** | **1,431,471** | 8,059,100 | 2,243,777 | Nginx tốn gấp **5.6x** syscalls so với Velda |
| | **System Calls / Request (`syscalls / req`)** | **48.17** | 400.07 | 94.00 | Nginx tốn gấp **8.3x**, Envoy tốn gấp **1.95x** syscalls/req |
| | **Thời gian chạy trong kernel (Kernel Time)** | **~2.80s** | ~3.50s | ~3.30s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 4,019 | **0** | 3,604 | Nginx multi-process không futex; Velda & Envoy dùng async runtime |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.28% | **0.00%** | 0.16% | Contention lock trong runtime Tokio của Velda dưới 0.3% |

---

### KB 04: buffered_h1_h2 — Micro Payload (~64B Bridge to H2 Multiplex)
- **Mục tiêu thử nghiệm**: Chuyển tiếp H1 downstream thành stream ghép kênh trên H2 upstream connection pool.
- **Kích thước tải trọng & Quy chuẩn**: ~64 bytes headers (GET /ping HTTP/1.1)
- **Lệnh thực thi chuẩn**: `wrk -t6 -c200 -d10s --latency -s compare/wrk_433_h2_ping.lua http://192.168.122.14:8080/`
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31.6** (C Mainline) | **Envoy 1.31** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **57,631.63** | 59,218.50 | 42,895.04 | Velda vượt Envoy **+34.4%**; bám sát Nginx 1.31.6 (-2.7%) |
| | **Băng thông truyền tải (Throughput MB/s)** | **6.98 MB/s** | 8.53 MB/s | 6.30 MB/s | Nginx trả thêm banner headers nên tổng bytes nhỉnh hơn |
| | **Độ trễ trung bình (Mean Latency)** | **3.460 ms** | 3.707 ms | 4.742 ms | Velda thấp hơn Nginx **6.7%**, thấp hơn Envoy **27.0%** |
| | **Độ trễ tối thiểu (Min Latency)** | 0.076 ms (76 µs) | **0.061 ms** (61 µs) | 0.256 ms (256 µs) | Ngưỡng trễ phản hồi gói đầu tiên qua H2 stream multiplex |
| | **Độ trễ phân vị P50** | 3.257 ms | **3.122 ms** | 4.079 ms | Velda và Nginx tương đương (~3.1 - 3.2ms), Envoy chậm hơn 30% |
| | **Độ trễ phân vị P90** | **5.373 ms** | 7.531 ms | 7.758 ms | Velda tốt hơn Nginx **28.7%**, tốt hơn Envoy **30.7%** |
| | **Độ trễ phân vị P99 (Tail Latency)** | **8.148 ms** | 12.708 ms | 13.346 ms | Tail latency Velda giảm **35.9%** vs Nginx, **38.9%** vs Envoy |
| | **Độ trễ phân vị P99.9** | **14.849 ms** | 34.640 ms | 21.245 ms | Velda kiểm soát đuôi trễ dưới 15ms; Nginx vọt gấp **2.3x** |
| | **Độ trễ tối đa (Max Latency)** | **38.926 ms** | 49.580 ms | 35.171 ms | Velda và Envoy kiểm soát jitter tối đa tốt hơn Nginx |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **1.683 ms** | 3.160 ms | 2.483 ms | Velda ổn định nhất: SD chỉ bằng gần một nửa Nginx |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **3.460 ms** | 3.707 ms | 4.742 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.150 ms** | 0.140 ms | 0.240 ms | Bắt tay kết nối downstream HTTP/1.1 |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **582,059 2xx** (0 err) | 598,159 2xx (0 err) | 429,153 2xx (0 err) | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | 0 lỗi kết nối, 0 timeout |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **19.5 MB** | 151.6 MB | 59.2 MB | Velda tiêu tốn RAM chỉ bằng **1/7.8** Nginx, **1/3.0** Envoy |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 62,161,707,501 | 52,544,145,393 | 89,321,765,026 | Envoy tốn nhiều lệnh CPU nhất |
| | **Số lệnh CPU / Request (`insn / req`)** | **106,796.2** | 87,843.1 | 208,135.0 | Envoy tốn gần gấp đôi số lệnh CPU so với Velda |
| | **Tổng chu kỳ CPU (`cycles`)** | 70,442,846,915 | 67,730,933,694 | 112,049,791,937 | Chu kỳ xung nhịp CPU thực tế |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **121,023.5** | 113,232.3 | 261,095.2 | Envoy tốn gấp **2.16x** chu kỳ CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.882** | 0.776 | 0.797 | Velda đạt IPC cao nhất (+13.7% vs Nginx, +10.7% vs Envoy) |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 10,858,784,192 | 10,217,469,556 | 16,148,097,973 | Rẽ nhánh điều hướng stream multiplex |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **18,655.8** | 17,081.5 | 37,627.8 | Envoy rẽ nhánh gấp **2.0x** Velda |
| | **CPU đoán sai nhánh (`branch-misses`)** | 140,076,144 | 133,281,878 | 336,683,284 | Dự đoán nhánh CPU |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.29%** | 1.30% | 2.08% | Velda và Nginx đoán nhánh ngang nhau, Envoy gấp 1.6x |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 9.06% | 11.11% | **8.31%** | Miss rate L1 Data Cache |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | KVM vCPU không expose LLC counter uncore |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 138,664 | 61,101 | **55,157** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.238 | **0.102** | 0.129 | Tác vụ chuyển ngữ cảnh |
| | **Tổng số System Calls (`syscalls`)** | **1,800,700** | 3,247,239 | 1,939,110 | Velda tiết kiệm syscalls nhất (-44.5% vs Nginx) |
| | **System Calls / Request (`syscalls / req`)** | **3.09** | 5.43 | 4.52 | Velda chỉ tốn **3.09** syscalls/req (so với Nginx 5.43, Envoy 4.52) |
| | **Thời gian chạy trong kernel (Kernel Time)** | **~2.35s** | ~2.65s | ~2.80s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 40,768 | **0** | 124 | Wakeup channel H2 multiplex streams trong Tokio task runtime |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 2.26% | **0.00%** | 0.01% | Futex stream synchronization trong Tokio pool |

---

### KB 05: buffered_h1_h2 — Medium Payload (REST 12KB to H2)
- **Mục tiêu thử nghiệm**: Ghép tải 12KB REST JSON vào DATA frames của HTTP/2 connection pool.
- **Kích thước tải trọng & Quy chuẩn**: 12,288 bytes Content-Length JSON body
- **Lệnh thực thi chuẩn**: `wrk -t6 -c200 -d10s --latency -s compare/wrk_433_h2_12kb.lua http://192.168.122.14:8080/`
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31.6** (C Mainline) | **Envoy 1.31** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **41,994.19** | 11,660.74 | 33,636.03 | Velda vượt Nginx **+260.1%** (+3.6x); vượt Envoy **+24.8%** |
| | **Băng thông truyền tải (Throughput MB/s)** | **501.01 MB/s** | 139.39 MB/s | 402.16 MB/s | Velda bão hòa bus mạng 501 MB/s (+3.6x Nginx, +1.25x Envoy) |
| | **Độ trễ trung bình (Mean Latency)** | **4.700 ms** | 18.952 ms | 5.947 ms | Velda thấp hơn Nginx **75.2%** (gần 1/4), thấp hơn Envoy **21.0%** |
| | **Độ trễ tối thiểu (Min Latency)** | 0.165 ms (165 µs) | **0.084 ms** (84 µs) | 0.262 ms (262 µs) | Phản hồi gói đầu tiên qua H2 stream multiplex |
| | **Độ trễ phân vị P50** | **4.541 ms** | 23.730 ms | 5.062 ms | P50 Velda nhanh hơn Nginx gấp **5.2x**, nhanh hơn Envoy 10.3% |
| | **Độ trễ phân vị P90** | **6.989 ms** | 41.155 ms | 10.190 ms | Velda nhanh hơn Nginx gấp **5.9x**, nhanh hơn Envoy **31.4%** |
| | **Độ trễ phân vị P99 (Tail Latency)** | **9.491 ms** | 42.604 ms | 15.220 ms | Tail latency Velda duy trì dưới 10ms; Nginx vọt gấp **4.5x**, Envoy gấp **1.6x** |
| | **Độ trễ phân vị P99.9** | **12.485 ms** | 66.218 ms | 19.971 ms | Velda kiểm soát đuôi trễ dưới 12.5ms; Nginx lên đến 66.2ms |
| | **Độ trễ tối đa (Max Latency)** | **24.002 ms** | 96.599 ms | 26.274 ms | Jitter tối đa của Velda thấp nhất (24ms vs 96.6ms của Nginx) |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **1.755 ms** | 17.361 ms | 2.972 ms | Velda phân phối độ trễ cực kỳ tập trung; Nginx SD gấp **9.9x** |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **4.700 ms** | 18.952 ms | 5.947 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.160 ms** | 0.150 ms | 0.250 ms | Bắt tay kết nối downstream HTTP/1.1 |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **424,132 2xx** (0 err) | 117,781 2xx (0 err) | 339,720 2xx (0 err) | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | 0 lỗi kết nối, 0 timeout, 0 lỗi status |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **27.2 MB** | 158.6 MB | 64.2 MB | Velda tiêu tốn RAM chỉ bằng **1/5.8** Nginx, **1/2.4** Envoy |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 61,752,480,412 | 21,248,125,685 | 86,822,351,520 | Envoy tốn nhiều lệnh nhất dù xử lý ít req hơn Velda |
| | **Số lệnh CPU / Request (`insn / req`)** | **145,597.4** | 180,403.7 | 255,570.3 | Velda xử lý ít lệnh/req nhất (-19.3% vs Nginx, -43.0% vs Envoy) |
| | **Tổng chu kỳ CPU (`cycles`)** | 76,594,206,649 | 29,554,464,689 | 109,000,936,285 | Chu kỳ xung nhịp CPU thực tế |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **180,590.5** | 250,927.3 | 320,855.2 | Velda tiết kiệm chu kỳ CPU vượt trội (-28.0% vs Nginx, -43.7% vs Envoy) |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.806** | 0.719 | 0.797 | Velda đạt IPC cao nhất (+12.1% vs Nginx, +1.1% vs Envoy) |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 10,544,108,108 | 4,002,453,530 | 14,753,817,972 | Phân luồng framing HTTP/2 DATA và H1 serialization |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **24,860.4** | 33,982.2 | 43,429.3 | Velda rẽ nhánh ít nhất/req (-26.8% vs Nginx, -42.8% vs Envoy) |
| | **CPU đoán sai nhánh (`branch-misses`)** | 145,854,675 | 68,423,536 | 282,864,773 | Đoán nhánh phần cứng CPU |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.38%** | 1.71% | 1.92% | Velda kiểm soát branch-miss thấp nhất (1.38% vs 1.71% Nginx, 1.92% Envoy) |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 14.54% | 13.43% | **9.81%** | Tải 12KB payload tạo áp lực lên L1 D-Cache |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | KVM vCPU không expose LLC counter uncore |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 98,934 | 97,491 | **47,014** | Chuyển ngữ cảnh tiến trình/task |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.233 | 0.828 | **0.138** | Nginx tốn CS/req gấp **3.5x** Velda do nghẽn buffer H2 stream |
| | **Tổng số System Calls (`syscalls`)** | 2,064,810 | 1,572,121 | **1,526,580** | Syscalls thực thi I/O |
| | **System Calls / Request (`syscalls / req`)** | 4.87 | 13.35 | **4.49** | Nginx dính phân mảnh syscalls (13.35/req, gấp **2.7x** Velda) |
| | **Thời gian chạy trong kernel (Kernel Time)** | **~2.60s** | ~3.80s | ~3.10s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 27,827 | **0** | 186 | Nginx multi-process không futex; Tokio runtime đồng bộ hoá kênh stream H2 |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 1.35% | **0.00%** | 0.01% | Futex stream coordination trong Tokio task pool |

---

### KB 06: buffered_h1_h2 — Large Payload (1MB to H2 Mux)
- **Mục tiêu thử nghiệm**: Chia nhỏ 1MB thành các DATA frames 16KB truyền qua multiplexed H2 pipe.
- **Kích thước tải trọng & Quy chuẩn**: 1,048,576 bytes body (~1MB)
- **Lệnh thực thi chuẩn**: Cố định **10,000 requests** (~10.5 GB dữ liệu) (`wrk -t5 -c60` full throttle)
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31.6** (C Mainline) | **Envoy 1.31** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Thời gian hoàn thành (Completion Time)** | **6.29 s** | 10.83 s | 6.59 s | Velda về đích nhanh nhất; Nginx chậm hơn **+72.3%** |
| | **Throughput (req/s hoặc transfers/s)** | **1,590.91** | 923.42 | 1,518.11 | Velda vượt Nginx **+72.3%**; vượt Envoy **+4.8%** |
| | **Băng thông truyền tải (Throughput MB/s)** | **1,597.44 MB/s** | 921.60 MB/s | 1,515.52 MB/s | Velda và Envoy chạm trần băng thông mạng VM (~1.6 GB/s) |
| | **Độ trễ trung bình (Mean Latency)** | **36.790 ms** | 42.819 ms | 38.173 ms | Velda thấp hơn Nginx **14.1%**, thấp hơn Envoy **3.6%** |
| | **Độ trễ tối thiểu (Min Latency)** | 5.640 ms | **1.008 ms** | 2.452 ms | Ngưỡng trễ gói 1MB đầu tiên qua H2 stream pipe |
| | **Độ trễ phân vị P50** | 34.782 ms | **33.757 ms** | 33.979 ms | P50 cả 3 proxy tương đương nhau (~34ms) do giới hạn I/O |
| | **Độ trễ phân vị P90** | **51.138 ms** | 84.823 ms | 64.371 ms | Velda thấp hơn Nginx **39.7%**, thấp hơn Envoy **20.6%** |
| | **Độ trễ phân vị P99 (Tail Latency)** | **73.846 ms** | 187.218 ms | 103.573 ms | Đuôi trễ Velda tốt vượt trội: Nginx gấp **2.53x**, Envoy gấp **1.40x** |
| | **Độ trễ phân vị P99.9** | **96.902 ms** | 256.538 ms | 140.096 ms | Velda giữ đuôi trễ dưới 100ms; Nginx vọt lên 256.5ms |
| | **Độ trễ tối đa (Max Latency)** | **137.992 ms** | 282.700 ms | 160.467 ms | Velda kiểm soát jitter tối đa tốt nhất (138ms vs 283ms của Nginx) |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **11.306 ms** | 35.783 ms | 19.671 ms | Độ phân tán của Velda thấp nhất: SD chỉ bằng **1/3.16** Nginx |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **36.790 ms** | 42.819 ms | 38.173 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.250 ms** | 0.280 ms | 0.310 ms | Bắt tay kết nối downstream HTTP/1.1 |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **10,000 2xx** (0 err) | 10,001 2xx (0 err) | 10,002 2xx (0 err) | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | 0 lỗi kết nối, 0 timeout (truyền trọn vẹn 10 GB payload) |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 142.6 MB | 152.8 MB | **91.6 MB** | Bộ đệm streaming chia nhỏ 16KB DATA frames |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **30,187,392,036** | 47,605,517,158 | 41,840,624,663 | Velda tiêu tốn ít lệnh CPU nhất (-36.6% vs Nginx, -27.8% vs Envoy) |
| | **Số lệnh CPU / Request (`insn / req`)** | **3,018,739.2** | 4,760,551.7 | 4,184,062.5 | Nginx tốn hơn **+57.7%**, Envoy tốn hơn **+38.6%** lệnh CPU |
| | **Tổng chu kỳ CPU (`cycles`)** | 61,292,230,004 | **54,274,745,558** | 59,939,422,570 | Chu kỳ xung nhịp CPU thực tế trên 10k request |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 6,129,223.0 | **5,427,474.6** | 5,993,942.3 | Các bên tương đương nhau do xử lý luồng byte lớn |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.493 | **0.877** | 0.698 | Nginx đạt IPC cao nhất ở tác vụ sao chép bộ nhớ thuần |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **4,742,669,776** | 8,515,882,048 | 5,166,508,833 | Nginx tốn gần gấp đôi rẽ nhánh điều hướng frame buffer |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **474,267.0** | 851,588.2 | 516,650.9 | Velda rẽ nhánh ít nhất (-44.3% vs Nginx, -8.2% vs Envoy) |
| | **CPU đoán sai nhánh (`branch-misses`)** | **62,408,077** | 89,408,025 | 67,855,777 | Velda đoán sai nhánh ít nhất |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.32% | **1.05%** | 1.31% | Các bên kiểm soát đoán nhánh tốt |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 32.72% | **14.64%** | 16.30% | Payload 1MB liên tục tràn dung lượng L1 cache của CPU |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | KVM vCPU không expose LLC counter uncore |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 16,770 | **12,177** | 15,220 | Chuyển ngữ cảnh tiến trình/task |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 1.68 | **1.22** | 1.52 | Nginx ít CS/req nhất |
| | **Tổng số System Calls (`syscalls`)** | **1,259,304** | 4,116,312 | 1,300,481 | Nginx vọt gấp **3.27x** syscalls so với Velda |
| | **System Calls / Request (`syscalls / req`)** | **125.93** | 411.63 | 130.05 | Nginx dính phân mảnh syscalls (411 vs 126 của Velda) |
| | **Thời gian chạy trong kernel (Kernel Time)** | **~2.10s** | ~3.40s | ~2.30s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 9,838 | **0** | 538 | Tokio runtime đồng bộ hoá kênh stream H2 |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.78% | **0.00%** | 0.04% | Futex stream synchronization trong Tokio pool |

---

### KB 07: buffered_h1_h3 — Micro Payload (~64B Bridge to H3 QUIC)
- **Mục tiêu thử nghiệm**: Đo chi phí QPACK encoding và UDP socket batching khi bridge H1 sang QUIC upstream.
- **Kích thước tải trọng & Quy chuẩn**: ~64 bytes headers
- **Lệnh thực thi chuẩn**: Cố định **1,000,000 requests** (`wrk -t5 -c200` full throttle)
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31.6** (C Mainline) | **Envoy 1.31** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Thời gian hoàn thành (Completion Time)** | **37.53 s** | *N/A (Không hỗ trợ)* | 50.45 s | Velda về đích nhanh hơn Envoy **+34.4%** (nhanh hơn 12.9 giây) |
| | **Throughput (req/s hoặc transfers/s)** | **26,645.17** | *N/A* | 19,822.42 | Velda vượt Envoy **+34.4%** throughput |
| | **Băng thông truyền tải (Throughput MB/s)** | **3.82 MB/s** | *N/A* | 3.23 MB/s | Băng thông truyền tải phản hồi micro headers |
| | **Độ trễ trung bình (Mean Latency)** | 20.749 ms | *N/A* | **10.519 ms** | Envoy kiểm soát mean latency tốt hơn ở pool UDP lớn |
| | **Độ trễ tối thiểu (Min Latency)** | **0.090 ms** (90 µs) | *N/A* | 0.423 ms (423 µs) | Gói đầu tiên Velda phản hồi nhanh hơn gấp **4.7x** |
| | **Độ trễ phân vị P50** | **6.549 ms** | *N/A* | 8.272 ms | P50 Velda nhanh hơn Envoy **20.8%** |
| | **Độ trễ phân vị P90** | **16.441 ms** | *N/A* | 18.740 ms | P90 Velda duy trì tốt hơn Envoy |
| | **Độ trễ phân vị P99 (Tail Latency)** | 525.365 ms | *N/A* | **33.793 ms** | Đuôi P99 Velda dính backpressure UDP buffer ở 200 conns |
| | **Độ trễ phân vị P99.9** | 992.739 ms | *N/A* | **50.410 ms** | Jitter đuôi sâu trên kênh stream multiplex |
| | **Độ trễ tối đa (Max Latency)** | 1197.455 ms | *N/A* | **75.626 ms** | Trễ tối đa do packet pacing trong QUIC runtime |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 85.882 ms | *N/A* | **6.378 ms** | Envoy phân bổ độ trễ tập trung hơn |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 20.749 ms | *N/A* | **10.519 ms** | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.140 ms** | *N/A* | 0.220 ms | Bắt tay downstream HTTP/1.1 TCP |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **978,623 2xx** (21.3k 5xx) | *N/A* | 986,330 2xx (13.6k 5xx) | Tỷ lệ lỗi dưới 2.1% do giới hạn UDP buffer VM |
| | **Tỷ lệ thành công (Success Rate %)** | **97.9%** | *N/A* | **98.6%** | Cả hai gateway đều đạt trên 97.9% thành công |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 539.4 MB | *N/A* | **64.7 MB** | Velda cấp phát ring-buffer UDP streams cho 1M requests |
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
- **Lệnh thực thi chuẩn**: `wrk -t6 -c40 -d10s --latency http://192.168.122.14:8080/bridge-h3/1mb`
- **Nhánh Upstream**: Dual-Upstream (50% H3 Cleartext UDP : 50% TLS 1.3 H3 ALPN)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 10: server_stream_h1_h1 — Micro Payload (SSE Ping / Heartbeat ~64B)
- **Mục tiêu thử nghiệm**: Đo độ trễ chuyển tiếp từng chunk cực nhỏ (Server-Sent Events) không đọng buffer.
- **Kích thước tải trọng & Quy chuẩn**: 4-64 bytes SSE chunked frames
- **Lệnh thực thi chuẩn**: `wrk -t4 -c100 -d10s --latency http://192.168.122.14:8080/stream/sse-ping`
- **Nhánh Upstream**: Dual-Upstream (50% Cleartext h1 : 50% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 11: server_stream_h1_h1 — Medium Payload (Chunked Stream 64KB)
- **Mục tiêu thử nghiệm**: Upstream đẩy luồng dữ liệu 64KB qua 16 chunk 4KB, gateway forward trực tiếp.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes (16 chunks x 4KB)
- **Lệnh thực thi chuẩn**: `wrk -t6 -c150 -d10s --latency http://192.168.122.14:8080/stream/64kb`
- **Nhánh Upstream**: Dual-Upstream (50% Cleartext h1 : 50% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 12: server_stream_h1_h1 — Large Payload (10MB Download - Chunked)
- **Mục tiêu thử nghiệm**: Tải luồng file 10MB chunked, kiểm tra khả năng kiểm soát backpressure và tail latency P99.
- **Kích thước tải trọng & Quy chuẩn**: 10,485,760 bytes chunked stream
- **Lệnh thực thi chuẩn**: `wrk -t4 -c20 -d10s --latency http://192.168.122.14:8080/stream/10mb`
- **Nhánh Upstream**: Dual-Upstream (50% Cleartext h1 : 50% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 13: server_stream_h1_h2 — Micro SSE via H2
- **Mục tiêu thử nghiệm**: Nhận SSE stream từ H2 DATA frame và chuyển đổi thành H1 Chunked Transfer Encoding.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes SSE chunks
- **Lệnh thực thi chuẩn**: `wrk -t4 -c100 -d10s --latency http://192.168.122.14:8080/stream-h2/sse`
- **Nhánh Upstream**: Dual-Upstream (50% h2c Cleartext : 50% TLS 1.3 h2 ALPN)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 14: server_stream_h1_h2 — Medium Stream (64KB via H2)
- **Mục tiêu thử nghiệm**: Chuyển luồng DATA frames H2 64KB sang chunked downstream.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes chunked data
- **Lệnh thực thi chuẩn**: `wrk -t4 -c100 -d10s --latency http://192.168.122.14:8080/stream-h2/64kb`
- **Nhánh Upstream**: Dual-Upstream (50% h2c Cleartext : 50% TLS 1.3 h2 ALPN)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 15: server_stream_h1_h2 — Large Stream (10MB via H2)
- **Mục tiêu thử nghiệm**: Kéo stream 10MB từ Upstream H2 về Downstream H1.
- **Kích thước tải trọng & Quy chuẩn**: 10,485,760 bytes stream
- **Lệnh thực thi chuẩn**: `wrk -t4 -c20 -d10s --latency http://192.168.122.14:8080/stream-h2/10mb`
- **Nhánh Upstream**: Dual-Upstream (50% h2c Cleartext : 50% TLS 1.3 h2 ALPN)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 16: server_stream_h1_h3 — Micro SSE via H3 QUIC
- **Mục tiêu thử nghiệm**: Upstream QUIC stream đẩy dữ liệu SSE sang H1 chunked downstream.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes SSE chunks
- **Lệnh thực thi chuẩn**: `wrk -t4 -c80 -d10s --latency http://192.168.122.14:8080/stream-h3/sse`
- **Nhánh Upstream**: Dual-Upstream (50% H3 Cleartext UDP : 50% TLS 1.3 H3 ALPN)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 17: server_stream_h1_h3 — Medium Stream (64KB via H3)
- **Mục tiêu thử nghiệm**: Nhận stream 64KB từ QUIC upstream stream, pump sang client.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes data
- **Lệnh thực thi chuẩn**: `wrk -t4 -c80 -d10s --latency http://192.168.122.14:8080/stream-h3/64kb`
- **Nhánh Upstream**: Dual-Upstream (50% H3 Cleartext UDP : 50% TLS 1.3 H3 ALPN)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 18: server_stream_h1_h3 — Large Stream (10MB via H3)
- **Mục tiêu thử nghiệm**: Tải luồng 10MB từ QUIC stream chuyển tiếp downstream.
- **Kích thước tải trọng & Quy chuẩn**: 10,485,760 bytes stream
- **Lệnh thực thi chuẩn**: `wrk -t4 -c15 -d10s --latency http://192.168.122.14:8080/stream-h3/10mb`
- **Nhánh Upstream**: Dual-Upstream (50% H3 Cleartext UDP : 50% TLS 1.3 H3 ALPN)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 19: client_stream_h1_h1 — Micro Upload (Drip Ingest ~64B)
- **Mục tiêu thử nghiệm**: Client đẩy sự kiện IoT nhỏ dạng chunked liên tục.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes chunked upload body
- **Lệnh thực thi chuẩn**: `wrk -t4 -c100 -d10s -s upload_micro.lua http://192.168.122.14:8080/upload/micro`
- **Nhánh Upstream**: Dual-Upstream (50% Cleartext h1 : 50% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 20: client_stream_h1_h1 — Medium Upload (Document 64KB)
- **Mục tiêu thử nghiệm**: Upload tài liệu 64KB chunked stream lên upstream server.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes chunked upload body
- **Lệnh thực thi chuẩn**: `wrk -t4 -c50 -d10s -s upload_64kb.lua http://192.168.122.14:8080/upload/64kb`
- **Nhánh Upstream**: Dual-Upstream (50% Cleartext h1 : 50% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 21: client_stream_h1_h1 — Large Upload (5MB File Upload)
- **Mục tiêu thử nghiệm**: Đẩy luồng file 5MB liên tục lên upstream, kiểm tra cơ chế zero-alloc chunk parsing.
- **Kích thước tải trọng & Quy chuẩn**: 5,242,880 bytes chunked upload body
- **Lệnh thực thi chuẩn**: `wrk -t4 -c20 -d10s -s upload_5mb.lua http://192.168.122.14:8080/upload/5mb`
- **Nhánh Upstream**: Dual-Upstream (50% Cleartext h1 : 50% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 22: client_stream_h1_h2 — Micro Upload to H2
- **Mục tiêu thử nghiệm**: Nhận chunked upload từ client và biến thành H2 DATA frames đẩy vào upstream.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes chunked upload body
- **Lệnh thực thi chuẩn**: `wrk -t4 -c100 -d10s -s upload_micro.lua http://192.168.122.14:8080/upload-h2/micro`
- **Nhánh Upstream**: Dual-Upstream (50% h2c Cleartext : 50% TLS 1.3 h2 ALPN)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 23: client_stream_h1_h2 — Medium Upload to H2 (64KB)
- **Mục tiêu thử nghiệm**: Chuyển 64KB chunked stream từ client H1 sang H2 DATA stream.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes chunked upload body
- **Lệnh thực thi chuẩn**: `wrk -t4 -c50 -d10s -s upload_64kb.lua http://192.168.122.14:8080/upload-h2/64kb`
- **Nhánh Upstream**: Dual-Upstream (50% h2c Cleartext : 50% TLS 1.3 h2 ALPN)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 24: client_stream_h1_h2 — Large Upload to H2 (5MB)
- **Mục tiêu thử nghiệm**: Chuyển luồng upload 5MB từ client H1 sang H2 upstream có backpressure window.
- **Kích thước tải trọng & Quy chuẩn**: 5,242,880 bytes chunked upload body
- **Lệnh thực thi chuẩn**: `wrk -t4 -c20 -d10s -s upload_5mb.lua http://192.168.122.14:8080/upload-h2/5mb`
- **Nhánh Upstream**: Dual-Upstream (50% h2c Cleartext : 50% TLS 1.3 h2 ALPN)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 25: client_stream_h1_h3 — Micro Upload to H3
- **Mục tiêu thử nghiệm**: Client H1 upload micro chunks, gateway đóng gói sang QUIC frames.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes chunked upload body
- **Lệnh thực thi chuẩn**: `wrk -t4 -c80 -d10s -s upload_micro.lua http://192.168.122.14:8080/upload-h3/micro`
- **Nhánh Upstream**: Dual-Upstream (50% H3 Cleartext UDP : 50% TLS 1.3 H3 ALPN)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 26: client_stream_h1_h3 — Medium Upload to H3 (64KB)
- **Mục tiêu thử nghiệm**: Client upload 64KB chunked sang QUIC stream.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes chunked upload body
- **Lệnh thực thi chuẩn**: `wrk -t4 -c40 -d10s -s upload_64kb.lua http://192.168.122.14:8080/upload-h3/64kb`
- **Nhánh Upstream**: Dual-Upstream (50% H3 Cleartext UDP : 50% TLS 1.3 H3 ALPN)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 27: client_stream_h1_h3 — Large Upload to H3 (5MB)
- **Mục tiêu thử nghiệm**: Client upload 5MB chunked sang QUIC stream.
- **Kích thước tải trọng & Quy chuẩn**: 5,242,880 bytes chunked upload body
- **Lệnh thực thi chuẩn**: `wrk -t4 -c15 -d10s -s upload_5mb.lua http://192.168.122.14:8080/upload-h3/5mb`
- **Nhánh Upstream**: Dual-Upstream (50% H3 Cleartext UDP : 50% TLS 1.3 H3 ALPN)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 28: duplex_h1_h1 — Micro Interactive Echo (~64B)
- **Mục tiêu thử nghiệm**: Client vừa gửi vừa nhận đồng thời từng gói 64B qua giao thức chunked song công.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes bidi frames
- **Lệnh thực thi chuẩn**: `python3 bidi_tool.py --conns 100 --payload 64 http://192.168.122.14:8080/duplex/echo`
- **Nhánh Upstream**: Dual-Upstream (50% Cleartext h1 : 50% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 29: duplex_h1_h1 — Medium Bidirectional Messaging (64KB)
- **Mục tiêu thử nghiệm**: Vừa stream 64KB lên, vừa nhận 64KB về đồng thời trên cùng một connection.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes in / 65,536 bytes out
- **Lệnh thực thi chuẩn**: `python3 bidi_tool.py --conns 50 --payload 65536 http://192.168.122.14:8080/duplex/msg`
- **Nhánh Upstream**: Dual-Upstream (50% Cleartext h1 : 50% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 30: duplex_h1_h1 — Large Heavy Duplex Pump (5MB In / 5MB Out)
- **Mục tiêu thử nghiệm**: Bơm liên tục 5MB hai chiều đồng thời, kiểm tra điều phối asymmetric backpressure.
- **Kích thước tải trọng & Quy chuẩn**: 5,242,880 bytes in / 5,242,880 bytes out
- **Lệnh thực thi chuẩn**: `python3 bidi_tool.py --conns 10 --payload 5242880 http://192.168.122.14:8080/duplex/heavy`
- **Nhánh Upstream**: Dual-Upstream (50% Cleartext h1 : 50% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 31: duplex_h1_h2 — Micro Duplex H1 to H2 Stream
- **Mục tiêu thử nghiệm**: Ghép luồng duplex H1 vào một HTTP/2 bidirectional stream duy nhất.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes bidi frames
- **Lệnh thực thi chuẩn**: `python3 bidi_tool.py --conns 100 --payload 64 http://192.168.122.14:8080/duplex-h2/echo`
- **Nhánh Upstream**: Dual-Upstream (50% h2c Cleartext : 50% TLS 1.3 h2 ALPN)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 32: duplex_h1_h2 — Medium Duplex H1 to H2 (64KB)
- **Mục tiêu thử nghiệm**: Bơm song công 64KB qua HTTP/2 multiplexing.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes in / out
- **Lệnh thực thi chuẩn**: `python3 bidi_tool.py --conns 50 --payload 65536 http://192.168.122.14:8080/duplex-h2/msg`
- **Nhánh Upstream**: Dual-Upstream (50% h2c Cleartext : 50% TLS 1.3 h2 ALPN)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 33: duplex_h1_h2 — Large Duplex H1 to H2 (5MB)
- **Mục tiêu thử nghiệm**: Đẩy 5MB in / 5MB out qua HTTP/2 data frames.
- **Kích thước tải trọng & Quy chuẩn**: 5,242,880 bytes in / out
- **Lệnh thực thi chuẩn**: `python3 bidi_tool.py --conns 10 --payload 5242880 http://192.168.122.14:8080/duplex-h2/heavy`
- **Nhánh Upstream**: Dual-Upstream (50% h2c Cleartext : 50% TLS 1.3 h2 ALPN)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 34: duplex_h1_h3 — Micro Duplex H1 to H3 QUIC
- **Mục tiêu thử nghiệm**: Bidi stream H1 downstream ghép vào QUIC bidirectional stream upstream.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes bidi frames
- **Lệnh thực thi chuẩn**: `python3 bidi_tool.py --conns 80 --payload 64 http://192.168.122.14:8080/duplex-h3/echo`
- **Nhánh Upstream**: Dual-Upstream (50% H3 Cleartext UDP : 50% TLS 1.3 H3 ALPN)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 35: duplex_h1_h3 — Medium Duplex H1 to H3 (64KB)
- **Mục tiêu thử nghiệm**: Bơm song công 64KB qua QUIC bidi stream.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes in / out
- **Lệnh thực thi chuẩn**: `python3 bidi_tool.py --conns 40 --payload 65536 http://192.168.122.14:8080/duplex-h3/msg`
- **Nhánh Upstream**: Dual-Upstream (50% H3 Cleartext UDP : 50% TLS 1.3 H3 ALPN)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 36: duplex_h1_h3 — Large Duplex H1 to H3 (5MB)
- **Mục tiêu thử nghiệm**: Bơm 5MB song công qua QUIC stream.
- **Kích thước tải trọng & Quy chuẩn**: 5,242,880 bytes in / out
- **Lệnh thực thi chuẩn**: `python3 bidi_tool.py --conns 10 --payload 5242880 http://192.168.122.14:8080/duplex-h3/heavy`
- **Nhánh Upstream**: Dual-Upstream (50% H3 Cleartext UDP : 50% TLS 1.3 H3 ALPN)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 37: HTTP Request Smuggling (CL.TE & TE.CL Desync Attack)
- **Mục tiêu thử nghiệm**: Kiểm tra tính bất khả xâm phạm trước kỹ thuật desync HTTP Pipeline Smuggling.
- **Kích thước tải trọng & Quy chuẩn**: Malformed CL + TE mixed headers
- **Lệnh thực thi chuẩn**: `python3 test_smuggling.py --target http://192.168.122.14:8080/`
- **Nhánh Upstream**: Dual-Upstream (50% Cleartext : 50% TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 38: Corrupted Chunk Framing (Invalid Hex, Broken CRLF, Premature FIN)
- **Mục tiêu thử nghiệm**: Bắn các gói chunk sai cấu trúc hex và ngắt socket giữa chừng để thử độ bền parser.
- **Kích thước tải trọng & Quy chuẩn**: Invalid hex size, missing CRLF, early TCP RST/FIN
- **Lệnh thực thi chuẩn**: `python3 test_corrupted_chunk.py --target http://192.168.122.14:8080/`
- **Nhánh Upstream**: Dual-Upstream (50% Cleartext : 50% TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 39: Header Flooding & Giant Line (8KB Header, 250+ Junk Headers)
- **Mục tiêu thử nghiệm**: Kiểm tra giới hạn phòng vệ cạn kiệt tài nguyên bộ nhớ header table (RFC 9112).
- **Kích thước tải trọng & Quy chuẩn**: 8KB oversized header line, 250 header fields
- **Lệnh thực thi chuẩn**: `python3 test_header_flood.py --target http://192.168.122.14:8080/`
- **Nhánh Upstream**: Dual-Upstream (50% Cleartext : 50% TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 40: Slowloris Attack & Idle Starvation (Drip 1B / 3s, Idle Watchdog)
- **Mục tiêu thử nghiệm**: Mở đồng thời 5,000 socket client, drip 1 byte/3s chiếm FD hệ thống.
- **Kích thước tải trọng & Quy chuẩn**: 5,000 slow connections (1 byte per 3s)
- **Lệnh thực thi chuẩn**: `python3 test_slowloris.py --target 192.168.122.14:8080 --sockets 5000`
- **Nhánh Upstream**: Dual-Upstream (50% Cleartext : 50% TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 41: Chaos Upstream Sudden Death (TCP RST Injection & Instant Failover)
- **Mục tiêu thử nghiệm**: Tiêm TCP RST ngắt 50% backend nodes đột ngột dưới 50,000 RPS, kiểm tra retry & passive health.
- **Kích thước tải trọng & Quy chuẩn**: REST JSON 12KB dưới tác động RST injection
- **Lệnh thực thi chuẩn**: `wrk -t6 -c200 -d15s http://192.168.122.14:8080/buffered/12kb (with iptables RST chaos)`
- **Nhánh Upstream**: Multi-Endpoint Upstream (Auto Failover)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 42: Cgroup RAM 60MB Starvation (O(1) Memory Footprint vs OOM Killer)
- **Mục tiêu thử nghiệm**: Giới hạn memory.max = 60M, dội đồng thời 100 stream 10MB chunked download.
- **Kích thước tải trọng & Quy chuẩn**: 100 concurrent 10MB chunked streams dưới cgroup 60MB
- **Lệnh thực thi chuẩn**: `wrk -t6 -c100 -d30s http://192.168.122.14:8080/stream/10mb`
- **Nhánh Upstream**: Dual-Upstream (50% Cleartext : 50% TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 43: CPU 100% Saturation & Adaptive Overload Shedding (CoDel Shedder)
- **Mục tiêu thử nghiệm**: Ép máy chủ 100% CPU utilization, dội tải đột biến 300,000 RPS kiểm tra early drop 503.
- **Kích thước tải trọng & Quy chuẩn**: 300,000 RPS flood
- **Lệnh thực thi chuẩn**: `wrk -t6 -c1000 -d20s http://192.168.122.14:8080/buffered/ping`
- **Nhánh Upstream**: Dual-Upstream (50% Cleartext : 50% TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---

### KB 44: Atomic Hot-Reload Contention (100 Reloads/s at 100K RPS, Zero Dropped Conns)
- **Mục tiêu thử nghiệm**: Kích hoạt 100 lần reload runtime snapshot/giây dưới 100,000 RPS, kiểm tra zero-lock read path.
- **Kích thước tải trọng & Quy chuẩn**: 100,000 RPS + 100 atomic snapshot swaps/sec
- **Lệnh thực thi chuẩn**: `wrk -t6 -c200 -d15s http://192.168.122.14:8080/buffered/ping (with reload script)`
- **Nhánh Upstream**: Dual-Upstream (50% Cleartext : 50% TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | — | — | — | — |
| | **Băng thông truyền tải (Throughput MB/s)** | — | — | — | — |
| | **Độ trễ trung bình (Mean Latency)** | — | — | — | — |
| | **Độ trễ tối thiểu (Min Latency)** | — | — | — | — |
| | **Độ trễ phân vị P50** | — | — | — | — |
| | **Độ trễ phân vị P90** | — | — | — | — |
| | **Độ trễ phân vị P99** | — | — | — | — |
| | **Độ trễ phân vị P99.9** | — | — | — | — |
| | **Độ trễ tối đa (Max Latency)** | — | — | — | — |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | — | — | — | — |
| | **Thời gian nhận byte đầu (TTFB Avg)** | — | — | — | — |
| | **Thời gian kết nối (Connect Time Avg)** | — | — | — | — |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | — | — | — | — |
| | **Tỷ lệ thành công (Success Rate %)** | — | — | — | — |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | — | — | — | — |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | — | — | — | — |
| | **Số lệnh CPU / Request (`insn / req`)** | — | — | — | — |
| | **Tổng chu kỳ CPU (`cycles`)** | — | — | — | — |
| | **Chu kỳ CPU / Request (`cycles / req`)** | — | — | — | — |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | — | — | — | — |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | — | — | — | — |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | — | — | — | — |
| | **CPU đoán sai nhánh (`branch-misses`)** | — | — | — | — |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | — | — | — | — |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | — |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | — | — | — | — |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | — | — | — | — |
| | **Tổng số System Calls (`syscalls`)** | — | — | — | — |
| | **System Calls / Request (`syscalls / req`)** | — | — | — | — |
| | **Thời gian chạy trong kernel (Kernel Time)** | — | — | — | — |
| | **Thời gian chờ khóa Futex (Lock Contention)** | — | — | — | — |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | — | — | — | — |

---
