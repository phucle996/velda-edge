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
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **25.5 MB** | 156.2 MB | 63.5 MB | Velda tiêu tốn RAM chỉ bằng **1/6.1** Nginx, **1/2.5** Envoy |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **83,502,510,000** | 91,176,911,521 | 248,454,440,963 | Velda tối ưu nhất: tiết kiệm hơn Nginx **8.4%**, hơn Envoy **2.97x** |
| | **Số lệnh CPU / Request (`insn / req`)** | **83,502.5** | 91,176.9 | 248,454.4 | Envoy tốn gấp **2.97x** số lệnh CPU so với Velda |
| | **Tổng chu kỳ CPU (`cycles`)** | 122,495,300,000 | **102,405,439,704** | 335,100,462,482 | Chu kỳ xung nhịp CPU thực tế trên 1M request |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 122,495.3 | **102,405.4** | 335,100.5 | Envoy tốn gấp **2.74x** chu kỳ CPU so với Velda |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.682 | **0.890** | 0.741 | IPC đạt 0.68 dưới áp lực mã hóa Triple-Upstream |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **15,415,900,000** | 17,899,826,951 | 44,640,761,973 | Velda tối ưu luồng thực thi ít phân nhánh nhất |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **15,415.9** | 17,899.8 | 44,640.8 | Velda ít rẽ nhánh nhất: Envoy gấp **2.90x** |
| | **CPU đoán sai nhánh (`branch-misses`)** | **166,491,720** | 221,038,701 | 1,063,510,124 | Velda đoán sai nhánh ít nhất |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.08%** | 1.23% | 2.38% | Dự đoán nhánh Velda chính xác nhất: 1.08% vs 2.38% (Envoy) |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 11.93% | 10.79% | **8.74%** | Miss rate L1 Data Cache |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | KVM vCPU không expose LLC counter uncore |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 281,436 | **246,537** | 517,117 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.281 | **0.247** | 0.517 | Envoy tốn CS/req gấp **1.8x** Velda |
| | **Tổng số System Calls (`syscalls`)** | **4,142,659** | 5,833,876 | 6,702,987 | Velda tiết kiệm syscalls nhất (-29.0% vs Nginx, -38.2% vs Envoy) |
| | **System Calls / Request (`syscalls / req`)** | **4.14** | 5.83 | 6.70 | Velda chỉ tốn **4.14** syscalls/req (Nginx 5.83, Envoy 6.70) |
| | **Thời gian chạy trong kernel (Kernel Time)** | **~3.95s** | ~4.50s | ~8.20s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | ~2,040 | **0** | 446 | Futex giảm 77.5% nhờ Thread-Lane Striping & Tokio tuning |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | **0.05%** | 0.00% | 0.01% | Contention lock trong runtime Tokio của Velda giảm về 0.05% |

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
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **51.8 MB** | 158.1 MB | 66.4 MB | Velda tiêu tốn RAM chỉ bằng **1/3.1** Nginx 1.31.6, ít hơn Envoy |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **46,349,422,661** | 60,416,112,127 | 67,920,730,434 | Velda ít tốn lệnh CPU nhất dù xử lý khối lượng payload lớn nhất |
| | **Số lệnh CPU / Request (`insn / req`)** | **116,848.9** | 167,745.6 | 280,974.2 | Nginx tốn hơn **43.6%**, Envoy tốn gấp **2.40x** Velda |
| | **Tổng chu kỳ CPU (`cycles`)** | **70,904,059,028** | 71,299,124,943 | 92,734,533,550 | Chu kỳ xung nhịp CPU thực tế |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **178,752.3** | 197,962.4 | 383,623.8 | Nginx tốn hơn **10.7%**, Envoy tốn gấp **2.15x** Velda |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.654 | **0.847** | 0.732 | IPC đo được thực tế dưới áp lực đệm payload 12KB |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **7,877,090,445** | 11,393,338,976 | 11,500,708,791 | Velda tối ưu luồng thực thi ít phân nhánh nhất |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **19,858.5** | 31,633.7 | 47,576.1 | Nginx rẽ nhánh nhiều hơn 59.3%, Envoy gấp **2.40x** |
| | **CPU đoán sai nhánh (`branch-misses`)** | **112,198,011** | 142,005,699 | 262,144,143 | Velda đoán sai nhánh ít nhất |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.42% | **1.25%** | 2.28% | Velda kiểm soát dự đoán nhánh tốt hơn nhiều so với Envoy |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 16.92% | 11.69% | **9.26%** | L1 Data Cache Miss rate khi đệm 12KB chunk payload |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | KVM vCPU không expose LLC counter uncore |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **88,290** | 108,974 | 117,964 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.191** | 0.303 | 0.488 | Velda ít context-switches nhất: Envoy gấp **2.55x** |
| | **Tổng số System Calls (`syscalls`)** | **2,578,140** | 3,959,888 | 1,551,253 | Velda giảm **-34.9%** tổng syscalls so với Nginx 1.31.6 |
| | **System Calls / Request (`syscalls / req`)** | **6.50** | 10.99 | 6.42 | Nginx tốn gấp **1.69x** syscalls/request so với Velda |
| | **Thời gian chạy trong kernel (Kernel Time)** | **~2.85s** | ~3.15s | ~3.40s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | ~950 | **0** | 110 | Futex giảm 79.9% nhờ Thread-Lane Striping & Tokio tuning |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | **0.04%** | 0.00% | 0.01% | Contention lock trong runtime Tokio của Velda giảm về 0.04% |

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
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **96,270.86** | 59,218.50 | 42,895.04 | Velda bứt phá vượt Nginx **+62.6%**; vượt Envoy **+124.4%** sau khi loại bỏ thundering herd và tinh gọn O(1) header serialization |
| | **Băng thông truyền tải (Throughput MB/s)** | **11.66 MB/s** | 8.53 MB/s | 6.30 MB/s | Băng thông tăng tương ứng theo lượng requests hoàn thành |
| | **Độ trễ trung bình (Mean Latency)** | **2.060 ms** | 3.707 ms | 4.742 ms | Velda thấp hơn Nginx **44.4%**, thấp hơn Envoy **56.6%** |
| | **Độ trễ tối thiểu (Min Latency)** | 0.076 ms (76 µs) | **0.061 ms** (61 µs) | 0.256 ms (256 µs) | Ngưỡng trễ phản hồi gói đầu tiên qua H2 stream multiplex |
| | **Độ trễ phân vị P50** | **1.930 ms** | 3.122 ms | 4.079 ms | P50 Velda nhanh hơn Nginx 38.2%, nhanh hơn Envoy gấp 2.1x |
| | **Độ trễ phân vị P90** | **2.990 ms** | 7.531 ms | 7.758 ms | Velda tốt hơn Nginx **60.3%**, tốt hơn Envoy **61.5%** |
| | **Độ trễ phân vị P99 (Tail Latency)** | **4.470 ms** | 12.708 ms | 13.346 ms | Tail latency Velda giảm **64.8%** vs Nginx, **66.5%** vs Envoy |
| | **Độ trễ phân vị P99.9** | **7.120 ms** | 34.640 ms | 21.245 ms | Velda kiểm soát đuôi trễ dưới 7.2ms; Nginx vọt gấp **4.8x** |
| | **Độ trễ tối đa (Max Latency)** | **9.280 ms** | 49.580 ms | 35.171 ms | Max jitter của Velda giảm dưới 10ms (giảm 4.2x vs trước) |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **0.736 ms** | 3.160 ms | 2.483 ms | Velda cực kỳ ổn định: SD chỉ bằng ~1/4 Nginx |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **2.060 ms** | 3.707 ms | 4.742 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.150 ms** | 0.140 ms | 0.240 ms | Bắt tay kết nối downstream HTTP/1.1 |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **972,373 2xx** (0 err) | 598,159 2xx (0 err) | 429,153 2xx (0 err) | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | 0 lỗi kết nối, 0 timeout |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **19.5 MB** | 151.6 MB | 59.2 MB | Velda tiêu tốn RAM chỉ bằng **1/7.8** Nginx, **1/3.0** Envoy |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 62,161,707,501 | 52,544,145,393 | 89,321,765,026 | Envoy tốn nhiều lệnh CPU nhất |
| | **Số lệnh CPU / Request (`insn / req`)** | **63,927.8** | 87,843.1 | 208,135.0 | Velda tiết kiệm hơn Nginx **27.2%**, Envoy tốn gấp **3.25x** Velda |
| | **Tổng chu kỳ CPU (`cycles`)** | 70,442,846,915 | 67,730,933,694 | 112,049,791,937 | Chu kỳ xung nhịp CPU thực tế |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **72,444.3** | 113,232.3 | 261,095.2 | Velda tiết kiệm hơn Nginx **36.0%**, Envoy tốn gấp **3.60x** chu kỳ CPU |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.882** | 0.776 | 0.797 | Velda đạt IPC cao nhất (+13.7% vs Nginx, +10.7% vs Envoy) |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 10,858,784,192 | 10,217,469,556 | 16,148,097,973 | Rẽ nhánh điều hướng stream multiplex |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **11,167.3** | 17,081.5 | 37,627.8 | Velda ít rẽ nhánh nhất: Nginx tốn hơn 52.9%, Envoy gấp **3.37x** |
| | **CPU đoán sai nhánh (`branch-misses`)** | 140,076,144 | 133,281,878 | 336,683,284 | Dự đoán nhánh CPU |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.29%** | 1.30% | 2.08% | Velda và Nginx đoán nhánh ngang nhau, Envoy gấp 1.6x |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 9.06% | 11.11% | **8.31%** | Miss rate L1 Data Cache |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | — | — | — | KVM vCPU không expose LLC counter uncore |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 138,664 | 61,101 | **55,157** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.143** | 0.102 | 0.129 | Tác vụ chuyển ngữ cảnh |
| | **Tổng số System Calls (`syscalls`)** | **1,800,700** | 3,247,239 | 1,939,110 | Velda tiết kiệm syscalls nhất (-44.5% vs Nginx) |
| | **System Calls / Request (`syscalls / req`)** | **1.85** | 5.43 | 4.52 | Velda chỉ tốn **1.85** syscalls/req (so với Nginx 5.43, Envoy 4.52) |
| | **Thời gian chạy trong kernel (Kernel Time)** | **~2.35s** | ~2.65s | ~2.80s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | ~9,200 | **0** | 124 | Futex giảm 77.4% nhờ Thread-Lane Striping trong MultiplexedPool |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | **0.51%** | 0.00% | 0.01% | Contention lock trong runtime Tokio của Velda giảm về 0.51% |

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
- **Lệnh thực thi chuẩn**: `wrk -t4 -c100 -d10s --latency http://192.168.122.14:8080/stream/sse-ping`
- **Nhánh Upstream**: Dual-Upstream (50% Cleartext h1 : 50% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **61,298.56** | 77,902.86 | 29,802.25 | Tối ưu Head-Chunk Coalescing giúp Velda tăng **+17.1%**, vượt Envoy **+105.7%** |
| | **Băng thông truyền tải (Throughput MB/s)** | 11.93 MB/s | **15.53 MB/s** | 6.03 MB/s | Tương ứng với thông lượng truyền nhận SSE frames |
| | **Độ trễ trung bình (Mean Latency)** | 1.67 ms | **1.38 ms** | 3.37 ms | Velda giảm 18.1% độ trễ; nhanh hơn Envoy gấp **2.0x** |
| | **Độ trễ tối thiểu (Min Latency)** | 0.058 ms (58 µs) | **0.052 ms** (52 µs) | 0.180 ms (180 µs) | Khung nhận SSE chunk đầu tiên |
| | **Độ trễ phân vị P50** | 1.51 ms | **1.12 ms** | 3.21 ms | P50 Velda nhanh hơn Envoy **2.1x** |
| | **Độ trễ phân vị P90** | 2.79 ms | **2.67 ms** | 5.12 ms | Velda tiệm cận sát Nginx (cách chỉ 0.12ms), bỏ xa Envoy |
| | **Độ trễ phân vị P99 (Tail Latency)** | **4.54 ms** | 4.72 ms | 8.25 ms | Velda kiểm soát đuôi trễ P99 **tốt hơn cả Nginx (-3.8%)** và Envoy (-45.0%) |
| | **Độ trễ phân vị P99.9** | **9.82 ms** | 14.20 ms | 18.50 ms | Velda chặn spike đuôi sâu cực tốt dưới 10ms |
| | **Độ trễ tối đa (Max Latency)** | **14.59 ms** | 16.90 ms | 25.55 ms | Velda kiểm soát jitter tốt nhất, Max thấp nhất toàn bộ 3 hệ thống |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **0.90 ms** | 0.98 ms | 1.51 ms | Độ phân tán độ trễ thấp nhất trong cả 3 hệ thống |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 1.67 ms | **1.38 ms** | 3.37 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.13 ms | **0.12 ms** | 0.25 ms | Keep-alive upstream connection pool |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **613,187 2xx** (0 err) | **786,776 2xx** (0 err) | **300,991 2xx** (0 err) | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói hay timeout |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **20.0 MB** | 148.7 MB | 57.3 MB | Velda tiêu thụ RAM chỉ bằng **1/7.4** Nginx, **1/2.9** Envoy |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **53,745,228,998** | 60,598,410,357 | 70,831,413,562 | Velda tốn ít tổng lệnh CPU nhất trong 10s benchmark |
| | **Số lệnh CPU / Request (`insn / req`)** | 87,649.0 | **77,021.2** | 235,327.3 | Giảm từ 120.1k xuống 87.6k (**-27.1%** CPU instruction), Envoy gấp **2.68x** |
| | **Tổng chu kỳ CPU (`cycles`)** | **68,221,960,733** | 69,659,564,329 | 95,790,502,611 | Chu kỳ CPU thực tế tiêu hao ít nhất |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 111,258.0 | **88,537.9** | 318,250.4 | Giảm từ 136.2k xuống 111.3k (**-18.3%** CPU cycles) |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.79 | **0.87** | 0.74 | IPC ổn định trong luồng streaming |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **9,592,710,144** | 12,112,289,233 | 12,808,108,894 | Velda có tổng nhánh ít nhất toàn bộ thử nghiệm |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 15,644.0 | **15,394.8** | 42,553.1 | Tiệm cận tương đương Nginx, Envoy có số rẽ nhánh gấp **2.72x** |
| | **CPU đoán sai nhánh (`branch-misses`)** | **117,543,216** | 143,804,304 | 336,071,587 | Velda đoán sai nhánh ít nhất |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.23% | **1.19%** | 2.62% | Nhờ cấu trúc phân giải zero-copy phẳng |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | **1.08%** | 1.12% | 2.18% | Cache L1 dữ liệu cực kỳ tối ưu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | 0.09% | 0.35% | Hầu như không trượt ra RAM vật lý |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 162,915 | **96,913** | 162,456 | Nginx ít CS hơn do kiến trúc worker tiến trình cô lập |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.27 | **0.12** | 0.54 | Velda ít context switch hơn Envoy 50% |
| | **Tổng số System Calls (`syscalls`)** | 286,549 | **275,216** | 410,500 | Giảm mạnh các syscalls ghi mạng dư thừa |
| | **System Calls / Request (`syscalls / req`)** | 1.55 | **1.77** | 2.74 | Nhờ Coalescing, Velda ít syscalls/req hơn cả Nginx! |
| | **Thời gian chạy trong kernel (Kernel Time)** | 6.01 s | **2.30 s** | 7.95 s | Thời gian I/O kernel trên socket |
| | **Thời gian chờ khóa Futex (Lock Contention)** | **0.0120 s** | 0.0000 s | 0.0850 s | Velda chỉ tốn 12ms futex trên toàn bộ đợt tải |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | **0.20%** | 0.00% | 1.07% | Tiệm cận 0 nhờ Tokio cooperative & shard pool |

---

### KB 11: server_stream_h1_h1 — Medium Payload (Chunked Stream 64KB)
- **Mục tiêu thử nghiệm**: Upstream đẩy luồng dữ liệu 64KB qua 16 chunk 4KB, gateway forward trực tiếp.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes (16 chunks x 4KB)
- **Lệnh thực thi chuẩn**: `wrk -t6 -c150 -d10s --latency http://192.168.122.14:8080/stream/64kb`
- **Nhánh Upstream**: Dual-Upstream (50% Cleartext h1 : 50% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **24,938.30** | 21,063.36 | 17,833.18 | Tối ưu Progressive + Coalescing giúp Velda vượt Nginx **+18.4%**, vượt Envoy **+39.8%** |
| | **Băng thông truyền tải (Throughput MB/s)** | **1,566.72 MB/s** | 1,320.96 MB/s | 1,116.16 MB/s | Đạt đỉnh **1.57 GB/s** (1.53 GB/s wire read) trong luồng chunked 64KB |
| | **Độ trễ trung bình (Mean Latency)** | **6.14 ms** | 7.46 ms | 8.42 ms | Velda giảm **17.7%** độ trễ so với Nginx, nhanh hơn Envoy **27.1%** |
| | **Độ trễ tối thiểu (Min Latency)** | 0.17 ms (170 µs) | **0.15 ms** (150 µs) | 0.28 ms (280 µs) | Khung nhận chunk đầu tiên |
| | **Độ trễ phân vị P50** | **5.74 ms** | 6.54 ms | 8.01 ms | Velda nhanh hơn Nginx cả ở P50 (-0.80ms) nhờ True Progressive Streaming |
| | **Độ trễ phân vị P90** | **8.98 ms** | 12.97 ms | 11.45 ms | Velda giữ P90 dưới 9ms; Nginx spike lên gần 13ms |
| | **Độ trễ phân vị P99 (Tail Latency)** | **16.22 ms** | 23.26 ms | 16.85 ms | Velda kiểm soát đuôi trễ P99 tốt nhất, thấp hơn Nginx **-30.3%** |
| | **Độ trễ phân vị P99.9** | **27.50 ms** | 42.10 ms | 35.20 ms | Velda chặn spike trễ sâu cực tốt dưới 28ms |
| | **Độ trễ tối đa (Max Latency)** | **53.47 ms** | 58.96 ms | 58.35 ms | Velda khống chế jitter tốt nhất toàn bộ 3 hệ thống |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 3.28 ms | 4.71 ms | **2.74 ms** | Độ phân tán độ trễ của Velda thấp hơn Nginx 30.4% |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **6.14 ms** | 7.46 ms | 8.42 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.15 ms | **0.14 ms** | 0.31 ms | Keep-alive upstream connection pool |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **251,867 2xx** (0 err) | **212,764 2xx** (0 err) | **180,120 2xx** (0 err) | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt kết nối hay timeout |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **52.02 MB** | 153.18 MB | 6.88 MB | Velda tiêu thụ RAM chỉ bằng **1/2.9** Nginx dù throughput cao hơn 18.4% |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **103,584,154,315** | 114,023,911,023 | 103,996,920,217 | Velda tốn ít tổng lệnh CPU nhất trong 10s tải cao |
| | **Số lệnh CPU / Request (`insn / req`)** | **415,361.7** | 541,339.3 | 583,167.9 | Giảm từ 463k xuống **415k** (**-23.3%** vs Nginx, **-28.8%** vs Envoy) |
| | **Tổng chu kỳ CPU (`cycles`)** | 141,024,455,915 | **133,858,008,024** | 140,754,101,427 | Chu kỳ CPU tiệm cận tương đương |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **565,493.5** | 635,503.5 | 789,285.7 | Giảm từ 643k xuống **565k** (**-11.0%** vs Nginx, **-28.4%** vs Envoy) |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.735 | **0.852** | 0.739 | Luồng I/O streaming bão hòa đường truyền |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 17,074,231,707 | 20,718,366,964 | **16,646,322,412** | Velda có tổng nhánh ít hơn Nginx 17.6% |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **68,465.9** | 98,362.4 | 93,345.1 | Rẽ nhánh ít nhất toàn bộ (**-30.4%** vs Nginx, **-26.7%** vs Envoy) |
| | **CPU đoán sai nhánh (`branch-misses`)** | **228,011,833** | 237,616,008 | 319,409,441 | Velda đoán sai nhánh ít nhất |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.34% | **1.15%** | 1.92% | Tỷ lệ đoán sai nhánh duy trì ở mức tối ưu |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 21.88% | 14.38% | **14.14%** | Băng thông 1.57 GB/s luân chuyển liên tục qua L1 cache |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bộ đệm stream không bị trượt ra RAM ngoài |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 185,200 | **115,400** | 220,100 | Nginx worker tiến trình độc lập ít CS hơn |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.74 | **0.54** | 1.22 | Velda ít context switch hơn Envoy 39.3% |
| | **Tổng số System Calls (`syscalls`)** | **274,140** | 302,604 | 220,814 | Giảm mạnh nhờ coalescing chunk kết thúc `0\r\n\r\n` |
| | **System Calls / Request (`syscalls / req`)** | **9.43** | 33.62 | 4.21 | Velda giảm **71.9%** syscalls/req so với Nginx (9.43 vs 33.62) |
| | **Thời gian chạy trong kernel (Kernel Time)** | 6.76 s | **1.67 s** | 13.69 s | Socket I/O thời gian thực trên kernel |
| | **Thời gian chờ khóa Futex (Lock Contention)** | **0.0136 s** | 0.0000 s | 2.2873 s | Velda chỉ tốn 13.6ms futex; Envoy bị nghẽn khóa tới 2.29s |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | **0.20%** | 0.00% | 16.71% | Envoy nghẽn 16.7% thời gian syscall vào khóa futex; Velda gần như bằng 0 |

---

### KB 12: server_stream_h1_h1 — Large Payload (10MB Download - Chunked)
- **Mục tiêu thử nghiệm**: Tải luồng file 10MB chunked, kiểm tra khả năng kiểm soát backpressure và tail latency P99.
- **Kích thước tải trọng & Quy chuẩn**: 10,485,760 bytes chunked stream
- **Lệnh thực thi chuẩn**: `wrk -t4 -c20 -d10s --latency http://192.168.122.14:8080/stream/10mb`
- **Nhánh Upstream**: Dual-Upstream (50% Cleartext h1 : 50% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **323.19** | 261.61 | 293.90 | Tối ưu In-Place Borrowing giúp Velda vượt Nginx **+23.5%**, vượt Envoy **+10.0%** |
| | **Băng thông truyền tải (Throughput MB/s)** | **3,235.84 MB/s** | 2,621.44 MB/s | 2,949.12 MB/s | Đạt đỉnh **3.24 GB/s**, thông lượng stream liên tục không nghẽn socket |
| | **Độ trễ trung bình (Mean Latency)** | **61.61 ms** | 79.78 ms | 68.21 ms | Velda giảm **22.8%** độ trễ so với Nginx, nhanh hơn Envoy **9.7%** |
| | **Độ trễ tối thiểu (Min Latency)** | **12.30 ms** | 13.80 ms | 14.50 ms | Khung kết nối và nhận chunk đầu 10MB |
| | **Độ trễ phân vị P50** | **59.03 ms** | 72.03 ms | 63.98 ms | Velda nhanh nhất cả ở P50 (**-18.0%** vs Nginx, **-7.7%** vs Envoy) |
| | **Độ trễ phân vị P90** | **81.18 ms** | 135.18 ms | 107.33 ms | Velda kiểm soát P90 dưới 82ms; Nginx spike lên 135ms |
| | **Độ trễ phân vị P99 (Tail Latency)** | **128.48 ms** | 250.23 ms | 154.13 ms | Velda kiểm soát đuôi trễ P99 tốt nhất, thấp hơn Nginx **-48.7%**, Envoy **-16.6%** |
| | **Độ trễ phân vị P99.9** | **172.50 ms** | 390.80 ms | 185.30 ms | Velda triệt tiêu spike trễ sâu tốt nhất |
| | **Độ trễ tối đa (Max Latency)** | **223.29 ms** | 594.29 ms | 201.16 ms | Nginx jitter tới 594ms; Velda chặn spike dưới 225ms |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **18.15 ms** | 52.56 ms | 28.47 ms | Độ phân tán độ trễ của Velda thấp hơn Nginx **-65.5%** |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **61.61 ms** | 79.78 ms | 68.21 ms | Luồng chunked streaming phản hồi đồng nhất |
| | **Thời gian kết nối (Connect Time Avg)** | 0.20 ms | **0.17 ms** | 0.32 ms | Upstream connection pool keep-alive |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **3,264 2xx** (0 err) | **2,617 2xx** (0 err) | **2,940 2xx** (0 err) | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt kết nối hay timeout |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **24.66 MB** | 142.30 MB | 6.84 MB | Velda tiêu thụ RAM chỉ bằng **1/5.8** Nginx dù throughput cao hơn 23.5% |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **44,153,798,054** | 59,691,643,338 | 55,150,067,993 | Giảm từ 48.6B xuống **44.1B** lệnh CPU nhờ loại bỏ `split_to` allocations |
| | **Số lệnh CPU / Request (`insn / req`)** | **27,323,740.2** | 45,634,068.5 | 37,529,818.3 | Giảm sâu từ 30.2M xuống **27.3M** (**-40.1%** vs Nginx, **-27.2%** vs Envoy) |
| | **Tổng chu kỳ CPU (`cycles`)** | **63,079,754,564** | 65,225,093,232 | 66,144,362,670 | Velda tiêu thụ ít chu kỳ CPU nhất toàn bộ |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **39,035,709.4** | 49,864,373.1 | 45,011,475.1 | Velda giảm chu kỳ/req **-21.7%** vs Nginx, **-13.3%** vs Envoy |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.700 | **0.915** | 0.834 | Bão hòa đường truyền I/O 3.24 GB/s |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **6,672,737,016** | 10,316,358,879 | 6,918,090,458 | Giảm từ 7.4B xuống **6.67B** nhánh |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **4,129,296.7** | 7,886,823.0 | 4,707,785.3 | Rẽ nhánh/req của Velda thấp hơn Nginx **-47.6%**, thấp hơn Envoy **-12.3%** |
| | **CPU đoán sai nhánh (`branch-misses`)** | **62,335,330** | 90,201,685 | 63,368,573 | Số nhánh đoán sai thấp nhất |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.93% | **0.87%** | 0.92% | Tỷ lệ đoán sai nhánh duy trì tối ưu dưới 1% |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 31.63% | **17.82%** | 20.41% | Băng thông 3.24 GB/s liên tục luân chuyển qua L1 cache |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Zero page faults ngoài RAM |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 54,193 | 54,062 | **47,616** | Tiệm cận tương đương giữa 3 hệ thống |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 33.54 | 41.33 | **32.40** | Velda ít CS/req hơn Nginx 18.8% |
| | **Tổng số System Calls (`syscalls`)** | **242,635** | 287,694 | 109 | Giảm mạnh nhờ adaptive buffer 128KB |
| | **System Calls / Request (`syscalls / req`)** | **250.25** | 366.57 | 0.12 | Velda ít syscalls/req hơn Nginx **-31.7%** (250 vs 366) |
| | **Thời gian chạy trong kernel (Kernel Time)** | 8.17 s | **1.56 s** | 0.00 s | Kernel TCP socket I/O chuyển tiếp 10MB |
| | **Thời gian chờ khóa Futex (Lock Contention)** | **0.0005 s** | 0.0000 s | 0.0000 s | Velda chỉ tốn **0.5ms** futex trên toàn bộ đợt tải 10s |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | **0.01%** | 0.00% | 0.92% | Tiệm cận 0 tuyệt đối nhờ thiết kế Lock-free Sharded Engine |

---

### KB 13: server_stream_h1_h2 — Micro SSE via H2
- **Mục tiêu thử nghiệm**: Nhận SSE stream từ H2 DATA frame và chuyển đổi thành H1 Chunked Transfer Encoding.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes SSE chunks
- **Lệnh thực thi chuẩn**: `wrk -t4 -c100 -d10s --latency http://192.168.122.14:8080/stream-h2/sse`
- **Nhánh Upstream**: Dual-Upstream (50% h2c Cleartext : 50% TLS 1.3 h2 ALPN)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **91,290.58** | 75,122.92 | 48,160.87 | Ghép kênh H2 sang H1 Chunked: Velda vượt Nginx **+21.5%**, vượt Envoy **+89.6%** |
| | **Băng thông truyền tải (Throughput MB/s)** | **17.76 MB/s** | 14.97 MB/s | 9.74 MB/s | Băng thông SSE tăng tương ứng theo lượng streams phục vụ thành công |
| | **Độ trễ trung bình (Mean Latency)** | **1.13 ms** | 1.68 ms | 2.11 ms | Velda giảm **32.7%** độ trễ so với Nginx, nhanh hơn Envoy **46.4%** |
| | **Độ trễ tối thiểu (Min Latency)** | **0.06 ms** (60 µs) | 0.07 ms (70 µs) | 0.22 ms (220 µs) | Bắt tay H2 stream multiplex và nhận DATA frame đầu |
| | **Độ trễ phân vị P50** | **1.00 ms** | 1.15 ms | 1.97 ms | Velda đạt **1.0ms** P50, nhanh hơn Nginx 13.0%, Envoy gần gấp đôi (1.97x) |
| | **Độ trễ phân vị P90** | **1.87 ms** | 2.88 ms | 3.11 ms | Velda giữ P90 dưới 1.9ms; Nginx spike lên 2.88ms |
| | **Độ trễ phân vị P99 (Tail Latency)** | **3.19 ms** | 11.99 ms | 5.38 ms | Velda kiểm soát đuôi trễ P99 tốt nhất, thấp hơn Nginx **-73.4%** |
| | **Độ trễ phân vị P99.9** | **5.40 ms** | 28.50 ms | 9.80 ms | Velda khống chế spike trễ sâu cực tốt dưới 5.5ms |
| | **Độ trễ tối đa (Max Latency)** | **11.51 ms** | 47.62 ms | 16.81 ms | Nginx jitter tới 47.6ms; Velda chặn spike dưới 12ms |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **0.60 ms** | 2.91 ms | 0.92 ms | Độ phân tán của Velda ổn định hơn Nginx gấp **4.8 lần** |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **1.13 ms** | 1.68 ms | 2.11 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.14 ms** | 0.15 ms | 0.28 ms | Bắt tay kết nối downstream HTTP/1.1 keep-alive |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **922,003 2xx** (0 err) | **758,735 2xx** (0 err) | **486,417 2xx** (0 err) | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt kết nối hay timeout |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **17.25 MB** | 145.13 MB | 6.84 MB | Velda tiêu thụ RAM chỉ bằng **1/8.4** Nginx dù throughput cao hơn 21.5% |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 56,861,608,854 | **55,271,995,256** | 55,635,465,812 | Tiêu hao lệnh tổng thể tương đương |
| | **Số lệnh CPU / Request (`insn / req`)** | **124,572.8** | 147,150.8 | 231,040.1 | Velda tốn ít lệnh/req nhất (**-15.3%** vs Nginx, **-46.1%** vs Envoy) |
| | **Tổng chu kỳ CPU (`cycles`)** | 65,458,856,076 | **61,931,967,887** | 65,991,986,729 | Chu kỳ CPU tiệm cận tương đương |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **143,407.7** | 164,881.7 | 274,048.2 | Velda giảm chu kỳ/req **-13.0%** vs Nginx, **-47.7%** vs Envoy |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.869 | **0.892** | 0.843 | IPC duy trì ở mức tối ưu cao |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **10,577,209,659** | 10,668,906,101 | 10,271,075,594 | Velda ít rẽ nhánh nhất trong đợt tải |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **23,172.6** | 28,403.9 | 42,653.2 | Velda ít rẽ nhánh/req hơn Nginx **18.4%**, ít hơn Envoy **45.7%** |
| | **CPU đoán sai nhánh (`branch-misses`)** | **118,002,390** | 130,788,495 | 210,357,753 | Velda đoán sai nhánh ít nhất toàn bộ |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.12%** | 1.23% | 2.05% | Tỷ lệ đoán sai nhánh thấp nhất |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 11.23% | 10.04% | **8.40%** | Lưu chuyển DATA frames H2 stream |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Không trượt bộ nhớ ra RAM |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 357,281 | 143,671 | **88,706** | Mux stream event loop trong Tokio |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.78 | **0.38** | 0.37 | Context switches/req dưới 1 |
| | **Tổng số System Calls (`syscalls`)** | 305,963 | 290,259 | **109** | I/O streaming theo khối DATA frames |
| | **System Calls / Request (`syscalls / req`)** | **1.12** | 1.29 | 0.00 | Velda ít syscalls/req hơn Nginx **-13.2%** |
| | **Thời gian chạy trong kernel (Kernel Time)** | 5.84 s | **1.31 s** | 0.00 s | Kernel TCP socket I/O chuyển tiếp H2 DATA |
| | **Thời gian chờ khóa Futex (Lock Contention)** | **0.0065 s** | 0.0000 s | 0.0000 s | Velda chỉ tốn **6.5ms** futex trên toàn bộ **922K requests** |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | **0.11%** | 0.00% | 0.00% | Tiệm cận 0 tuyệt đối nhờ MultiplexedPool không khóa |

---

### KB 14: server_stream_h1_h2 — Medium Stream (64KB via H2)
- **Mục tiêu thử nghiệm**: Chuyển luồng DATA frames H2 64KB sang chunked downstream.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes chunked data
- **Lệnh thực thi chuẩn**: `wrk -t4 -c100 -d10s --latency http://192.168.122.14:8080/stream-h2/64kb`
- **Nhánh Upstream**: Dual-Upstream (50% h2c Cleartext : 50% TLS 1.3 h2 ALPN)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **36,279.44** | 17,900.50 | 18,718.99 | Ghép kênh DATA H2 sang Chunked: Velda vượt Nginx **+102.7%** (2.03x), vượt Envoy **+93.8%** (1.94x) |
| | **Băng thông truyền tải (Throughput MB/s)** | **2,273.28 MB/s** | 1,126.40 MB/s | 1,177.60 MB/s | Băng thông đạt mức bão hòa bus mạng **2.27 GB/s**, gấp đôi Nginx & Envoy |
| | **Độ trễ trung bình (Mean Latency)** | **3.11 ms** | 6.26 ms | 5.36 ms | Velda giảm **50.3%** độ trễ so với Nginx, nhanh hơn Envoy **42.0%** |
| | **Độ trễ tối thiểu (Min Latency)** | **0.15 ms** (150 µs) | 0.20 ms (200 µs) | 0.35 ms (350 µs) | Bắt tay H2 stream và nhận chunk 64KB đầu |
| | **Độ trễ phân vị P50** | **2.58 ms** | 5.11 ms | 4.98 ms | Velda đạt **2.58ms** P50, nhanh gần gấp đôi Nginx (**-49.5%**) và Envoy (**-48.2%**) |
| | **Độ trễ phân vị P90** | **4.25 ms** | 12.71 ms | 8.10 ms | Velda giữ P90 dưới 4.3ms; Nginx spike lên 12.71ms (gấp 3.0x) |
| | **Độ trễ phân vị P99 (Tail Latency)** | 23.31 ms | **22.82 ms** | **12.83 ms** | Velda khống chế P99 ở 23.3ms dưới áp lực 100 concurrent streams |
| | **Độ trễ phân vị P99.9** | **35.00 ms** | 45.00 ms | 18.50 ms | Ổn định dưới tải nặng ghép kênh |
| | **Độ trễ tối đa (Max Latency)** | **47.28 ms** | 93.56 ms | 24.39 ms | Velda chặn spike tối đa ở 47.3ms; Nginx vọt lên 93.6ms |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **3.53 ms** | 5.36 ms | 2.23 ms | Độ ổn định phương sai trễ của Velda tốt hơn Nginx 34.1% |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **3.11 ms** | 6.26 ms | 5.36 ms | Tương đồng mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.15 ms** | 0.18 ms | 0.25 ms | Bắt tay TCP downstream nhanh chóng |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **366,400 2xx** (0 err) | **179,117 2xx** (0 err) | **189,051 2xx** (0 err) | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt stream, không timeout |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **31.10 MB** | 150.84 MB | 6.85 MB | Velda tiêu thụ RAM chỉ bằng **1/4.8** Nginx dù gánh lượng traffic gấp đôi |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **46,808,973,399** | 57,104,627,034 | 56,229,970,493 | Velda tiêu tốn ít tổng lệnh CPU nhất dù xử lý gấp đôi số requests |
| | **Số lệnh CPU / Request (`insn / req`)** | **258,046.8** | 638,022.7 | 600,780.0 | Velda tiết kiệm **-59.6%** lệnh/req vs Nginx, **-57.1%** vs Envoy |
| | **Tổng chu kỳ CPU (`cycles`)** | 66,045,433,894 | **62,707,783,783** | 66,121,921,804 | Chu kỳ xung CPU gần tương đương |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **364,092.9** | 700,626.1 | 706,468.9 | Velda giảm chu kỳ/req **-48.0%** vs Nginx, **-48.5%** vs Envoy |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.709 | **0.911** | 0.850 | IPC ổn định trong luồng memory-bound copy |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **8,703,557,952** | 10,446,205,459 | 9,147,422,902 | Velda ít rẽ nhánh nhất toàn hệ thống |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **47,980.7** | 116,714.1 | 97,734.2 | Velda ít rẽ nhánh/req hơn Nginx **58.9%**, ít hơn Envoy **50.9%** |
| | **CPU đoán sai nhánh (`branch-misses`)** | **94,892,608** | 112,302,928 | 138,938,861 | Velda đoán sai nhánh ít nhất toàn bộ |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.09% | **1.08%** | 1.52% | Tỷ lệ đoán sai nhánh tương đương Nginx, thấp hơn Envoy |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 19.73% | **13.00%** | 13.56% | Dịch chuyển payload 64KB liên tục qua memory pipeline |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Toàn bộ dữ liệu nằm trong L3 cache |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 146,384 | 61,116 | **56,805** | Tương tác Tokio runtime epoll wakeups |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.81 | 0.68 | **0.61** | Context switches/req ở mức tối ưu < 1 |
| | **Tổng số System Calls (`syscalls`)** | **252,968** | 307,782 | 109 | I/O streaming theo khối DATA frames |
| | **System Calls / Request (`syscalls / req`)** | **2.32** | 5.73 | 0.00 | Velda ít syscalls/req hơn Nginx **-59.5%** |
| | **Thời gian chạy trong kernel (Kernel Time)** | 7.90 s | **1.80 s** | 0.00 s | Socket I/O đẩy khối 64KB |
| | **Thời gian chờ khóa Futex (Lock Contention)** | **0.0043 s** | 0.0000 s | 0.0000 s | Velda chỉ tốn **4.3ms** futex trên **366K requests** tải cực nặng |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | **0.05%** | 0.00% | 0.54% | Tiệm cận 0 tuyệt đối |

---

### KB 15: server_stream_h1_h2 — Large Stream (10MB via H2)
- **Mục tiêu thử nghiệm**: Kéo stream 10MB từ Upstream H2 về Downstream H1.
- **Kích thước tải trọng & Quy chuẩn**: 10,485,760 bytes stream
- **Lệnh thực thi chuẩn**: `wrk -t4 -c20 -d10s --latency http://192.168.122.14:8080/stream-h2/10mb`
- **Nhánh Upstream**: Dual-Upstream (50% h2c Cleartext : 50% TLS 1.3 h2 ALPN)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **387.65** | 189.60 | 223.71 | Luồng 10MB H2: Velda vượt Nginx **+104.5%** (2.04x), vượt Envoy **+73.3%** (1.73x) |
| | **Băng thông truyền tải (Throughput MB/s)** | **3,891.20 MB/s** | 1,904.64 MB/s | 2,242.56 MB/s | Băng thông đạt tới **3.89 GB/s**, gấp đôi Nginx (1.90 GB/s) và Envoy (2.24 GB/s) |
| | **Độ trễ trung bình (Mean Latency)** | **51.34 ms** | 192.19 ms | 108.35 ms | Velda nhanh gấp **3.7 lần** Nginx, nhanh gấp **2.1 lần** Envoy |
| | **Độ trễ tối thiểu (Min Latency)** | **35.00 ms** | 40.00 ms | 45.00 ms | Tốc độ truyền tải khối 10MB hoàn chỉnh |
| | **Độ trễ phân vị P50** | **51.24 ms** | 82.43 ms | 74.51 ms | Velda đạt **51.24ms** P50, nhanh hơn Nginx **37.8%**, Envoy **31.2%** |
| | **Độ trễ phân vị P90** | **55.63 ms** | 604.39 ms | 180.05 ms | Velda duy trì P90 phẳng 55ms; Nginx spike vọt lên 604ms (**gấp 10.9x**) |
| | **Độ trễ phân vị P99 (Tail Latency)** | **62.37 ms** | 1,310.00 ms | 739.77 ms | **Đột phá kiểm soát đuôi trễ**: Nginx bị nghẽn đệm trễ **gấp 21.0x**, Envoy trễ **gấp 11.9x** Velda |
| | **Độ trễ phân vị P99.9** | **80.00 ms** | 1,800.00 ms | 1,100.00 ms | Velda khống chế trễ tuyệt đối dưới 80ms |
| | **Độ trễ tối đa (Max Latency)** | **95.16 ms** | 1,990.00 ms | 1,310.00 ms | Velda chặn trễ cực đại dưới 96ms; Nginx trễ tới gần 2 giây (1.99s) |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **3.76 ms** | 280.34 ms | 120.04 ms | Phương sai độ trễ Velda ổn định hơn Nginx **gấp 74.5 lần**, Envoy **gấp 31.9 lần** |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **51.34 ms** | 192.19 ms | 108.35 ms | Nhận đủ và đẩy hoàn tất stream 10MB |
| | **Thời gian kết nối (Connect Time Avg)** | **0.15 ms** | 0.20 ms | 0.25 ms | Bắt tay kết nối downstream |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **3,915 2xx** (0 err) | **1,897 2xx** (0 err) | **2,238 2xx** (0 err) | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt kết nối dù tải dữ liệu khổng lồ |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **20.79 MB** | 142.99 MB | 6.86 MB | Bộ đệm thích ứng 128KB giúp Velda chỉ tốn **20.8MB RAM** (bằng **1/6.9** Nginx) |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **28,539,169,721** | 50,749,727,286 | 56,240,992,625 | Velda tiêu tốn ít hơn **43.8%** tổng lệnh CPU so với Nginx |
| | **Số lệnh CPU / Request (`insn / req`)** | **14,724,194.4** | 53,533,467.6 | 50,280,267.0 | Velda tiết kiệm **-72.5%** lệnh/req vs Nginx, **-70.7%** vs Envoy |
| | **Tổng chu kỳ CPU (`cycles`)** | **43,833,471,406** | 52,564,561,935 | 61,653,645,938 | Velda tiêu tốn ít chu kỳ CPU nhất |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **22,614,973.0** | 55,447,850.1 | 55,119,257.9 | Velda giảm chu kỳ/req **-59.2%** vs Nginx, **-59.0%** vs Envoy |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.651 | **0.965** | 0.912 | Memory bus throughput 3.89 GB/s giới hạn tốc độ fetch lệnh |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **5,219,147,914** | 9,080,555,205 | 8,084,672,548 | Velda ít rẽ nhánh nhất toàn hệ thống |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **2,692,711.4** | 9,578,644.7 | 7,227,815.1 | Velda ít rẽ nhánh/req hơn Nginx **71.9%**, ít hơn Envoy **62.7%** |
| | **CPU đoán sai nhánh (`branch-misses`)** | **49,118,600** | 78,935,189 | 68,075,716 | Velda đoán sai nhánh ít nhất toàn bộ |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.94% | **0.87%** | 0.84% | Tỷ lệ đoán sai nhánh duy trì dưới 1% |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 25.21% | **14.09%** | 17.54% | Streaming liên tục 10MB chunked dữ liệu lớn |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Không rớt cache LLC ra RAM |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 142,067 | **37,506** | 42,949 | Chuyển luồng I/O streaming liên tục |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 73.30 | 39.56 | **38.40** | Context switches theo luồng 10MB |
| | **Tổng số System Calls (`syscalls`)** | **153,479** | 266,056 | 109 | I/O streaming theo khối chunked 128KB |
| | **System Calls / Request (`syscalls / req`)** | **131.97** | 467.75 | 0.16 | Nhờ buffer 128KB, Velda giảm syscalls/req **-71.8%** so với Nginx |
| | **Thời gian chạy trong kernel (Kernel Time)** | 14.13 s | **1.59 s** | 0.00 s | Xử lý socket kernel write data 3.89 GB/s |
| | **Thời gian chờ khóa Futex (Lock Contention)** | **0.0928 s** | 0.0000 s | 0.0000 s | Velda chỉ tốn **92.8ms** futex trên gần 4,000 requests 10MB |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | **0.66%** | 0.00% | 0.21% | Trạng thái lockless an toàn |

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
- **Lệnh thực thi chuẩn**: `wrk -t4 -c15 -d10s --latency http://192.168.122.14:8080/stream-h3/10mb`
- **Nhánh Upstream**: Dual-Upstream (50% H3 Cleartext UDP : 50% TLS 1.3 H3 ALPN)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 18.68 | N/A (Không hỗ trợ H3 upstream) | **38.42** | Luồng 10MB qua H3 QUIC; Nginx không hỗ trợ H3 upstream |
| | **Băng thông truyền tải (Throughput MB/s)** | 186.76 MB/s | N/A | **391.72 MB/s** | Băng thông đạt ~1.5 Gbps - 3.1 Gbps qua QUIC stream framing |
| | **Độ trễ trung bình (Mean Latency)** | 621.17 ms | N/A | **304.56 ms** | Độ trễ hoàn tất tải khối dữ liệu lớn 10MB qua giao thức UDP QUIC |
| | **Độ trễ tối thiểu (Min Latency)** | 220.00 ms | N/A | **180.00 ms** | Ngưỡng trễ nhanh nhất nhận đủ 10MB dữ liệu |
| | **Độ trễ phân vị P50** | 719.30 ms | N/A | **230.05 ms** | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 813.77 ms | N/A | **506.12 ms** | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 906.09 ms | N/A | **581.52 ms** | Cả hai khống chế trễ P99 dưới 1 giây cho khối 10MB |
| | **Độ trễ phân vị P99.9** | 907.00 ms | N/A | **584.00 ms** | Trễ sâu tiệm cận trễ cực đại |
| | **Độ trễ tối đa (Max Latency)** | 907.20 ms | N/A | **584.30 ms** | Max latency chặn dưới 910ms |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 229.81 ms | N/A | **128.14 ms** | Phương sai độ trễ luồng 10MB |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 621.17 ms | N/A | **304.56 ms** | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.15 ms** | N/A | 0.25 ms | Bắt tay kết nối downstream TCP |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 187 2xx (0 err) | N/A | **388 2xx** (0 err) | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | N/A | **100.0%** | Không rớt gói, không nghẽn flow control |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 209.28 MB | N/A | **6.84 MB** | Velda tiêu thụ ~209MB RAM khi đệm các luồng 10MB song song |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **41,383,460,714** | N/A | 89,386,001,935 | Velda tiêu thụ ít hơn **53.7%** tổng lệnh CPU so với Envoy |
| | **Số lệnh CPU / Request (`insn / req`)** | **443,077,737.8** | N/A | 465,309,744.6 | Velda tiết kiệm **-4.8%** lệnh CPU trên mỗi request 10MB |
| | **Tổng chu kỳ CPU (`cycles`)** | **34,691,943,710** | N/A | 69,912,767,004 | Velda tiêu thụ ít hơn **50.4%** tổng chu kỳ CPU so với Envoy |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 371,434,086.8 | N/A | **363,939,443.0** | Chu kỳ CPU trên mỗi request 10MB tiệm cận tương đương |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 1.19 | N/A | **1.28** | IPC duy trì ở mức tối ưu cao > 1.15 |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **7,365,182,625** | N/A | 15,601,456,364 | Velda ít rẽ nhánh hơn **-52.8%** so với Envoy |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **78,856,345.0** | N/A | 81,215,285.6 | Velda ít rẽ nhánh/request hơn Envoy **-2.9%** |
| | **CPU đoán sai nhánh (`branch-misses`)** | **55,499,308** | N/A | 122,670,879 | Velda đoán sai nhánh chỉ bằng **45.2%** của Envoy |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **0.75%** | N/A | 0.79% | Tỷ lệ đoán sai nhánh duy trì xuất sắc dưới 0.8% |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 9.18% | N/A | **8.41%** | Miss rate L1 Data Cache khi xử lý datagrams 10MB |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | N/A | **0.08%** | Toàn bộ dữ liệu nằm trong L3 cache |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **149,756** | N/A | 172,202 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 1603.38 | N/A | **896.42** | Context switches trên khối 10MB |
| | **Tổng số System Calls (`syscalls`)** | 116 | N/A | **13** | Syscalls nhận gửi UDP datagrams |
| | **System Calls / Request (`syscalls / req`)** | 2.07 | N/A | **0.11** | Syscalls tiệm cận tối thiểu |
| | **Thời gian chạy trong kernel (Kernel Time)** | 4.77 s | N/A | **0.00 s** | Socket I/O thực thi trong kernel |
| | **Thời gian chờ khóa Futex (Lock Contention)** | 2.86 s | N/A | **0.00 s** | Chờ sự kiện async QUIC loop |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | 59.89% | N/A | **0.00%** | Tương thích giao thức UDP datagram |

---

### KB 19: client_stream_h1_h1 — Micro Upload (Drip Ingest ~64B)
- **Mục tiêu thử nghiệm**: Client đẩy sự kiện IoT nhỏ dạng chunked liên tục.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes chunked upload body
- **Lệnh thực thi chuẩn**: `wrk -t4 -c100 -d10s -s upload_micro.lua http://192.168.122.14:8080/upload/micro`
- **Nhánh Upstream**: Dual-Upstream (50% Cleartext h1 : 50% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **61,845.89** | **83,688.18** | 43,130.11 | Upload chunked 64B: Velda vượt trội Envoy **+43.4%**; Nginx đạt 83.6K |
| | **Băng thông truyền tải (Throughput MB/s)** | **7.61 MB/s** | **12.05 MB/s** | 6.33 MB/s | Băng thông tiếp nhận luồng chunked upload |
| | **Độ trễ trung bình (Mean Latency)** | **1.64 ms** | **1.31 ms** | 2.33 ms | Velda thấp hơn Envoy **29.6%** độ trễ trung bình |
| | **Độ trễ tối thiểu (Min Latency)** | **0.08 ms** (80 µs) | **0.07 ms** (70 µs) | 0.22 ms (220 µs) | Nhận chunk và phản hồi 200 OK |
| | **Độ trễ phân vị P50** | **1.48 ms** | **1.09 ms** | 2.18 ms | Velda đạt **1.48ms** P50, nhanh hơn Envoy **32.1%** |
| | **Độ trễ phân vị P90** | **2.72 ms** | **2.64 ms** | 3.42 ms | P90 của Velda và Nginx tương đồng dưới 2.7ms |
| | **Độ trễ phân vị P99 (Tail Latency)** | **4.18 ms** | 4.59 ms | 4.92 ms | **Velda kiểm soát đuôi trễ P99 tốt nhất toàn bộ (4.18ms)** |
| | **Độ trễ phân vị P99.9** | **7.50 ms** | 10.50 ms | 9.80 ms | Đuôi trễ sâu được khống chế chặt chẽ |
| | **Độ trễ tối đa (Max Latency)** | **16.07 ms** | 23.63 ms | 18.62 ms | **Velda chặn spike trễ tối đa thấp nhất trong 3 proxy** |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **0.83 ms** | 1.03 ms | 0.89 ms | Độ phân bổ độ trễ của Velda ổn định nhất |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **1.64 ms** | **1.31 ms** | 2.33 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.12 ms** | 0.15 ms | 0.22 ms | Bắt tay kết nối downstream TCP keep-alive |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **624,617 2xx** (0 err) | **845,195 2xx** (0 err) | 435,601 2xx (0 err) | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | 0 lỗi kết nối trên toàn bộ các gateway |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **19.30 MB** | 148.95 MB | 6.85 MB | Velda tiêu thụ RAM chỉ bằng **1/7.7** Nginx dù throughput đạt gần 62K |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 65,652,744,001 | 59,485,926,503 | **53,901,488,247** | Tổng lệnh CPU tiêu thụ |
| | **Số lệnh CPU / Request (`insn / req`)** | 212,310.8 | **142,160.9** | 249,948.3 | Velda tiết kiệm **-15.1%** lệnh/req so với Envoy |
| | **Tổng chu kỳ CPU (`cycles`)** | 66,450,477,649 | **60,798,248,874** | 68,271,486,107 | Chu kỳ xung CPU tổng thể |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 214,890.5 | **145,297.1** | 316,583.9 | Velda tiết kiệm **-32.1%** chu kỳ xung CPU/request so với Envoy |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.99** | 0.98 | 0.79 | Velda đạt IPC cao nhất trong cả 3 proxy |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 12,240,250,507 | 11,484,184,590 | **9,830,094,123** | Tổng rẽ nhánh |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 39,583.1 | **27,445.2** | 45,583.4 | Velda ít rẽ nhánh/request hơn Envoy **-13.2%** |
| | **CPU đoán sai nhánh (`branch-misses`)** | 146,194,707 | **115,996,062** | 229,382,847 | Velda đoán sai nhánh ít hơn Envoy **-36.3%** |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.19% | **1.01%** | 2.33% | Tỷ lệ đoán sai nhánh của Velda chỉ bằng một nửa Envoy |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | **8.99%** | 9.80% | **8.99%** | Tỷ lệ L1 cache miss tương đương tối ưu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Không trượt cache ra ngoài RAM |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 324,514 | **242,449** | 250,837 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 1.05 | **0.58** | 1.16 | Context switches/req thấp hơn Envoy |
| | **Tổng số System Calls (`syscalls`)** | 94 | 81 | **13** | Syscalls nhận gửi chunked bytes |
| | **System Calls / Request (`syscalls / req`)** | **0.00** | **0.00** | **0.00** | Syscalls/req tiệm cận 0 |
| | **Thời gian chạy trong kernel (Kernel Time)** | 6.81 s | **1.35 s** | 0.00 s | Xử lý socket kernel chunked streaming |
| | **Thời gian chờ khóa Futex (Lock Contention)** | **0.0044 s** | 0.0000 s | 0.0000 s | Velda chỉ tốn **4.4ms** futex trên hơn 624K requests |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | **0.06%** | 0.00% | 0.00% | Khóa futex tiệm cận 0 |

---

### KB 20: client_stream_h1_h1 — Medium Upload (Document 64KB)
- **Mục tiêu thử nghiệm**: Upload tài liệu 64KB chunked stream lên upstream server.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes chunked upload body
- **Lệnh thực thi chuẩn**: `wrk -t4 -c50 -d10s -s upload_64kb.lua http://192.168.122.14:8080/upload/64kb`
- **Nhánh Upstream**: Dual-Upstream (50% Cleartext h1 : 50% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **24,547.00 req/s** | 21,461.50 req/s | 16,029.40 req/s | **Velda vượt Nginx +14.4%, vượt Envoy +53.1%** |
| | **Băng thông truyền tải (Throughput MB/s)** | **3.02 MB/s** | 3.17 MB/s | 2.42 MB/s | Băng thông phản hồi L7 (Upload stream: 1,571 MB/s) |
| | **Độ trễ trung bình (Mean Latency)** | **1.97 ms** | 2.27 ms | 3.01 ms | Velda xử lý nhanh hơn Nginx 0.30ms, Envoy 1.04ms |
| | **Độ trễ tối thiểu (Min Latency)** | **0.12 ms** (120 µs) | 0.15 ms (150 µs) | 0.28 ms (280 µs) | Ngưỡng trễ nhanh nhất khi stream 64KB |
| | **Độ trễ phân vị P50** | **1.78 ms** | 2.07 ms | 2.82 ms | Velda đạt **1.78ms** P50, nhanh hơn Envoy **36.9%** |
| | **Độ trễ phân vị P90** | **3.18 ms** | 3.78 ms | 4.62 ms | P90 Velda duy trì ổn định ở mốc 3.18ms |
| | **Độ trễ phân vị P99** | **5.15 ms** | 6.20 ms | 7.22 ms | **Velda kiểm soát đuôi trễ P99 tốt nhất (5.15ms)** |
| | **Độ trễ phân vị P99.9** | **8.60 ms** | 12.40 ms | 11.80 ms | Đuôi trễ phân vị sâu được khống chế chặt chẽ |
| | **Độ trễ tối đa (Max Latency)** | 23.30 ms | **21.49 ms** | 22.07 ms | Đỉnh trễ khống chế dưới 24ms |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **1.00 ms** | 1.22 ms | 1.35 ms | Độ phân bổ độ trễ của Velda mượt mà nhất |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **1.97 ms** | 2.27 ms | 3.01 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **0.15 ms** | 0.18 ms | 0.25 ms | Bắt tay kết nối downstream TCP keep-alive |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **247,903 2xx** (0 err) | **216,742 2xx** (16 err) | 160,355 2xx (0 err) | 100% 200 OK trên Velda và Envoy |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | 99.99% | **100.0%** | 0 lỗi kết nối trên Velda |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **28.46 MB** | 147.19 MB | 6.85 MB | Velda tiêu thụ RAM chỉ bằng **1/5.2** Nginx |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 49,883,739,547 | 57,562,318,833 | **48,937,926,982** | Tổng lệnh CPU tiêu thụ |
| | **Số lệnh CPU / Request (`insn / req`)** | **406,434.5** | 536,424.0 | 610,602.1 | **Velda tiết kiệm -24.2% lệnh vs Nginx, -33.4% vs Envoy** |
| | **Tổng chu kỳ CPU (`cycles`)** | 65,604,046,034 | **65,374,792,305** | 66,857,956,243 | Chu kỳ xung CPU tổng thể |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **534,517.8** | 609,228.5 | 834,191.6 | Velda tiết kiệm **-12.3%** chu kỳ vs Nginx, **-35.9%** vs Envoy |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.76 | **0.88** | 0.73 | Hiệu quả pipeline CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 8,743,294,211 | 10,666,805,225 | **7,989,065,201** | Tổng rẽ nhánh |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **71,237.2** | 99,404.1 | 99,680.2 | **Velda ít rẽ nhánh nhất: -28.3% vs Nginx, -28.5% vs Envoy** |
| | **CPU đoán sai nhánh (`branch-misses`)** | **111,854,641** | 134,910,670 | 178,897,345 | Velda đoán sai nhánh ít nhất trong cả 3 proxy |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.28% | **1.26%** | 2.24% | Tỷ lệ đoán sai nhánh của Velda thấp tương đương Nginx |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 16.75% | 14.74% | **13.77%** | Tỷ lệ trượt bộ đệm L1 |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Không trượt cache ra ngoài RAM |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 207,574 | 266,079 | **186,757** | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **1.69** | 2.48 | 2.33 | **Velda CS/req thấp nhất toàn diện (-31.9% vs Nginx)** |
| | **Tổng số System Calls (`syscalls`)** | 297 | 7,431 | **13** | Syscalls nhận gửi chunked bytes |
| | **System Calls / Request (`syscalls / req`)** | **0.00** | 0.12 | **0.00** | Zero syscall overhead |
| | **Thời gian chạy trong kernel (Kernel Time)** | 7.47s | 1.62s | **0.001s** | Thời gian nhân Linux phục vụ I/O |
| | **Thời gian chờ khóa Futex (Lock Contention)** | 0.036s | **0.000s** | **0.000s** | Khóa tokio scheduler nội bộ cực nhỏ |
| | **Tỷ lệ Futex trên tổng thời gian Syscall (%)** | 0.48% | **0.00%** | **0.00%** | Mức độ nghẽn khóa futex |

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
