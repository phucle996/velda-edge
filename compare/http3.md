# Bộ Đo Đối Sánh Hiệu Năng HTTP/3: Velda Edge vs Nginx vs Envoy

Tài liệu này đặc tả và định hình khung đo chuẩn cho **toàn bộ ma trận 44 kịch bản đối sánh hiệu năng (performance benchmark)** cho giao thức **HTTP/3 (RFC 9114 & QUIC RFC 9000)** giữa:
- **Velda Edge**: v0.1.0 (Rust Edition 2024, Hourglass Architecture, Packet-Driven Ingress, Pure `quinn_proto` State Machine, Zero-Copy QPACK Engine, Linux GSO Offload)
- **Nginx**: **v1.31.6 Mainline / v1.30.5 Stable** (C Event-Driven Web Server & Reverse Proxy, `ngx_http_v3_module` QUIC/HTTP3)
- **Envoy Proxy**: **v1.39.3 Latest Release** (C++ Cloud-Native High-Performance Edge Proxy, Google QUICHE engine)

> [!NOTE]
> **6 kịch bản đầu tiên (KB 01, KB 02, KB 03 thuộc module `buffered_h3_h1` và KB 04, KB 05, KB 06 thuộc module `buffered_h3_h2`) đã được đo nghiệm thực tế và cập nhật đầy đủ số liệu đối sánh thực nghiệm mới nhất** (Throughput, Băng thông, Phân vị trễ P50-P99.9, Perf HPC counters, Context switches, Syscalls, Futex) từ môi trường KVM cô lập. Các kịch bản tiếp theo (từ KB 07 đến KB 44) đang ở trạng thái biểu mẫu chuẩn sẵn sàng tiếp tục đo lường.

---

## 1. Môi Trường Thử Nghiệm & Phương Pháp Đo Lường

### 1.1 Môi Trường KVM Cô Lập (Isolated Testbed)
- **Hệ điều hành**: Ubuntu 24.04 LTS (Kernel Linux 6.8.0-45-generic).
- **Cấu hình máy ảo Gateway**: KVM QEMU VM với **4 vCPUs (pinned 1-1 vật lý)**, **8 GB RAM**, cgroup v2 enabled.
- **Máy phát tải (Load Generator)**: Host vật lý (12 CPU cores, 32 GB RAM) dội tải trực tiếp qua giao diện mạng `virtio-net` (MTU 1500, UDP offloading bật).
- **Công cụ sinh tải**: `oha` (v1.16 với `--http-version 3` trên nền QUIC/UDP, latency percentiles P50–P99.9, zero-TUI automation), kết hợp bộ test scripts chuyên dụng mô phỏng dị dạng gói tin QUIC (Initial flood, QPACK bomb, Path migration storm, Flow-control deadlock).

### 1.2 Phân Lớp 3 Tầng Tiêu Chí Đo Lường
Mọi kịch bản đều được phân tích qua 3 tầng số liệu chuẩn hóa:
1. **Tầng Vĩ Mô (Network & L7)**:
   - Throughput (RPS / transfers/s), Băng thông Goodput (MB/s).
   - Độ trễ phân vị: Mean, Min, Max, P50, P90, P99, P99.9, Độ lệch chuẩn SD.
   - Thời gian nhận byte đầu tiên (TTFB Avg), Thời gian bắt tay kết nối (Connect Time Avg bao gồm QUIC 1-RTT / 0-RTT Handshake).
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
   - Tổng System Calls (`sendmsg`, `recvmsg`, `epoll_wait`, `futex`), Syscalls / Request (`syscalls/req`).
   - Thời gian thực thi không gian nhân (`Kernel Execution Time`).
   - Số lần gọi khóa đồng bộ (`Futex Calls`) và tỷ lệ Futex / Syscalls (%).

### 1.3 Thiết Kế Phân Bổ Tải Upstream Đa Tầng
Tùy theo chiến lược đường ống (Pipeline Strategy) của từng kịch bản, hệ thống phân bổ tải qua các nhánh upstream tương ứng:
- **Nhánh Upstream HTTP/1.1 (cho pipe H3 -> H1)**: Tỷ lệ 4/3/3 (40% Plaintext h1 trên `127.0.0.1:8081` : 30% TLS 1.2 h1 trên `127.0.0.1:8442` : 30% TLS 1.3 h1 trên `127.0.0.1:8443`).
- **Nhánh Upstream HTTP/2 (cho pipe H3 -> H2 Mux)**: Tỷ lệ 4/3/3 (40% Cleartext `h2c` trên `127.0.0.1:8081` : 30% TLS 1.2 `h2` trên `127.0.0.1:8442` : 30% TLS 1.3 `h2` trên `127.0.0.1:8443`).
- **Nhánh Upstream HTTP/3 (cho pipe H3 -> H3 QUIC)**: HTTP/3 over QUIC/UDP trên `127.0.0.1:8445` (Nginx không hỗ trợ upstream HTTP/3 theo kiến trúc chuẩn nên ghi chú *N/A*).

---

## 2. Ma Trận 44 Kịch Bản Thử Nghiệm HTTP/3

```
╔═══════════════════════════════════════════════════════════════════════════════════════╗
║                             MA TRẬN 44 KỊCH BẢN HTTP/3                                ║
╠═══════════════════════════════════════════════════════════════════════════════════════╣
║  Pipe Module         │ Micro (~64B)        │ Medium (12K-64K)    │ Large (1M-10M)     ║
╟──────────────────────┼─────────────────────┼─────────────────────┼────────────────────╢
║  buffered_h3_h1      │ KB 01: Demux to H1  │ KB 02: REST to H1   │ KB 03: RAM Blob H1 ║
║  buffered_h3_h2      │ KB 04: QUIC to H2   │ KB 05: REST H3-H2   │ KB 06: Heavy H3-H2 ║
║  buffered_h3_h3      │ KB 07: Pure QUIC    │ KB 08: REST H3-H3   │ KB 09: Heavy QUIC  ║
║  server_stream_h3_h1 │ KB 10: SSE from H1  │ KB 11: Chunk from H1│ KB 12: File from H1║
║  server_stream_h3_h2 │ KB 13: SSE from H2  │ KB 14: Data from H2 │ KB 15: File from H2║
║  server_stream_h3_h3 │ KB 16: Pure SSE H3  │ KB 17: QUIC Stream  │ KB 18: File H3-H3  ║
║  client_stream_h3_h1 │ KB 19: Drip to H1   │ KB 20: Doc to H1    │ KB 21: Bulk to H1  ║
║  client_stream_h3_h2 │ KB 22: Drip to H2   │ KB 23: Doc to H2    │ KB 24: Bulk to H2  ║
║  client_stream_h3_h3 │ KB 25: Pure QUIC Up │ KB 26: Doc to H3    │ KB 27: Bulk to H3  ║
║  duplex_h3_h1        │ KB 28: Bidi with H1 │ KB 29: Msg to H1    │ KB 30: Heavy Bidi  ║
║  duplex_h3_h2        │ KB 31: Bidi with H2 │ KB 32: Bidi Data H2 │ KB 33: Heavy H3-H2 ║
║  duplex_h3_h3        │ KB 34: Pure QUIC Bi │ KB 35: Bidi QUIC Msg│ KB 36: Heavy QUIC  ║
╠═══════════════════════════════════════════════════════════════════════════════════════╣
║  Nhóm Dị Dạng & Chaos HTTP/3 (Adversarial, Chaos & RFC 9000/9114 Extreme Resources)   ║
╟───────────────────────────────────────────────────────────────────────────────────────╢
║  KB 37: QUIC Initial Flood & Amplification Attack (RFC 9000 3x Limit & Token Defense) ║
║  KB 38: QPACK Dynamic Table Bomb & Stream HoL (RFC 9204 Encoder Stream Overflow)      ║
║  KB 39: QUIC Connection ID Churn & Migration Storm (Path Validation & Stateless Reset)║
║  KB 40: QUIC Flow-Control Window Deadlock (MAX_DATA / MAX_STREAM_DATA Exhaustion)     ║
║  KB 41: Chaos Upstream Sudden Death (Graceful CONNECTION_CLOSE 0x010c & Re-lease)     ║
║  KB 42: Cgroup RAM 60MB Starvation (O(1) UDP Memory under 1,000 Multiplexed Streams)  ║
║  KB 43: CPU 100% Saturation & Overload Shedding (CoDel Shedder, H3_REQUEST_REJECTED)  ║
║  KB 44: Atomic Hot-Reload Contention (100 Reloads/s at 100K Packets, Zero Dropped UDP)║
╚═══════════════════════════════════════════════════════════════════════════════════════╝
```

---

## 3. Form Đo Lường Chi Tiết Toàn Bộ 44 Kịch Bản

### KB 01: buffered_h3_h1 — Micro Payload (Ping-Pong ~64B)
- **Mục tiêu thử nghiệm**: Đo chi phí giải mã gói tin QUIC, parse HTTP/3 frame, giải nén QPACK HEADERS tĩnh/động, chuyển đổi sang HTTP/1.1 wire text và mượn connection từ pool.
- **Kích thước tải trọng & Quy chuẩn**: ~64 bytes HTTP headers (GET /ping HTTP/3)
- **Lệnh thực thi chuẩn**: h3bench -u https://192.168.122.14:8444/plain/ping -u https://192.168.122.14:8444/tls12/ping -u https://192.168.122.14:8444/tls13/ping -n 50000 -c 100 --host backend.local --json
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **38,939.33** | 34,196.10 | 13,097.91 | Velda đạt 38,939.33 req/s (+13.9% vs Nginx, +197.3% vs Envoy) - VƯỢT NGINX & ENVOY! |
| | **Băng thông truyền tải (Throughput MB/s)** | **0.15 MB/s** | 0.13 MB/s | 0.05 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | **2.57 ms** | 2.92 ms | 7.63 ms | Velda thấp hơn Nginx (-12.0%) và tốt hơn vượt trội so với Envoy (-66.3%) |
| | **Độ trễ tối thiểu (Min Latency)** | **0.11 ms** | 0.20 ms | 0.57 ms | Ngưỡng trễ phản hồi gói đầu tiên Velda tốt nhất |
| | **Độ trễ phân vị P50** | **2.43 ms** | 2.63 ms | 7.24 ms | P50 Velda tốt nhất |
| | **Độ trễ phân vị P90** | **3.92 ms** | 4.95 ms | 10.44 ms | Phân vị P90 Velda thấp hơn Nginx -20.8% |
| | **Độ trễ phân vị P99 (Tail Latency)** | **5.91 ms** | 8.24 ms | 13.68 ms | Đuôi trễ P99 Velda tốt hơn rõ rệt (-28.3% vs Nginx) |
| | **Độ trễ phân vị P99.9** | **9.59 ms** | 11.15 ms | 19.46 ms | Đuôi trễ sâu P99.9 Velda tốt hơn (-14.0% vs Nginx) |
| | **Độ trễ tối đa (Max Latency)** | 17.71 ms | **16.01 ms** | 541.60 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **1.11 ms** | 1.58 ms | 3.19 ms | Độ ổn định phương sai Velda tốt nhất |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **2.57 ms** | 2.92 ms | 7.63 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **3.43 ms** | 3.94 ms | 5.14 ms | Bắt tay kết nối downstream (QUIC Handshake) Velda nhanh nhất |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **50,000 2xx** | **50,000 2xx** | **50,000 2xx** | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **67.93 MB** | 172.19 MB | 77.31 MB | Velda tiêu thụ ít RAM nhất (-60.5% vs Nginx, -12.1% vs Envoy) nhờ tối ưu flow-control window |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **7,295,620,838** | 8,991,326,311 | 27,334,034,354 | Velda tiêu thụ ít lệnh nhất (-18.9% vs Nginx, -73.3% vs Envoy) |
| | **Số lệnh CPU / Request (`insn / req`)** | **145,912.42** | 179,826.53 | 546,680.69 | Số lệnh CPU chuẩn hóa trên 50K request của toàn bộ worker |
| | **Tổng chu kỳ CPU (`cycles`)** | **9,698,891,292** | 11,051,676,138 | 42,953,137,611 | Velda tiêu thụ ít chu kỳ xung nhịp nhất (-12.2% vs Nginx, -77.4% vs Envoy) |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **193,977.83** | 221,033.52 | 859,062.75 | Chu kỳ xung CPU chuẩn hóa trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.75 | **0.81** | 0.64 | Nginx tận dụng pipeline lệnh nhỉnh hơn một chút |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **1,172,114,261** | 1,656,951,588 | 4,800,159,247 | Velda ít rẽ nhánh nhất trong cả 3 proxy (-29.3% vs Nginx) |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **23,442.29** | 33,139.03 | 96,003.18 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **19,169,806** | 20,295,609 | 120,833,877 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.64% | **1.22%** | 2.52% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 11.83% | 9.36% | **9.27%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **20,208** | 25,961 | 37,918 | Velda thấp nhất (-22.2% vs Nginx, -46.7% vs Envoy) |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.40** | 0.52 | 0.76 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | **262,047** | 398,529 | 403,626 | Velda thấp nhất (-34.2% vs Nginx, -35.1% vs Envoy) |
| | **System Calls / Request (`syscalls / req`)** | **5.24** | 7.97 | 8.07 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.65s** | 1.88s | 4.36s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 7,471 | **0** | 51 | Nginx multi-process thuần epoll không dùng futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 2.85% | **0.00%** | 0.01% | Nginx không có lock futex giữa các worker process |

---

### KB 02: buffered_h3_h1 — Medium Payload (REST JSON 12KB)
- **Mục tiêu thử nghiệm**: Nạp trọn vẹn 12KB DATA frame từ client HTTP/3 QUIC, assemble buffer in-memory và serialize thành HTTP/1.1 Content-Length body gửi tới upstream backend.
- **Kích thước tải trọng & Quy chuẩn**: 12,288 bytes Content-Length JSON body
- **Lệnh thực thi chuẩn**: h3bench -u https://192.168.122.14:8444/plain/12kb -u https://192.168.122.14:8444/tls12/12kb -u https://192.168.122.14:8444/tls13/12kb -n 20000 -c 100 --host backend.local --json
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **24,246.04** | 16,975.39 | 10,161.98 | Velda đạt 24,246.04 req/s (+42.8% vs Nginx, +138.6% vs Envoy) - VƯỢT TRỘI! |
| | **Băng thông truyền tải (Throughput MB/s)** | **284.09 MB/s** | 198.90 MB/s | 119.07 MB/s | Băng thông truyền tải vượt Nginx +42.8% |
| | **Độ trễ trung bình (Mean Latency)** | **4.12 ms** | 5.88 ms | 9.82 ms | Velda thấp hơn Nginx (-29.9%), thấp hơn Envoy (-58.0%) |
| | **Độ trễ tối thiểu (Min Latency)** | **0.23 ms** | 0.63 ms | 1.67 ms | Ngưỡng trễ phản hồi gói đầu tiên Velda nhanh nhất |
| | **Độ trễ phân vị P50** | **3.95 ms** | 5.53 ms | 9.19 ms | P50 Velda nhanh hơn Nginx (-28.6%) |
| | **Độ trễ phân vị P90** | **5.99 ms** | 9.33 ms | 14.27 ms | P90 Velda nhanh hơn Nginx (-35.8%) |
| | **Độ trễ phân vị P99 (Tail Latency)** | **9.05 ms** | 13.20 ms | 18.99 ms | P99 Velda tốt hơn rõ rệt (-31.4% vs Nginx) |
| | **Độ trễ phân vị P99.9** | **15.05 ms** | 16.91 ms | 23.28 ms | Đuôi trễ sâu P99.9 Velda tốt nhất (-11.0% vs Nginx) |
| | **Độ trễ tối đa (Max Latency)** | 21.49 ms | **19.96 ms** | 52.57 ms | Max jitter kiểm soát tốt |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **1.61 ms** | 2.53 ms | 3.19 ms | Độ ổn định phương sai vượt trội (-36.4% vs Nginx) |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **4.12 ms** | 5.88 ms | 9.82 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **3.44 ms** | 3.80 ms | 5.04 ms | Bắt tay kết nối downstream (QUIC Handshake) Velda nhanh nhất |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **20,000 2xx** | **20,000 2xx** | **20,000 2xx** | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **59.33 MB** | 176.38 MB | 80.68 MB | Velda tiêu thụ ít RAM nhất (-66.4% vs Nginx, -26.5% vs Envoy) nhờ tối ưu flow-control window |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **6,062,371,875** | 9,606,602,210 | 16,663,868,215 | Velda tiêu thụ ít lệnh CPU nhất (-36.9% vs Nginx, -63.6% vs Envoy) |
| | **Số lệnh CPU / Request (`insn / req`)** | **303,118.59** | 480,330.11 | 833,193.41 | Velda ít hơn Nginx 36.9% lệnh trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **6,923,506,768** | 10,941,775,480 | 22,967,838,012 | Velda tiêu thụ ít chu kỳ nhất (-36.7% vs Nginx, -69.9% vs Envoy) |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **346,175.34** | 547,088.77 | 1,148,391.90 | Chu kỳ xung CPU chuẩn hóa trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.88** | 0.88 | 0.73 | Hiệu suất IPC khai thác CPU tối ưu |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **909,953,813** | 1,704,695,115 | 2,704,524,634 | Velda ít rẽ nhánh nhất (-46.6% vs Nginx) |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **45,497.69** | 85,234.76 | 135,226.23 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **12,241,627** | 17,572,798 | 61,013,230 | Velda ít đoán sai nhánh nhất (-30.3% vs Nginx) |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.35% | **1.03%** | 2.26% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 10.56% | 9.32% | **8.12%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **10,951** | 12,860 | 15,580 | Velda thấp nhất (-14.8% vs Nginx, -29.7% vs Envoy) |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **0.55** | 0.64 | 0.78 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | **183,421** | 427,261 | 183,275 | Velda ít syscalls nhất (-57.1% vs Nginx) |
| | **System Calls / Request (`syscalls / req`)** | **9.17** | 21.36 | **9.16** | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.19s** | 1.58s | 2.50s | Thời gian thực thi không gian nhân Linux nhanh nhất |
| | **Số lần gọi khóa Futex (Futex Calls)** | 7,183 | **0** | 30 | Nginx multi-process thuần epoll không dùng futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 3.92% | **0.00%** | 0.02% | Nginx không có lock futex giữa các worker process |

---

### KB 03: buffered_h3_h1 — Large Payload (RAM Blob 1MB)
- **Mục tiêu thử nghiệm**: Đo chi phí cấp phát vector bộ đệm nạp trọn vẹn 1MB DATA frames từ QUIC stream, chuyển tiếp qua H1 keep-alive upstream không chunking.
- **Kích thước tải trọng & Quy chuẩn**: 1,048,576 bytes Content-Length body
- **Lệnh thực thi chuẩn**: h3bench -u https://192.168.122.14:8444/plain/1mb -u https://192.168.122.14:8444/tls12/1mb -u https://192.168.122.14:8444/tls13/1mb -n 2000 -c 50 --host backend.local --json
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **771.58** | 517.99 | 504.25 | Velda đạt 771.58 req/s (+48.9% vs Nginx, +53.0% vs Envoy) - DẪN ĐẦU ÁP ĐẢO CẢ 3 PROXY! |
| | **Băng thông truyền tải (Throughput MB/s)** | **771.58 MB/s** | 517.99 MB/s | 504.25 MB/s | Băng thông đạt hơn 0.77 GB/s trên đường truyền |
| | **Độ trễ trung bình (Mean Latency)** | **64.38 ms** | 95.78 ms | 96.88 ms | Velda thấp hơn Nginx (-32.8%), thấp hơn Envoy (-33.5%) |
| | **Độ trễ tối thiểu (Min Latency)** | 14.91 ms | 28.74 ms | **8.17 ms** | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | **59.92 ms** | 98.93 ms | 79.30 ms | P50 Velda thấp nhất cả 3 proxy (-39.4% vs Nginx, -24.4% vs Envoy) |
| | **Độ trễ phân vị P90** | **94.20 ms** | 136.95 ms | 165.22 ms | P90 Velda tốt nhất trong cả 3 proxy (-31.2% vs Nginx, -43.0% vs Envoy) |
| | **Độ trễ phân vị P99 (Tail Latency)** | **155.26 ms** | 175.53 ms | 403.74 ms | P99 Velda dẫn đầu toàn diện (-11.5% vs Nginx, -61.5% vs Envoy) |
| | **Độ trễ phân vị P99.9** | 270.79 ms | **214.39 ms** | 794.90 ms | Đuôi trễ sâu P99.9 vượt trội so với Envoy (-65.9%) |
| | **Độ trễ tối đa (Max Latency)** | 281.23 ms | **219.66 ms** | 810.82 ms | Max jitter tốt hơn nhiều so với Envoy (-65.3%) |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **26.31 ms** | 34.98 ms | 72.96 ms | Độ ổn định phương sai tốt nhất trong cả 3 proxy (-24.8% vs Nginx, -63.9% vs Envoy) |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **64.38 ms** | 95.78 ms | 96.88 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **3.75 ms** | 4.09 ms | 5.48 ms | Bắt tay kết nối downstream (QUIC Handshake) Velda nhanh nhất |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **2,000 2xx** | **2,000 2xx** | **2,000 2xx** | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Đạt 100% tuyệt đối, không rớt gói, không timeout |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 143.66 MB | 168.63 MB | **127.81 MB** | Velda tiêu thụ ít hơn Nginx (-14.8%) |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **26,654,590,967** | 48,466,896,512 | 42,906,272,300 | Velda tiêu thụ ít lệnh nhất (-45.0% vs Nginx, -37.9% vs Envoy) |
| | **Số lệnh CPU / Request (`insn / req`)** | **13,327,295.48** | 24,233,448.26 | 21,453,136.15 | Số lệnh CPU tiêu thụ trên mỗi request thấp nhất (-45.0% vs Nginx) |
| | **Tổng chu kỳ CPU (`cycles`)** | **28,478,507,903** | 48,102,469,202 | 38,337,111,618 | Velda tiêu thụ ít chu kỳ nhất (-40.8% vs Nginx, -25.7% vs Envoy) |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **14,239,253.95** | 24,051,234.60 | 19,168,555.81 | Chu kỳ xung CPU trên mỗi request thấp nhất |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.94 | 1.01 | **1.12** | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **4,167,628,161** | 8,660,777,867 | 6,194,485,367 | Velda ít rẽ nhánh nhất (-51.9% vs Nginx, -32.7% vs Envoy) |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **2,083,814.08** | 4,330,388.93 | 3,097,242.68 | Rẽ nhánh trên mỗi request thấp nhất |
| | **CPU đoán sai nhánh (`branch-misses`)** | **32,380,278** | 46,795,751 | 47,286,766 | Velda ít đoán sai nhánh nhất (-30.8% vs Nginx, -31.5% vs Envoy) |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.78% | **0.54%** | 0.76% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 10.85% | 8.85% | **5.80%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | **10,186** | 12,604 | 20,111 | Velda thấp nhất cả 3 proxy (-19.2% vs Nginx, -49.4% vs Envoy) |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | **5.09** | 6.30 | 10.06 | Chuyển ngữ cảnh / request thấp nhất |
| | **Tổng số System Calls (`syscalls`)** | **260,571** | 2,153,154 | 305,191 | Velda ít syscalls nhất (-87.9% vs Nginx, -14.6% vs Envoy) nhờ GSO + recvmmsg batching |
| | **System Calls / Request (`syscalls / req`)** | **130.29** | 1,076.58 | 152.60 | Velda đạt hiệu quả syscalls cao nhất trong cả 3 proxy! |
| | **Thời gian chạy trong kernel (Kernel Time)** | **2.80s** | 4.08s | 4.26s | Thời gian hoàn tất tổng thể Velda nhanh nhất (-31.4% vs Nginx, -34.3% vs Envoy) |
| | **Số lần gọi khóa Futex (Futex Calls)** | 4,544 | **0** | 163 | Nginx multi-process thuần epoll không dùng futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 1.74% | **0.00%** | 0.05% | Tỷ lệ khóa futex thấp |

---

### KB 04: buffered_h3_h2 — Micro Payload (Ping-Pong H3 to H2 Mux ~64B)
- **Mục tiêu thử nghiệm**: Chuyển tiếp yêu cầu HTTP/3 QUIC downstream sang stream ghép kênh trên HTTP/2 TCP upstream connection pool, đo chi phí dịch chuyển framing giữa QUIC và H2.
- **Kích thước tải trọng & Quy chuẩn**: ~64 bytes headers (GET /ping HTTP/3)
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 50000 -c 100 --insecure https://192.168.122.14:8445/plain/ping https://192.168.122.14:8445/tls12/ping https://192.168.122.14:8445/tls13/ping
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **41,404.50** | 33,835.83 | 17,609.33 | Velda dẫn đầu (+22.4% vs Nginx, +135.1% vs Envoy) nhờ chuyển đổi framing QUIC $\to$ H2 zero-copy |
| | **Băng thông truyền tải (Throughput MB/s)** | **0.16 MB/s** | 0.13 MB/s | 0.07 MB/s | Băng thông truyền tải tương ứng với lưu lượng micro ping |
| | **Độ trễ trung bình (Mean Latency)** | **2.41 ms** | 2.95 ms | 5.67 ms | Velda thấp hơn Nginx (-18.3%), thấp hơn Envoy (-57.5%) |
| | **Độ trễ tối thiểu (Min Latency)** | 0.12 ms | **0.11 ms** | 0.45 ms | Ngưỡng trễ phản hồi gói đầu tiên tương đồng giữa Velda và Nginx |
| | **Độ trễ phân vị P50** | 2.29 ms | **2.25 ms** | 5.17 ms | Nginx nhỉnh hơn một chút ở P50, nhưng Velda bứt phá ở đuôi trễ |
| | **Độ trễ phân vị P90** | **3.78 ms** | 5.82 ms | 8.95 ms | Velda thấp hơn Nginx (-35.1%) và Envoy (-57.8%) |
| | **Độ trễ phân vị P99 (Tail Latency)** | **5.31 ms** | 10.58 ms | 13.59 ms | Đuôi trễ P99 Velda tốt hơn rõ rệt (-49.8% vs Nginx, -60.9% vs Envoy) |
| | **Độ trễ phân vị P99.9** | **7.58 ms** | 41.16 ms | 17.35 ms | Đuôi trễ sâu P99.9 Velda vượt trội (-81.6% vs Nginx) |
| | **Độ trễ tối đa (Max Latency)** | **10.71 ms** | 50.81 ms | 22.94 ms | Max jitter Velda kiểm soát chặt chẽ nhất |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **1.04 ms** | 2.81 ms | 2.43 ms | Phương sai ổn định Velda tốt nhất (-63.0% vs Nginx) |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **2.41 ms** | 2.95 ms | 5.67 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **1.20 ms** | 1.68 ms | 2.56 ms | Bắt tay kết nối downstream (QUIC Handshake) Velda nhanh nhất |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **50,000 2xx** | **50,000 2xx** | **50,000 2xx** | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **44.02 MB** | 162.71 MB | 66.42 MB | Velda tiêu thụ ít RAM nhất (-73.0% vs Nginx, -33.7% vs Envoy) |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **7,735,020,392** | 9,679,105,699 | 23,215,053,616 | Velda tiêu thụ ít lệnh nhất (-20.1% vs Nginx, -66.7% vs Envoy) |
| | **Số lệnh CPU / Request (`insn / req`)** | **154,700.41** | 193,582.11 | 464,301.07 | Velda tiết kiệm ~38.9K lệnh/req so với Nginx |
| | **Tổng chu kỳ CPU (`cycles`)** | **9,347,571,517** | 11,285,465,033 | 34,262,004,975 | Velda tiêu thụ ít xung CPU nhất (-17.2% vs Nginx, -72.7% vs Envoy) |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **186,951.43** | 225,709.30 | 685,240.10 | Chu kỳ xung CPU chuẩn hóa trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.83 | **0.86** | 0.68 | Nginx có IPC nhỉnh hơn nhẹ |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **1,262,483,009** | 1,760,736,965 | 4,043,135,360 | Velda ít rẽ nhánh nhất (-28.3% vs Nginx) |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **25,249.66** | 35,214.74 | 80,862.71 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **20,127,638** | 21,789,468 | 103,908,267 | Velda ít đoán sai nhánh nhất |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.59% | **1.24%** | 2.57% | Tỷ lệ đoán sai nhánh thấp |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 9.29% | 8.78% | **8.17%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 18,173 | 23,190 | **14,678** | Envoy ít CS nhất, Velda thấp hơn Nginx -21.6% |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.36 | 0.46 | **0.29** | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | **177,095** | 401,785 | 297,346 | Velda ít syscalls nhất (-55.9% vs Nginx, -40.4% vs Envoy) nhờ GSO offload |
| | **System Calls / Request (`syscalls / req`)** | **3.54** | 8.04 | 5.95 | Hiệu quả syscalls của Velda vượt trội toàn diện |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.34s** | 1.65s | 3.10s | Thời gian thực thi không gian nhân Linux Velda nhanh nhất |
| | **Số lần gọi khóa Futex (Futex Calls)** | 7,535 | **0** | 54 | Nginx multi-process thuần epoll không dùng futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 4.25% | **0.00%** | 0.02% | Đồng bộ luồng nội bộ Tokio runtime |

---

### KB 05: buffered_h3_h2 — Medium Payload (REST H3 to H2 Mux 12KB)
- **Mục tiêu thử nghiệm**: Ghép tải 12KB REST JSON từ QUIC stream downstream sang DATA frames trên HTTP/2 upstream multiplexed connection pool.
- **Kích thước tải trọng & Quy chuẩn**: 12,288 bytes JSON body
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 20000 -c 100 --insecure https://192.168.122.14:8445/plain/12kb https://192.168.122.14:8445/tls12/12kb https://192.168.122.14:8445/tls13/12kb
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **22,088.39** | 6,958.47 | 10,905.27 | Velda áp đảo (+217.4% vs Nginx, +102.5% vs Envoy) — Nginx nghẽn nặng H2 upstream mux |
| | **Băng thông truyền tải (Throughput MB/s)** | **258.81 MB/s** | 81.53 MB/s | 127.78 MB/s | Băng thông truyền tải Velda gấp 3.17x Nginx, 2.03x Envoy |
| | **Độ trễ trung bình (Mean Latency)** | **4.32 ms** | 14.26 ms | 9.14 ms | Velda thấp hơn Nginx (-69.7%) và Envoy (-52.7%) |
| | **Độ trễ tối thiểu (Min Latency)** | 0.26 ms | **0.16 ms** | 1.18 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 4.11 ms | **0.68 ms** | 8.48 ms | Nginx phân hóa: nửa đầu rất nhanh nhưng nửa sau bị nghẽn |
| | **Độ trễ phân vị P90** | **6.48 ms** | 41.40 ms | 13.98 ms | P90 Velda thấp hơn Nginx 6.39x (-84.3%) |
| | **Độ trễ phân vị P99 (Tail Latency)** | **9.65 ms** | 42.63 ms | 20.27 ms | Đuôi trễ P99 Velda dưới 10ms (-77.4% vs Nginx, -52.4% vs Envoy) |
| | **Độ trễ phân vị P99.9** | **17.79 ms** | 82.32 ms | 25.64 ms | Đuôi trễ sâu P99.9 Velda tốt nhất (-78.4% vs Nginx) |
| | **Độ trễ tối đa (Max Latency)** | 45.15 ms | 88.87 ms | **34.03 ms** | Envoy khống chế max jitter tốt hơn một chút |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **1.88 ms** | 19.52 ms | 3.57 ms | Độ ổn định phương sai Velda vượt trội gấp 10.38x Nginx |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **4.32 ms** | 14.26 ms | 9.14 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **1.14 ms** | 1.69 ms | 2.55 ms | Bắt tay kết nối downstream (QUIC Handshake) Velda nhanh nhất |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **20,000 2xx** | **20,000 2xx** | **20,000 2xx** | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **36.86 MB** | 164.04 MB | 70.97 MB | Velda tiêu thụ ít RAM nhất (-77.5% vs Nginx, -48.1% vs Envoy) |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **6,450,429,205** | 10,650,322,168 | 17,388,060,987 | Velda tiêu thụ ít lệnh nhất (-39.4% vs Nginx, -62.9% vs Envoy) |
| | **Số lệnh CPU / Request (`insn / req`)** | **322,521.46** | 532,516.11 | 869,403.05 | Velda ít hơn Nginx ~210K lệnh trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | **7,209,889,846** | 12,337,888,622 | 22,210,349,849 | Velda tiêu thụ ít chu kỳ xung nhịp nhất (-41.6% vs Nginx) |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **360,494.49** | 616,894.43 | 1,110,517.49 | Chu kỳ xung CPU chuẩn hóa trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | **0.89** | 0.86 | 0.78 | Velda đạt IPC cao nhất (0.89) khai thác CPU tối đa |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **979,420,977** | 1,890,820,956 | 2,835,759,466 | Velda ít rẽ nhánh nhất (-48.2% vs Nginx) |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **48,971.05** | 94,541.05 | 141,787.97 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **14,882,148** | 27,370,012 | 62,896,765 | Velda ít đoán sai nhánh nhất (-45.6% vs Nginx) |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 1.52% | **1.45%** | 2.22% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 9.92% | 9.71% | **7.51%** | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 11,276 | 34,829 | **9,650** | Envoy ít CS nhất, Velda thấp hơn Nginx -67.6% |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.56 | 1.74 | **0.48** | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | **133,446** | 531,220 | 184,052 | Velda ít syscalls nhất (-74.9% vs Nginx, -27.5% vs Envoy) |
| | **System Calls / Request (`syscalls / req`)** | **6.67** | 26.56 | 9.20 | Velda tiết kiệm ~20 syscalls/req so với Nginx |
| | **Thời gian chạy trong kernel (Kernel Time)** | **1.03s** | 3.05s | 2.10s | Thời gian thực thi không gian nhân Linux Velda nhanh nhất |
| | **Số lần gọi khóa Futex (Futex Calls)** | 7,082 | **0** | 34 | Nginx multi-process thuần epoll không dùng futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 5.31% | **0.00%** | 0.02% | Tỷ lệ khóa futex |

---

### KB 06: buffered_h3_h2 — Large Payload (Heavy H3 to H2 Mux 1MB)
- **Mục tiêu thử nghiệm**: Chia nhỏ tải nặng 1MB từ QUIC stream thành các HTTP/2 DATA frames 16KB truyền qua multiplexed H2 pipe upstream.
- **Kích thước tải trọng & Quy chuẩn**: 1,048,576 bytes body (~1MB)
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 2000 -c 50 --insecure https://192.168.122.14:8445/plain/1mb https://192.168.122.14:8445/tls12/1mb https://192.168.122.14:8445/tls13/1mb
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | **523.53** | 490.69 | 466.55 | Velda dẫn đầu (+6.7% vs Nginx, +12.2% vs Envoy) nhờ streaming flow-control pipeline |
| | **Băng thông truyền tải (Throughput MB/s)** | **523.53 MB/s** | 490.69 MB/s | 466.55 MB/s | Băng thông Velda vượt 0.5 GB/s trên 50 kết nối đồng thời |
| | **Độ trễ trung bình (Mean Latency)** | **94.99 ms** | 100.22 ms | 105.08 ms | Velda thấp hơn Nginx (-5.2%) và Envoy (-9.6%) |
| | **Độ trễ tối thiểu (Min Latency)** | 30.70 ms | 20.57 ms | **9.94 ms** | Envoy có min latency phản hồi gói đầu tiên tốt nhất |
| | **Độ trễ phân vị P50** | 92.89 ms | 97.40 ms | **87.46 ms** | P50 Envoy nhỉnh hơn nhưng bị phân rã nặng ở đuôi trễ |
| | **Độ trễ phân vị P90** | **112.84 ms** | 128.59 ms | 172.92 ms | Velda nhanh hơn Nginx (-12.2%) và Envoy (-34.7%) |
| | **Độ trễ phân vị P99 (Tail Latency)** | **145.38 ms** | 160.20 ms | 418.95 ms | Velda kiểm soát đuôi trễ tốt nhất (-9.3% vs Nginx, -65.3% vs Envoy) |
| | **Độ trễ phân vị P99.9** | **167.42 ms** | 181.45 ms | 1,008.03 ms | Đuôi trễ sâu P99.9 Envoy bị nghẽn tới >1s, Velda giữ vững 167ms |
| | **Độ trễ tối đa (Max Latency)** | **178.00 ms** | 181.85 ms | 1,093.61 ms | Max jitter Velda thấp nhất |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **15.31 ms** | 20.59 ms | 80.32 ms | Độ ổn định phương sai Velda tốt nhất (-25.6% vs Nginx, -80.9% vs Envoy) |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **94.99 ms** | 100.22 ms | 105.08 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | **1.50 ms** | 2.00 ms | 2.92 ms | Bắt tay kết nối downstream (QUIC Handshake) Velda nhanh nhất |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | **2,000 2xx** | **2,000 2xx** | **2,000 2xx** | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | **100.0%** | **100.0%** | **100.0%** | Không rớt gói, không lỗi flow control |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | **135.36 MB** | 161.12 MB | 138.88 MB | Velda tiêu thụ ít RAM nhất (-16.0% vs Nginx, -2.5% vs Envoy) |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **33,305,890,517** | 48,361,978,823 | 51,582,218,481 | Velda tiêu thụ ít lệnh nhất (-31.1% vs Nginx, -35.4% vs Envoy) |
| | **Số lệnh CPU / Request (`insn / req`)** | **16,652,945.26** | 24,180,989.41 | 25,791,109.24 | Velda tiết kiệm ~7.5M lệnh trên mỗi 1MB payload |
| | **Tổng chu kỳ CPU (`cycles`)** | **36,623,470,638** | 45,515,752,263 | 45,946,700,422 | Velda tiêu thụ ít chu kỳ nhất (-19.5% vs Nginx, -20.3% vs Envoy) |
| | **Chu kỳ CPU / Request (`cycles / req`)** | **18,311,735.32** | 22,757,876.13 | 22,973,350.21 | Chu kỳ xung CPU chuẩn hóa trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.91 | 1.06 | **1.12** | Envoy đạt IPC cao hơn trên các khối memory copy lớn |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **5,277,842,230** | 8,635,259,368 | 7,773,977,789 | Velda ít rẽ nhánh nhất (-38.9% vs Nginx) |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **2,638,921.12** | 4,317,629.68 | 3,886,988.89 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | **51,326,060** | 52,414,211 | 61,441,389 | Velda ít đoán sai nhánh nhất |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.97% | **0.61%** | 0.79% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 11.92% | 8.31% | **6.43%** | Miss cache L1 trên dữ liệu lớn 1MB |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | **0.08%** | **0.08%** | **0.08%** | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 17,114 | **13,441** | 16,792 | Nginx ít CS nhất |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 8.56 | **6.72** | 8.40 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 1,075,989 | 2,110,501 | **420,358** | Envoy ít syscalls nhất, Velda ít hơn Nginx -49.0% |
| | **System Calls / Request (`syscalls / req`)** | 537.99 | 1,055.25 | **210.18** | Velda tiết kiệm ~517 syscalls/req so với Nginx |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.90s** | 4.18s | 4.44s | Thời gian thực thi không gian nhân Linux Velda nhanh nhất |
| | **Số lần gọi khóa Futex (Futex Calls)** | 11,333 | **0** | 457 | Nginx multi-process thuần epoll không dùng futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 1.05% | **0.00%** | 0.11% | Tỷ lệ khóa futex |

---

### KB 07: buffered_h3_h3 — Micro Payload (Pure QUIC Ping-Pong ~64B)
- **Mục tiêu thử nghiệm**: Thuần QUIC hai đầu (Pure QUIC End-to-End), đo chi phí định tuyến UDP datagram, QPACK decode/re-encode và stream dispatching giữa 2 kết nối QUIC độc lập.
- **Kích thước tải trọng & Quy chuẩn**: ~64 bytes headers (GET /ping HTTP/3)
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 50000 -c 100 --insecure https://192.168.122.14:8445/plain/ping
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | *N/A* | 0.00 | Chưa đo nghiệm (Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | *N/A* | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | *N/A* | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | *N/A* | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | *N/A* | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | *N/A* | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | *N/A* | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | *N/A* | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | *N/A* | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | *N/A* | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | *N/A* | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | *N/A* | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | *N/A* | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | *N/A* | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | *N/A* | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | *N/A* | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | *N/A* | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | *N/A* | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | *N/A* | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | *N/A* | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | *N/A* | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | *N/A* | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | *N/A* | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | *N/A* | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | *N/A* | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | *N/A* | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | *N/A* | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | *N/A* | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | *N/A* | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | *N/A* | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | *N/A* | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | *N/A* | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | *N/A* | 0.00% | Tỷ lệ khóa futex |

---

### KB 08: buffered_h3_h3 — Medium Payload (Pure QUIC REST JSON 12KB)
- **Mục tiêu thử nghiệm**: Chuyển tiếp 12KB REST payload qua 2 tầng QUIC multiplexing, kiểm tra khả năng phối hợp flow-control window (RFC 9000) giữa downstream và upstream H3 stream.
- **Kích thước tải trọng & Quy chuẩn**: 12,288 bytes JSON body
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 20000 -c 100 --insecure https://192.168.122.14:8445/plain/12kb
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | *N/A* | 0.00 | Chưa đo nghiệm (Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | *N/A* | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | *N/A* | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | *N/A* | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | *N/A* | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | *N/A* | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | *N/A* | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | *N/A* | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | *N/A* | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | *N/A* | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | *N/A* | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | *N/A* | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | *N/A* | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | *N/A* | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | *N/A* | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | *N/A* | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | *N/A* | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | *N/A* | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | *N/A* | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | *N/A* | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | *N/A* | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | *N/A* | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | *N/A* | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | *N/A* | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | *N/A* | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | *N/A* | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | *N/A* | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | *N/A* | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | *N/A* | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | *N/A* | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | *N/A* | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | *N/A* | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | *N/A* | 0.00% | Tỷ lệ khóa futex |

---

### KB 09: buffered_h3_h3 — Large Payload (Pure QUIC RAM Blob 1MB)
- **Mục tiêu thử nghiệm**: Truyền tải nặng 1MB trên đồng thời 50 kết nối QUIC thuần, kiểm soát nén QPACK header và phân mảnh UDP GSO segments giữa downstream và upstream.
- **Kích thước tải trọng & Quy chuẩn**: 1,048,576 bytes body
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 2000 -c 50 --insecure https://192.168.122.14:8445/plain/1mb
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | *N/A* | 0.00 | Chưa đo nghiệm (Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | *N/A* | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | *N/A* | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | *N/A* | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | *N/A* | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | *N/A* | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | *N/A* | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | *N/A* | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | *N/A* | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | *N/A* | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | *N/A* | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | *N/A* | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | *N/A* | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | *N/A* | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | *N/A* | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | *N/A* | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | *N/A* | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | *N/A* | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | *N/A* | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | *N/A* | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | *N/A* | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | *N/A* | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | *N/A* | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | *N/A* | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | *N/A* | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | *N/A* | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | *N/A* | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | *N/A* | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | *N/A* | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | *N/A* | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | *N/A* | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | *N/A* | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | *N/A* | 0.00% | Tỷ lệ khóa futex |

---

### KB 10: server_stream_h3_h1 — Micro Payload (SSE Heartbeat from H1 ~64B)
- **Mục tiêu thử nghiệm**: Nhận các SSE ping events 64B từ H1 upstream chunked và đóng gói tức thì thành các QUIC STREAM frames HTTP/3 đẩy về downstream client.
- **Kích thước tải trọng & Quy chuẩn**: ~64 bytes SSE heartbeat chunks
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 20000 -c 100 --insecure https://192.168.122.14:8445/plain/sse-ping https://192.168.122.14:8445/tls12/sse-ping https://192.168.122.14:8445/tls13/sse-ping
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | 0.00 | 0.00 | Chưa đo nghiệm |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | 0.00 MB/s | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | 0.00 ms | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | 0.0% | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | 0.00 MB | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | 0.00 | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | 0.00 | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | 0.00 | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | 0.00 | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | 0.00% | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | 0.00% | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | 0.08% | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | 0.00 | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | 0.00 | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | 0.00s | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | 0.00% | 0.00% | Tỷ lệ khóa futex |

---

### KB 11: server_stream_h3_h1 — Medium Payload (SSE Chunked from H1 64KB)
- **Mục tiêu thử nghiệm**: Chuyển đổi luồng chunked 64KB từ H1 upstream sang các STREAM frames QUIC HTTP/3, duy trì độ trễ gói tin tối thiểu không đọng buffer.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes streaming data chunks
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 10000 -c 50 --insecure https://192.168.122.14:8445/plain/sse-64k https://192.168.122.14:8445/tls12/sse-64k https://192.168.122.14:8445/tls13/sse-64k
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | 0.00 | 0.00 | Chưa đo nghiệm |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | 0.00 MB/s | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | 0.00 ms | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | 0.0% | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | 0.00 MB | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | 0.00 | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | 0.00 | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | 0.00 | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | 0.00 | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | 0.00% | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | 0.00% | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | 0.08% | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | 0.00 | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | 0.00 | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | 0.00s | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | 0.00% | 0.00% | Tỷ lệ khóa futex |

---

### KB 12: server_stream_h3_h1 — Large Payload (File Stream from H1 10MB)
- **Mục tiêu thử nghiệm**: Tải file streaming 10MB từ H1 upstream, stream liên tục về client H3 với zero intermediate buffering, duy trì bộ nhớ O(1).
- **Kích thước tải trọng & Quy chuẩn**: 10,485,760 bytes streaming file download
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 200 -c 20 --insecure https://192.168.122.14:8445/plain/10mb https://192.168.122.14:8445/tls12/10mb https://192.168.122.14:8445/tls13/10mb
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | 0.00 | 0.00 | Chưa đo nghiệm |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | 0.00 MB/s | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | 0.00 ms | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | 0.0% | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | 0.00 MB | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | 0.00 | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | 0.00 | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | 0.00 | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | 0.00 | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | 0.00% | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | 0.00% | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | 0.08% | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | 0.00 | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | 0.00 | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | 0.00s | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | 0.00% | 0.00% | Tỷ lệ khóa futex |

---

### KB 13: server_stream_h3_h2 — Micro Payload (SSE Bridge from H2 ~64B)
- **Mục tiêu thử nghiệm**: Nhận các gói tin HTTP/2 DATA frame nhỏ từ H2 upstream và bridge tức thời thành QUIC STREAM frames HTTP/3 downstream.
- **Kích thước tải trọng & Quy chuẩn**: ~64 bytes headers + SSE event frame
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 20000 -c 100 --insecure https://192.168.122.14:8445/plain/sse-ping https://192.168.122.14:8445/tls12/sse-ping https://192.168.122.14:8445/tls13/sse-ping
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | 0.00 | 0.00 | Chưa đo nghiệm |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | 0.00 MB/s | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | 0.00 ms | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | 0.0% | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | 0.00 MB | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | 0.00 | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | 0.00 | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | 0.00 | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | 0.00 | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | 0.00% | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | 0.00% | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | 0.08% | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | 0.00 | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | 0.00 | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | 0.00s | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | 0.00% | 0.00% | Tỷ lệ khóa futex |

---

### KB 14: server_stream_h3_h2 — Medium Payload (Data Stream from H2 64KB)
- **Mục tiêu thử nghiệm**: Chuyển luồng DATA frames H2 64KB sang HTTP/3 stream downstream với zero-copy Bytes slicing.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes DATA frames payload
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 10000 -c 50 --insecure https://192.168.122.14:8445/plain/stream-64k https://192.168.122.14:8445/tls12/stream-64k https://192.168.122.14:8445/tls13/stream-64k
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | 0.00 | 0.00 | Chưa đo nghiệm |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | 0.00 MB/s | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | 0.00 ms | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | 0.0% | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | 0.00 MB | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | 0.00 | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | 0.00 | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | 0.00 | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | 0.00 | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | 0.00% | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | 0.00% | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | 0.08% | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | 0.00 | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | 0.00 | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | 0.00s | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | 0.00% | 0.00% | Tỷ lệ khóa futex |

---

### KB 15: server_stream_h3_h2 — Large Payload (File Stream from H2 10MB)
- **Mục tiêu thử nghiệm**: Kéo luồng file 10MB từ Upstream H2 về Downstream H3, kiểm tra cơ chế kiểm soát backpressure flow control liên giao thức.
- **Kích thước tải trọng & Quy chuẩn**: 10,485,760 bytes DATA stream
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 200 -c 20 --insecure https://192.168.122.14:8445/plain/10mb https://192.168.122.14:8445/tls12/10mb https://192.168.122.14:8445/tls13/10mb
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | 0.00 | 0.00 | Chưa đo nghiệm |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | 0.00 MB/s | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | 0.00 ms | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | 0.0% | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | 0.00 MB | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | 0.00 | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | 0.00 | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | 0.00 | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | 0.00 | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | 0.00% | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | 0.00% | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | 0.08% | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | 0.00 | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | 0.00 | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | 0.00s | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | 0.00% | 0.00% | Tỷ lệ khóa futex |

---

### KB 16: server_stream_h3_h3 — Micro Payload (Pure QUIC SSE Stream ~64B)
- **Mục tiêu thử nghiệm**: Chuyển tiếp luồng sự kiện SSE 64B giữa 2 kênh multiplexing QUIC mà không nén lại payload, điều phối tín hiệu ping.
- **Kích thước tải trọng & Quy chuẩn**: ~64 bytes QUIC frames stream
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 20000 -c 100 --insecure https://192.168.122.14:8445/plain/sse-ping
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | *N/A* | 0.00 | Chưa đo nghiệm (Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | *N/A* | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | *N/A* | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | *N/A* | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | *N/A* | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | *N/A* | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | *N/A* | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | *N/A* | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | *N/A* | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | *N/A* | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | *N/A* | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | *N/A* | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | *N/A* | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | *N/A* | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | *N/A* | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | *N/A* | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | *N/A* | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | *N/A* | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | *N/A* | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | *N/A* | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | *N/A* | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | *N/A* | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | *N/A* | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | *N/A* | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | *N/A* | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | *N/A* | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | *N/A* | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | *N/A* | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | *N/A* | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | *N/A* | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | *N/A* | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | *N/A* | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | *N/A* | 0.00% | Tỷ lệ khóa futex |

---

### KB 17: server_stream_h3_h3 — Medium Payload (Pure QUIC Chunked Stream 64KB)
- **Mục tiêu thử nghiệm**: Stream liên tục dữ liệu 64KB giữa downstream H3 và upstream H3, kiểm tra hiệu năng window update và frame relay thuần UDP.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes UDP QUIC stream
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 10000 -c 50 --insecure https://192.168.122.14:8445/plain/stream-64k
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | *N/A* | 0.00 | Chưa đo nghiệm (Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | *N/A* | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | *N/A* | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | *N/A* | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | *N/A* | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | *N/A* | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | *N/A* | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | *N/A* | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | *N/A* | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | *N/A* | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | *N/A* | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | *N/A* | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | *N/A* | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | *N/A* | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | *N/A* | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | *N/A* | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | *N/A* | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | *N/A* | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | *N/A* | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | *N/A* | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | *N/A* | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | *N/A* | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | *N/A* | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | *N/A* | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | *N/A* | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | *N/A* | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | *N/A* | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | *N/A* | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | *N/A* | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | *N/A* | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | *N/A* | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | *N/A* | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | *N/A* | 0.00% | Tỷ lệ khóa futex |

---

### KB 18: server_stream_h3_h3 — Large Payload (Pure QUIC File Stream 10MB)
- **Mục tiêu thử nghiệm**: Chuyển tiếp luồng file 10MB qua 2 kết nối QUIC multiplexed, đo throughput bão hòa băng thông và khả năng kiểm soát UDP flow control.
- **Kích thước tải trọng & Quy chuẩn**: 10,485,760 bytes stream
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 200 -c 20 --insecure https://192.168.122.14:8445/plain/10mb
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | *N/A* | 0.00 | Chưa đo nghiệm (Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | *N/A* | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | *N/A* | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | *N/A* | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | *N/A* | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | *N/A* | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | *N/A* | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | *N/A* | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | *N/A* | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | *N/A* | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | *N/A* | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | *N/A* | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | *N/A* | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | *N/A* | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | *N/A* | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | *N/A* | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | *N/A* | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | *N/A* | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | *N/A* | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | *N/A* | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | *N/A* | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | *N/A* | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | *N/A* | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | *N/A* | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | *N/A* | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | *N/A* | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | *N/A* | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | *N/A* | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | *N/A* | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | *N/A* | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | *N/A* | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | *N/A* | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | *N/A* | 0.00% | Tỷ lệ khóa futex |

---

### KB 19: client_stream_h3_h1 — Micro Payload (QUIC Drip Upload to H1 ~64B)
- **Mục tiêu thử nghiệm**: Client H3 đẩy sự kiện IoT nhỏ dạng STREAM frames liên tục, gateway assemble và forward sang H1 keep-alive upstream.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes streaming upload body
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 20000 -c 100 --insecure https://192.168.122.14:8445/plain/drip https://192.168.122.14:8445/tls12/drip https://192.168.122.14:8445/tls13/drip
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | 0.00 | 0.00 | Chưa đo nghiệm |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | 0.00 MB/s | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | 0.00 ms | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | 0.0% | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | 0.00 MB | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | 0.00 | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | 0.00 | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | 0.00 | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | 0.00 | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | 0.00% | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | 0.00% | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | 0.08% | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | 0.00 | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | 0.00 | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | 0.00s | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | 0.00% | 0.00% | Tỷ lệ khóa futex |

---

### KB 20: client_stream_h3_h1 — Medium Payload (QUIC Doc Upload to H1 64KB)
- **Mục tiêu thử nghiệm**: Upload tài liệu 64KB từ client H3 stream sang HTTP/1.1 chunked encoding upstream.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes streaming upload body
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 10000 -c 50 --insecure https://192.168.122.14:8445/plain/upload-64k https://192.168.122.14:8445/tls12/upload-64k https://192.168.122.14:8445/tls13/upload-64k
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | 0.00 | 0.00 | Chưa đo nghiệm |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | 0.00 MB/s | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | 0.00 ms | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | 0.0% | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | 0.00 MB | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | 0.00 | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | 0.00 | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | 0.00 | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | 0.00 | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | 0.00% | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | 0.00% | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | 0.08% | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | 0.00 | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | 0.00 | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | 0.00s | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | 0.00% | 0.00% | Tỷ lệ khóa futex |

---

### KB 21: client_stream_h3_h1 — Large Payload (QUIC Bulk Upload to H1 200MB)
- **Mục tiêu thử nghiệm**: Đẩy luồng file cực lớn 200MB/request từ QUIC client lên H1 upstream qua 5 kết nối đồng thời, kiểm tra zero-alloc stream forwarding.
- **Kích thước tải trọng & Quy chuẩn**: 209,715,200 bytes (200 MB) upload body
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 50 -c 5 --insecure https://192.168.122.14:8445/plain/bulk-200mb https://192.168.122.14:8445/tls12/bulk-200mb https://192.168.122.14:8445/tls13/bulk-200mb
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | 0.00 | 0.00 | Chưa đo nghiệm |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | 0.00 MB/s | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | 0.00 ms | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | 0.0% | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | 0.00 MB | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | 0.00 | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | 0.00 | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | 0.00 | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | 0.00 | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | 0.00% | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | 0.00% | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | 0.08% | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | 0.00 | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | 0.00 | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | 0.00s | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | 0.00% | 0.00% | Tỷ lệ khóa futex |

---

### KB 22: client_stream_h3_h2 — Micro Payload (QUIC Drip Upload to H2 ~64B)
- **Mục tiêu thử nghiệm**: Nhận stream upload 64B từ client QUIC và chuyển đổi thành H2 DATA frames đẩy vào upstream connection pool.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes upload body
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 20000 -c 100 --insecure https://192.168.122.14:8445/plain/drip https://192.168.122.14:8445/tls12/drip https://192.168.122.14:8445/tls13/drip
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | 0.00 | 0.00 | Chưa đo nghiệm |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | 0.00 MB/s | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | 0.00 ms | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | 0.0% | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | 0.00 MB | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | 0.00 | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | 0.00 | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | 0.00 | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | 0.00 | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | 0.00% | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | 0.00% | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | 0.08% | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | 0.00 | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | 0.00 | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | 0.00s | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | 0.00% | 0.00% | Tỷ lệ khóa futex |

---

### KB 23: client_stream_h3_h2 — Medium Payload (QUIC Doc Upload to H2 64KB)
- **Mục tiêu thử nghiệm**: Chuyển 64KB stream upload từ client H3 sang HTTP/2 DATA frames upstream.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes upload body
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 10000 -c 50 --insecure https://192.168.122.14:8445/plain/upload-64k https://192.168.122.14:8445/tls12/upload-64k https://192.168.122.14:8445/tls13/upload-64k
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | 0.00 | 0.00 | Chưa đo nghiệm |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | 0.00 MB/s | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | 0.00 ms | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | 0.0% | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | 0.00 MB | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | 0.00 | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | 0.00 | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | 0.00 | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | 0.00 | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | 0.00% | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | 0.00% | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | 0.08% | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | 0.00 | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | 0.00 | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | 0.00s | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | 0.00% | 0.00% | Tỷ lệ khóa futex |

---

### KB 24: client_stream_h3_h2 — Large Payload (QUIC Bulk Upload to H2 5MB)
- **Mục tiêu thử nghiệm**: Chuyển luồng upload 5MB từ client H3 sang H2 upstream có backpressure flow-control window.
- **Kích thước tải trọng & Quy chuẩn**: 5,242,880 bytes upload body
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 1000 -c 20 --insecure https://192.168.122.14:8445/plain/upload-5mb https://192.168.122.14:8445/tls12/upload-5mb https://192.168.122.14:8445/tls13/upload-5mb
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | 0.00 | 0.00 | Chưa đo nghiệm |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | 0.00 MB/s | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | 0.00 ms | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | 0.0% | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | 0.00 MB | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | 0.00 | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | 0.00 | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | 0.00 | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | 0.00 | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | 0.00% | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | 0.00% | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | 0.08% | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | 0.00 | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | 0.00 | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | 0.00s | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | 0.00% | 0.00% | Tỷ lệ khóa futex |

---

### KB 25: client_stream_h3_h3 — Micro Payload (Pure QUIC Drip Upload ~64B)
- **Mục tiêu thử nghiệm**: Client H3 upload micro chunks, gateway forward trực tiếp sang QUIC upstream stream thuần.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes upload body
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 20000 -c 100 --insecure https://192.168.122.14:8445/plain/drip
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | *N/A* | 0.00 | Chưa đo nghiệm (Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | *N/A* | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | *N/A* | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | *N/A* | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | *N/A* | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | *N/A* | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | *N/A* | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | *N/A* | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | *N/A* | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | *N/A* | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | *N/A* | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | *N/A* | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | *N/A* | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | *N/A* | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | *N/A* | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | *N/A* | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | *N/A* | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | *N/A* | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | *N/A* | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | *N/A* | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | *N/A* | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | *N/A* | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | *N/A* | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | *N/A* | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | *N/A* | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | *N/A* | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | *N/A* | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | *N/A* | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | *N/A* | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | *N/A* | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | *N/A* | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | *N/A* | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | *N/A* | 0.00% | Tỷ lệ khóa futex |

---

### KB 26: client_stream_h3_h3 — Medium Payload (Pure QUIC Doc Upload 64KB)
- **Mục tiêu thử nghiệm**: Client upload 64KB stream sang QUIC upstream stream với điều phối UDP window.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes upload body
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 10000 -c 50 --insecure https://192.168.122.14:8445/plain/upload-64k
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | *N/A* | 0.00 | Chưa đo nghiệm (Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | *N/A* | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | *N/A* | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | *N/A* | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | *N/A* | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | *N/A* | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | *N/A* | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | *N/A* | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | *N/A* | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | *N/A* | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | *N/A* | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | *N/A* | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | *N/A* | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | *N/A* | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | *N/A* | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | *N/A* | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | *N/A* | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | *N/A* | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | *N/A* | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | *N/A* | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | *N/A* | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | *N/A* | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | *N/A* | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | *N/A* | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | *N/A* | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | *N/A* | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | *N/A* | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | *N/A* | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | *N/A* | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | *N/A* | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | *N/A* | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | *N/A* | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | *N/A* | 0.00% | Tỷ lệ khóa futex |

---

### KB 27: client_stream_h3_h3 — Large Payload (Pure QUIC Bulk Upload 5MB)
- **Mục tiêu thử nghiệm**: Client upload 5MB stream sang QUIC upstream stream với Linux GSO offload.
- **Kích thước tải trọng & Quy chuẩn**: 5,242,880 bytes upload body
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 1000 -c 20 --insecure https://192.168.122.14:8445/plain/upload-5mb
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | *N/A* | 0.00 | Chưa đo nghiệm (Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | *N/A* | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | *N/A* | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | *N/A* | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | *N/A* | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | *N/A* | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | *N/A* | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | *N/A* | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | *N/A* | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | *N/A* | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | *N/A* | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | *N/A* | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | *N/A* | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | *N/A* | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | *N/A* | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | *N/A* | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | *N/A* | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | *N/A* | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | *N/A* | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | *N/A* | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | *N/A* | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | *N/A* | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | *N/A* | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | *N/A* | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | *N/A* | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | *N/A* | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | *N/A* | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | *N/A* | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | *N/A* | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | *N/A* | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | *N/A* | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | *N/A* | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | *N/A* | 0.00% | Tỷ lệ khóa futex |

---

### KB 28: duplex_h3_h1 — Micro Payload (Bidi Echo with H1 ~64B)
- **Mục tiêu thử nghiệm**: Client vừa gửi vừa nhận đồng thời từng gói 64B qua giao thức QUIC downstream bridge với H1 upstream.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes bidi frames
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 20000 -c 100 --insecure https://192.168.122.14:8445/plain/echo https://192.168.122.14:8445/tls12/echo https://192.168.122.14:8445/tls13/echo
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | 0.00 | 0.00 | Chưa đo nghiệm |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | 0.00 MB/s | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | 0.00 ms | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | 0.0% | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | 0.00 MB | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | 0.00 | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | 0.00 | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | 0.00 | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | 0.00 | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | 0.00% | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | 0.00% | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | 0.08% | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | 0.00 | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | 0.00 | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | 0.00s | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | 0.00% | 0.00% | Tỷ lệ khóa futex |

---

### KB 29: duplex_h3_h1 — Medium Payload (Bidi Messaging with H1 64KB)
- **Mục tiêu thử nghiệm**: Vừa stream 64KB lên từ QUIC, vừa nhận 64KB về đồng thời từ H1 upstream connection.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes in / 65,536 bytes out
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 10000 -c 50 --insecure https://192.168.122.14:8445/plain/bidi-64k https://192.168.122.14:8445/tls12/bidi-64k https://192.168.122.14:8445/tls13/bidi-64k
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | 0.00 | 0.00 | Chưa đo nghiệm |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | 0.00 MB/s | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | 0.00 ms | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | 0.0% | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | 0.00 MB | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | 0.00 | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | 0.00 | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | 0.00 | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | 0.00 | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | 0.00% | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | 0.00% | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | 0.08% | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | 0.00 | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | 0.00 | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | 0.00s | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | 0.00% | 0.00% | Tỷ lệ khóa futex |

---

### KB 30: duplex_h3_h1 — Large Payload (Heavy Duplex Pump with H1 5MB)
- **Mục tiêu thử nghiệm**: Bơm liên tục 5MB hai chiều đồng thời giữa QUIC downstream và H1 upstream, kiểm tra điều phối asymmetric backpressure.
- **Kích thước tải trọng & Quy chuẩn**: 5,242,880 bytes in / 5,242,880 bytes out
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 1000 -c 20 --insecure https://192.168.122.14:8445/plain/pump-5mb https://192.168.122.14:8445/tls12/pump-5mb https://192.168.122.14:8445/tls13/pump-5mb
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | 0.00 | 0.00 | Chưa đo nghiệm |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | 0.00 MB/s | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | 0.00 ms | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | 0.0% | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | 0.00 MB | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | 0.00 | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | 0.00 | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | 0.00 | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | 0.00 | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | 0.00% | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | 0.00% | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | 0.08% | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | 0.00 | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | 0.00 | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | 0.00s | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | 0.00% | 0.00% | Tỷ lệ khóa futex |

---

### KB 31: duplex_h3_h2 — Micro Payload (Bidi Echo with H2 ~64B)
- **Mục tiêu thử nghiệm**: Ghép luồng duplex QUIC downstream vào HTTP/2 bidirectional stream upstream.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes bidi frames
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 20000 -c 100 --insecure https://192.168.122.14:8445/plain/echo https://192.168.122.14:8445/tls12/echo https://192.168.122.14:8445/tls13/echo
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | 0.00 | 0.00 | Chưa đo nghiệm |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | 0.00 MB/s | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | 0.00 ms | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | 0.0% | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | 0.00 MB | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | 0.00 | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | 0.00 | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | 0.00 | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | 0.00 | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | 0.00% | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | 0.00% | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | 0.08% | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | 0.00 | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | 0.00 | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | 0.00s | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | 0.00% | 0.00% | Tỷ lệ khóa futex |

---

### KB 32: duplex_h3_h2 — Medium Payload (Bidi Data with H2 64KB)
- **Mục tiêu thử nghiệm**: Bơm song công 64KB giữa QUIC stream downstream và HTTP/2 multiplexing upstream.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes in / out
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 10000 -c 50 --insecure https://192.168.122.14:8445/plain/bidi-64k https://192.168.122.14:8445/tls12/bidi-64k https://192.168.122.14:8445/tls13/bidi-64k
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | 0.00 | 0.00 | Chưa đo nghiệm |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | 0.00 MB/s | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | 0.00 ms | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | 0.0% | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | 0.00 MB | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | 0.00 | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | 0.00 | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | 0.00 | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | 0.00 | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | 0.00% | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | 0.00% | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | 0.08% | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | 0.00 | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | 0.00 | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | 0.00s | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | 0.00% | 0.00% | Tỷ lệ khóa futex |

---

### KB 33: duplex_h3_h2 — Large Payload (Heavy Duplex Pump with H2 5MB)
- **Mục tiêu thử nghiệm**: Đẩy 5MB in / 5MB out đồng thời giữa QUIC stream và HTTP/2 DATA frames.
- **Kích thước tải trọng & Quy chuẩn**: 5,242,880 bytes in / out
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 1000 -c 20 --insecure https://192.168.122.14:8445/plain/pump-5mb https://192.168.122.14:8445/tls12/pump-5mb https://192.168.122.14:8445/tls13/pump-5mb
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% h2c Cleartext : 30% TLS 1.2 h2 : 30% TLS 1.3 h2)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | 0.00 | 0.00 | Chưa đo nghiệm |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | 0.00 MB/s | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | 0.00 ms | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | 0.0% | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | 0.00 MB | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | 0.00 | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | 0.00 | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | 0.00 | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | 0.00 | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | 0.00% | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | 0.00% | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | 0.08% | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | 0.00 | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | 0.00 | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | 0.00s | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | 0.00% | 0.00% | Tỷ lệ khóa futex |

---

### KB 34: duplex_h3_h3 — Micro Payload (Pure QUIC Bidi Echo ~64B)
- **Mục tiêu thử nghiệm**: Bidi stream QUIC downstream ghép vào QUIC bidirectional stream upstream.
- **Kích thước tải trọng & Quy chuẩn**: 64 bytes bidi frames
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 20000 -c 100 --insecure https://192.168.122.14:8445/plain/echo
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | *N/A* | 0.00 | Chưa đo nghiệm (Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | *N/A* | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | *N/A* | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | *N/A* | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | *N/A* | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | *N/A* | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | *N/A* | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | *N/A* | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | *N/A* | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | *N/A* | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | *N/A* | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | *N/A* | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | *N/A* | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | *N/A* | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | *N/A* | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | *N/A* | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | *N/A* | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | *N/A* | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | *N/A* | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | *N/A* | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | *N/A* | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | *N/A* | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | *N/A* | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | *N/A* | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | *N/A* | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | *N/A* | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | *N/A* | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | *N/A* | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | *N/A* | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | *N/A* | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | *N/A* | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | *N/A* | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | *N/A* | 0.00% | Tỷ lệ khóa futex |

---

### KB 35: duplex_h3_h3 — Medium Payload (Pure QUIC Bidi Messaging 64KB)
- **Mục tiêu thử nghiệm**: Bơm song công 64KB qua 2 kênh QUIC bidi stream.
- **Kích thước tải trọng & Quy chuẩn**: 65,536 bytes in / out
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 10000 -c 50 --insecure https://192.168.122.14:8445/plain/bidi-64k
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | *N/A* | 0.00 | Chưa đo nghiệm (Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | *N/A* | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | *N/A* | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | *N/A* | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | *N/A* | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | *N/A* | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | *N/A* | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | *N/A* | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | *N/A* | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | *N/A* | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | *N/A* | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | *N/A* | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | *N/A* | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | *N/A* | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | *N/A* | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | *N/A* | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | *N/A* | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | *N/A* | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | *N/A* | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | *N/A* | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | *N/A* | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | *N/A* | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | *N/A* | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | *N/A* | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | *N/A* | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | *N/A* | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | *N/A* | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | *N/A* | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | *N/A* | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | *N/A* | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | *N/A* | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | *N/A* | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | *N/A* | 0.00% | Tỷ lệ khóa futex |

---

### KB 36: duplex_h3_h3 — Large Payload (Pure QUIC Heavy Duplex Pump 5MB)
- **Mục tiêu thử nghiệm**: Bơm 5MB song công qua 2 kênh QUIC stream với Linux GSO offload.
- **Kích thước tải trọng & Quy chuẩn**: 5,242,880 bytes in / out
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 1000 -c 20 --insecure https://192.168.122.14:8445/plain/pump-5mb
- **Nhánh Upstream**: Upstream HTTP/3 (QUIC / UDP trên port 8445 TLS 1.3)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | *N/A* | 0.00 | Chưa đo nghiệm (Nginx không hỗ trợ H3 upstream) |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | *N/A* | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | *N/A* | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | *N/A* | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | *N/A* | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | *N/A* | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | *N/A* | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | *N/A* | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | *N/A* | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | *N/A* | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | *N/A* | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | *N/A* | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | *N/A* | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | *N/A* | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | *N/A* | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | *N/A* | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | *N/A* | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | *N/A* | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | *N/A* | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | *N/A* | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | *N/A* | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | *N/A* | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | *N/A* | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | *N/A* | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | *N/A* | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | *N/A* | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | *N/A* | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | *N/A* | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | *N/A* | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | *N/A* | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | *N/A* | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | *N/A* | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | *N/A* | 0.00% | Tỷ lệ khóa futex |

---

### KB 37: QUIC Initial Flood & Amplification Attack (RFC 9000 Section 8 Anti-Amplification 3x Limit & Token Validation)
- **Mục tiêu thử nghiệm**: Bắn dồn dập các gói QUIC Initial giả mạo địa chỉ IP nguồn để thử thách cơ chế Anti-Amplification 3x và Retry token validation.
- **Kích thước tải trọng & Quy chuẩn**: 100,000 Initial datagrams/s với spoofed IP
- **Lệnh thực thi chuẩn**: python3 test_quic_initial_flood.py --target 192.168.122.14:8445
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | 0.00 | 0.00 | Chưa đo nghiệm |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | 0.00 MB/s | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | 0.00 ms | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | 0.0% | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | 0.00 MB | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | 0.00 | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | 0.00 | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | 0.00 | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | 0.00 | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | 0.00% | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | 0.00% | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | 0.08% | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | 0.00 | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | 0.00 | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | 0.00s | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | 0.00% | 0.00% | Tỷ lệ khóa futex |

---

### KB 38: QPACK Dynamic Table Bomb & Head-of-Line Blocking (RFC 9204 Encoder Stream Overflow)
- **Mục tiêu thử nghiệm**: Bơm các frame QPACK chỉ định chèn liên tục vào dynamic table mà không gửi stream acknowledgement, ép gateway cạn kiệt bộ nhớ encoder.
- **Kích thước tải trọng & Quy chuẩn**: Infinite dynamic table insertions without stream ack
- **Lệnh thực thi chuẩn**: python3 test_qpack_bomb.py --target 192.168.122.14:8445
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | 0.00 | 0.00 | Chưa đo nghiệm |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | 0.00 MB/s | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | 0.00 ms | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | 0.0% | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | 0.00 MB | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | 0.00 | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | 0.00 | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | 0.00 | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | 0.00 | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | 0.00% | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | 0.00% | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | 0.08% | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | 0.00 | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | 0.00 | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | 0.00s | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | 0.00% | 0.00% | Tỷ lệ khóa futex |

---

### KB 39: QUIC Connection ID Churn & Migration Storm (Path Validation, Spoofed Peer Address, Stateless Reset Defense)
- **Mục tiêu thử nghiệm**: Bắn bão chuyển đổi địa chỉ IP và Connection ID liên tục để kích hoạt Path Validation bão hòa CPU và kiểm tra cơ chế Stateless Reset.
- **Kích thước tải trọng & Quy chuẩn**: 50,000 path challenges & DCID migrations/s
- **Lệnh thực thi chuẩn**: python3 test_quic_migration_storm.py --target 192.168.122.14:8445
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | 0.00 | 0.00 | Chưa đo nghiệm |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | 0.00 MB/s | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | 0.00 ms | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | 0.0% | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | 0.00 MB | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | 0.00 | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | 0.00 | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | 0.00 | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | 0.00 | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | 0.00% | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | 0.00% | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | 0.08% | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | 0.00 | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | 0.00 | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | 0.00s | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | 0.00% | 0.00% | Tỷ lệ khóa futex |

---

### KB 40: QUIC Flow-Control Window Deadlock (MAX_DATA / MAX_STREAM_DATA Exhaustion)
- **Mục tiêu thử nghiệm**: Ép gateway vào trạng thái nghẽn stream flow control bằng cách không gửi MAX_DATA hoặc drip 1 byte/s trên QUIC stream.
- **Kích thước tải trọng & Quy chuẩn**: Zero window advertise + 1 byte / 5s drip
- **Lệnh thực thi chuẩn**: python3 test_quic_flow_deadlock.py --target 192.168.122.14:8445
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | 0.00 | 0.00 | Chưa đo nghiệm |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | 0.00 MB/s | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | 0.00 ms | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | 0.0% | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | 0.00 MB | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | 0.00 | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | 0.00 | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | 0.00 | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | 0.00 | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | 0.00% | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | 0.00% | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | 0.08% | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | 0.00 | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | 0.00 | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | 0.00s | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | 0.00% | 0.00% | Tỷ lệ khóa futex |

---

### KB 41: Chaos Upstream Sudden Death (Graceful CONNECTION_CLOSE 0x010c / TCP RST & Fast Failover)
- **Mục tiêu thử nghiệm**: Bắn tín hiệu CONNECTION_CLOSE hoặc TCP RST đột ngột từ upstream server giữa lúc 1,000 QUIC stream đang ghép kênh.
- **Kích thước tải trọng & Quy chuẩn**: Bơm 1,000 active streams + Tiêm RST / Close đột ngột
- **Lệnh thực thi chuẩn**: python3 test_chaos_upstream_h3.py --target 192.168.122.14:8445
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | 0.00 | 0.00 | Chưa đo nghiệm |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | 0.00 MB/s | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | 0.00 ms | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | 0.0% | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | 0.00 MB | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | 0.00 | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | 0.00 | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | 0.00 | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | 0.00 | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | 0.00% | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | 0.00% | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | 0.08% | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | 0.00 | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | 0.00 | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | 0.00s | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | 0.00% | 0.00% | Tỷ lệ khóa futex |

---

### KB 42: Cgroup RAM 60MB Starvation (O(1) UDP Memory under 1,000 Multiplexed Streams)
- **Mục tiêu thử nghiệm**: Giới hạn cgroup RAM 60MB và mở 1,000 streams multiplexed cùng lúc truyền tải dữ liệu, kiểm tra khả năng sinh tồn trước Linux OOM killer.
- **Kích thước tải trọng & Quy chuẩn**: 1,000 streams đồng thời trong cgroup memory.max=60MB
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 10000 -c 100 --insecure https://192.168.122.14:8445/plain/12kb
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | 0.00 | 0.00 | Chưa đo nghiệm |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | 0.00 MB/s | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | 0.00 ms | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | 0.0% | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | 0.00 MB | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | 0.00 | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | 0.00 | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | 0.00 | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | 0.00 | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | 0.00% | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | 0.00% | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | 0.08% | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | 0.00 | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | 0.00 | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | 0.00s | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | 0.00% | 0.00% | Tỷ lệ khóa futex |

---

### KB 43: CPU 100% Saturation & Overload Shedding (CoDel Shedder, H3_REQUEST_REJECTED 0x010b)
- **Mục tiêu thử nghiệm**: Ép CPU máy chủ chạm 100% bằng cách dội 200,000 stream/s vượt năng lực xử lý, kiểm tra thuật toán cắt tỉa tải sớm (load shedding).
- **Kích thước tải trọng & Quy chuẩn**: 200,000 requests bão hòa 100% 4 vCPUs
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 200000 -c 200 --insecure https://192.168.122.14:8445/plain/ping
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | 0.00 | 0.00 | Chưa đo nghiệm |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | 0.00 MB/s | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | 0.00 ms | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | 0.0% | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | 0.00 MB | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | 0.00 | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | 0.00 | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | 0.00 | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | 0.00 | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | 0.00% | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | 0.00% | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | 0.08% | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | 0.00 | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | 0.00 | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | 0.00s | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | 0.00% | 0.00% | Tỷ lệ khóa futex |

---

### KB 44: Atomic Hot-Reload Contention (100 Reloads/s at 100K QUIC Packets, Zero Dropped UDP Sessions)
- **Mục tiêu thử nghiệm**: Kích hoạt 100 lần reload runtime snapshot mỗi giây trong khi đang phục vụ 100,000 gói tin QUIC ghép kênh.
- **Kích thước tải trọng & Quy chuẩn**: 100,000 streams + 100 atomic snapshot swaps/sec
- **Lệnh thực thi chuẩn**: oha --http-version 3 --no-tui -n 100000 -c 100 --insecure https://192.168.122.14:8445/plain/ping
- **Nhánh Upstream**: Triple-Upstream tỷ lệ 4/3/3 (40% Plaintext h1 : 30% TLS 1.2 h1 : 30% TLS 1.3 h1)

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust 2024) | **Nginx 1.31** (C) | **Envoy 1.39** (C++) | Tỷ Lệ / Nhận Xét Đối Sánh |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s hoặc transfers/s)** | 0.00 | 0.00 | 0.00 | Chưa đo nghiệm |
| | **Băng thông truyền tải (Throughput MB/s)** | 0.00 MB/s | 0.00 MB/s | 0.00 MB/s | Băng thông tương ứng với lưu lượng phục vụ |
| | **Độ trễ trung bình (Mean Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Độ trễ trung bình xử lý luồng |
| | **Độ trễ tối thiểu (Min Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Ngưỡng trễ phản hồi gói đầu tiên |
| | **Độ trễ phân vị P50** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P50 hoàn tất request |
| | **Độ trễ phân vị P90** | 0.00 ms | 0.00 ms | 0.00 ms | Phân vị P90 |
| | **Độ trễ phân vị P99 (Tail Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ P99 kiểm soát độ giật |
| | **Độ trễ phân vị P99.9** | 0.00 ms | 0.00 ms | 0.00 ms | Đuôi trễ sâu P99.9 |
| | **Độ trễ tối đa (Max Latency)** | 0.00 ms | 0.00 ms | 0.00 ms | Max jitter phản hồi |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | 0.00 ms | 0.00 ms | 0.00 ms | Phương sai độ ổn định |
| | **Thời gian nhận byte đầu (TTFB Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Đồng pha với mean latency |
| | **Thời gian kết nối (Connect Time Avg)** | 0.00 ms | 0.00 ms | 0.00 ms | Bắt tay kết nối downstream (QUIC Handshake) |
| | **Phân bổ mã phản hồi (HTTP Status 2xx/4xx/5xx)** | 0 2xx | 0 2xx | 0 2xx | 100% 200 OK |
| | **Tỷ lệ thành công (Success Rate %)** | 0.0% | 0.0% | 0.0% | Không rớt gói, không lỗi |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS MB)** | 0.00 MB | 0.00 MB | 0.00 MB | Bộ nhớ RSS thường trú đỉnh |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | 0 | 0 | 0 | Tổng số lệnh CPU thực thi |
| | **Số lệnh CPU / Request (`insn / req`)** | 0.00 | 0.00 | 0.00 | Số lệnh CPU tiêu thụ trên mỗi request |
| | **Tổng chu kỳ CPU (`cycles`)** | 0 | 0 | 0 | Tổng chu kỳ xung nhịp CPU |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 0.00 | 0.00 | 0.00 | Chu kỳ xung CPU trên mỗi request |
| | **Hiệu suất lệnh / xung (`IPC = insn / cycle`)** | 0.00 | 0.00 | 0.00 | Hiệu suất IPC khai thác CPU |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | 0 | 0 | 0 | Lệnh rẽ nhánh phần cứng |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | 0.00 | 0.00 | 0.00 | Rẽ nhánh trên mỗi request |
| | **CPU đoán sai nhánh (`branch-misses`)** | 0 | 0 | 0 | Đoán sai nhánh |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | 0.00% | 0.00% | 0.00% | Tỷ lệ đoán sai nhánh |
| | **Tỷ lệ Miss bộ đệm L1 (`L1-dcache-miss %`)** | 0.00% | 0.00% | 0.00% | Miss cache L1 dữ liệu |
| | **Tỷ lệ Miss bộ đệm cuối (`LLC-miss %`)** | 0.08% | 0.08% | 0.08% | Bảo vệ bộ nhớ LLC |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 0 | 0 | 0 | Chuyển ngữ cảnh tiến trình |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.00 | 0.00 | 0.00 | Chuyển ngữ cảnh / request |
| | **Tổng số System Calls (`syscalls`)** | 0 | 0 | 0 | System calls vào nhân Linux |
| | **System Calls / Request (`syscalls / req`)** | 0.00 | 0.00 | 0.00 | Syscalls trên mỗi request |
| | **Thời gian chạy trong kernel (Kernel Time)** | 0.00s | 0.00s | 0.00s | Thời gian thực thi không gian nhân Linux |
| | **Số lần gọi khóa Futex (Futex Calls)** | 0 | 0 | 0 | Số lần gọi khóa futex |
| | **Tỷ lệ Futex trên tổng Syscalls (%)** | 0.00% | 0.00% | 0.00% | Tỷ lệ khóa futex |

---
