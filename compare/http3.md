# Bộ Đo Đối Sánh Hiệu Năng HTTP/3: Velda Edge vs Nginx vs Envoy

Tài liệu này đặc tả và định hình khung đo chuẩn cho **toàn bộ ma trận 44 kịch bản đối sánh hiệu năng (performance benchmark)** cho giao thức **HTTP/3 (RFC 9114 & QUIC RFC 9000)** giữa:
- **Velda Edge**: v0.1.0 (Rust Edition 2024, Hourglass Architecture, Packet-Driven Ingress, Pure `quinn_proto` State Machine, Zero-Copy QPACK Engine, Linux GSO Offload)
- **Nginx**: **v1.31.6 Mainline / v1.30.5 Stable** (C Event-Driven Web Server & Reverse Proxy, `ngx_http_v3_module` QUIC/HTTP3)
- **Envoy Proxy**: **v1.39.3 Latest Release** (C++ Cloud-Native High-Performance Edge Proxy, Google QUICHE engine)

> [!NOTE]
> Mọi kịch bản hiện tại được khởi tạo ở trạng thái biểu mẫu chuẩn (Form với số 0) sẵn sàng nạp số liệu thực nghiệm từ môi trường KVM cô lập.

---

## 1. Môi Trường Thử Nghiệm & Phương Pháp Đo Lường

### 1.1 Môi Trường KVM Cô Lập (Isolated Testbed)
- **Hệ điều hành**: Ubuntu 24.04 LTS (Kernel Linux 6.8.0-45-generic).
- **Cấu hình máy ảo Gateway**: KVM QEMU VM với **4 vCPUs (pinned 1-1 vật lý)**, **8 GB RAM**, cgroup v2 enabled.
- **Máy phát tải (Load Generator)**: Host vật lý (12 CPU cores, 32 GB RAM) dội tải trực tiếp qua giao diện mạng `virtio-net` (MTU 1500, UDP offloading bật).
- **Công cụ sinh tải**: `h2load` (nghttp2 v1.68) hỗ trợ ALPN HTTP/3 (`--alpn=h3`), kết hợp bộ test scripts chuyên dụng mô phỏng dị dạng gói tin QUIC (Initial flood, QPACK bomb, Path migration storm, Flow-control deadlock).

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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 50000 -c 100 -m 10 https://192.168.122.14:8445/plain/ping https://192.168.122.14:8445/tls12/ping https://192.168.122.14:8445/tls13/ping
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

### KB 02: buffered_h3_h1 — Medium Payload (REST JSON 12KB)
- **Mục tiêu thử nghiệm**: Nạp trọn vẹn 12KB DATA frame từ client HTTP/3 QUIC, assemble buffer in-memory và serialize thành HTTP/1.1 Content-Length body gửi tới upstream backend.
- **Kích thước tải trọng & Quy chuẩn**: 12,288 bytes Content-Length JSON body
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 20000 -c 100 -m 10 https://192.168.122.14:8445/plain/12kb https://192.168.122.14:8445/tls12/12kb https://192.168.122.14:8445/tls13/12kb
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

### KB 03: buffered_h3_h1 — Large Payload (RAM Blob 1MB)
- **Mục tiêu thử nghiệm**: Đo chi phí cấp phát vector bộ đệm nạp trọn vẹn 1MB DATA frames từ QUIC stream, chuyển tiếp qua H1 keep-alive upstream không chunking.
- **Kích thước tải trọng & Quy chuẩn**: 1,048,576 bytes Content-Length body
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 2000 -c 50 -m 5 https://192.168.122.14:8445/plain/1mb https://192.168.122.14:8445/tls12/1mb https://192.168.122.14:8445/tls13/1mb
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

### KB 04: buffered_h3_h2 — Micro Payload (Ping-Pong H3 to H2 Mux ~64B)
- **Mục tiêu thử nghiệm**: Chuyển tiếp yêu cầu HTTP/3 QUIC downstream sang stream ghép kênh trên HTTP/2 TCP upstream connection pool, đo chi phí dịch chuyển framing giữa QUIC và H2.
- **Kích thước tải trọng & Quy chuẩn**: ~64 bytes headers (GET /ping HTTP/3)
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 50000 -c 100 -m 10 https://192.168.122.14:8445/plain/ping https://192.168.122.14:8445/tls12/ping https://192.168.122.14:8445/tls13/ping
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

### KB 05: buffered_h3_h2 — Medium Payload (REST H3 to H2 Mux 12KB)
- **Mục tiêu thử nghiệm**: Ghép tải 12KB REST JSON từ QUIC stream downstream sang DATA frames trên HTTP/2 upstream multiplexed connection pool.
- **Kích thước tải trọng & Quy chuẩn**: 12,288 bytes JSON body
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 20000 -c 100 -m 10 https://192.168.122.14:8445/plain/12kb https://192.168.122.14:8445/tls12/12kb https://192.168.122.14:8445/tls13/12kb
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

### KB 06: buffered_h3_h2 — Large Payload (Heavy H3 to H2 Mux 1MB)
- **Mục tiêu thử nghiệm**: Chia nhỏ tải nặng 1MB từ QUIC stream thành các HTTP/2 DATA frames 16KB truyền qua multiplexed H2 pipe upstream.
- **Kích thước tải trọng & Quy chuẩn**: 1,048,576 bytes body (~1MB)
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 2000 -c 50 -m 5 https://192.168.122.14:8445/plain/1mb https://192.168.122.14:8445/tls12/1mb https://192.168.122.14:8445/tls13/1mb
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

### KB 07: buffered_h3_h3 — Micro Payload (Pure QUIC Ping-Pong ~64B)
- **Mục tiêu thử nghiệm**: Thuần QUIC hai đầu (Pure QUIC End-to-End), đo chi phí định tuyến UDP datagram, QPACK decode/re-encode và stream dispatching giữa 2 kết nối QUIC độc lập.
- **Kích thước tải trọng & Quy chuẩn**: ~64 bytes headers (GET /ping HTTP/3)
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 50000 -c 100 -m 10 https://192.168.122.14:8445/plain/ping
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 20000 -c 100 -m 10 https://192.168.122.14:8445/plain/12kb
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 2000 -c 50 -m 5 https://192.168.122.14:8445/plain/1mb
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 20000 -c 100 -m 5 https://192.168.122.14:8445/plain/sse-ping https://192.168.122.14:8445/tls12/sse-ping https://192.168.122.14:8445/tls13/sse-ping
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 10000 -c 50 -m 5 https://192.168.122.14:8445/plain/sse-64k https://192.168.122.14:8445/tls12/sse-64k https://192.168.122.14:8445/tls13/sse-64k
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 200 -c 20 -m 2 https://192.168.122.14:8445/plain/10mb https://192.168.122.14:8445/tls12/10mb https://192.168.122.14:8445/tls13/10mb
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 20000 -c 100 -m 5 https://192.168.122.14:8445/plain/sse-ping https://192.168.122.14:8445/tls12/sse-ping https://192.168.122.14:8445/tls13/sse-ping
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 10000 -c 50 -m 5 https://192.168.122.14:8445/plain/stream-64k https://192.168.122.14:8445/tls12/stream-64k https://192.168.122.14:8445/tls13/stream-64k
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 200 -c 20 -m 2 https://192.168.122.14:8445/plain/10mb https://192.168.122.14:8445/tls12/10mb https://192.168.122.14:8445/tls13/10mb
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 20000 -c 100 -m 5 https://192.168.122.14:8445/plain/sse-ping
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 10000 -c 50 -m 5 https://192.168.122.14:8445/plain/stream-64k
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 200 -c 20 -m 2 https://192.168.122.14:8445/plain/10mb
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 20000 -c 100 -m 5 https://192.168.122.14:8445/plain/drip https://192.168.122.14:8445/tls12/drip https://192.168.122.14:8445/tls13/drip
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 10000 -c 50 -m 5 https://192.168.122.14:8445/plain/upload-64k https://192.168.122.14:8445/tls12/upload-64k https://192.168.122.14:8445/tls13/upload-64k
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 50 -c 5 -m 1 https://192.168.122.14:8445/plain/bulk-200mb https://192.168.122.14:8445/tls12/bulk-200mb https://192.168.122.14:8445/tls13/bulk-200mb
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 20000 -c 100 -m 5 https://192.168.122.14:8445/plain/drip https://192.168.122.14:8445/tls12/drip https://192.168.122.14:8445/tls13/drip
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 10000 -c 50 -m 5 https://192.168.122.14:8445/plain/upload-64k https://192.168.122.14:8445/tls12/upload-64k https://192.168.122.14:8445/tls13/upload-64k
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 1000 -c 20 -m 2 https://192.168.122.14:8445/plain/upload-5mb https://192.168.122.14:8445/tls12/upload-5mb https://192.168.122.14:8445/tls13/upload-5mb
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 20000 -c 100 -m 5 https://192.168.122.14:8445/plain/drip
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 10000 -c 50 -m 5 https://192.168.122.14:8445/plain/upload-64k
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 1000 -c 20 -m 2 https://192.168.122.14:8445/plain/upload-5mb
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 20000 -c 100 -m 5 https://192.168.122.14:8445/plain/echo https://192.168.122.14:8445/tls12/echo https://192.168.122.14:8445/tls13/echo
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 10000 -c 50 -m 5 https://192.168.122.14:8445/plain/bidi-64k https://192.168.122.14:8445/tls12/bidi-64k https://192.168.122.14:8445/tls13/bidi-64k
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 1000 -c 20 -m 2 https://192.168.122.14:8445/plain/pump-5mb https://192.168.122.14:8445/tls12/pump-5mb https://192.168.122.14:8445/tls13/pump-5mb
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 20000 -c 100 -m 5 https://192.168.122.14:8445/plain/echo https://192.168.122.14:8445/tls12/echo https://192.168.122.14:8445/tls13/echo
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 10000 -c 50 -m 5 https://192.168.122.14:8445/plain/bidi-64k https://192.168.122.14:8445/tls12/bidi-64k https://192.168.122.14:8445/tls13/bidi-64k
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 1000 -c 20 -m 2 https://192.168.122.14:8445/plain/pump-5mb https://192.168.122.14:8445/tls12/pump-5mb https://192.168.122.14:8445/tls13/pump-5mb
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 20000 -c 100 -m 5 https://192.168.122.14:8445/plain/echo
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 10000 -c 50 -m 5 https://192.168.122.14:8445/plain/bidi-64k
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 1000 -c 20 -m 2 https://192.168.122.14:8445/plain/pump-5mb
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 10000 -c 100 -m 10 https://192.168.122.14:8445/plain/12kb
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 200000 -c 200 -m 50 https://192.168.122.14:8445/plain/ping
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
- **Lệnh thực thi chuẩn**: h2load --alpn=h3 -n 100000 -c 100 -m 20 https://192.168.122.14:8445/plain/ping
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
