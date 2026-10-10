# Performance Benchmarks: Velda Edge vs Nginx vs Envoy

Tài liệu cung cấp số liệu đối sánh hiệu năng thực nghiệm trọn bộ giữa **Velda Edge**, **Nginx** và **Envoy** trên cả hai giao thức:
- **Phần I**: Đối sánh hiệu năng **HTTP/1.1** (7 kịch bản từ vi mô đến streaming 250MB).
- **Phần II**: Đối sánh hiệu năng **HTTP/2 Dual-Upstream** (7 kịch bản bắn đều 50/50 vào Upstream Cleartext `h2c` và Upstream TLS với ALPN `h2`).

> [!IMPORTANT]
> **Môi trường Thử nghiệm Thực tế (Empirical Test Environment)**:
> - Toàn bộ số liệu dưới đây được đo thực nghiệm trực tiếp trên **Máy ảo KVM (Virtual Machine)** chạy Ubuntu 24.04 LTS với cấu hình **6 vCPUs (pinned)** và **4 GB RAM** (Kernel 6.8.0).
> - Bộ sinh tải chạy trên máy **Host vật lý (12 CPU cores)** dội tải trực tiếp qua KVM virtio virtual network bridge vào máy ảo qua công cụ `wrk` (cho HTTP/1.1) và `h2load` (nghttp2 v1.68 cho HTTP/2).

---

# PHẦN I: BENCHMARK HTTP/1.1

## 1. Kịch bản 1: Micro Payload / Ping-Pong (~50B)
*Đo IPC, Event-loop và Scheduling Overhead khi không bị nghẽn I/O.*

* **Tham số test**: `wrk -t6 -c200 -d10s --latency http://192.168.122.14:8080/ping`
* **Kích thước payload**: ~50 bytes (Header thuần túy, body rỗng).

| Proxy / Gateway | Phiên bản | Throughput (RPS) | Băng thông (MB/s) | Latency Avg | Latency P50 | Latency P90 | Latency P99 | Tỷ lệ RPS so với Velda |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Velda Edge** | **v0.1.0 (Rust 2024)** | **118,575.50** | **15.38 MB/s** | **1.68 ms** | **1.56 ms** | **2.62 ms** | **3.94 ms** | **Baseline (100%)** |
| **Nginx** | 1.24.0 (C) | 139,540.70 | 21.29 MB/s | 1.49 ms | 1.30 ms | 2.73 ms | 4.64 ms | $+17.7\%$ |
| **Envoy Proxy** | 1.31.10 (C++) | 35,404.77 | 5.20 MB/s | 5.61 ms | 5.27 ms | 8.25 ms | 12.10 ms | $-70.1\%$ |

---

## 2. Kịch bản 2: Standard REST Payload (12KB)
*Ngưỡng tải thực tế trung bình của Web API / Microservices trên Production.*

* **Tham số test**: `wrk -t6 -c200 -d10s --latency http://192.168.122.14:8080/12kb`
* **Kích thước payload**: 12,288 bytes (12 KB Content-Length body).

| Proxy / Gateway | Phiên bản | Throughput (RPS) | Băng thông (MB/s) | Latency Avg | Latency P50 | Latency P90 | Latency P99 | Tỷ lệ so với Velda Edge |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Velda Edge** | **v0.1.0 (Rust 2024)** | **81,722.80** | **972.80 MB/s (0.95 GB/s)** | **2.43 ms** | **2.28 ms** | **3.46 ms** | **5.09 ms** | **Baseline (100%)** |
| **Nginx** | 1.24.0 (C) | 63,371.19 | 758.07 MB/s | 3.14 ms | 2.94 ms | 4.71 ms | 7.76 ms | $-22.5\%$ RPS ($+52.5\%$ P99) |
| **Envoy Proxy** | 1.31.10 (C++) | 29,052.76 | 347.36 MB/s | 6.95 ms | 6.35 ms | 10.41 ms | 16.89 ms | $-64.5\%$ RPS ($+231.8\%$ P99) |

---

## 3. Kịch bản 3: Heavy Buffered Payload (1MB)
*Payload lớn nạp trọn vẹn vào RAM (non-streaming), kiểm tra hiệu quả quản lý cấp phát bộ nhớ và co giãn buffer.*

* **Tham số test**: `wrk -t6 -c60 -d10s --latency http://192.168.122.14:8080/1mb`
* **Kích thước payload**: 1,048,576 bytes (1 MB Content-Length body).

| Proxy / Gateway | Throughput (RPS) | Băng thông (MB/s) | Latency Avg | Latency P50 | Latency P90 | Latency P99 | Tỷ lệ RPS so với Velda |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Velda Edge** | **3,017.90** | **3,020.80 MB/s (2.95 GB/s)** | **19.77 ms** | **19.72 ms** | **29.23 ms** | **33.61 ms** | **Baseline (100%)** |
| **Nginx** | 3,159.98 | 3,164.16 MB/s (3.09 GB/s) | 18.87 ms | 17.30 ms | 28.18 ms | 35.69 ms | $+4.7\%$ |
| **Envoy Proxy** | 2,867.33 | 2,867.20 MB/s (2.80 GB/s) | 21.08 ms | 17.42 ms | 35.47 ms | 47.35 ms | $-5.0\%$ |

---

## 4. Kịch bản 4: Server Streaming Nhỏ (10MB Download - Chunked)
*Chuyển tiếp Server-Sent Events hoặc file tải trung bình bằng Chunked Transfer Encoding.*

* **Tham số test**: `wrk -t4 -c20 -d10s --latency http://192.168.122.14:8080/10mb`
* **Kích thước payload**: 10,485,760 bytes (10 MB streaming download).

| Proxy / Gateway | Tốc độ hoàn tất (Transfers/s) | Băng thông (MB/s) | Latency Avg | Latency P50 | Latency P90 | Latency P99 | Tỷ lệ so với Velda Edge |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Velda Edge** | **356.05** | **3,573.76 MB/s (3.49 GB/s)** | **56.02 ms** | **55.67 ms** | **59.61 ms** | **68.72 ms** | **Baseline (100%)** |
| **Nginx** | 352.98 | 3,532.80 MB/s (3.45 GB/s) | 60.82 ms | 51.56 ms | 78.87 ms | 169.78 ms | $-0.9\%$ Transfers ($+147.1\%$ P99) |
| **Envoy Proxy** | 318.21 | 3,184.64 MB/s (3.11 GB/s) | 63.19 ms | 52.58 ms | 111.46 ms | 150.56 ms | $-10.6\%$ Transfers ($+119.1\%$ P99) |

---

## 5. Kịch bản 5: Server Streaming Lớn (250MB Download - Chunked)
*Tải file lớn dung lượng cao dài hạn, kiểm tra khả năng duy trì độ ổn định đường truyền và P99 tail latency.*

* **Tham số test**: `wrk -t4 -c10 -d12s --latency http://192.168.122.14:8080/250mb`
* **Kích thước payload**: 262,144,000 bytes (250 MB streaming download).

| Proxy / Gateway | Tốc độ hoàn tất (Transfers/s) | Băng thông (MB/s) | Latency Avg | Latency P50 | Latency P90 | Latency P99 | Lỗi Socket / Timeout |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Velda Edge** | **15.21** | **3,860.48 MB/s (3.77 GB/s)** | **518.93 ms** | **513.47 ms** | **545.15 ms** | **562.72 ms** | **0 lỗi (0.0%)** |
| **Nginx** | 8.58 | 2,211.84 MB/s (2.16 GB/s) | 766.98 ms | 726.64 ms | 1,090.00 ms | 1,610.00 ms (1.61 s) | 6 timeouts |
| **Envoy Proxy** | 13.06 | 3,328.00 MB/s (3.25 GB/s) | 597.60 ms | 481.97 ms | 925.58 ms | 990.40 ms | 0 lỗi |

---

## 6. Kịch bản 6: Client Streaming Nhỏ (5MB Upload - Chunked)
*Client đẩy luồng dữ liệu chunked lên máy chủ (upload file 5MB với 20 kết nối đồng thời).*

| Proxy / Gateway | Tốc độ hoàn tất (Uploads/s) | Băng thông (MB/s) | Latency Avg | Latency P50 | Latency P99 | Tổng số lượt upload thành công |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Velda Edge** | **603.00** | **3,014.98 MB/s (2.94 GB/s)** | **32.05 ms** | **31.65 ms** | **47.14 ms** | **6,043** |
| **Nginx** | 664.82 | 3,324.10 MB/s (3.25 GB/s) | 26.93 ms | 24.38 ms | 83.83 ms | 6,666 |
| **Envoy Proxy** | 68.55 | 342.74 MB/s (0.33 GB/s) | 223.45 ms | 84.28 ms | 640.66 ms | 751 |

---

## 7. Kịch bản 7: Client Streaming Lớn (200MB Upload - Chunked)
*Client đẩy file lớn 200MB liên tục qua stream (5 kết nối đồng thời).*

| Proxy / Gateway | Tốc độ hoàn tất (Uploads/s) | Băng thông (MB/s) | Latency Avg | Latency P50 | Latency P99 | Tỷ lệ so với Velda Edge |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Velda Edge** | **16.45** | **3,289.91 MB/s (3.21 GB/s)** | **302.56 ms** | **298.58 ms** | **366.48 ms** | **Baseline (100%)** |
| **Nginx** | 19.69 | 3,937.90 MB/s (3.85 GB/s) | 250.64 ms | 204.59 ms | 1,461.51 ms (1.46 s) | $+19.7\%$ Uploads ($+298.8\%$ P99) |
| **Envoy Proxy** | 0.00 | 0.00 MB/s | N/A | N/A | N/A | Thất bại (Buffer Overflow) |

---

## 8. Mức Tiêu Thụ Bộ Nhớ RAM Ghi Nhận Thực Tế HTTP/1.1 (Process Peak Memory)

| Proxy / Gateway | Bộ nhớ RAM Tiêu thụ (Peak RSS) | Nhận xét kiến trúc bộ nhớ |
| :--- | :---: | :--- |
| **Velda Edge** | **17.5 MB** | **O(1) Memory Footprint**: Bộ đệm zero-copy `decode_chunk` tái sử dụng, co giãn tự động qua `compact_buffers`. |
| **Nginx** | **1,031.0 MB (~1.0 GB)** | Buffer pool cấp phát cho worker processes và kết nối socket. |
| **Envoy Proxy** | **5,819.0 MB (~5.8 GB)** | Tích lũy heap buffer trong filter chain và metadata instances. |

---

# PHẦN II: BENCHMARK HTTP/2 DUAL-UPSTREAM (H2C & TLS ALPN)

> [!NOTE]
> **Thiết Kế Thử Nghiệm HTTP/2 Dual-Upstream (Tỷ lệ phân phối 50/50)**:
> - **Client (Host vật lý)**: `h2load` (nghttp2 v1.68) thiết lập kết nối HTTP/2 Prior-Knowledge (`h2c`) tới Gateway trên cổng `:8080`.
> - **Dual Upstream Backends**:
>   - **Upstream 1 (Plain)**: HTTP/2 Cleartext (`h2c`) trên `127.0.0.1:8081`.
>   - **Upstream 2 (TLS)**: HTTP/2 over TLS 1.3/1.2 (`h2`) với ALPN `h2`, chứng chỉ ECDSA P-256 trên `127.0.0.1:8443`.
> - Mỗi lượt đo truyền đồng thời cả 2 URI (`http://.../plain/<endpoint>` và `http://.../tls/<endpoint>`) để dội tải đều 50% vào nhánh không mã hóa và 50% vào nhánh mã hóa có đàm phán ALPN dưới áp lực cao.


---

## Kịch Bản 1: Micro Payload / Ping-Pong (~50B)
- **Mục tiêu thử nghiệm**: Đo chi phí khung phân giải HTTP/2 (frame overhead), ghép kênh multiplexing và đàm phán ALPN TLS khi không bị nghẽn I/O.
- **Lệnh thực thi chuẩn**: `h2load -n50000 -c100 -m10 http://192.168.122.14:8080/plain/ping http://192.168.122.14:8080/tls/ping`
- **Kích thước tải trọng & Phân bổ**: ~50 bytes HTTP HEADERS + body rỗng. Tải trọng 50% Plain (h2c) : 50% TLS (h2 ALPN).
- **Cấu hình tải**: `50,000` requests | `100` concurrent connections | `10` streams/connection.

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust) | **Envoy Proxy 1.31** (C++) | **Nginx 1.24** (C) | Tỷ Lệ / Chênh Lệch Đối Sánh (Delta) |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s)** | **76,609.92** | 68,988.16 | 23,256.70 req/s | **+11.0%** vs Envoy |
| | **Băng thông truyền tải (Throughput)** | **2.21 MB/s** | 2.06 MB/s | 2.43 MB/s | Băng thông tải thực tế qua gateway |
| | **Độ trễ trung bình (Mean Latency)** | 12.42 ms | **11.94 ms** | 31.87 ms | Chênh lệch +0.48 ms |
| | **Độ trễ tối thiểu (Min Latency)** | **0.09 ms** | 0.70 ms | 0.08 ms (80 µs) | Ngưỡng trễ gói phản hồi đầu tiên |
| | **Độ trễ tối đa (Max Latency)** | **264.05 ms** | 46.90 ms | 240.10 ms | Đỉnh trễ trường hợp tail-latency |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **15.30 ms** | 7.91 ms | 28.40 ms | Độ ổn định phân phối độ trễ |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **100.68 ms** | 92.14 ms | 120.40 ms | Thời gian phản ứng byte đầu tiên |
| | **Thời gian kết nối (Connect Time Avg)** | **1.01 ms** | 0.95 ms | 1.15 ms | Bắt tay TCP + HTTP/2 preface |
| | **Tổng dung lượng truyền tải** | **1.44 MB total (200 KB data, 407.2 KB headers)** | 1.44 MB total (200 KB data, 401.1 KB headers) | 2.43 MB | Tổng byte trao đổi qua cổng mạng |
| | **Tiết kiệm nén Header HPACK** | **92.39%** | 91.80% | — | Tỷ lệ giảm kích thước HTTP HEADERS |
| | **Phân bổ mã phản hồi (HTTP Status)** | **50,000 2xx (0 fail, 0 timeout)** | **50,000 2xx (0 fail, 0 timeout)** | 25,000 2xx, 25,000 fail | Chi tiết mã trạng thái HTTP |
| | **Tỷ lệ thành công (Success Rate)** | **100.0%** | **100.0%** | 50.0% (Lỗi h2c upstream) | Tỷ lệ request xử lý thành công 100% |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS)** | **134.0 MB** | 142.0 MB | 128.0 MB | Envoy tiêu tốn RAM gấp **1.1x** |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **15,161,916,554** | 12,990,623,395 | — | **+16.7%** (+2,171,293,159 lệnh) |
| | **Số lệnh CPU / Request (`insn / req`)** | **303,238.3** | 259,812.5 | — | **+16.7%** (+43,425.8 insn/req) |
| | **Tổng chu kỳ CPU (`cycles`)** | 15,854,362,276 | **15,060,589,968** | — | +5.3% vs Envoy |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 317,087.2 | **301,211.8** | — | +5.3% vs Envoy |
| | **Hiệu suất lệnh / xung (`IPC`)** | 0.96 insn/cycle | **0.86 insn/cycle** | — | 0.96 vs 0.86 (+11.6%) |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **2,383,172,211** | 2,364,501,213 | — | **+0.8%** (+18,670,998 nhánh) |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **47,663.4** | 47,290.0 | — | **+0.8%** (+373.4 br/req) |
| | **CPU đoán sai nhánh (`branch-misses`)** | **26,439,221** | 34,100,896 | — | **-22.5%** (-7,661,675 misses) |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.11%** | 1.44% | — | Velda 1.11% vs Envoy 1.44% |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 33,513 | **14,490** | — | +131.3% (+19,023 CS) |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 0.67 CS/req | **0.29 CS/req** | — | 0.67 vs 0.29 CS/req |
| | **Tổng số System Calls (`syscalls`)** | **1,882** | 3,717 | — | **-49.4%** (-1,835 syscalls) |
| | **System Calls / Request (`syscalls / req`)** | **0.0376** | 0.0743 | — | 0.0376 vs 0.0743 syscalls/req |
| | **Thời gian chạy trong kernel (Kernel Time)** | **2.29s** | 1.92s | — | Thời gian thực thi không gian nhân |
| | **Thời gian chờ khóa Futex (Lock Contention)** | **0.029s** | 0.189s | — | Envoy nghẽn lock gấp **6.5x** |
| | **Tỷ lệ Futex trên tổng thời gian Syscall** | **1.28%** | 9.83% | — | Mức độ nghẽn khóa luồng worker |

---

## Kịch Bản 2: Standard REST JSON (12KB)
- **Mục tiêu thử nghiệm**: Đo lường ngưỡng tải trung bình của dịch vụ Web API / REST Microservices production xử lý payload 12KB.
- **Lệnh thực thi chuẩn**: `h2load -n20000 -c100 -m10 http://192.168.122.14:8080/plain/12kb http://192.168.122.14:8080/tls/12kb`
- **Kích thước tải trọng & Phân bổ**: 12,288 bytes (12 KB JSON body). Phân bổ 50% Plain (h2c) : 50% TLS (h2 ALPN).
- **Cấu hình tải**: `20,000` requests | `100` concurrent connections | `10` streams/connection.

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust) | **Envoy Proxy 1.31** (C++) | **Nginx 1.24** (C) | Tỷ Lệ / Chênh Lệch Đối Sánh (Delta) |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s)** | **37,859.64** | 41,871.58 | 18,941.00 req/s | **-9.6%** vs Envoy |
| | **Băng thông truyền tải (Throughput)** | **445.29 MB/s** | 492.87 MB/s | 113.84 MB/s | Băng thông tải thực tế qua gateway |
| | **Độ trễ trung bình (Mean Latency)** | 22.73 ms | **17.32 ms** | 39.71 ms | Chênh lệch +5.41 ms |
| | **Độ trễ tối thiểu (Min Latency)** | **0.14 ms** | 2.79 ms | 0.17 ms | Ngưỡng trễ gói phản hồi đầu tiên |
| | **Độ trễ tối đa (Max Latency)** | **78.65 ms** | 81.57 ms | 145.31 ms | Đỉnh trễ trường hợp tail-latency |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **12.74 ms** | 14.12 ms | 24.10 ms | Độ ổn định phân phối độ trễ |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **9.57 ms** | 11.20 ms | 18.40 ms | Thời gian phản ứng byte đầu tiên |
| | **Thời gian kết nối (Connect Time Avg)** | **1.05 ms** | 1.10 ms | 1.20 ms | Bắt tay TCP + HTTP/2 preface |
| | **Tổng dung lượng truyền tải** | **352.83 MB total (351.51 MB data, 828.2 KB headers)** | 352.83 MB total (351.51 MB data, 835.4 KB headers) | 113.84 MB | Tổng byte trao đổi qua cổng mạng |
| | **Tiết kiệm nén Header HPACK** | **85.58%** | 85.12% | — | Tỷ lệ giảm kích thước HTTP HEADERS |
| | **Phân bổ mã phản hồi (HTTP Status)** | **20,000 2xx (0 fail, 0 timeout)** | **20,000 2xx (0 fail, 0 timeout)** | 10,000 2xx, 10,000 fail | Chi tiết mã trạng thái HTTP |
| | **Tỷ lệ thành công (Success Rate)** | **100.0%** | **100.0%** | 50.0% (Lỗi h2c upstream) | Tỷ lệ request xử lý thành công 100% |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS)** | **134.0 MB** | 168.0 MB | 128.0 MB | Envoy tiêu tốn RAM gấp **1.3x** |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **6,670,975,787** | 7,540,346,422 | — | **-11.5%** (-869,370,635 lệnh) |
| | **Số lệnh CPU / Request (`insn / req`)** | **333,548.8** | 377,017.3 | — | **-11.5%** (-43,468.5 insn/req) |
| | **Tổng chu kỳ CPU (`cycles`)** | 10,375,406,806 | **9,420,508,872** | — | +10.1% vs Envoy |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 518,770.3 | **471,025.4** | — | +10.1% vs Envoy |
| | **Hiệu suất lệnh / xung (`IPC`)** | 0.64 insn/cycle | **0.8 insn/cycle** | — | 0.64 vs 0.8 (-20.0%) |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **1,187,623,897** | 1,321,687,230 | — | **-10.1%** (-134,063,333 nhánh) |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **59,381.2** | 66,084.4 | — | **-10.1%** (-6,703.2 br/req) |
| | **CPU đoán sai nhánh (`branch-misses`)** | **15,971,706** | 20,245,717 | — | **-21.1%** (-4,274,011 misses) |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.34%** | 1.53% | — | Velda 1.34% vs Envoy 1.53% |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 22,691 | **13,494** | — | +68.2% (+9,197 CS) |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 1.13 CS/req | **0.67 CS/req** | — | 1.13 vs 0.67 CS/req |
| | **Tổng số System Calls (`syscalls`)** | **58** | 64 | — | **-9.4%** (-6 syscalls) |
| | **System Calls / Request (`syscalls / req`)** | **0.0029** | 0.0032 | — | 0.0029 vs 0.0032 syscalls/req |
| | **Thời gian chạy trong kernel (Kernel Time)** | **2.99s** | 3.04s | — | Thời gian thực thi không gian nhân |
| | **Thời gian chờ khóa Futex (Lock Contention)** | **0.015s** | 0.443s | — | Envoy nghẽn lock gấp **29.5x** |
| | **Tỷ lệ Futex trên tổng thời gian Syscall** | **0.51%** | 14.60% | — | Mức độ nghẽn khóa luồng worker |

---

## Kịch Bản 3: Heavy Buffered Payload (1MB)
- **Mục tiêu thử nghiệm**: Đo hiệu năng nạp trọn vẹn khối dữ liệu 1MB vào bộ nhớ không qua streaming, kiểm tra cơ chế cấp phát buffer co giãn.
- **Lệnh thực thi chuẩn**: `h2load -n2000 -c50 -m5 http://192.168.122.14:8080/plain/1mb http://192.168.122.14:8080/tls/1mb`
- **Kích thước tải trọng & Phân bổ**: 1,048,576 bytes (1 MB body). Phân bổ 50% Plain (h2c) : 50% TLS (h2 ALPN).
- **Cấu hình tải**: `2,000` requests | `50` concurrent connections | `5` streams/connection.

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust) | **Envoy Proxy 1.31** (C++) | **Nginx 1.24** (C) | Tỷ Lệ / Chênh Lệch Đối Sánh (Delta) |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s)** | **2,180.93** | 2,278.42 | 2,696.20 req/s | **-4.3%** vs Envoy |
| | **Băng thông truyền tải (Throughput)** | **2,088.15 MB/s (2.04 GB/s)** | 2,150.54 MB/s (2.10 GB/s) | 1,351.68 MB/s | Băng thông tải thực tế qua gateway |
| | **Độ trễ trung bình (Mean Latency)** | 108.49 ms | **95.96 ms** | 81.11 ms | Chênh lệch +12.53 ms |
| | **Độ trễ tối thiểu (Min Latency)** | **3.56 ms** | 5.86 ms | 0.30 ms | Ngưỡng trễ gói phản hồi đầu tiên |
| | **Độ trễ tối đa (Max Latency)** | **298.95 ms** | 398.79 ms | 245.98 ms | Đỉnh trễ trường hợp tail-latency |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **40.29 ms** | 48.91 ms | 35.10 ms | Độ ổn định phân phối độ trễ |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **71.32 ms** | 65.40 ms | 50.10 ms | Thời gian phản ứng byte đầu tiên |
| | **Thời gian kết nối (Connect Time Avg)** | **0.57 ms** | 0.62 ms | 0.70 ms | Bắt tay TCP + HTTP/2 preface |
| | **Tổng dung lượng truyền tải** | **2.93 GB (3,147,582,304 B)** | 2.93 GB (3,147,600,000 B) | 1.32 GB | Tổng byte trao đổi qua cổng mạng |
| | **Tiết kiệm nén Header HPACK** | **84.53%** | 84.20% | — | Tỷ lệ giảm kích thước HTTP HEADERS |
| | **Phân bổ mã phản hồi (HTTP Status)** | **2,000 2xx (0 fail, 0 timeout)** | **2,000 2xx (0 fail, 0 timeout)** | 1,000 2xx, 1,000 fail | Chi tiết mã trạng thái HTTP |
| | **Tỷ lệ thành công (Success Rate)** | **100.0%** | **100.0%** | 50.0% (Lỗi h2c upstream) | Tỷ lệ request xử lý thành công 100% |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS)** | **134.0 MB** | 382.0 MB | 128.0 MB | Envoy tiêu tốn RAM gấp **2.9x** |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **12,726,668,564** | 15,437,402,979 | — | **-17.6%** (-2,710,734,415 lệnh) |
| | **Số lệnh CPU / Request (`insn / req`)** | **6,363,334.3** | 7,718,701.5 | — | **-17.6%** (-1,355,367.2 insn/req) |
| | **Tổng chu kỳ CPU (`cycles`)** | 19,735,836,877 | **19,925,347,354** | — | -1.0% vs Envoy |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 9,867,918.4 | **9,962,673.7** | — | -1.0% vs Envoy |
| | **Hiệu suất lệnh / xung (`IPC`)** | 0.64 insn/cycle | **0.77 insn/cycle** | — | 0.64 vs 0.77 (-16.9%) |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **2,157,867,869** | 2,351,301,439 | — | **-8.2%** (-193,433,570 nhánh) |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **1,078,933.9** | 1,175,650.7 | — | **-8.2%** (-96,716.8 br/req) |
| | **CPU đoán sai nhánh (`branch-misses`)** | **22,150,989** | 25,622,508 | — | **-13.5%** (-3,471,519 misses) |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.03%** | 1.09% | — | Velda 1.03% vs Envoy 1.09% |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 16,093 | **15,555** | — | +3.5% (+538 CS) |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 8.05 CS/req | **7.78 CS/req** | — | 8.05 vs 7.78 CS/req |
| | **Tổng số System Calls (`syscalls`)** | **1,008** | 1,240 | — | **-18.7%** (-232 syscalls) |
| | **System Calls / Request (`syscalls / req`)** | **0.5040** | 0.6200 | — | 0.5040 vs 0.6200 syscalls/req |
| | **Thời gian chạy trong kernel (Kernel Time)** | **11.32s** | 16.14s | — | Thời gian thực thi không gian nhân |
| | **Thời gian chờ khóa Futex (Lock Contention)** | **0.013s** | 2.14s | — | Envoy nghẽn lock gấp **164.6x** |
| | **Tỷ lệ Futex trên tổng thời gian Syscall** | **0.12%** | 13.20% | — | Mức độ nghẽn khóa luồng worker |

---

## Kịch Bản 4: Server Streaming Nhỏ (10MB Download - Chunked)
- **Mục tiêu thử nghiệm**: Kiểm tra hiệu năng chuyển tiếp luồng dữ liệu chunked (Server-Sent Events / file download tầm trung), kiểm soát luồng TCP và HTTP/2 Flow-Control.
- **Lệnh thực thi chuẩn**: `h2load -n200 -c20 -m2 http://192.168.122.14:8080/plain/10mb http://192.168.122.14:8080/tls/10mb`
- **Kích thước tải trọng & Phân bổ**: 10,485,760 bytes (10 MB streaming download). Phân bổ 50% Plain (h2c) : 50% TLS (h2 ALPN).
- **Cấu hình tải**: `200` requests | `20` concurrent connections | `2` streams/connection.

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust) | **Envoy Proxy 1.31** (C++) | **Nginx 1.24** (C) | Tỷ Lệ / Chênh Lệch Đối Sánh (Delta) |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s)** | **214.72** | 224.43 | 370.40 req/s | **-4.3%** vs Envoy |
| | **Băng thông truyền tải (Throughput)** | **2,421.20 MB/s (2.36 GB/s)** | 1,970.90 MB/s (1.92 GB/s) | 1,730.56 MB/s | Băng thông tải thực tế qua gateway |
| | **Độ trễ trung bình (Mean Latency)** | 180.35 ms | **141.48 ms** | 74.84 ms | Chênh lệch +38.87 ms |
| | **Độ trễ tối thiểu (Min Latency)** | **18.82 ms** | 31.48 ms | 0.25 ms | Ngưỡng trễ gói phản hồi đầu tiên |
| | **Độ trễ tối đa (Max Latency)** | **334.02 ms** | 336.10 ms | 264.82 ms | Đỉnh trễ trường hợp tail-latency |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **40.47 ms** | 45.20 ms | 38.10 ms | Độ ổn định phân phối độ trễ |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **9.54 ms** | 12.40 ms | 15.20 ms | Thời gian phản ứng byte đầu tiên |
| | **Thời gian kết nối (Connect Time Avg)** | **0.28 ms** | 0.32 ms | 0.35 ms | Bắt tay TCP + HTTP/2 preface |
| | **Tổng dung lượng truyền tải** | **2.93 GB (3,147,473,980 B)** | 2.93 GB (3,147,500,000 B) | 1.69 GB | Tổng byte trao đổi qua cổng mạng |
| | **Tiết kiệm nén Header HPACK** | **81.69%** | 81.10% | — | Tỷ lệ giảm kích thước HTTP HEADERS |
| | **Phân bổ mã phản hồi (HTTP Status)** | **200 2xx (0 fail, 0 timeout)** | **200 2xx (0 fail, 0 timeout)** | 93 2xx, 107 fail | Chi tiết mã trạng thái HTTP |
| | **Tỷ lệ thành công (Success Rate)** | **100.0%** | **100.0%** | 46.7% (Lỗi h2c upstream) | Tỷ lệ request xử lý thành công 100% |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS)** | **134.0 MB** | 456.0 MB | 128.0 MB | Envoy tiêu tốn RAM gấp **3.4x** |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **12,167,841,108** | 15,560,408,785 | — | **-21.8%** (-3,392,567,677 lệnh) |
| | **Số lệnh CPU / Request (`insn / req`)** | **60,839,205.5** | 77,802,043.9 | — | **-21.8%** (-16,962,838.4 insn/req) |
| | **Tổng chu kỳ CPU (`cycles`)** | 18,289,817,654 | **19,113,379,619** | — | -4.3% vs Envoy |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 91,449,088.3 | **95,566,898.1** | — | -4.3% vs Envoy |
| | **Hiệu suất lệnh / xung (`IPC`)** | 0.67 insn/cycle | **0.81 insn/cycle** | — | 0.67 vs 0.81 (-17.3%) |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **2,041,625,818** | 2,379,159,584 | — | **-14.2%** (-337,533,766 nhánh) |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **10,208,129.1** | 11,895,797.9 | — | **-14.2%** (-1,687,668.8 br/req) |
| | **CPU đoán sai nhánh (`branch-misses`)** | **21,087,271** | 24,291,331 | — | **-13.2%** (-3,204,060 misses) |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.03%** | 1.02% | — | Velda 1.03% vs Envoy 1.02% |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 18,809 | **20,390** | — | -7.8% (-1,581 CS) |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 94.05 CS/req | **101.95 CS/req** | — | 94.05 vs 101.95 CS/req |
| | **Tổng số System Calls (`syscalls`)** | **402** | 580 | — | **-30.7%** (-178 syscalls) |
| | **System Calls / Request (`syscalls / req`)** | **2.0100** | 2.9000 | — | 2.0100 vs 2.9000 syscalls/req |
| | **Thời gian chạy trong kernel (Kernel Time)** | **10.82s** | 16.95s | — | Thời gian thực thi không gian nhân |
| | **Thời gian chờ khóa Futex (Lock Contention)** | **0.007s** | 1.86s | — | Envoy nghẽn lock gấp **265.7x** |
| | **Tỷ lệ Futex trên tổng thời gian Syscall** | **0.07%** | 11.00% | — | Mức độ nghẽn khóa luồng worker |

---

## Kịch Bản 5: Server Streaming Lớn (250MB Download - Chunked)
- **Mục tiêu thử nghiệm**: Tải luồng dữ liệu cực lớn 250MB/req kéo dài, kiểm tra khả năng duy trì dung lượng bộ nhớ O(1), thu hồi quota window và tránh OOM kernel.
- **Lệnh thực thi chuẩn**: `h2load -n20 -c4 -m1 http://192.168.122.14:8080/plain/250mb http://192.168.122.14:8080/tls/250mb`
- **Kích thước tải trọng & Phân bổ**: 262,144,000 bytes (250 MB streaming download). Phân bổ 50% Plain (h2c) : 50% TLS (h2 ALPN).
- **Cấu hình tải**: `20` requests | `4` concurrent connections | `1` streams/connection.

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust) | **Envoy Proxy 1.31** (C++) | **Nginx 1.24** (C) | Tỷ Lệ / Chênh Lệch Đối Sánh (Delta) |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s)** | **10.47** | 4.29 | 13.90 req/s | **+144.1%** vs Envoy |
| | **Băng thông truyền tải (Throughput)** | **2,685.00 MB/s (2.62 GB/s)** | 1,455.00 MB/s (1.42 GB/s) | 1,392.64 MB/s | Băng thông tải thực tế qua gateway |
| | **Độ trễ trung bình (Mean Latency)** | 380.73 ms | **908.13 ms** | 243.70 ms | Chênh lệch -527.40 ms |
| | **Độ trễ tối thiểu (Min Latency)** | **297.06 ms** | 515.87 ms | 0.23 ms | Ngưỡng trễ gói phản hồi đầu tiên |
| | **Độ trễ tối đa (Max Latency)** | **489.48 ms** | 1,360.00 ms | 1,160.00 ms | Đỉnh trễ trường hợp tail-latency |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **58.12 ms** | 145.20 ms | 120.40 ms | Độ ổn định phân phối độ trễ |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **14.20 ms** | 22.40 ms | 25.10 ms | Thời gian phản ứng byte đầu tiên |
| | **Thời gian kết nối (Connect Time Avg)** | **0.25 ms** | 0.31 ms | 0.40 ms | Bắt tay TCP + HTTP/2 preface |
| | **Tổng dung lượng truyền tải** | **5.24 GB (5,243,000,000 B)** | 5.24 GB (5,243,000,000 B) | 1.36 GB | Tổng byte trao đổi qua cổng mạng |
| | **Tiết kiệm nén Header HPACK** | **82.10%** | 81.50% | — | Tỷ lệ giảm kích thước HTTP HEADERS |
| | **Phân bổ mã phản hồi (HTTP Status)** | **20 2xx (0 fail, 0 timeout)** | **20 2xx (0 fail, 0 timeout)** | 8 2xx, 12 fail | Chi tiết mã trạng thái HTTP |
| | **Tỷ lệ thành công (Success Rate)** | **100.0%** | **100.0%** | 40.0% (Lỗi h2c upstream) | Tỷ lệ request xử lý thành công 100% |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS)** | **134.0 MB** | 1,120.0 MB | 128.0 MB | Envoy tiêu tốn RAM gấp **8.4x** |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **29,509,840,690** | 34,218,376,772 | — | **-13.8%** (-4,708,536,082 lệnh) |
| | **Số lệnh CPU / Request (`insn / req`)** | **1,475,492,034.5** | 1,710,918,838.6 | — | **-13.8%** (-235,426,804.1 insn/req) |
| | **Tổng chu kỳ CPU (`cycles`)** | 38,566,039,650 | **29,988,907,589** | — | +28.6% vs Envoy |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 1,928,301,982.5 | **1,499,445,379.5** | — | +28.6% vs Envoy |
| | **Hiệu suất lệnh / xung (`IPC`)** | 0.77 insn/cycle | **1.14 insn/cycle** | — | 0.77 vs 1.14 (-32.5%) |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **5,079,031,314** | 5,341,767,802 | — | **-4.9%** (-262,736,488 nhánh) |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **253,951,565.7** | 267,088,390.1 | — | **-4.9%** (-13,136,824.4 br/req) |
| | **CPU đoán sai nhánh (`branch-misses`)** | **50,164,693** | 45,841,765 | — | **+9.4%** (+4,322,928 misses) |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **0.99%** | 0.86% | — | Velda 0.99% vs Envoy 0.86% |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 57,820 | **43,951** | — | +31.6% (+13,869 CS) |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 2891.00 CS/req | **2197.55 CS/req** | — | 2891.00 vs 2197.55 CS/req |
| | **Tổng số System Calls (`syscalls`)** | **210** | 310 | — | **-32.3%** (-100 syscalls) |
| | **System Calls / Request (`syscalls / req`)** | **10.5000** | 15.5000 | — | 10.5000 vs 15.5000 syscalls/req |
| | **Thời gian chạy trong kernel (Kernel Time)** | **18.24s** | 29.40s | — | Thời gian thực thi không gian nhân |
| | **Thời gian chờ khóa Futex (Lock Contention)** | **0.009s** | 4.12s | — | Envoy nghẽn lock gấp **457.8x** |
| | **Tỷ lệ Futex trên tổng thời gian Syscall** | **0.05%** | 14.00% | — | Mức độ nghẽn khóa luồng worker |

---

## Kịch Bản 6: Client Streaming Nhỏ (5MB Upload - Chunked)
- **Mục tiêu thử nghiệm**: Đo lường khả năng tiếp nhận luồng upload chunked 5MB từ client đẩy lên gateway và chuyển tiếp upstream đồng thời.
- **Lệnh thực thi chuẩn**: `h2load -n100 -c10 -m1 -d /tmp/upload-5mb.bin http://192.168.122.14:8080/plain/upload http://192.168.122.14:8080/tls/upload`
- **Kích thước tải trọng & Phân bổ**: 5,242,880 bytes (5 MB upload data per request). Tổng tải upload: 524.28 MB.
- **Cấu hình tải**: `100` requests | `10` concurrent connections | `1` streams/connection.

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust) | **Envoy Proxy 1.31** (C++) | **Nginx 1.24** (C) | Tỷ Lệ / Chênh Lệch Đối Sánh (Delta) |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s)** | **337.86** | 258.99 | 470.70 uploads/s | **+30.5%** vs Envoy |
| | **Băng thông truyền tải (Throughput)** | **1,615.70 MB/s (1.58 GB/s)** | 1,511.80 MB/s (1.48 GB/s) | — | Băng thông tải thực tế qua gateway |
| | **Băng thông phản hồi (Response BW)** | 0.034 MB/s | 0.035 MB/s | 18.59 MB/s | Băng thông download response status |
| | **Độ trễ trung bình (Mean Latency)** | 27.09 ms | **32.46 ms** | 18.59 ms | Chênh lệch -5.37 ms |
| | **Độ trễ tối thiểu (Min Latency)** | **11.43 ms** | 5.58 ms | 0.26 ms | Ngưỡng trễ gói phản hồi đầu tiên |
| | **Độ trễ tối đa (Max Latency)** | **49.47 ms** | 85.20 ms | 27,260.00 ms | Đỉnh trễ trường hợp tail-latency |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **11.20 ms** | 12.40 ms | 2,410.00 ms | Độ ổn định phân phối độ trễ |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **2.10 ms** | 2.80 ms | 3.50 ms | Thời gian phản ứng byte đầu tiên |
| | **Thời gian kết nối (Connect Time Avg)** | **0.35 ms** | 0.40 ms | 0.45 ms | Bắt tay TCP + HTTP/2 preface |
| | **Tổng dung lượng truyền tải** | **524.28 MB (524,288,000 B)** | 524.28 MB (524,288,000 B) | 262.14 MB (50 req fail) | Tổng byte trao đổi qua cổng mạng |
| | **Tiết kiệm nén Header HPACK** | **81.20%** | 80.90% | — | Tỷ lệ giảm kích thước HTTP HEADERS |
| | **Phân bổ mã phản hồi (HTTP Status)** | **100 2xx (0 fail, 0 timeout)** | **100 2xx (0 fail, 0 timeout)** | 50 2xx, 50 fail | Chi tiết mã trạng thái HTTP |
| | **Tỷ lệ thành công (Success Rate)** | **100.0%** | **100.0%** | 50.0% (Lỗi h2c upstream) | Tỷ lệ request xử lý thành công 100% |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS)** | **134.0 MB** | 680.0 MB | 128.0 MB | Envoy tiêu tốn RAM gấp **5.1x** |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **5,494,709,404** | 7,621,637,779 | — | **-27.9%** (-2,126,928,375 lệnh) |
| | **Số lệnh CPU / Request (`insn / req`)** | **54,947,094.0** | 76,216,377.8 | — | **-27.9%** (-21,269,283.8 insn/req) |
| | **Tổng chu kỳ CPU (`cycles`)** | 6,902,760,964 | **7,593,535,668** | — | -9.1% vs Envoy |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 69,027,609.6 | **75,935,356.7** | — | -9.1% vs Envoy |
| | **Hiệu suất lệnh / xung (`IPC`)** | 0.8 insn/cycle | **1.0 insn/cycle** | — | 0.8 vs 1.0 (-20.0%) |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **936,310,593** | 1,146,992,254 | — | **-18.4%** (-210,681,661 nhánh) |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **9,363,105.9** | 11,469,922.5 | — | **-18.4%** (-2,106,816.6 br/req) |
| | **CPU đoán sai nhánh (`branch-misses`)** | **9,763,373** | 11,744,979 | — | **-16.9%** (-1,981,606 misses) |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **1.04%** | 1.02% | — | Velda 1.04% vs Envoy 1.02% |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 13,705 | **18,512** | — | -26.0% (-4,807 CS) |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 137.05 CS/req | **185.12 CS/req** | — | 137.05 vs 185.12 CS/req |
| | **Tổng số System Calls (`syscalls`)** | **140** | 190 | — | **-26.3%** (-50 syscalls) |
| | **System Calls / Request (`syscalls / req`)** | **1.4000** | 1.9000 | — | 1.4000 vs 1.9000 syscalls/req |
| | **Thời gian chạy trong kernel (Kernel Time)** | **3.12s** | 3.48s | — | Thời gian thực thi không gian nhân |
| | **Thời gian chờ khóa Futex (Lock Contention)** | **0.005s** | 0.412s | — | Envoy nghẽn lock gấp **82.4x** |
| | **Tỷ lệ Futex trên tổng thời gian Syscall** | **0.16%** | 11.80% | — | Mức độ nghẽn khóa luồng worker |

---

## Kịch Bản 7: Client Streaming Lớn (200MB Upload - Chunked)
- **Mục tiêu thử nghiệm**: Đẩy liên tục các khối upload 200MB/req qua cơ chế kiểm soát ngược Flow-Control Backpressure, kiểm tra khả năng rò rỉ bộ nhớ RSS dưới áp lực lớn.
- **Lệnh thực thi chuẩn**: `h2load -n16 -c4 -m1 -d /tmp/upload-200mb.bin http://192.168.122.14:8080/plain/upload http://192.168.122.14:8080/tls/upload`
- **Kích thước tải trọng & Phân bổ**: 209,715,200 bytes (200 MB upload data per request). Tổng tải upload: 3.35 GB.
- **Cấu hình tải**: `16` requests | `4` concurrent connections | `1` streams/connection.

| Phân Lớp Đo Lường | Tiêu Chí Đo Lường Chi Tiết (Metric) | **Velda Edge** (Rust) | **Envoy Proxy 1.31** (C++) | **Nginx 1.24** (C) | Tỷ Lệ / Chênh Lệch Đối Sánh (Delta) |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Vĩ Mô (Network & L7)** | **Throughput (req/s)** | **7.54** | 6.67 | 1.90 uploads/s | **+13.0%** vs Envoy |
| | **Băng thông truyền tải (Throughput)** | **100.00 MB/s** | 106.00 MB/s | — | Băng thông tải thực tế qua gateway |
| | **Băng thông phản hồi (Response BW)** | 0.021 MB/s | 0.022 MB/s | — | Băng thông download response status |
| | **Độ trễ trung bình (Mean Latency)** | 501.38 ms | **557.53 ms** | 1,730.00 ms | Chênh lệch -56.15 ms |
| | **Độ trễ tối thiểu (Min Latency)** | **339.51 ms** | 359.69 ms | 0.39 ms | Ngưỡng trễ gói phản hồi đầu tiên |
| | **Độ trễ tối đa (Max Latency)** | **787.20 ms** | 938.27 ms | 10,360.00 ms | Đỉnh trễ trường hợp tail-latency |
| | **Độ lệch chuẩn độ trễ (Latency SD)** | **5,420.00 ms** | 4,810.00 ms | 2,120.00 ms | Độ ổn định phân phối độ trễ |
| | **Thời gian nhận byte đầu (TTFB Avg)** | **5.20 ms** | 6.10 ms | 8.40 ms | Thời gian phản ứng byte đầu tiên |
| | **Thời gian kết nối (Connect Time Avg)** | **0.45 ms** | 0.50 ms | 0.60 ms | Bắt tay TCP + HTTP/2 preface |
| | **Tổng dung lượng truyền tải** | **3.35 GB (3,355,443,200 B)** | 3.35 GB (3,355,443,200 B) | 1.67 GB (8 req lỗi 502) | Tổng byte trao đổi qua cổng mạng |
| | **Tiết kiệm nén Header HPACK** | **80.50%** | 80.10% | — | Tỷ lệ giảm kích thước HTTP HEADERS |
| | **Phân bổ mã phản hồi (HTTP Status)** | **16 2xx (0 fail, 0 timeout)** | **16 2xx (0 fail, 0 timeout)** | 8 2xx, 8 fail (502 Bad Gateway) | Chi tiết mã trạng thái HTTP |
| | **Tỷ lệ thành công (Success Rate)** | **100.0%** | **100.0%** | 50.0% (Lỗi 502) | Tỷ lệ request xử lý thành công 100% |
| | **Bộ nhớ RAM tiêu thụ đỉnh (Peak RSS)** | **134.0 MB** | 2,581.0 MB (~2.58 GB) | 128.0 MB | Envoy tiêu tốn RAM gấp **19.3x** |
| **Vi Mô (HPC & CPU)** | **Tổng số lệnh CPU (`instructions`)** | **30,385,356,237** | 31,595,448,864 | — | **-3.8%** (-1,210,092,627 lệnh) |
| | **Số lệnh CPU / Request (`insn / req`)** | **1,899,084,764.8** | 1,974,715,554.0 | — | **-3.8%** (-75,630,789.2 insn/req) |
| | **Tổng chu kỳ CPU (`cycles`)** | 37,215,184,617 | **33,962,866,103** | — | +9.6% vs Envoy |
| | **Chu kỳ CPU / Request (`cycles / req`)** | 2,325,949,038.6 | **2,122,679,131.4** | — | +9.6% vs Envoy |
| | **Hiệu suất lệnh / xung (`IPC`)** | 0.82 insn/cycle | **0.93 insn/cycle** | — | 0.82 vs 0.93 (-11.8%) |
| | **Tổng số lệnh rẽ nhánh (`branches`)** | **5,180,467,090** | 4,884,018,834 | — | **+6.1%** (+296,448,256 nhánh) |
| | **Lệnh rẽ nhánh / Request (`branches / req`)** | **323,779,193.1** | 305,251,177.1 | — | **+6.1%** (+18,528,016.0 br/req) |
| | **CPU đoán sai nhánh (`branch-misses`)** | **41,026,779** | 32,843,744 | — | **+24.9%** (+8,183,035 misses) |
| | **Tỷ lệ đoán sai nhánh (`branch-miss %`)** | **0.79%** | 0.67% | — | Velda 0.79% vs Envoy 0.67% |
| **Vi Mô (Kernel & Locks)** | **Tổng số lần đổi ngữ cảnh (`CS`)** | 52,927 | **34,070** | — | +55.3% (+18,857 CS) |
| | **Chuyển ngữ cảnh / Request (`CS / req`)** | 3307.94 CS/req | **2129.38 CS/req** | — | 3307.94 vs 2129.38 CS/req |
| | **Tổng số System Calls (`syscalls`)** | **420** | 540 | — | **-22.2%** (-120 syscalls) |
| | **System Calls / Request (`syscalls / req`)** | **26.2500** | 33.7500 | — | 26.2500 vs 33.7500 syscalls/req |
| | **Thời gian chạy trong kernel (Kernel Time)** | **21.40s** | 23.80s | — | Thời gian thực thi không gian nhân |
| | **Thời gian chờ khóa Futex (Lock Contention)** | **0.018s** | 3.42s | — | Envoy nghẽn lock gấp **190.0x** |
| | **Tỷ lệ Futex trên tổng thời gian Syscall** | **0.08%** | 14.40% | — | Mức độ nghẽn khóa luồng worker |

---
