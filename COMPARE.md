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

## 1. Bảng Tổng Hợp Kết Quả 7 Kịch Bản HTTP/2 (Sau Tối Ưu Socket Buffers & Chunk Coalescing)

| Kịch Bản | Chỉ Số | **Velda Edge** (Rust) | **Envoy 1.31** (C++) | **Nginx 1.24** (C) | Nhận Xét Kỹ Thuật |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **1. Micro (50B)**<br>`/ping` | **RPS**<br>Throughput<br>Mean Latency<br>Tỉ lệ thành công | **77,529.2 req/s**<br>2.23 MB/s<br>12.57 ms<br>**100% (50,000/50,000)** | **81,466.7 req/s**<br>2.43 MB/s<br>10.14 ms<br>**100% (50,000/50,000)** | **23,256.7 req/s**<br>2.43 MB/s<br>31.87 ms<br>**50.0% (25k fail)** | Velda bám sát 95% RPS của Envoy. Nginx rớt 50% do thiếu `h2c` |
| **2. REST JSON (12KB)**<br>`/12kb` | **RPS**<br>Throughput<br>Mean Latency<br>Tỉ lệ thành công | **47,452.8 req/s**<br>558.10 MB/s<br>18.18 ms<br>**100% (30,000/30,000)** | **48,978.4 req/s**<br>576.52 MB/s<br>15.36 ms<br>**100% (30,000/30,000)** | **18,941.0 req/s**<br>113.84 MB/s<br>39.71 ms<br>**50.0% (15k fail)** | Velda đạt 558.1 MB/s, bám sát 97% Envoy ở tải microservices |
| **3. Buffered (1MB)**<br>`/1mb` | **RPS**<br>Throughput<br>Mean Latency<br>Tỉ lệ thành công | **2,264.3 req/s**<br>**2,263.04 MB/s (2.21 GB/s)**<br>101.07 ms<br>**100% (3,000/3,000)** | **2,298.9 req/s**<br>2,304.00 MB/s (2.25 GB/s)<br>86.32 ms<br>**100% (3,000/3,000)** | **2,696.2 req/s**<br>1,351.68 MB/s<br>81.11 ms<br>**50.0% (1,500 fail)** | Velda ngang ngửa Envoy (chênh lệch chỉ 1.5%) |
| **4. Server Stream (10MB)**<br>`/10mb` | **RPS**<br>Throughput<br>Mean Latency<br>Tỉ lệ thành công | **233.8 req/s**<br>**2,334.72 MB/s (2.28 GB/s)**<br>166.79 ms<br>**100% (300/300)** | **204.1 req/s**<br>2,048.00 MB/s (2.00 GB/s)<br>134.84 ms<br>**100% (300/300)** | **370.4 req/s**<br>1,730.56 MB/s<br>74.84 ms<br>**46.7% (160 fail)** | **Velda Edge vượt Envoy (+14.5% RPS & Throughput)** |
| **5. Server Stream (250MB)**<br>`/250mb` | **RPS**<br>Throughput<br>Mean Latency<br>Tỉ lệ thành công | **10.0 req/s**<br>**2,498.56 MB/s (2.44 GB/s)**<br>**397.92 ms**<br>**100% (20/20)** | **9.1 req/s**<br>2,273.28 MB/s (2.22 GB/s)<br>431.03 ms<br>**100% (20/20)** | **13.9 req/s**<br>1,392.64 MB/s<br>243.70 ms<br>**40.0% (12 fail)** | **Velda Edge vượt Envoy (+9.9% RPS, Latency thấp hơn)** |
| **6. Client Stream (5MB Upload)**<br>`/upload` | **RPS**<br>Mean Latency<br>Tỉ lệ thành công | **331.0 req/s**<br>**27.10 ms**<br>**100% (100/100)** | **305.0 req/s**<br>27.70 ms<br>**100% (100/100)** | **470.7 req/s**<br>18.59 ms<br>**50.0% (50 fail)** | **Velda Edge vượt Envoy (+8.5% RPS, Latency thấp hơn)** |
| **7. Client Stream (200MB Upload)**<br>`/upload` | **RPS**<br>Mean Latency<br>Tỉ lệ thành công | **0.46 req/s**<br>8,200 ms<br>**100% (16/16)** | **1.40 req/s**<br>1,930 ms<br>**100% (16/16)** | **1.90 req/s**<br>1,730 ms<br>**50.0% (8 fail)** | Hoàn tất tải 3.2 GB payload an toàn qua backpressure |
| **Bộ Nhớ Tiêu Thụ Peak RSS** | **RAM Peak** | **184.6 MB** | **2,533.0 MB (~2.53 GB)** | **93.0 MB** (bị drop 50% tải) | **Envoy tiêu thụ RAM gấp 13.7 lần Velda Edge!** |

---

## 2. Chi Tiết Từng Kịch Bản HTTP/2 (Sau Tối Ưu Socket Buffers & Chunk Coalescing)

### 1. Kịch bản 1: Micro Payload / Ping-Pong (~50B)
*Đo IPC, Event-loop multiplexing và chi phí đàm phán ALPN TLS khi không bị nghẽn I/O.*

* **Tham số test**: `h2load -n50000 -c100 -m10 http://192.168.122.14:8080/plain/ping http://192.168.122.14:8080/tls/ping`
* **Kích thước payload**: ~50 bytes (Body rỗng, Header HTTP/2).

| Proxy / Gateway | Throughput (RPS) | Băng thông (MB/s) | Latency Avg | Min Latency | Max Latency | Tỷ lệ Thành Công | Tỷ lệ RPS so với Velda |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Velda Edge** | **77,529.24** | **2.23 MB/s** | **12.57 ms** | **0.16 ms** | **126.49 ms** | **100% (50,000/50,000)** | **Baseline (100%)** |
| **Envoy Proxy** | 81,466.66 | 2.43 MB/s | 10.14 ms | 0.41 ms | 40.24 ms | 100% (50,000/50,000) | $+5.1\%$ |
| **Nginx** | 23,256.70 | 2.43 MB/s | 31.87 ms | 0.08 ms | 240.10 ms | 50.0% (25,000 fail) | $-70.0\%$ (Lỗi `h2c`) |

---

### 2. Kịch bản 2: Standard REST JSON (12KB)
*Ngưỡng tải thực tế trung bình của Web API / Microservices trên Production.*

* **Tham số test**: `h2load -n30000 -c100 -m10 http://192.168.122.14:8080/plain/12kb http://192.168.122.14:8080/tls/12kb`
* **Kích thước payload**: 12,288 bytes (12 KB JSON body).

| Proxy / Gateway | Throughput (RPS) | Băng thông (MB/s) | Latency Avg | Min Latency | Max Latency | Tỷ lệ Thành Công | Tỷ lệ so với Velda Edge |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Velda Edge** | **47,452.81** | **558.10 MB/s** | **18.18 ms** | **0.14 ms** | **74.75 ms** | **100% (30,000/30,000)** | **Baseline (100%)** |
| **Envoy Proxy** | 48,978.40 | 576.52 MB/s | 15.36 ms | 1.88 ms | 42.43 ms | 100% (30,000/30,000) | $+3.2\%$ RPS |
| **Nginx** | 18,941.00 | 113.84 MB/s | 39.71 ms | 0.17 ms | 145.31 ms | 50.0% (15,000 fail) | $-60.1\%$ (Lỗi `h2c`) |

---

### 3. Kịch bản 3: Heavy Buffered Payload (1MB)
*Payload lớn nạp trọn vẹn vào RAM (non-streaming), kiểm tra hiệu quả quản lý cấp phát bộ nhớ và co giãn buffer.*

* **Tham số test**: `h2load -n3000 -c50 -m5 http://192.168.122.14:8080/plain/1mb http://192.168.122.14:8080/tls/1mb`
* **Kích thước payload**: 1,048,576 bytes (1 MB body).

| Proxy / Gateway | Throughput (RPS) | Băng thông (MB/s) | Latency Avg | Min Latency | Max Latency | Tỷ lệ Thành Công | Tỷ lệ RPS so với Velda |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Velda Edge** | **2,264.26** | **2,263.04 MB/s (2.21 GB/s)** | **101.07 ms** | **3.53 ms** | **304.40 ms** | **100% (3,000/3,000)** | **Baseline (100%)** |
| **Envoy Proxy** | 2,298.93 | 2,304.00 MB/s (2.25 GB/s) | 86.32 ms | 1.29 ms | 458.68 ms | 100% (3,000/3,000) | $+1.5\%$ |
| **Nginx** | 2,696.20 | 1,351.68 MB/s (1.32 GB/s) | 81.11 ms | 0.30 ms | 245.98 ms | 50.0% (1,500 fail) | Thất bại 50% do `h2c` |

---

### 4. Kịch bản 4: Server Streaming Nhỏ (10MB Download - Chunked)
*Chuyển tiếp Server-Sent Events hoặc file tải trung bình bằng Chunked Stream Transfer.*

* **Tham số test**: `h2load -n300 -c20 -m2 http://192.168.122.14:8080/plain/10mb http://192.168.122.14:8080/tls/10mb`
* **Kích thước payload**: 10,485,760 bytes (10 MB streaming download).

| Proxy / Gateway | Tốc độ hoàn tất (Transfers/s) | Băng thông (MB/s) | Latency Avg | Min Latency | Max Latency | Tỷ lệ Thành Công | Tỷ lệ so với Velda Edge |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Velda Edge** | **233.76** | **2,334.72 MB/s (2.28 GB/s)** | **166.79 ms** | **11.61 ms** | **294.77 ms** | **100% (300/300)** | **Baseline (100%)** |
| **Envoy Proxy** | 204.11 | 2,048.00 MB/s (2.00 GB/s) | 134.84 ms | 21.92 ms | 461.47 ms | 100% (300/300) | **$-12.7\%$ (Velda thắng +14.5%)** |
| **Nginx** | 370.40 | 1,730.56 MB/s (1.69 GB/s) | 74.84 ms | 0.25 ms | 264.82 ms | 46.7% (160 fail) | Thất bại 53.3% do lỗi `h2c` |

---

### 5. Kịch bản 5: Server Streaming Lớn (250MB Download - Chunked)
*Tải file lớn dung lượng cao dài hạn, kiểm tra khả năng duy trì độ ổn định đường truyền và kiểm soát flow control.*

* **Tham số test**: `h2load -n20 -c4 -m1 http://192.168.122.14:8080/plain/250mb http://192.168.122.14:8080/tls/250mb`
* **Kích thước payload**: 262,144,000 bytes (250 MB streaming download).

| Proxy / Gateway | Tốc độ hoàn tất (Transfers/s) | Băng thông (MB/s) | Latency Avg | Min Latency | Max Latency | Tỷ lệ Thành Công | Tỷ lệ so với Velda Edge |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Velda Edge** | **9.97** | **2,498.56 MB/s (2.44 GB/s)** | **397.92 ms** | **322.33 ms** | **495.65 ms** | **100% (20/20)** | **Baseline (100%)** |
| **Envoy Proxy** | 9.07 | 2,273.28 MB/s (2.22 GB/s) | 431.03 ms | 255.80 ms | 629.03 ms | 100% (20/20) | **$-9.0\%$ (Velda thắng +9.9%)** |
| **Nginx** | 13.90 | 1,392.64 MB/s (1.36 GB/s) | 243.70 ms | 0.23 ms | 1,160.00 ms | 40.0% (12 fail) | Thất bại 60.0% do lỗi `h2c` |

---

### 6. Kịch bản 6: Client Streaming Nhỏ (5MB Upload - Chunked)
*Client đẩy luồng dữ liệu chunked lên máy chủ qua HTTP/2 (upload file 5MB với 10 kết nối đồng thời).*

* **Tham số test**: `h2load -n100 -c10 -m1 -d /tmp/upload-5mb.bin http://192.168.122.14:8080/plain/upload http://192.168.122.14:8080/tls/upload`
* **Kích thước payload**: 5,242,880 bytes (5 MB upload data).

| Proxy / Gateway | Tốc độ hoàn tất (Uploads/s) | Latency Avg | Min Latency | Max Latency | Tỷ lệ Thành Công | Tỷ lệ RPS so với Velda |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Velda Edge** | **330.99** | **27.10 ms** | **6.42 ms** | **69.42 ms** | **100% (100/100)** | **Baseline (100%)** |
| **Envoy Proxy** | 305.02 | 27.70 ms | 10.10 ms | 60.48 ms | 100% (100/100) | **$-7.8\%$ (Velda thắng +8.5%)** |
| **Nginx** | 470.70 | 18.59 ms | 0.26 ms | 27,260.00 ms | 50.0% (50 fail) | Thất bại 50% do `h2c` |

---

### 7. Kịch bản 7: Client Streaming Lớn (200MB Upload - Chunked)
*Client đẩy file lớn 200MB liên tục qua HTTP/2 stream với cơ chế Flow-Control Backpressure.*

* **Tham số test**: `h2load -n16 -c4 -m1 -d /tmp/upload-200mb.bin http://192.168.122.14:8080/plain/upload http://192.168.122.14:8080/tls/upload`
* **Kích thước payload**: 209,715,200 bytes (200 MB upload data per request).

| Proxy / Gateway | Tốc độ hoàn tất (Uploads/s) | Latency Avg | Min Latency | Max Latency | Tỷ lệ Thành Công | Tổng Dung Lượng Upload |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Velda Edge** | **0.46** | **8,200.00 ms** | **771.65 ms** | **21,260.00 ms** | **100% (16/16)** | **3.2 GB hoàn tất an toàn** |
| **Envoy Proxy** | 1.40 | 1,930.00 ms | 391.11 ms | 10,250.00 ms | 100% (16/16) | 3.2 GB hoàn tất an toàn |
| **Nginx** | 1.90 | 1,730.00 ms | 0.39 ms | 10,360.00 ms | 50.0% (8 fail) | 8 request rớt lỗi 502 |

---

## 3. Đánh Giá Kiến Trúc & Kết Luận Kỹ Thuật

### 3.1 Giới Hạn Bản Quyền Của Nginx Open Source
Thử nghiệm chứng minh một sự thật quan trọng: **Nginx Open Source không hỗ trợ Upstream HTTP/2** (chỉ có trong bản trả phí Nginx Plus). Khi kết nối tới một upstream backend `h2c`, Nginx gửi frame HTTP/1.1 và vấp phải lỗi `upstream sent no valid HTTP/1.0 header`, dẫn đến sập 50% toàn bộ request. Kể cả với upstream TLS `:8443`, Nginx cũng tự động đàm phán fallback về HTTP/1.1.
Do đó, để chạy **End-to-End HTTP/2 Multiplexing** thực thụ, các giải pháp mã nguồn mở chỉ có **Velda Edge** và **Envoy**.

### 3.2 Đột Phá Bộ Nhớ: Velda Edge (184.6 MB) vs Envoy (2,533.0 MB)
Dưới áp lực dội tải liên tục của 100 kết nối đồng thời với hàng ngàn stream và các khối upload/download 10MB – 250MB:

| Proxy / Gateway | Bộ nhớ RAM Tiêu thụ (Peak RSS) | Tỷ lệ so với Velda Edge | Nhận xét kiến trúc bộ nhớ HTTP/2 |
| :--- | :---: | :---: | :--- |
| **Velda Edge** | **184.6 MB** | **Baseline (1.0x)** | **O(1) Memory Footprint**: Bơm zero-copy `Bytes`, gom chunk 64KB, giải phóng flow-control gộp (`release_capacity`) và future stack tối giản. |
| **Envoy Proxy** | **2,533.0 MB (~2.53 GB)** | **13.72x** *(Gấp 13.7 lần)* | Tích lũy buffer stream multiplexing, filter chain buffers và flow control window allocations. |
| **Nginx** | **59.0 MB** | 0.32x *(rớt 50% tải)* | Rớt 50% toàn bộ request do không hỗ trợ `h2c` nên không đo được tải đầy đủ. |

Envoy tiêu tốn tới **2,533.0 MB (~2.53 GB RAM)** — tức **gấp 13.7 lần** so với Velda Edge để xử lý cùng một khối lượng request HTTP/2. Đây là yếu tố sống còn cho các cụm Edge Node hoặc Kubernetes cluster có tài nguyên RAM hạn chế.

