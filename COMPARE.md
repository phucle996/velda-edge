# Performance Benchmarks: Velda Edge vs Nginx vs Envoy

Tài liệu cung cấp số liệu đối sánh hiệu năng thực nghiệm trọn bộ cho **HTTP/1.1** giữa **Velda Edge**, **Nginx (1.30.5)** và **Envoy (1.31.10)** qua 7 kịch bản từ tải vi mô, tải nặng có đệm (buffered) đến streaming dung lượng lớn (server stream và client stream).

> [!IMPORTANT]
> **Môi trường Thử nghiệm Thực tế (Empirical Test Environment)**:
> - Toàn bộ số liệu dưới đây được đo thực nghiệm trực tiếp trên **Máy ảo KVM (Virtual Machine)** chạy Ubuntu 24.04 LTS với cấu hình **6 vCPUs (pinned)** và **8 GB RAM**.
> - Bộ sinh tải chạy trên máy **Host vật lý (12 CPU cores)** dội tải trực tiếp qua KVM virtio virtual network bridge vào máy ảo qua công cụ `wrk` và HTTP/1.1 chunked load generator.
> - Upstream backend: Instance Nginx độc lập trên cổng `8081` trong cùng máy ảo, cấu hình HTTP keep-alive.

---

## 1. Kịch bản 1: Micro Payload / Ping-Pong (~50B)
*Đo IPC, Event-loop và Scheduling Overhead khi không bị nghẽn I/O.*

* **Tham số test**: `wrk -t6 -c200 -d10s --latency http://192.168.122.14:8080/ping`
* **Kích thước payload**: ~50 bytes (Header thuần túy, body rỗng).

| Proxy / Gateway | Phiên bản | Throughput (RPS) | Băng thông (MB/s) | Latency Avg | Latency P50 | Latency P90 | Latency P99 | Tỷ lệ RPS so với Velda |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Velda Edge** | **v0.1.0 (Rust 2024)** | **118,575.50** | **15.38 MB/s** | **1.68 ms** | **1.56 ms** | **2.62 ms** | **3.94 ms** | **Baseline (100%)** |
| **Nginx** | 1.30.5 (C) | 139,540.70 | 21.29 MB/s | 1.49 ms | 1.30 ms | 2.73 ms | 4.64 ms | $+17.7\%$ |
| **Envoy Proxy** | 1.31.10 (C++) | 35,404.77 | 5.20 MB/s | 5.61 ms | 5.27 ms | 8.25 ms | 12.10 ms | $-70.1\%$ |

---

## 2. Kịch bản 2: Standard REST Payload (12KB)
*Ngưỡng tải thực tế trung bình của Web API / Microservices trên Production.*

* **Tham số test**: `wrk -t6 -c200 -d10s --latency http://192.168.122.14:8080/12kb`
* **Kích thước payload**: 12,288 bytes (12 KB Content-Length body).

| Proxy / Gateway | Phiên bản | Throughput (RPS) | Băng thông (MB/s) | Latency Avg | Latency P50 | Latency P90 | Latency P99 | Tỷ lệ so với Velda Edge |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Velda Edge** | **v0.1.0 (Rust 2024)** | **81,722.80** | **972.80 MB/s (0.95 GB/s)** | **2.43 ms** | **2.28 ms** | **3.46 ms** | **5.09 ms** | **Baseline (100%)** |
| **Nginx** | 1.30.5 (C) | 63,371.19 | 758.07 MB/s | 3.14 ms | 2.94 ms | 4.71 ms | 7.76 ms | $-22.5\%$ RPS ($+52.5\%$ P99) |
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

## 8. Mức Tiêu Thụ Bộ Nhớ RAM Ghi Nhận Thực Tế (Process Peak Memory)

Dung lượng bộ nhớ tiến trình (Resident Set Size - RSS) được đo đạc trực tiếp trên VM sau toàn bộ chuỗi tải:

| Proxy / Gateway | Bộ nhớ RAM Tiêu thụ (Peak RSS) | Nhận xét kiến trúc bộ nhớ |
| :--- | :---: | :--- |
| **Velda Edge** | **17.5 MB** | **O(1) Memory Footprint**: Bộ đệm zero-copy `decode_chunk` tái sử dụng, co giãn tự động qua `compact_buffers`. |
| **Nginx** | **1,031.0 MB (~1.0 GB)** | Buffer pool cấp phát cho worker processes và kết nối socket. |
| **Envoy Proxy** | **5,819.0 MB (~5.8 GB)** | Tích lũy heap buffer trong filter chain và metadata instances. |
