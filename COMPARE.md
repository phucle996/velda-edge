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
| **Velda Edge** | **v0.1.0 (Rust 2024)** | **115,473.25** | **14.98 MB/s** | **1.72 ms** | **1.60 ms** | **2.70 ms** | **3.96 ms** | **Baseline (100%)** |
| **Nginx** | 1.30.5 (C) | 146,264.75 | 22.32 MB/s | 1.44 ms | 1.23 ms | 2.73 ms | 4.77 ms | $+26.7\%$ |
| **Envoy Proxy** | 1.31.10 (C++) | 54,412.83 | 7.99 MB/s | 3.65 ms | 3.40 ms | 5.46 ms | 8.03 ms | $-52.9\%$ |

---

## 2. Kịch bản 2: Standard REST Payload (12KB)
*Ngưỡng tải thực tế trung bình của Web API / Microservices trên Production.*

* **Tham số test**: `wrk -t6 -c200 -d10s --latency http://192.168.122.14:8080/12kb`
* **Kích thước payload**: 12,288 bytes (12 KB Content-Length body).

| Proxy / Gateway | Phiên bản | Throughput (RPS) | Băng thông (MB/s) | Latency Avg | Latency P50 | Latency P90 | Latency P99 | Tỷ lệ so với Velda Edge |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Velda Edge** | **v0.1.0 (Rust 2024)** | **80,349.88** | **962.56 MB/s (0.94 GB/s)** | **2.47 ms** | **2.30 ms** | **3.58 ms** | **5.38 ms** | **Baseline (100%)** |
| **Nginx** | 1.30.5 (C) | 66,778.38 | 798.85 MB/s | 3.00 ms | 2.82 ms | 4.47 ms | 7.53 ms | $-16.9\%$ RPS ($+40.0\%$ P99) |
| **Envoy Proxy** | 1.31.10 (C++) | 41,846.02 | 500.32 MB/s | 4.76 ms | 4.57 ms | 7.38 ms | 10.70 ms | $-47.9\%$ RPS ($+98.9\%$ P99) |

---

## 3. Kịch bản 3: Heavy Buffered Payload (1MB)
*Payload lớn nạp trọn vẹn vào RAM (non-streaming), kiểm tra hiệu quả quản lý cấp phát bộ nhớ và co giãn buffer.*

* **Tham số test**: `wrk -t6 -c60 -d10s --latency http://192.168.122.14:8080/1mb`
* **Kích thước payload**: 1,048,576 bytes (1 MB Content-Length body).

| Proxy / Gateway | Throughput (RPS) | Băng thông (MB/s) | Latency Avg | Latency P50 | Latency P90 | Latency P99 | Tỷ lệ RPS so với Velda |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Velda Edge** | **2,277.71** | **2,283.52 MB/s (2.23 GB/s)** | **26.16 ms** | **28.44 ms** | **33.53 ms** | **37.74 ms** | **Baseline (100%)** |
| **Nginx** | 3,343.67 | 3,348.48 MB/s (3.27 GB/s) | 17.89 ms | 15.30 ms | 28.85 ms | 34.37 ms | $+46.8\%$ |
| **Envoy Proxy** | 3,039.89 | 3,041.28 MB/s (2.97 GB/s) | 19.85 ms | 18.36 ms | 37.57 ms | 43.00 ms | $+33.5\%$ |

---

## 4. Kịch bản 4: Server Streaming Nhỏ (10MB Download - Chunked)
*Chuyển tiếp Server-Sent Events hoặc file tải trung bình bằng Chunked Transfer Encoding.*

* **Tham số test**: `wrk -t4 -c20 -d10s --latency http://192.168.122.14:8080/10mb`
* **Kích thước payload**: 10,485,760 bytes (10 MB streaming download).

| Proxy / Gateway | Tốc độ hoàn tất (Transfers/s) | Băng thông (MB/s) | Latency Avg | Latency P50 | Latency P90 | Latency P99 | Tỷ lệ so với Velda Edge |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Velda Edge** | **336.44** | **3,368.96 MB/s (3.29 GB/s)** | **59.16 ms** | **60.11 ms** | **64.90 ms** | **76.09 ms** | **Baseline (100%)** |
| **Nginx** | 367.04 | 3,676.16 MB/s (3.59 GB/s) | 54.37 ms | 52.59 ms | 68.66 ms | 87.93 ms | $+9.1\%$ Transfers ($+15.6\%$ P99) |
| **Envoy Proxy** | 328.93 | 3,297.28 MB/s (3.22 GB/s) | 60.32 ms | 60.69 ms | 74.73 ms | 106.80 ms | $-2.2\%$ Transfers ($+40.4\%$ P99) |

---

## 5. Kịch bản 5: Server Streaming Lớn (250MB Download - Chunked)
*Tải file lớn dung lượng cao dài hạn, kiểm tra khả năng duy trì độ ổn định đường truyền và P99 tail latency.*

* **Tham số test**: `wrk -t4 -c10 -d12s --latency http://192.168.122.14:8080/250mb`
* **Kích thước payload**: 262,144,000 bytes (250 MB streaming download).

| Proxy / Gateway | Tốc độ hoàn tất (Transfers/s) | Băng thông (MB/s) | Latency Avg | Latency P50 | Latency P90 | Latency P99 | Lỗi Socket / Timeout |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Velda Edge** | **14.22** | **3,614.72 MB/s (3.53 GB/s)** | **551.31 ms** | **549.75 ms** | **574.43 ms** | **600.13 ms** | **0 lỗi (0.0%)** |
| **Nginx** | 10.00 | 2,570.24 MB/s (2.51 GB/s) | 704.08 ms | 652.41 ms | 1,160.00 ms | 1,900.00 ms (1.90 s) | 6 timeouts |
| **Envoy Proxy** | 13.31 | 3,409.92 MB/s (3.33 GB/s) | 586.50 ms | 580.14 ms | 629.73 ms | 851.75 ms | 0 lỗi |

---

## 6. Kịch bản 6: Client Streaming Nhỏ (5MB Upload - Chunked)
*Client đẩy luồng dữ liệu chunked lên máy chủ (upload file 5MB với 20 kết nối đồng thời).*

| Proxy / Gateway | Tốc độ hoàn tất (Uploads/s) | Băng thông (MB/s) | Latency Avg | Latency P50 | Latency P90 | Latency P99 | Tổng số lượt upload thành công |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Velda Edge** | **563.21** | **2,816.07 MB/s (2.75 GB/s)** | **34.25 ms** | **34.14 ms** | **38.80 ms** | **43.57 ms** | **5,646** |
| **Nginx** | 892.44 | 4,462.19 MB/s (4.36 GB/s) | 19.89 ms | 19.57 ms | 24.12 ms | 28.62 ms | 8,945 |
| **Envoy Proxy** | 66.12 | 330.60 MB/s (0.32 GB/s) | 220.04 ms | 82.91 ms | 480.10 ms | 643.95 ms | 716 |

---

## 7. Kịch bản 7: Client Streaming Lớn (200MB Upload - Chunked)
*Client đẩy file lớn 200MB liên tục qua stream (5 kết nối đồng thời).*

| Proxy / Gateway | Tốc độ hoàn tất (Uploads/s) | Băng thông (MB/s) | Latency Avg | Latency P50 | Latency P90 | Latency P99 | Tỷ lệ so với Velda Edge |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Velda Edge** | **16.37** | **3,273.49 MB/s (3.20 GB/s)** | **304.38 ms** | **303.25 ms** | **340.10 ms** | **374.81 ms** | **Baseline (100%)** |
| **Nginx** | 27.58 | 5,515.64 MB/s (5.39 GB/s) | 178.73 ms | 178.27 ms | 188.50 ms | 194.23 ms | $+68.5\%$ |
| **Envoy Proxy** | 0.00 | 0.00 MB/s | N/A | N/A | N/A | N/A | Thất bại (Buffer Overflow) |

---

## 8. Mức Tiêu Thụ Bộ Nhớ RAM Ghi Nhận Thực Tế (Process Peak Memory)

Dung lượng bộ nhớ tiến trình (Resident Set Size - RSS) được đo đạc trực tiếp trên VM sau toàn bộ chuỗi tải:

| Proxy / Gateway | Bộ nhớ RAM Tiêu thụ (Peak RSS) | Nhận xét kiến trúc bộ nhớ |
| :--- | :---: | :--- |
| **Velda Edge** | **18.1 MB** | **O(1) Memory Footprint**: Bộ đệm zero-copy `split_to` tái sử dụng, co giãn tự động qua `compact_buffers`. |
| **Nginx** | **1,017.0 MB (~1.0 GB)** | Buffer pool cấp phát cho worker processes và kết nối socket. |
| **Envoy Proxy** | **5,814.0 MB (~5.8 GB)** | Tích lũy heap buffer trong filter chain và metadata instances. |
