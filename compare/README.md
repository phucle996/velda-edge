# Trung Tâm Đối Sánh Hiệu Năng: Velda Edge Benchmark Suite

Thư mục này chứa tài liệu và số liệu đo kiểm thực nghiệm hiệu năng giữa **Velda Edge**, **Nginx 1.24 (C)** và **Envoy Proxy 1.31 (C++)** trên các giao thức mạng L7.

---

## 1. Cấu Trúc Tài Liệu

- **[HTTP/1.1 Benchmark Suite (44 Kịch Bản)](../compare/http1.md)**:
  - **12 Pipeline Strategies $\times$ 3 Dải Payload** (Micro ~64B, Medium 12K-64K, Large 1M-10M) = 36 kịch bản.
  - **Dual Upstream**: Kiểm thử độc lập trên cả 2 nhánh **Cleartext (No-SSL)** và **TLS 1.3 ALPN (SSL)**.
  - **8 Kịch bản Dị dạng & Thảm họa**: HTTP Request Smuggling, Corrupted Chunk, Header Flooding, Slowloris, Chaos Upstream RST, Cgroup 60MB RAM OOM Containment, CPU 100% Adaptive Shedding, Atomic Hot-Reload Contention.
- **[HTTP/2 Benchmark Suite (44 Kịch Bản)](../compare/http2.md)**:
  - **12 Pipeline Strategies $\times$ 3 Dải Payload** (Micro ~64B, Medium 12K-64K, Large 1M-10M) = 36 kịch bản.
  - **8 Kịch bản Dị dạng & Thảm họa RFC 9113**: Rapid Reset Flood, CONTINUATION Flood, HPACK Bomb, Zero-Window Deadlock, Chaos RST, Cgroup 60MB RAM OOM, CPU 100% CoDel Shedding, Atomic Hot-Reload Contention.
- **[HTTP/3 Benchmark Suite (44 Kịch Bản)](../compare/http3.md)**:
  - **12 Pipeline Strategies $\times$ 3 Dải Payload** (Micro ~64B, Medium 12K-64K, Large 1M-10M) = 36 kịch bản.
  - **8 Kịch bản Dị dạng & Thảm họa RFC 9000 / RFC 9114**: QUIC Initial Flood & Amplification Attack, QPACK Dynamic Table Bomb, CID Churn & Migration Storm, Flow-Control Deadlock, Chaos Upstream Sudden Death, Cgroup 60MB RAM OOM, CPU 100% CoDel Shedding, Atomic Hot-Reload Contention.

---

## 2. Môi Trường Thử Nghiệm Chuẩn KVM VM

```
Host vật lý (12 CPU Cores, 32 GB RAM, Ubuntu 24.04 LTS)
  │
  │─── Virtio Network Bridge (MTU 1500) ───┐
  │                                        ▼
  │                           KVM Guest VM (Ubuntu 24.04 LTS)
  │                           • 6 vCPUs (pinned 1-1 vật lý)
  │                           • 4 GB RAM (cgroup v2)
  │                           • Gateway: Velda / Nginx / Envoy (:8080)
  │                           • Upstream 1: Cleartext (:8081)
  │                           • Upstream 2: TLS 1.3 ALPN (:8443)
```

---

## 3. Cách Thức Chạy Kiểm Thử Thực Nghiệm

### HTTP/1.1 Standard & Streaming
```bash
# Micro ping-pong
wrk -t6 -c200 -d10s --latency http://192.168.122.14:8080/buffered/ping

# REST Medium JSON
wrk -t6 -c200 -d10s --latency http://192.168.122.14:8080/buffered/12kb

# Heavy Chunked Download 10MB
wrk -t4 -c20 -d10s --latency http://192.168.122.14:8080/stream/10mb
```

### HTTP/2 Dual-Upstream
```bash
h2load -n50000 -c100 -m10 \
  http://192.168.122.14:8080/plain/ping \
  http://192.168.122.14:8080/tls/ping
```

### Thu Thập Chỉ Số Phần Cứng Vi Mô (`perf stat`)
```bash
perf stat -e instructions,cycles,branches,branch-misses,L1-dcache-load-misses,context-switches,cpu-migrations \
  -p $(pgrep -f velda-edge) -- sleep 10
```
