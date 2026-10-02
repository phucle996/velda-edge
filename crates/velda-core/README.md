# velda-core

`velda-core` là crate nền tảng (*foundation vocabulary*) của toàn bộ Data Plane Velda Edge. Crate này chỉ chứa các **data models**, **strongly typed IDs**, **lifecycle contracts**, và **error taxonomy** dùng chung giữa các crate khác.

> **Quy tắc cốt lõi**: `velda-core` **không xử lý request**; `velda-core` chỉ **định nghĩa request và luật giao tiếp** giữa các thành phần xử lý request.

---

## 1. Vị trí & Ranh giới trong hệ thống

`velda-core` nằm ở đáy của đồ thị phụ thuộc (`dependency graph`), không phụ thuộc vào bất kỳ crate nội bộ nào khác trong Velda:

```text
       velda-transport   velda-router   velda-upstream   velda-plugin
              │                │               │               │
              └────────────────┴───────┬───────┴───────────────┘
                                       │ (dùng chung contracts)
                                       ▼
                                  velda-core
```

Trong luồng xử lý request runtime (*request pipeline*), `velda-core` **không phải là một bước xử lý (phase)**. Nó là tập hợp các kiểu dữ liệu (`types`) được truyền qua lại giữa các bước:

```text
L4: Client ──► velda-transport ──► Router ──► Upstream
L7: Client ──► velda-transport ──► TLS ──► HTTP ──► Router ──► Plugin ──► Upstream
```

---

## 2. Các thành phần chính do `velda-core` sở hữu

### 2.1 Strongly Typed Identifiers (`src/types.rs`)
Bọc các số nguyên nguyên thủy (`u64`, `u32`) thành các kiểu dữ liệu có định danh rõ ràng để chống nhầm lẫn domain tại compile-time:

- `RequestId(pub u64)`: Định danh duy nhất cho một L7 request. Kích thước đúng 8 bytes, 0 overhead.
- `RouteId(pub u32)`: Định danh cho một quy tắc định tuyến đã biên dịch. Kích thước đúng 4 bytes.
- `UpstreamId(pub u32)`: Định danh cho một cụm backend upstream đã chọn. Kích thước đúng 4 bytes.
- `ConnectionId(pub u64)`: Định danh cho một kết nối mạng vật lý L4. Kích thước đúng 8 bytes.

Tất cả ID đều implement: `Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`, `Hash`, `Display`.

---

### 2.2 Model mạng Layer 4 (`src/l4/`)
Biểu diễn thông tin tầng Transport (TCP/UDP), hoàn toàn độc lập với HTTP:

- `L4Request`: Chứa `connection_id`, `client_addr`, `local_addr`, `protocol` (`Tcp` hoặc `Udp`), `peer`.
- `L4Response`: Chứa hành động L4 (`L4Action::Forward`, `L4Action::Close`, `L4Action::Reject`) và địa chỉ đích `target: Option<SocketAddr>`.

---

### 2.3 Model ứng dụng Layer 7 (`src/l7/`)
Biểu diễn ngữ nghĩa HTTP dùng cho routing, plugins và upstream:

- `L7Request`: Chứa `Method`, `Uri`, `Version`, `HeaderMap`, và `Body`.
- `L7Response`: Chứa `StatusCode`, `Version`, `HeaderMap`, và `Body`.
- `Body`: Enum biểu diễn payload dữ liệu (`Body::Empty`, `Body::Bytes(Bytes)`).

---

### 2.4 Context & State chung (`src/context.rs`)
Phân tách ranh giới rõ ràng giữa vòng đời kết nối mạng và vòng đời request:

- **`ConnectionContext`**: Đại diện cho 1 kết nối vật lý (mang `L4Request`). Một kết nối có thể tồn tại lâu và phục vụ nhiều request liên tiếp (HTTP keep-alive) hoặc đồng thời (HTTP/2 multiplexing).
- **`RequestContext`**: Đại diện cho 1 chu kỳ xử lý request L7. Bao gồm:
  - `l4`: Tham chiếu tới `ConnectionContext`.
  - `l7`: Đối tượng `L7Request`.
  - `state`: Trạng thái xử lý nội bộ `RequestState`.
- **`RequestState`**: Struct chứa state có kiểu dữ liệu tường minh:
  ```rust
  pub struct RequestState {
      pub request_id: RequestId,
      pub route: Option<RouteId>,
      pub upstream: Option<UpstreamId>,
      pub routed: bool,
      pub upstream_started: bool,
      pub upstream_completed: bool,
  }
  ```
  *(Kích thước `RequestState` là 32 bytes, nằm gọn trong 1 CPU Cacheline 64 bytes).*

---

### 2.5 Hợp đồng Vòng đời & Hook (`src/lifecycle.rs`)
Định nghĩa các pha trong vòng đời và quyền hạn của các plugin hook:

- **`Phase`**: Enum 7 pha tuần tự của request (kích thước 1 byte):
  `Accept`, `Decode`, `PreRoute`, `Route`, `PreUpstream`, `Upstream`, `PostResponse`.
- **`HookPhase`**: Enum các pha mở rộng cho plugin (kích thước 1 byte):
  `PreRoute`, `PreUpstream`, `PostResponse`.
- **`Action`**: Quyền hạn hữu hạn mà một hook được phép trả về:
  ```rust
  pub enum Action {
      Continue,              // Tiếp tục luồng xử lý bình thường
      Respond(L7Response),   // Trả về response sớm (401, 403, 429...) và ngắt pipeline
      Reject(Error),         // Dừng request với lỗi cụ thể
  }
  ```
- **`Hook` trait**: Contract cho plugin hook (`Send + Sync` thread-safe).

---

### 2.6 Hợp đồng Lỗi chung (`src/error.rs`)
Cung cấp ngôn ngữ báo lỗi thống nhất (`ErrorKind`, kích thước 1 byte) để các crate giao tiếp với nhau mà không phụ thuộc vào cách ánh xạ mã HTTP status:

```rust
pub enum ErrorKind {
    InvalidRequest,       // Request sai định dạng
    Protocol,             // Vi phạm giao thức (HTTP/TLS framing)
    RouteNotFound,        // Không tìm thấy route phù hợp
    RouteConfig,          // Cấu hình route lỗi
    UpstreamUnavailable,  // Backend chết hoặc pool cạn kiệt
    UpstreamFailure,      // Lỗi kết nối tới backend
    Timeout,              // Hết thời gian chờ (connect / read timeout)
    Connection,           // Lỗi socket I/O vật lý
    Rejected,             // Bị plugin từ chối (WAF, Rate limit)
    Canceled,             // Client ngắt kết nối giữa chừng
    Internal,             // Lỗi nội bộ không xác định
}
```

---

### 2.7 Hardware Topology: Phân tách Probe CPU & Memory (`src/hardware/`)
Cung cấp khả năng nhận diện tài nguyên hệ thống thực tế (CPU cores, RAM) một lần duy nhất lúc cold start từ kernel/cgroups và lưu vào RAM (`OnceLock`), không đọc từ biến môi trường:
- `src/hardware/cpu.rs`:
  - `probe_cpu()`: Đọc cgroups v2 (`/sys/fs/cgroup/cpu.max`), cgroups v1 (`cpu.cfs_quota_us`), hoặc `available_parallelism` từ hệ thống.
  - **`CpuTier`**: `Constrained` (1-2 cores), `Small` (3-4 cores), `Medium` (5-8 cores), `Large` (9-16 cores), `XLarge` (17-32 cores), `TwoXLarge` (33-64 cores), `Ultra` (> 64 cores).
  - Dùng để tự động tính toán concurrency scaling và channel capacities theo năng lực phần cứng thực tế.
- `src/hardware/memory.rs`:
  - `probe_memory()`: Đọc cgroups v2 (`memory.max`), cgroups v1 (`memory.limit_in_bytes`), hoặc `/proc/meminfo` MemTotal.
  - **`MemoryTier`**: `Constrained` (< 512 MB), `Small` (512 MB – 2 GB), `Medium` (2 GB – 8 GB), `Large` (8 GB – 32 GB), `XLarge` (32 GB – 64 GB), `TwoXLarge` (64 GB – 128 GB), `Ultra` (> 128 GB).
  - Dùng để scale socket buffers (TCP/UDP), socket backlog, và DNS/LKG cache capacities.
- `src/hardware/mod.rs`:
  - `HardwareTopology`: Tổng hợp `CpuProfile` và `MemoryProfile`.
  - Hỗ trợ đầy đủ backward-compatibility (`available_cores`, `worker_threads`, `memory_bytes`, `resource_tier()`, alias `ResourceTier = MemoryTier`).

---

## 3. Ranh giới: Những việc `velda-core` TUYỆT ĐỐI KHÔNG làm

Để giữ cho `velda-core` siêu nhẹ, ổn định và không chứa business logic, crate này **KHÔNG BAO GIỜ**:

- ❌ Mở socket TCP/UDP hoặc thực hiện Network I/O *(thuộc `velda-transport`)*.
- ❌ Thực hiện TLS handshake hoặc quản lý chứng chỉ *(thuộc `velda-tls`)*.
- ❌ Parse HTTP wire format *(thuộc `velda-http`)*.
- ❌ Thực thi thuật toán so khớp route *(thuộc `velda-router`)*.
- ❌ Thực thi phân giải DNS hoặc Load Balancing *(thuộc `velda-lb` và `velda-upstream`)*.
- ❌ Quản lý kết nối connection pool *(thuộc `velda-connection-pool`)*.
- ❌ Quản lý danh sách và thứ tự chạy plugin *(thuộc `velda-plugin`)*.
- ❌ Quản lý Tokio runtime hoặc background tasks *(thuộc `velda-edge`)*.
- ❌ Đọc/Parse file cấu hình JSON *(thuộc `velda-sync`)*.

---

## 4. Kiểm định Layout, Zero-Allocation & Dirty Inputs

Crate đi kèm 2 bộ test suite kiểm tra nghiêm ngặt:

1. **`tests/layout_and_alloc.rs`**:
   - Sử dụng `CountingAllocator` để xác nhận **0 heap allocation** trên các thao tác hot-path: tạo và tra cứu ID, chuyển đổi `RequestState`, đọc/ghi context lookup, và so sánh `Action::Continue`.
   - Kiểm tra `size_of` đảm bảo `RequestState <= 64 bytes` (1 L1 Cacheline) và các enum không có payload chỉ tốn đúng 1 byte.
2. **`tests/dirty_inputs.rs`**:
   - Kiểm tra các giới hạn số nguyên (`0`, `u64::MAX`, `u32::MAX`).
   - Kiểm tra các địa chỉ IP/Socket bẩn và đặc biệt (`0.0.0.0`, `255.255.255.255`, multicast, link-local, IPv6).
   - Kiểm tra các chuỗi URI bẩn, traversal, percent-encoding và payload body lớn (1MB).
   - Kiểm tra các chuỗi chuyển đổi trạng thái xung đột hoặc out-of-order.
   - Kiểm tra message lỗi cực lớn (64KB string) và lỗi lồng nhau (`with_source`).

---

## 5. Tiêu chuẩn kiểm thử & Xác thực

```bash
cargo fmt --check -p velda-core
cargo check -p velda-core
cargo test -p velda-core
cargo clippy -p velda-core --all-targets --all-features -- -D warnings
```
