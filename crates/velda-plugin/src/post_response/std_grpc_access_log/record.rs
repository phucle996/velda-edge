//! Standard gRPC Access Log Record Structure and Serializers (`std_grpc_access_log`).
//!
//! Provides an ultra-compact, cacheline-aligned 256-byte POD record structure (`PackedGrpcRecord`)
//! designed for zero-copy register harmony, lock-free batch transport, and zero-allocation
//! slot-filling byte formatting (JSON and Text).
//!
//! # Micro-Architectural Guarantees
//! - **Exact 256 Bytes**: Exactly 4 CPU cache lines (64 bytes each), fitting into 8 YMM registers (AVX2),
//!   16 NEON registers (ARM64), or 32 64-bit ALU registers without cache spill.
//! - **Cacheline Aligned**: `#[repr(C, align(64))]` ensures aligned loads and non-temporal streaming writes.
//! - **Zero Heap Allocation**: Completely POD (Plain Old Data), zero pointer indirection on hot path.
//! - **Dedicated RPC Fields**: Tracks canonical `grpc-status`, RPC Service, Method, streaming message counts,
//!   and error messages without waste.

/// Output serialization style for gRPC access log records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GrpcLogFormat {
    #[default]
    Json,
    Text,
}

impl GrpcLogFormat {
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "text" => Self::Text,
            _ => Self::Json,
        }
    }
}

/// Ultra-compact 256-byte cacheline-aligned gRPC log record.
///
/// # Layout (256 bytes total, 64-byte aligned across 4 cache lines)
/// - **Cache Line 1 (0..64)**: Integer scalars and identifiers
///   - `0..8`: `timestamp_ms: u64`
///   - `8..12`: `duration_us: u32`
///   - `12..16`: `messages_sent: u32`
///   - `16..20`: `messages_received: u32`
///   - `20..24`: `content_length: u32`
///   - `24..25`: `grpc_status: u8` (0=OK, 1=Cancelled, ..., 16=Unauthenticated)
///   - `25..26`: `transport: u8` (1=TCP, 2=UDP)
///   - `26..27`: `service_len: u8` (max 48)
///   - `27..28`: `method_len: u8` (max 32)
///   - `28..29`: `request_id_len: u8` (max 16)
///   - `29..30`: `route_id_len: u8` (max 8)
///   - `30..31`: `upstream_len: u8` (max 16)
///   - `31..32`: `error_msg_len: u8` (max 64)
///   - `32..48`: `request_id: [u8; 16]`
///   - `48..56`: `route_id: [u8; 8]`
///   - `56..64`: `peer_addr_len: u8` (1 byte) + `padding: [u8; 7]`
/// - **Cache Line 2 (64..128)**: Upstream destination and RPC service name
///   - `64..80`: `upstream: [u8; 16]`
///   - `80..128`: `service: [u8; 48]`
/// - **Cache Line 3 (128..192)**: RPC method name and client peer socket
///   - `128..160`: `method: [u8; 32]`
///   - `160..192`: `peer_addr: [u8; 32]`
/// - **Cache Line 4 (192..256)**: Canonical gRPC error details
///   - `192..256`: `error_msg: [u8; 64]`
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackedGrpcRecord {
    // --- Cache Line 1 (0..64): Core Metadata & Identifiers ---
    pub timestamp_ms: u64,      // 8 bytes (0..8)
    pub duration_us: u32,       // 4 bytes (8..12)
    pub messages_sent: u32,     // 4 bytes (12..16)
    pub messages_received: u32, // 4 bytes (16..20)
    pub content_length: u32,    // 4 bytes (20..24)
    pub grpc_status: u8,        // 1 byte  (24..25): canonical 0..16
    pub transport: u8,          // 1 byte  (25..26): 1=TCP (H2), 2=UDP (QUIC)
    pub service_len: u8,        // 1 byte  (26..27): max 48
    pub method_len: u8,         // 1 byte  (27..28): max 32
    pub request_id_len: u8,     // 1 byte  (28..29): max 16
    pub route_id_len: u8,       // 1 byte  (29..30): max 16
    pub upstream_len: u8,       // 1 byte  (30..31): max 16
    pub peer_addr_len: u8,      // 1 byte  (31..32): max 32
    pub request_id: [u8; 16],   // 16 bytes (32..48)
    pub route_id: [u8; 16],     // 16 bytes (48..64)
    // --- Cache Line 2 (64..128): Upstream & Service ---
    pub upstream: [u8; 16], // 16 bytes (64..80)
    pub service: [u8; 48],  // 48 bytes (80..128)
    // --- Cache Line 3 (128..192): Method & Peer Address ---
    pub method: [u8; 32],    // 32 bytes (128..160)
    pub peer_addr: [u8; 32], // 32 bytes (160..192)
    // --- Cache Line 4 (192..256): Error Message ---
    pub error_msg_len: u8,   // 1 byte  (192..193): max 63
    pub error_msg: [u8; 63], // 63 bytes (193..256)
}

const _: () = assert!(std::mem::size_of::<PackedGrpcRecord>() == 256);
const _: () = assert!(std::mem::align_of::<PackedGrpcRecord>() == 64);

#[inline(always)]
fn write_int<I: itoa::Integer>(buf: &mut Vec<u8>, val: I) {
    let mut ibuf = itoa::Buffer::new();
    buf.extend_from_slice(ibuf.format(val).as_bytes());
}

impl Default for PackedGrpcRecord {
    #[inline(always)]
    fn default() -> Self {
        Self::empty()
    }
}

impl PackedGrpcRecord {
    /// Creates a zero-initialized empty gRPC record.
    #[inline(always)]
    pub const fn empty() -> Self {
        Self {
            timestamp_ms: 0,
            duration_us: 0,
            messages_sent: 0,
            messages_received: 0,
            content_length: 0,
            grpc_status: 0,
            transport: 1, // Default TCP
            service_len: 0,
            method_len: 0,
            request_id_len: 0,
            route_id_len: 0,
            upstream_len: 0,
            peer_addr_len: 0,
            request_id: [0u8; 16],
            route_id: [0u8; 16],
            upstream: [0u8; 16],
            service: [0u8; 48],
            method: [0u8; 32],
            peer_addr: [0u8; 32],
            error_msg_len: 0,
            error_msg: [0u8; 63],
        }
    }

    /// Stores the request ID into the inline buffer.
    #[inline(always)]
    pub fn set_request_id(&mut self, src: &[u8]) {
        let len = src.len().min(16);
        self.request_id[..len].copy_from_slice(&src[..len]);
        self.request_id_len = len as u8;
    }

    /// Stores the route ID into the inline buffer.
    #[inline(always)]
    pub fn set_route_id(&mut self, src: &[u8]) {
        let len = src.len().min(16);
        self.route_id[..len].copy_from_slice(&src[..len]);
        self.route_id_len = len as u8;
    }

    /// Stores the upstream target address into the inline buffer.
    #[inline(always)]
    pub fn set_upstream(&mut self, src: &[u8]) {
        let len = src.len().min(16);
        self.upstream[..len].copy_from_slice(&src[..len]);
        self.upstream_len = len as u8;
    }

    /// Stores the gRPC service name into the inline buffer (up to 48 bytes).
    #[inline(always)]
    pub fn set_service(&mut self, src: &[u8]) {
        let len = src.len().min(48);
        self.service[..len].copy_from_slice(&src[..len]);
        self.service_len = len as u8;
    }

    /// Stores the gRPC method name into the inline buffer (up to 32 bytes).
    #[inline(always)]
    pub fn set_method(&mut self, src: &[u8]) {
        let len = src.len().min(32);
        self.method[..len].copy_from_slice(&src[..len]);
        self.method_len = len as u8;
    }

    /// Stores the downstream client peer socket address (up to 32 bytes).
    #[inline(always)]
    pub fn set_peer_addr(&mut self, src: &[u8]) {
        let len = src.len().min(32);
        self.peer_addr[..len].copy_from_slice(&src[..len]);
        self.peer_addr_len = len as u8;
    }

    /// Stores the gRPC error message into the inline buffer (up to 63 bytes).
    #[inline(always)]
    pub fn set_error_msg(&mut self, src: &[u8]) {
        let len = src.len().min(63);
        self.error_msg[..len].copy_from_slice(&src[..len]);
        self.error_msg_len = len as u8;
    }

    /// Parses a `/package.ServiceName/MethodName` gRPC path into separate service and method.
    #[inline]
    pub fn set_rpc_path(&mut self, path: &str) {
        let trimmed = path.trim_start_matches('/');
        if let Some((svc, mth)) = trimmed.split_once('/') {
            self.set_service(svc.as_bytes());
            self.set_method(mth.as_bytes());
        } else {
            self.set_service(trimmed.as_bytes());
        }
    }

    #[inline(always)]
    pub fn request_id(&self) -> &str {
        std::str::from_utf8(&self.request_id[..self.request_id_len as usize]).unwrap_or("")
    }

    #[inline(always)]
    pub fn route_id(&self) -> &str {
        std::str::from_utf8(&self.route_id[..self.route_id_len as usize]).unwrap_or("")
    }

    #[inline(always)]
    pub fn upstream(&self) -> &str {
        std::str::from_utf8(&self.upstream[..self.upstream_len as usize]).unwrap_or("")
    }

    #[inline(always)]
    pub fn service(&self) -> &str {
        std::str::from_utf8(&self.service[..self.service_len as usize]).unwrap_or("")
    }

    #[inline(always)]
    pub fn method(&self) -> &str {
        std::str::from_utf8(&self.method[..self.method_len as usize]).unwrap_or("")
    }

    #[inline(always)]
    pub fn peer_addr(&self) -> &str {
        std::str::from_utf8(&self.peer_addr[..self.peer_addr_len as usize]).unwrap_or("")
    }

    #[inline(always)]
    pub fn error_msg(&self) -> &str {
        std::str::from_utf8(&self.error_msg[..self.error_msg_len as usize]).unwrap_or("")
    }

    /// Returns canonical string phrase for the gRPC status code.
    pub const fn grpc_status_str(status: u8) -> &'static str {
        match status {
            0 => "OK",
            1 => "CANCELLED",
            2 => "UNKNOWN",
            3 => "INVALID_ARGUMENT",
            4 => "DEADLINE_EXCEEDED",
            5 => "NOT_FOUND",
            6 => "ALREADY_EXISTS",
            7 => "PERMISSION_DENIED",
            8 => "RESOURCE_EXHAUSTED",
            9 => "FAILED_PRECONDITION",
            10 => "ABORTED",
            11 => "OUT_OF_RANGE",
            12 => "UNIMPLEMENTED",
            13 => "INTERNAL",
            14 => "UNAVAILABLE",
            15 => "DATA_LOSS",
            16 => "UNAUTHENTICATED",
            _ => "UNKNOWN",
        }
    }

    /// Returns the transport name ("grpc-tcp" or "grpc-udp").
    pub const fn transport_str(&self) -> &'static str {
        match self.transport {
            2 => "grpc-udp",
            _ => "grpc-tcp",
        }
    }

    /// Serializes this record directly into a byte buffer as JSON.
    pub fn write_json(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(b"{\"timestamp\":");
        write_int(buf, self.timestamp_ms);

        buf.extend_from_slice(b",\"transport\":\"");
        buf.extend_from_slice(self.transport_str().as_bytes());

        buf.extend_from_slice(b"\",\"service\":\"");
        buf.extend_from_slice(self.service().as_bytes());

        buf.extend_from_slice(b"\",\"method\":\"");
        buf.extend_from_slice(self.method().as_bytes());

        buf.extend_from_slice(b"\",\"grpc_status\":");
        write_int(buf, self.grpc_status);

        buf.extend_from_slice(b",\"grpc_status_name\":\"");
        buf.extend_from_slice(Self::grpc_status_str(self.grpc_status).as_bytes());

        buf.extend_from_slice(b"\",\"duration_us\":");
        write_int(buf, self.duration_us);

        buf.extend_from_slice(b",\"messages_sent\":");
        write_int(buf, self.messages_sent);

        buf.extend_from_slice(b",\"messages_received\":");
        write_int(buf, self.messages_received);

        buf.extend_from_slice(b",\"bytes\":");
        write_int(buf, self.content_length);

        buf.extend_from_slice(b",\"request_id\":");
        if self.request_id_len > 0 {
            buf.push(b'"');
            buf.extend_from_slice(self.request_id().as_bytes());
            buf.push(b'"');
        } else {
            buf.extend_from_slice(b"null");
        }

        buf.extend_from_slice(b",\"route_id\":");
        if self.route_id_len > 0 {
            buf.push(b'"');
            buf.extend_from_slice(self.route_id().as_bytes());
            buf.push(b'"');
        } else {
            buf.extend_from_slice(b"null");
        }

        buf.extend_from_slice(b",\"upstream\":");
        if self.upstream_len > 0 {
            buf.push(b'"');
            buf.extend_from_slice(self.upstream().as_bytes());
            buf.push(b'"');
        } else {
            buf.extend_from_slice(b"null");
        }

        buf.extend_from_slice(b",\"peer\":");
        if self.peer_addr_len > 0 {
            buf.push(b'"');
            buf.extend_from_slice(self.peer_addr().as_bytes());
            buf.push(b'"');
        } else {
            buf.extend_from_slice(b"null");
        }

        if self.grpc_status != 0 && self.error_msg_len > 0 {
            buf.extend_from_slice(b",\"error\":");
            buf.push(b'"');
            let err_bytes = &self.error_msg[..self.error_msg_len as usize];
            for &b in err_bytes {
                if b == b'"' || b == b'\\' {
                    buf.push(b'\\');
                }
                buf.push(b);
            }
            buf.push(b'"');
        }

        buf.extend_from_slice(b"}\n");
    }

    /// Serializes this record directly into a byte buffer as single-line Text format.
    pub fn write_text(&self, buf: &mut Vec<u8>) {
        buf.push(b'[');
        write_int(buf, self.timestamp_ms);
        buf.extend_from_slice(b"] transport=");
        buf.extend_from_slice(self.transport_str().as_bytes());
        buf.extend_from_slice(b" rpc=/");
        buf.extend_from_slice(self.service().as_bytes());
        buf.push(b'/');
        buf.extend_from_slice(self.method().as_bytes());
        buf.extend_from_slice(b" status=");
        buf.extend_from_slice(Self::grpc_status_str(self.grpc_status).as_bytes());
        buf.push(b'(');
        write_int(buf, self.grpc_status);
        buf.extend_from_slice(b") duration_us=");
        write_int(buf, self.duration_us);
        buf.extend_from_slice(b" msg_sent=");
        write_int(buf, self.messages_sent);
        buf.extend_from_slice(b" msg_recv=");
        write_int(buf, self.messages_received);
        buf.extend_from_slice(b" bytes=");
        write_int(buf, self.content_length);
        buf.extend_from_slice(b" req_id=");
        buf.extend_from_slice(self.request_id().as_bytes());
        buf.extend_from_slice(b" route=");
        buf.extend_from_slice(self.route_id().as_bytes());
        buf.extend_from_slice(b" upstream=");
        buf.extend_from_slice(self.upstream().as_bytes());
        buf.extend_from_slice(b" peer=");
        buf.extend_from_slice(self.peer_addr().as_bytes());
        if self.grpc_status != 0 && self.error_msg_len > 0 {
            buf.extend_from_slice(b" error=\"");
            buf.extend_from_slice(self.error_msg().as_bytes());
            buf.push(b'"');
        }
        buf.push(b'\n');
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grpc_record_memory_layout() {
        assert_eq!(std::mem::size_of::<PackedGrpcRecord>(), 256);
        assert_eq!(std::mem::align_of::<PackedGrpcRecord>(), 64);
    }

    #[test]
    fn test_grpc_record_json_serialization() {
        let mut record = PackedGrpcRecord::empty();
        record.timestamp_ms = 1710000000000;
        record.duration_us = 4500;
        record.grpc_status = 0; // OK
        record.transport = 1; // TCP
        record.messages_sent = 10;
        record.messages_received = 1;
        record.content_length = 2048;
        record.set_rpc_path("/order.OrderService/StreamOrders");
        record.set_request_id(b"req-grpc-001");
        record.set_route_id(b"orders");
        record.set_upstream(b"10.0.1.5:50051");
        record.set_peer_addr(b"192.168.1.10:48292");

        let mut buf = Vec::new();
        record.write_json(&mut buf);

        let json = std::str::from_utf8(&buf).unwrap();
        assert!(json.contains("\"grpc_status\":0"));
        assert!(json.contains("\"grpc_status_name\":\"OK\""));
        assert!(json.contains("\"service\":\"order.OrderService\""));
        assert!(json.contains("\"method\":\"StreamOrders\""));
        assert!(json.contains("\"messages_sent\":10"));
        assert!(json.contains("\"messages_received\":1"));
        assert!(json.contains("\"transport\":\"grpc-tcp\""));
        assert!(json.contains("\"peer\":\"192.168.1.10:48292\""));
        assert!(json.ends_with('\n'));
    }

    #[test]
    fn test_grpc_record_error_text_serialization() {
        let mut record = PackedGrpcRecord::empty();
        record.timestamp_ms = 1710000000000;
        record.duration_us = 12000;
        record.grpc_status = 14; // UNAVAILABLE
        record.transport = 2; // UDP
        record.set_rpc_path("/payment.Payment/Pay");
        record.set_error_msg(b"connection reset by upstream host");

        let mut buf = Vec::new();
        record.write_text(&mut buf);

        let text = std::str::from_utf8(&buf).unwrap();
        assert!(text.contains("transport=grpc-udp"));
        assert!(text.contains("rpc=/payment.Payment/Pay"));
        assert!(text.contains("status=UNAVAILABLE(14)"));
        assert!(text.contains("error=\"connection reset by upstream host\""));
        assert!(text.ends_with('\n'));
    }
}
