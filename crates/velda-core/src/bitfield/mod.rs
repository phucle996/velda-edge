//! Compact Non-Colliding Bitmask & Bitfield Codebook (`velda-core::bitfield`).
//!
//! Symmetric counterpart to [`crate::lut`]: while `lut` maps discrete domain tokens into dense
//! integer enums (`0..=N-1`), `bitfield` defines canonical fixed-width bitmask layouts and flags
//! packed inside [`crate::RequestState`] (and future bit-packed state slots) with zero heap
//! allocation and zero protocol-parsing logic in `velda-core`.
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                     VELDA EDGE CANONICAL 64B REQUESTSTATE BITFIELDS                     │
//! ├───────────────────────┬───────────────┬────────────────┬────────────────────────────────┤
//! │ Bitfield Dictionary   │ Width / Slot  │ Owner Producer │ Hot-Path Semantics             │
//! ├───────────────────────┼───────────────┼────────────────┼────────────────────────────────┤
//! │ tls_traits            │ u16  (16 bit) │ velda-tls      │ TLS ver, ALPN, SNI, GREASE...  │
//! │ wire_traits           │ u40  (40 bit) │ http2/http3    │ Pseudo-order, SETTINGS, WIN... │
//! └───────────────────────┴───────────────┴────────────────┴────────────────────────────────┘
//! ```

pub mod tls_traits;
pub mod wire_traits;

pub use tls_traits::{
    TLS_ALPN_H1, TLS_ALPN_H2, TLS_ALPN_H3, TLS_ALPN_MASK, TLS_ALPN_NONE, TLS_ALPN_OTHER,
    TLS_ALPN_SHIFT, TLS_CIPHER_BUCKET_MASK, TLS_CIPHER_BUCKET_SHIFT, TLS_EXT_BUCKET_MASK,
    TLS_EXT_BUCKET_SHIFT, TLS_FLAG_HAS_ALPS, TLS_FLAG_HAS_CERT_COMPRESS, TLS_FLAG_HAS_GREASE,
    TLS_FLAG_HAS_PSK, TLS_FLAG_HAS_SESSION_TICKET, TLS_FLAG_HAS_SNI, TLS_VER_12, TLS_VER_13,
    TLS_VER_LEGACY, TLS_VER_MASK, TLS_VER_NONE,
};
pub use wire_traits::{
    PSEUDO_AUTHORITY, PSEUDO_METHOD, PSEUDO_ORDER_MAPS, PSEUDO_ORDER_MASP, PSEUDO_ORDER_MPAS,
    PSEUDO_ORDER_MSAP, PSEUDO_ORDER_MSPA, PSEUDO_ORDER_OTHER, PSEUDO_PATH, PSEUDO_SCHEME,
    WIRE_CONN_WIN_MASK, WIRE_CONN_WIN_SHIFT, WIRE_ENABLE_PUSH_MASK, WIRE_ENABLE_PUSH_SHIFT,
    WIRE_FLAG_H2_PRIORITY_USED, WIRE_FLAG_NO_RFC7540_PRIORITIES, WIRE_FLAG_ODD_SETTINGS,
    WIRE_HDR_TABLE_MASK, WIRE_HDR_TABLE_SHIFT, WIRE_INIT_WIN_MASK, WIRE_INIT_WIN_SHIFT,
    WIRE_MAX_FRAME_MASK, WIRE_MAX_FRAME_SHIFT, WIRE_MAX_STREAMS_MASK, WIRE_MAX_STREAMS_SHIFT,
    WIRE_PROTO_H1, WIRE_PROTO_H2, WIRE_PROTO_H3, WIRE_PROTO_MASK, WIRE_PROTO_NONE,
    WIRE_PROTO_SHIFT, WIRE_PSEUDO_ORDER_MASK, WIRE_SETTINGS_SEQ_MASK, WIRE_SETTINGS_SEQ_SHIFT,
    encode_pseudo_order,
};
