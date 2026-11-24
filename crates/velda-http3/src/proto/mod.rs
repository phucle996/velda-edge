//! RFC 9114 & RFC 9204 HTTP/3 Sans-IO Protocol Core.
//!
//! ```text
//! +-------------------------------------------------------------------+
//! |                VELDA-HTTP3 PROTOCOL CORE (`proto/`)               |
//! +-------------------------------------------------------------------+
//! |                                                                   |
//! |   QUIC Stream Bytes                                               |
//! |         │                                                         |
//! |         ▼                                                         |
//! |   [frame.rs]    RFC 9000 VarInt + RFC 9114 Frame Codec            |
//! |         │       (DATA, HEADERS, SETTINGS, CANCEL_PUSH, GOAWAY)    |
//! |         ▼                                                         |
//! |   [qpack.rs]    RFC 9204 Static-Only QPACK Encoder & Decoder      |
//! |         │       (99-entry Static Table, zero Dynamic Table HoL)   |
//! |         ▼                                                         |
//! |   [huffman.rs]  RFC 7541 / RFC 9204 Canonical Huffman Codec       |
//! |                                                                   |
//! +-------------------------------------------------------------------+
//! ```
//!
//! Strictly protocol-pure and socket-blind; Edge QUIC transport drivers live in
//! `server/` and `client/`.

pub mod frame;
pub mod huffman;
pub mod qpack;
