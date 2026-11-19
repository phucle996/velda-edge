//! ClickHouse Format Abstraction (`clickhouse_log_shipper::format`).

pub mod json;
pub mod row_binary;

pub use json::format_json_each_row;
pub use row_binary::format_row_binary;

/// Supported wire ingestion formats for ClickHouse HTTP interface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ClickHouseFormat {
    /// Native binary format (Ultra high-performance, little-endian primitive stream).
    #[default]
    RowBinary,
    /// Newline-delimited JSON format (Standard, human readable).
    JsonEachRow,
}

impl ClickHouseFormat {
    /// Returns the ClickHouse FORMAT clause identifier.
    #[inline(always)]
    pub const fn as_clickhouse_format_str(&self) -> &'static str {
        match self {
            Self::RowBinary => "RowBinary",
            Self::JsonEachRow => "JSONEachRow",
        }
    }

    /// Returns the HTTP Content-Type header value.
    #[inline(always)]
    pub const fn content_type(&self) -> &'static str {
        match self {
            Self::RowBinary => "application/octet-stream",
            Self::JsonEachRow => "application/x-ndjson",
        }
    }
}
