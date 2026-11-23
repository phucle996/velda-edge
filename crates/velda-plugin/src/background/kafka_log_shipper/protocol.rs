//! Kafka Wire Protocol Serialization & Parser (`kafka_log_shipper::protocol`).
//!
//! Provides pure-Rust encoding and decoding for:
//! - Metadata API (API Key = 3, Version = 4)
//! - SaslHandshake API (API Key = 17, Version = 1)
//! - ApiVersions API (API Key = 18, Version = 0)
//! - SaslAuthenticate API (API Key = 36, Version = 1)
//! - Produce API (API Key = 0, Version = 0..=8; default prebaked Version = 7)
//!
//! All frames conform to the Kafka wire specification with big-endian integer layout.

use crate::error::PluginError;

pub const API_KEY_PRODUCE: i16 = 0;
pub const API_KEY_METADATA: i16 = 3;
pub const API_KEY_SASL_HANDSHAKE: i16 = 17;
pub const API_KEY_API_VERSIONS: i16 = 18;
pub const API_KEY_SASL_AUTHENTICATE: i16 = 36;

pub const API_VERSION_PRODUCE: i16 = 7;
pub const API_VERSION_METADATA: i16 = 4;
pub const API_VERSION_SASL_HANDSHAKE: i16 = 1;
pub const API_VERSION_API_VERSIONS: i16 = 0;
pub const API_VERSION_SASL_AUTHENTICATE: i16 = 1;

// ── Kafka Wire Primitives ─────────────────────────────────────────────────────

#[inline]
pub fn write_string(buf: &mut Vec<u8>, s: &str) {
    let len = s.len() as i16;
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(s.as_bytes());
}

#[inline]
pub fn write_nullable_string(buf: &mut Vec<u8>, s: Option<&str>) {
    if let Some(str_val) = s {
        write_string(buf, str_val);
    } else {
        buf.extend_from_slice(&(-1i16).to_be_bytes());
    }
}

#[inline]
pub fn write_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    let len = bytes.len() as i32;
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(bytes);
}

#[inline]
pub fn write_request_header(
    buf: &mut Vec<u8>,
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: &str,
) {
    buf.extend_from_slice(&api_key.to_be_bytes());
    buf.extend_from_slice(&api_version.to_be_bytes());
    buf.extend_from_slice(&correlation_id.to_be_bytes());
    write_string(buf, client_id);
}

#[inline]
pub fn wrap_framed_request(mut body: Vec<u8>) -> Vec<u8> {
    let length = body.len() as i32;
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&length.to_be_bytes());
    frame.append(&mut body);
    frame
}

// ── Stream Reader Helpers ─────────────────────────────────────────────────────

#[inline]
pub fn read_i16(cur: &mut &[u8]) -> Result<i16, PluginError> {
    if cur.len() < 2 {
        return Err(PluginError::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Unexpected end of Kafka wire buffer reading i16",
        )));
    }
    let val = i16::from_be_bytes([cur[0], cur[1]]);
    *cur = &cur[2..];
    Ok(val)
}

#[inline]
pub fn read_i32(cur: &mut &[u8]) -> Result<i32, PluginError> {
    if cur.len() < 4 {
        return Err(PluginError::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Unexpected end of Kafka wire buffer reading i32",
        )));
    }
    let val = i32::from_be_bytes([cur[0], cur[1], cur[2], cur[3]]);
    *cur = &cur[4..];
    Ok(val)
}

#[inline]
pub fn read_i64(cur: &mut &[u8]) -> Result<i64, PluginError> {
    if cur.len() < 8 {
        return Err(PluginError::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Unexpected end of Kafka wire buffer reading i64",
        )));
    }
    let val = i64::from_be_bytes([
        cur[0], cur[1], cur[2], cur[3], cur[4], cur[5], cur[6], cur[7],
    ]);
    *cur = &cur[8..];
    Ok(val)
}

#[inline]
pub fn read_u8(cur: &mut &[u8]) -> Result<u8, PluginError> {
    if cur.is_empty() {
        return Err(PluginError::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Unexpected end of Kafka wire buffer reading u8",
        )));
    }
    let val = cur[0];
    *cur = &cur[1..];
    Ok(val)
}

#[inline]
pub fn read_string(cur: &mut &[u8]) -> Result<String, PluginError> {
    let len = read_i16(cur)?;
    if len < 0 {
        return Err(PluginError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Unexpected negative string length in Kafka wire protocol",
        )));
    }
    let usize_len = len as usize;
    if cur.len() < usize_len {
        return Err(PluginError::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Unexpected end of Kafka wire buffer reading string",
        )));
    }
    let s = String::from_utf8_lossy(&cur[..usize_len]).to_string();
    *cur = &cur[usize_len..];
    Ok(s)
}

#[inline]
pub fn read_nullable_string(cur: &mut &[u8]) -> Result<Option<String>, PluginError> {
    let len = read_i16(cur)?;
    if len < 0 {
        return Ok(None);
    }
    let usize_len = len as usize;
    if cur.len() < usize_len {
        return Err(PluginError::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Unexpected end of Kafka wire buffer reading nullable string",
        )));
    }
    let s = String::from_utf8_lossy(&cur[..usize_len]).to_string();
    *cur = &cur[usize_len..];
    Ok(Some(s))
}

#[inline]
pub fn read_bytes(cur: &mut &[u8]) -> Result<Vec<u8>, PluginError> {
    let len = read_i32(cur)?;
    if len < 0 {
        return Ok(Vec::new());
    }
    let usize_len = len as usize;
    if cur.len() < usize_len {
        return Err(PluginError::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Unexpected end of Kafka wire buffer reading byte payload",
        )));
    }
    let data = cur[..usize_len].to_vec();
    *cur = &cur[usize_len..];
    Ok(data)
}

// ── Metadata API (Key 3, Version 4) ───────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerInfo {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionInfo {
    pub partition_id: i32,
    pub leader_id: i32,
    pub error_code: i16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataResponse {
    pub brokers: Vec<BrokerInfo>,
    pub partitions: Vec<PartitionInfo>,
}

/// Encodes Metadata Request v4 for the specified topic.
pub fn encode_metadata_request(correlation_id: i32, client_id: &str, topic: &str) -> Vec<u8> {
    let mut body = Vec::with_capacity(64);
    write_request_header(
        &mut body,
        API_KEY_METADATA,
        API_VERSION_METADATA,
        correlation_id,
        client_id,
    );

    // Topics array: count = 1
    body.extend_from_slice(&1i32.to_be_bytes());
    write_string(&mut body, topic);

    // allow_auto_topic_creation: bool (false = 0)
    body.push(0);

    wrap_framed_request(body)
}

/// Decodes Metadata Response v4.
pub fn decode_metadata_response(mut buf: &[u8]) -> Result<MetadataResponse, PluginError> {
    // 1. Correlation ID
    let _correlation_id = read_i32(&mut buf)?;

    // 2. throttle_time_ms
    let _throttle_time = read_i32(&mut buf)?;

    // 3. Brokers array
    let broker_count = read_i32(&mut buf)?;
    let mut brokers = Vec::with_capacity(broker_count.max(0) as usize);
    for _ in 0..broker_count {
        let node_id = read_i32(&mut buf)?;
        let host = read_string(&mut buf)?;
        let port = read_i32(&mut buf)?;
        let _rack = read_nullable_string(&mut buf)?;
        brokers.push(BrokerInfo {
            node_id,
            host,
            port,
        });
    }

    // 4. cluster_id
    let _cluster_id = read_nullable_string(&mut buf)?;

    // 5. controller_id
    let _controller_id = read_i32(&mut buf)?;

    // 6. Topics array
    let topic_count = read_i32(&mut buf)?;
    let mut partitions = Vec::new();

    for _ in 0..topic_count {
        let topic_error = read_i16(&mut buf)?;
        let _name = read_string(&mut buf)?;
        let _is_internal = read_u8(&mut buf)?;

        let partition_count = read_i32(&mut buf)?;
        for _ in 0..partition_count {
            let part_error = read_i16(&mut buf)?;
            let partition_index = read_i32(&mut buf)?;
            let leader_id = read_i32(&mut buf)?;

            // Replicas array
            let replica_count = read_i32(&mut buf)?;
            for _ in 0..replica_count {
                let _ = read_i32(&mut buf)?;
            }

            // ISR array
            let isr_count = read_i32(&mut buf)?;
            for _ in 0..isr_count {
                let _ = read_i32(&mut buf)?;
            }

            partitions.push(PartitionInfo {
                partition_id: partition_index,
                leader_id,
                error_code: if topic_error != 0 {
                    topic_error
                } else {
                    part_error
                },
            });
        }
    }

    Ok(MetadataResponse {
        brokers,
        partitions,
    })
}

// ── Produce API (Key 0, Version 7) ────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionProduceResponse {
    pub partition_id: i32,
    pub error_code: i16,
    pub base_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceResponse {
    pub partitions: Vec<PartitionProduceResponse>,
    pub throttle_time_ms: i32,
}

/// Encodes Produce Request v7 for partition batches targeting a single topic.
pub fn encode_produce_request(
    correlation_id: i32,
    client_id: &str,
    acks: i16,
    timeout_ms: i32,
    topic: &str,
    partition_batches: &[(i32, &[u8])],
) -> Vec<u8> {
    let mut body = Vec::with_capacity(128);
    write_request_header(
        &mut body,
        API_KEY_PRODUCE,
        API_VERSION_PRODUCE,
        correlation_id,
        client_id,
    );

    // transactional_id: nullable string (null)
    write_nullable_string(&mut body, None);

    // acks
    body.extend_from_slice(&acks.to_be_bytes());

    // timeout_ms
    body.extend_from_slice(&timeout_ms.to_be_bytes());

    // topic_data array: 1 topic
    body.extend_from_slice(&1i32.to_be_bytes());
    write_string(&mut body, topic);

    // partition_data array
    let part_count = partition_batches.len() as i32;
    body.extend_from_slice(&part_count.to_be_bytes());

    for (part_id, record_bytes) in partition_batches {
        body.extend_from_slice(&part_id.to_be_bytes());
        write_bytes(&mut body, record_bytes);
    }

    wrap_framed_request(body)
}

/// Decodes Produce Response v7.
/// Decodes Produce Response across API versions `0..=8` matching the prebaked request version.
pub fn decode_produce_response(
    mut buf: &[u8],
    api_version: i16,
) -> Result<ProduceResponse, PluginError> {
    // 1. Correlation ID
    let _correlation_id = read_i32(&mut buf)?;

    // 2. Topics array
    let topic_count = read_i32(&mut buf)?;
    let mut partitions = Vec::new();

    for _ in 0..topic_count {
        let _topic_name = read_string(&mut buf)?;
        let partition_count = read_i32(&mut buf)?;

        for _ in 0..partition_count {
            let partition_id = read_i32(&mut buf)?;
            let error_code = read_i16(&mut buf)?;
            let base_offset = read_i64(&mut buf)?;
            if api_version >= 2 {
                let _log_append_time = read_i64(&mut buf)?;
            }
            if api_version >= 5 {
                let _log_start_offset = read_i64(&mut buf)?;
            }
            if api_version >= 8 {
                let rec_err_count = read_i32(&mut buf)?;
                for _ in 0..rec_err_count {
                    let _batch_index = read_i32(&mut buf)?;
                    let _error_message = read_nullable_string(&mut buf)?;
                }
                let _error_message = read_nullable_string(&mut buf)?;
            }

            partitions.push(PartitionProduceResponse {
                partition_id,
                error_code,
                base_offset,
            });
        }
    }

    let throttle_time_ms = if api_version >= 1 {
        read_i32(&mut buf).unwrap_or(0)
    } else {
        0
    };

    Ok(ProduceResponse {
        partitions,
        throttle_time_ms,
    })
}

/// Supported API version range advertised by a Kafka broker for a single `api_key`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiVersionRange {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

/// Encodes ApiVersions Request v0.
pub fn encode_api_versions_request(correlation_id: i32, client_id: &str) -> Vec<u8> {
    let mut body = Vec::with_capacity(24);
    write_request_header(
        &mut body,
        API_KEY_API_VERSIONS,
        API_VERSION_API_VERSIONS,
        correlation_id,
        client_id,
    );
    wrap_framed_request(body)
}

/// Decodes ApiVersions Response v0, returning `(error_code, ranges)`.
pub fn decode_api_versions_response(
    mut buf: &[u8],
) -> Result<(i16, Vec<ApiVersionRange>), PluginError> {
    let _correlation_id = read_i32(&mut buf)?;
    let error_code = read_i16(&mut buf)?;
    let count = read_i32(&mut buf)?.max(0) as usize;
    let mut ranges = Vec::with_capacity(count.min(128));
    for _ in 0..count {
        ranges.push(ApiVersionRange {
            api_key: read_i16(&mut buf)?,
            min_version: read_i16(&mut buf)?,
            max_version: read_i16(&mut buf)?,
        });
    }
    Ok((error_code, ranges))
}

// ── SASL Handshake API (Key 17, Version 1) ────────────────────────────────────

/// Encodes SaslHandshake Request v1.
pub fn encode_sasl_handshake_request(
    correlation_id: i32,
    client_id: &str,
    mechanism: &str,
) -> Vec<u8> {
    let mut body = Vec::with_capacity(32);
    write_request_header(
        &mut body,
        API_KEY_SASL_HANDSHAKE,
        API_VERSION_SASL_HANDSHAKE,
        correlation_id,
        client_id,
    );
    write_string(&mut body, mechanism);
    wrap_framed_request(body)
}

/// Decodes SaslHandshake Response v1, returning the error code (0 = success).
pub fn decode_sasl_handshake_response(mut buf: &[u8]) -> Result<i16, PluginError> {
    let _correlation_id = read_i32(&mut buf)?;
    let error_code = read_i16(&mut buf)?;
    Ok(error_code)
}

// ── SASL Authenticate API (Key 36, Version 1) ─────────────────────────────────

/// Encodes SaslAuthenticate Request v1.
pub fn encode_sasl_authenticate_request(
    correlation_id: i32,
    client_id: &str,
    auth_bytes: &[u8],
) -> Vec<u8> {
    let mut body = Vec::with_capacity(64 + auth_bytes.len());
    write_request_header(
        &mut body,
        API_KEY_SASL_AUTHENTICATE,
        API_VERSION_SASL_AUTHENTICATE,
        correlation_id,
        client_id,
    );
    write_bytes(&mut body, auth_bytes);
    wrap_framed_request(body)
}

/// Decodes SaslAuthenticate Response v1, returning `(error_code, auth_bytes)`.
pub fn decode_sasl_authenticate_response(mut buf: &[u8]) -> Result<(i16, Vec<u8>), PluginError> {
    let _correlation_id = read_i32(&mut buf)?;
    let error_code = read_i16(&mut buf)?;
    let _error_message = read_nullable_string(&mut buf)?;
    let auth_bytes = read_bytes(&mut buf)?;
    Ok((error_code, auth_bytes))
}

// ── SASL PLAIN Auth Generator ─────────────────────────────────────────────────

/// Generates the RFC 4616 SASL PLAIN payload: `\0username\0password`.
#[inline]
pub fn generate_sasl_plain_auth(username: &str, password: &str) -> Vec<u8> {
    let mut payload = Vec::with_capacity(username.len() + password.len() + 2);
    payload.push(0);
    payload.extend_from_slice(username.as_bytes());
    payload.push(0);
    payload.extend_from_slice(password.as_bytes());
    payload
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sasl_plain_payload() {
        let auth = generate_sasl_plain_auth("alice", "secret123");
        assert_eq!(auth, b"\0alice\0secret123");
    }

    #[test]
    fn test_produce_request_framing() {
        let req =
            encode_produce_request(42, "velda-test", 1, 5000, "test-topic", &[(0, b"batch0")]);
        assert!(req.len() > 4);
        let frame_len = i32::from_be_bytes([req[0], req[1], req[2], req[3]]) as usize;
        assert_eq!(frame_len, req.len() - 4);
    }
}
