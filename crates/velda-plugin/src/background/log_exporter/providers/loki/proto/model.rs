//! Grafana Loki PushRequest Protobuf Models (`loki_log_shipper::proto::model`).
//!
//! Provides Prost-compatible struct representations of the Grafana Loki
//! `logproto.PushRequest` specification for validation, decode verification,
//! and testing.

use prost::Message;

/// Root export request for Grafana Loki Push service (`logproto.PushRequest`).
#[derive(Clone, PartialEq, Message)]
pub struct PushRequest {
    #[prost(message, repeated, tag = "1")]
    pub streams: Vec<StreamAdapter>,
}

/// Stream representation containing Prometheus labels and entries (`logproto.StreamAdapter`).
#[derive(Clone, PartialEq, Message)]
pub struct StreamAdapter {
    #[prost(string, tag = "1")]
    pub labels: String,
    #[prost(message, repeated, tag = "2")]
    pub entries: Vec<EntryAdapter>,
}

/// Individual log entry (`logproto.EntryAdapter`).
#[derive(Clone, PartialEq, Message)]
pub struct EntryAdapter {
    #[prost(message, optional, tag = "1")]
    pub timestamp: Option<Timestamp>,
    #[prost(string, tag = "2")]
    pub line: String,
}

/// Standard Protobuf Timestamp representation (`google.protobuf.Timestamp`).
#[derive(Clone, PartialEq, Message)]
pub struct Timestamp {
    #[prost(int64, tag = "1")]
    pub seconds: i64,
    #[prost(int32, tag = "2")]
    pub nanos: i32,
}

/// Empty response for Loki Pusher service (`logproto.PushResponse`).
#[derive(Clone, PartialEq, Message)]
pub struct PushResponse {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_request_prost_roundtrip() {
        let req = PushRequest {
            streams: vec![StreamAdapter {
                labels: "{service=\"velda-edge\",protocol=\"http\"}".to_string(),
                entries: vec![EntryAdapter {
                    timestamp: Some(Timestamp {
                        seconds: 1_710_000_000,
                        nanos: 0,
                    }),
                    line: "request_id=0000000065eb6380aaaabbbbccccdddd proto=HTTP/1.1 status=200"
                        .to_string(),
                }],
            }],
        };

        let mut buf = Vec::new();
        req.encode(&mut buf).expect("Encode succeeds");
        assert!(!buf.is_empty());

        let decoded = PushRequest::decode(&buf[..]).expect("Decode succeeds");
        assert_eq!(req, decoded);
    }
}
