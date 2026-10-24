//! gRPC Protocol Router for Velda Benchmark Backend (Port 50051 Cleartext H2C, Port 50052 TLS H2).
//!
//! Serves production-grade canonical gRPC services over HTTP/2 transport:
//! - `/order.v1.OrderService/CreateOrder` (Unary RPC)
//! - `/payment.v1.PaymentService/ProcessPayment` (Unary RPC)
//! - `/test.UnaryService/EchoUnary` (Echo RPC)
//! - Fallback Unimplemented handler for unmatched RPC methods.

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
use velda_grpc::{GrpcConfig, GrpcServerConnection, GrpcStatus};

static ORDER_RESP: &[u8] =
    b"{\"order_id\":\"ord_real_azure_12345\",\"status\":\"CONFIRMED\",\"total_amount\":99.99}";
static PAYMENT_RESP: &[u8] =
    b"{\"payment_id\":\"pay_real_azure_99999\",\"status\":\"SUCCESS\",\"currency\":\"USD\"}";

pub struct GrpcRouter;

impl GrpcRouter {
    pub async fn run(
        h2c_addr: SocketAddr,
        tls_addr: Option<SocketAddr>,
        tls_config: Option<Arc<ServerConfig>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let grpc_config = Arc::new(GrpcConfig::default());

        // 1. Cleartext gRPC listener (h2c over raw TCP)
        let h2c_listener = TcpListener::bind(h2c_addr).await?;
        tracing::info!(addr = %h2c_addr, "gRPC Router (Cleartext H2C) listening");

        let h2c_cfg = grpc_config.clone();
        let h2c_task = async move {
            loop {
                let Ok((stream, _peer)) = h2c_listener.accept().await else {
                    continue;
                };
                let _ = stream.set_nodelay(true);
                let cfg = h2c_cfg.clone();

                tokio::spawn(async move {
                    if let Ok(mut conn) = GrpcServerConnection::handshake(stream, &cfg).await {
                        while let Ok(Some(mut server_stream)) = conn.accept().await {
                            tokio::spawn(async move {
                                let path = server_stream.parts.uri.path().to_string();
                                let req_msg = server_stream
                                    .read_unary_message(10 * 1024 * 1024)
                                    .await
                                    .unwrap_or(None);

                                match path.as_str() {
                                    "/order.v1.OrderService/CreateOrder" => {
                                        let _ = server_stream.respond.send_unary_response(
                                            GrpcStatus::Ok,
                                            Some(ORDER_RESP),
                                            None,
                                        );
                                    }
                                    "/payment.v1.PaymentService/ProcessPayment" => {
                                        let _ = server_stream.respond.send_unary_response(
                                            GrpcStatus::Ok,
                                            Some(PAYMENT_RESP),
                                            None,
                                        );
                                    }
                                    "/test.UnaryService/EchoUnary" => {
                                        let resp_bytes =
                                            req_msg.as_deref().unwrap_or(b"echo_empty");
                                        let _ = server_stream.respond.send_unary_response(
                                            GrpcStatus::Ok,
                                            Some(resp_bytes),
                                            None,
                                        );
                                    }
                                    _ => {
                                        let _ = server_stream.respond.send_unary_response(
                                            GrpcStatus::Unimplemented,
                                            None,
                                            None,
                                        );
                                    }
                                }
                            });
                        }
                    }
                });
            }
        };

        if let (Some(t_addr), Some(cfg)) = (tls_addr, tls_config) {
            let tls_listener = TcpListener::bind(t_addr).await?;
            let acceptor = TlsAcceptor::from(cfg);
            tracing::info!(addr = %t_addr, "gRPC Router (TLS H2) listening");

            let tls_task = async move {
                loop {
                    let Ok((stream, _peer)) = tls_listener.accept().await else {
                        continue;
                    };
                    let _ = stream.set_nodelay(true);
                    let acceptor = acceptor.clone();
                    let g_cfg = grpc_config.clone();

                    tokio::spawn(async move {
                        if let Ok(tls_stream) = acceptor.accept(stream).await
                            && let Ok(mut conn) =
                                GrpcServerConnection::handshake(tls_stream, &g_cfg).await
                        {
                            while let Ok(Some(mut server_stream)) = conn.accept().await {
                                tokio::spawn(async move {
                                    let path = server_stream.parts.uri.path().to_string();
                                    let req_msg = server_stream
                                        .read_unary_message(10 * 1024 * 1024)
                                        .await
                                        .unwrap_or(None);

                                    match path.as_str() {
                                        "/order.v1.OrderService/CreateOrder" => {
                                            let _ = server_stream.respond.send_unary_response(
                                                GrpcStatus::Ok,
                                                Some(ORDER_RESP),
                                                None,
                                            );
                                        }
                                        "/payment.v1.PaymentService/ProcessPayment" => {
                                            let _ = server_stream.respond.send_unary_response(
                                                GrpcStatus::Ok,
                                                Some(PAYMENT_RESP),
                                                None,
                                            );
                                        }
                                        "/test.UnaryService/EchoUnary" => {
                                            let resp_bytes =
                                                req_msg.as_deref().unwrap_or(b"echo_empty");
                                            let _ = server_stream.respond.send_unary_response(
                                                GrpcStatus::Ok,
                                                Some(resp_bytes),
                                                None,
                                            );
                                        }
                                        _ => {
                                            let _ = server_stream.respond.send_unary_response(
                                                GrpcStatus::Unimplemented,
                                                None,
                                                None,
                                            );
                                        }
                                    }
                                });
                            }
                        }
                    });
                }
            };

            tokio::select! {
                _ = h2c_task => Ok(()),
                _ = tls_task => Ok(()),
            }
        } else {
            h2c_task.await;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use http::{HeaderMap, Method, StatusCode, Uri, Version};
    use std::time::Duration;
    use velda_core::{Body, L7Request};
    use velda_grpc::frame::encode_grpc_frame;
    use velda_grpc::tcp::client::GrpcUpstreamConnector;

    #[tokio::test]
    async fn test_real_grpc_backend_order_service() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

        tokio::spawn(async move {
            let _ = GrpcRouter::run(addr, None, None).await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let test_config = GrpcConfig::default();
        let mut connector = GrpcUpstreamConnector::connect(addr, None, &test_config, None, None)
            .await
            .expect("Failed to connect to real gRPC backend");

        let mut req_body = BytesMut::new();
        encode_grpc_frame(b"{\"order_id\":\"ord_123\"}", false, &mut req_body);

        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/grpc".parse().unwrap());
        headers.insert("te", "trailers".parse().unwrap());

        let req = L7Request::new(
            Method::POST,
            Uri::from_static("http://127.0.0.1/order.v1.OrderService/CreateOrder"),
            Version::HTTP_2,
            headers,
            Body::Bytes(req_body.freeze()),
        );

        let resp = connector
            .invoke_unary(&req, &test_config)
            .await
            .expect("Failed to send unary request");

        assert_eq!(resp.status, StatusCode::OK);
        assert_eq!(
            resp.headers.get("content-type").unwrap(),
            "application/grpc"
        );
        assert_eq!(resp.headers.get("grpc-status").unwrap(), "0");

        if let Body::Bytes(body_bytes) = resp.body {
            let mut buf = BytesMut::from(&body_bytes[..]);
            let (_compressed, payload) = velda_grpc::frame::decode_grpc_frame(&mut buf)
                .unwrap()
                .unwrap();
            assert_eq!(&payload[..], ORDER_RESP);
        } else {
            panic!("Expected binary body in response");
        }
    }

    #[tokio::test]
    async fn test_real_grpc_backend_unimplemented_service() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

        tokio::spawn(async move {
            let _ = GrpcRouter::run(addr, None, None).await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let test_config = GrpcConfig::default();
        let mut connector = GrpcUpstreamConnector::connect(addr, None, &test_config, None, None)
            .await
            .expect("Failed to connect to real gRPC backend");

        let mut req_body = BytesMut::new();
        encode_grpc_frame(b"test", false, &mut req_body);

        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/grpc".parse().unwrap());
        headers.insert("te", "trailers".parse().unwrap());

        let req = L7Request::new(
            Method::POST,
            Uri::from_static("http://127.0.0.1/unknown.UnknownService/UnknownMethod"),
            Version::HTTP_2,
            headers,
            Body::Bytes(req_body.freeze()),
        );

        let resp = connector
            .invoke_unary(&req, &test_config)
            .await
            .expect("Failed to send unary request");

        assert_eq!(resp.status, StatusCode::OK);
        assert_eq!(resp.headers.get("grpc-status").unwrap(), "12"); // UNIMPLEMENTED
    }
}
