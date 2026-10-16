//! HTTP/2 Protocol Router (Port 8082 Cleartext H2C, Port 8442 TLS H2).
//!
//! Serves all 8 canonical routes over HTTP/2 binary framing and multiplexing.

use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;

use crate::routes::dispatch;

pub struct Http2Router;

impl Http2Router {
    pub async fn run(
        h2c_addr: SocketAddr,
        tls_addr: Option<SocketAddr>,
        tls_config: Option<Arc<ServerConfig>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let h2c_listener = TcpListener::bind(h2c_addr).await?;
        tracing::info!(addr = %h2c_addr, "HTTP/2 Router (Cleartext H2C) listening");

        let h2c_task = async move {
            loop {
                let Ok((stream, _peer)) = h2c_listener.accept().await else {
                    continue;
                };
                let _ = stream.set_nodelay(true);
                let io = TokioIo::new(stream);
                tokio::spawn(async move {
                    let service = service_fn(|req| async move {
                        Ok::<_, std::convert::Infallible>(dispatch(req, false).await)
                    });
                    let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                        .serve_connection(io, service)
                        .await;
                });
            }
        };

        if let (Some(t_addr), Some(cfg)) = (tls_addr, tls_config) {
            let tls_listener = TcpListener::bind(t_addr).await?;
            let acceptor = TlsAcceptor::from(cfg);
            tracing::info!(addr = %t_addr, "HTTP/2 Router (TLS H2) listening");

            let tls_task = async move {
                loop {
                    let Ok((stream, _peer)) = tls_listener.accept().await else {
                        continue;
                    };
                    let _ = stream.set_nodelay(true);
                    let acceptor = acceptor.clone();
                    tokio::spawn(async move {
                        if let Ok(tls_stream) = acceptor.accept(stream).await {
                            let io = TokioIo::new(tls_stream);
                            let service = service_fn(|req| async move {
                                Ok::<_, std::convert::Infallible>(dispatch(req, true).await)
                            });
                            let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                                .serve_connection(io, service)
                                .await;
                        }
                    });
                }
            };

            tokio::select! {
                _ = h2c_task => {},
                _ = tls_task => {},
            }
        } else {
            h2c_task.await;
        }

        Ok(())
    }
}
