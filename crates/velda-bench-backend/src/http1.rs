//! HTTP/1.1 Protocol Router (Port 8081 Plain, Port 8441 TLS).
//!
//! Serves all 8 canonical routes over HTTP/1.1 text streaming with keep-alive reuse.

use std::net::SocketAddr;
use std::sync::Arc;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;

use crate::routes::dispatch;

pub struct Http1Router;

impl Http1Router {
    pub async fn run(
        plain_addr: SocketAddr,
        tls_addr: Option<SocketAddr>,
        tls_config: Option<Arc<ServerConfig>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let plain_listener = TcpListener::bind(plain_addr).await?;
        tracing::info!(addr = %plain_addr, "HTTP/1.1 Router (Plain) listening");

        let plain_task = async move {
            loop {
                let Ok((stream, _peer)) = plain_listener.accept().await else {
                    continue;
                };
                let _ = stream.set_nodelay(true);
                let io = TokioIo::new(stream);
                tokio::spawn(async move {
                    let service = service_fn(|req| async move {
                        Ok::<_, std::convert::Infallible>(dispatch(req, false).await)
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(io, service)
                        .await;
                });
            }
        };

        if let (Some(t_addr), Some(cfg)) = (tls_addr, tls_config) {
            let tls_listener = TcpListener::bind(t_addr).await?;
            let acceptor = TlsAcceptor::from(cfg);
            tracing::info!(addr = %t_addr, "HTTP/1.1 Router (TLS) listening");

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
                            let _ = hyper::server::conn::http1::Builder::new()
                                .serve_connection(io, service)
                                .await;
                        }
                    });
                }
            };

            tokio::select! {
                _ = plain_task => {},
                _ = tls_task => {},
            }
        } else {
            plain_task.await;
        }

        Ok(())
    }
}
