// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use super::connection::serve_connection;
use super::Shared;

// Unix sockets have no peer address, so each connection gets its own name.
static NEXT_UNIX_PEER: AtomicU64 = AtomicU64::new(1);

pub(super) async fn stopped(shutdown: &mut watch::Receiver<bool>) {
    let _ = shutdown.wait_for(|stop| *stop).await;
}

pub enum Listener {
    Tcp(tokio::net::TcpListener),
    Unix(tokio::net::UnixListener),
}

pub async fn accept_loop(
    listener: Listener,
    shared: Arc<Shared>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let accepted = tokio::select! {
            _ = stopped(&mut shutdown) => return,
            accepted = accept(&listener) => accepted,
        };
        match accepted {
            Ok(Accepted::Tcp(stream, peer)) => {
                let _ = stream.set_nodelay(true);
                tokio::spawn(serve_connection(
                    stream,
                    peer,
                    shared.clone(),
                    shutdown.clone(),
                ));
            }
            Ok(Accepted::Unix(stream, peer)) => {
                tokio::spawn(serve_connection(
                    stream,
                    peer,
                    shared.clone(),
                    shutdown.clone(),
                ));
            }
            Err(err) => {
                // Out of file descriptors (EMFILE): back off instead of spinning.
                log::warn!("gRPC accept failed: {err}");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

enum Accepted {
    Tcp(tokio::net::TcpStream, Arc<str>),
    Unix(tokio::net::UnixStream, Arc<str>),
}

async fn accept(listener: &Listener) -> std::io::Result<Accepted> {
    match listener {
        Listener::Tcp(listener) => {
            let (stream, addr) = listener.accept().await?;
            let peer = match addr {
                std::net::SocketAddr::V4(addr) => format!("ipv4:{addr}"),
                std::net::SocketAddr::V6(addr) => {
                    format!("ipv6:%5B{}%5D:{}", addr.ip(), addr.port())
                }
            };
            Ok(Accepted::Tcp(stream, peer.into()))
        }
        Listener::Unix(listener) => {
            let (stream, _addr) = listener.accept().await?;
            let id = NEXT_UNIX_PEER.fetch_add(1, Ordering::Relaxed);
            Ok(Accepted::Unix(stream, format!("unix:{id}").into()))
        }
    }
}
