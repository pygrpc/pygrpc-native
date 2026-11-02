// SPDX-License-Identifier: Apache-2.0

use std::path::Path;

use hyper::client::conn::http2 as client_http2;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use tonic::{Code, Status};

use super::connection::{CONNECTION_WINDOW, STREAM_WINDOW};
use super::stream::RequestBody;

/// The connection to the grpcio server that serves the methods this front
/// does not know (out-of-tree services). Each client connection gets its own,
/// as it would talking to grpcio directly, so a client that stops reading only
/// stalls its own calls.
#[derive(Default)]
pub(super) struct Fallback {
    sender: tokio::sync::Mutex<Option<client_http2::SendRequest<RequestBody>>>,
}

impl Fallback {
    async fn sender(&self, path: &Path) -> std::io::Result<client_http2::SendRequest<RequestBody>> {
        let mut sender = self.sender.lock().await;
        if let Some(open) = sender.as_ref().filter(|open| !open.is_closed()) {
            return Ok(open.clone());
        }
        let stream = tokio::net::UnixStream::connect(path).await?;
        // The same windows as the client side, so a stalled client holds up
        // at most as much here.
        let (open, connection) = client_http2::Builder::new(TokioExecutor::new())
            .timer(TokioTimer::new())
            .initial_stream_window_size(STREAM_WINDOW)
            .initial_connection_window_size(CONNECTION_WINDOW)
            .handshake(TokioIo::new(stream))
            .await
            .map_err(std::io::Error::other)?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                log::debug!("fallback gRPC connection ended with error: {err}");
            }
        });
        *sender = Some(open.clone());
        Ok(open)
    }

    pub(super) async fn forward(
        &self,
        path: &Path,
        mut request: http::Request<RequestBody>,
    ) -> http::Response<tonic::body::Body> {
        let Some(path_and_query) = request.uri().path_and_query().cloned() else {
            return Status::new(Code::Unimplemented, "Method not found!").into_http();
        };
        let uri = http::Uri::builder()
            .scheme("http")
            .authority("localhost")
            .path_and_query(path_and_query)
            .build();
        match uri {
            Ok(uri) => *request.uri_mut() = uri,
            Err(err) => return Status::internal(err.to_string()).into_http(),
        }
        let mut sender = match self.sender(path).await {
            Ok(sender) => sender,
            Err(err) => {
                return Status::unavailable(format!("out-of-tree gRPC services: {err}")).into_http()
            }
        };
        match sender.send_request(request).await {
            Ok(response) => response.map(tonic::body::Body::new),
            Err(err) => {
                Status::unavailable(format!("out-of-tree gRPC services: {err}")).into_http()
            }
        }
    }
}
