//! Uncompressed native calls, framed here so messages skip tonic's codec buffers.

// SPDX-License-Identifier: Apache-2.0

use std::future::poll_fn;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{BufMut, Bytes, BytesMut};
use futures_core::Stream;
use http_body::{Body, Frame};
use tonic::metadata::MetadataMap;
use tonic::Status;

use super::calls::{ExtraTrailers, ResponseStream, StreamingCall, UnaryCall};
use super::stream::{CallSizes, RequestBody};
use super::Payload;

const HEADER_LEN: usize = 5;

// A message up to this size is copied behind its header into one DATA frame;
// a larger one is sent from its own buffer after a frame with the header.
const INLINE_MESSAGE: usize = 16 * 1024;

// Small stream messages that are ready together are sent as one frame, as
// tonic does, up to this many bytes.
const STREAM_BATCH: usize = 32 * 1024;

// Request frames this small on average cost more to keep than to copy; past
// this many, the message is read into one buffer instead, as tonic does.
const SMALL_FRAME: usize = 1024;
const SMALL_FRAMES: usize = 16;

// The buffer a stream's frames are cut from, kept for the whole stream so a
// message sent alone doesn't allocate, as tonic's encoder does.
const STREAM_BUFFER: usize = 8 * 1024;

/// Whether the request can be read here: tonic decompresses the others.
pub(super) fn handles(headers: &http::HeaderMap) -> bool {
    headers
        .get("grpc-encoding")
        .is_none_or(|value| value == "identity")
}

pub(super) async fn unary(
    mut call: UnaryCall,
    request: http::Request<RequestBody>,
    sizes: Option<Arc<CallSizes>>,
) -> http::Response<tonic::body::Body> {
    let request = match read_request(request).await {
        Ok(request) => request,
        Err(status) => return status.into_http(),
    };
    let (metadata, payload, mut extensions) = match call.start(request).await {
        Ok(response) => response.into_parts(),
        Err(status) => return status.into_http(),
    };
    if let Some(status) = too_large(payload.len()) {
        return status.into_http();
    }
    let mut trailers = ok_trailers();
    if let Some(ExtraTrailers(extra)) = extensions.remove::<ExtraTrailers>() {
        trailers.extend(extra);
    }
    let payload_len = payload.len() as u64;
    let mut first = BytesMut::new();
    let rest = put_message(&mut first, payload);
    response(
        metadata,
        UnaryBody {
            sent: sizes.map(|sizes| (sizes, payload_len)),
            first: Some(first.freeze()),
            rest,
            trailers: Some(trailers),
        },
    )
}

pub(super) async fn server_streaming(
    mut call: StreamingCall,
    request: http::Request<RequestBody>,
    sizes: Option<Arc<CallSizes>>,
) -> http::Response<tonic::body::Body> {
    let request = match read_request(request).await {
        Ok(request) => request,
        Err(status) => return status.into_http(),
    };
    let (metadata, stream, _) = match call.start(request).await {
        Ok(response) => response.into_parts(),
        Err(status) => return status.into_http(),
    };
    response(
        metadata,
        StreamBody {
            stream,
            sizes,
            buf: BytesMut::with_capacity(STREAM_BUFFER),
            held: None,
            trailers: None,
            done: false,
        },
    )
}

fn response<B>(metadata: MetadataMap, body: B) -> http::Response<tonic::body::Body>
where
    B: Body<Data = Bytes, Error = Status> + Send + 'static,
{
    let mut response = http::Response::new(tonic::body::Body::new(body));
    *response.version_mut() = http::Version::HTTP_2;
    let headers = response.headers_mut();
    *headers = metadata.into_headers();
    // As tonic's sanitizing does; Python can't set the grpc- ones.
    headers.remove(http::header::TE);
    headers.insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/grpc"),
    );
    response
}

/// tonic's error for a message its frame header can't give the length of.
fn too_large(len: usize) -> Option<Status> {
    (len > u32::MAX as usize).then(|| {
        Status::resource_exhausted(format!(
            "Cannot return body with more than 4GB of data but got {len} bytes"
        ))
    })
}

/// Status::to_header_map, which tonic keeps to itself.
fn status_trailers(status: &Status) -> Result<http::HeaderMap, Status> {
    let mut trailers = http::HeaderMap::with_capacity(3);
    status.add_header(&mut trailers)?;
    Ok(trailers)
}

fn ok_trailers() -> http::HeaderMap {
    let mut trailers = http::HeaderMap::with_capacity(1);
    trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
    trailers
}

/// Appends a message's frame header to `buf`, and the message too when it is
/// small; otherwise the message is returned to be sent after it.
fn put_message(buf: &mut BytesMut, message: Bytes) -> Option<Bytes> {
    let inline = message.len() <= INLINE_MESSAGE;
    buf.reserve(HEADER_LEN + if inline { message.len() } else { 0 });
    buf.put_u8(0);
    buf.put_u32(message.len() as u32);
    if !inline {
        return Some(message);
    }
    buf.put_slice(&message);
    None
}

/// Reads the request, as tonic does for a unary request: the first message,
/// once the client has sent all of it. The rest of the body is read too, so a
/// bad message after it fails the call and trailers join the metadata.
async fn read_request(
    request: http::Request<RequestBody>,
) -> Result<tonic::Request<Payload>, Status> {
    let (parts, mut body) = request.into_parts();
    let mut headers = parts.headers;
    let mut frames: Vec<Bytes> = Vec::new();
    let mut joined: Option<BytesMut> = None;
    let mut buffered = 0;
    let mut message = None;
    let mut rest = Rest::default();
    while let Some(frame) = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
        let frame = match frame.map_err(Status::from_error)?.into_data() {
            Ok(data) => data,
            Err(frame) => {
                if let Ok(trailers) = frame.into_trailers() {
                    headers.extend(trailers);
                }
                continue;
            }
        };
        if message.is_some() {
            rest.read(&frame)?;
            continue;
        }
        if frame.is_empty() {
            continue;
        }
        buffered += frame.len();
        match &mut joined {
            Some(joined) => joined.extend_from_slice(&frame),
            None => {
                frames.push(frame);
                if frames.len() > SMALL_FRAMES && buffered < frames.len() * SMALL_FRAME {
                    let mut buf = BytesMut::with_capacity(buffered);
                    for frame in frames.drain(..) {
                        buf.extend_from_slice(&frame);
                    }
                    joined = Some(buf);
                }
            }
        }
        if buffered < HEADER_LEN {
            continue;
        }
        let header = match &joined {
            Some(joined) => joined[..HEADER_LEN].try_into().unwrap(),
            None => prefix(&frames),
        };
        check_flag(header[0])?;
        let len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
        if buffered >= HEADER_LEN + len {
            let frames = match joined.take() {
                Some(joined) => vec![joined.freeze()],
                None => std::mem::take(&mut frames),
            };
            let last = frames.last().unwrap().clone();
            let used = last.len() - (buffered - HEADER_LEN - len);
            message = Some(take_message(frames, len));
            rest.read(&last[used..])?;
        }
    }
    let message = message.ok_or_else(|| {
        if buffered == 0 {
            Status::internal("Missing request message.")
        } else {
            Status::internal("Unexpected EOF decoding stream.")
        }
    })?;
    rest.end()?;
    Ok(tonic::Request::from_parts(
        MetadataMap::from_headers(headers),
        parts.extensions,
        message,
    ))
}

fn check_flag(flag: u8) -> Result<(), Status> {
    match flag {
        0 => Ok(()),
        1 => Err(Status::internal(
            "protocol error: received message with compressed-flag but no grpc-encoding was specified",
        )),
        flag => Err(Status::internal(format!(
            "protocol error: received message with invalid compression flag: {flag} (valid flags are 0 and 1), while sending request"
        ))),
    }
}

/// The messages after a unary request's first, which tonic decodes and drops.
#[derive(Default)]
struct Rest {
    header: [u8; HEADER_LEN],
    header_read: usize,
    body_left: usize,
}

impl Rest {
    fn read(&mut self, mut data: &[u8]) -> Result<(), Status> {
        while !data.is_empty() {
            if self.body_left > 0 {
                let n = self.body_left.min(data.len());
                self.body_left -= n;
                data = &data[n..];
                continue;
            }
            let n = (HEADER_LEN - self.header_read).min(data.len());
            self.header[self.header_read..self.header_read + n].copy_from_slice(&data[..n]);
            self.header_read += n;
            data = &data[n..];
            if self.header_read < HEADER_LEN {
                break;
            }
            self.header_read = 0;
            check_flag(self.header[0])?;
            self.body_left = u32::from_be_bytes(self.header[1..].try_into().unwrap()) as usize;
        }
        Ok(())
    }

    fn end(&self) -> Result<(), Status> {
        if self.header_read > 0 || self.body_left > 0 {
            return Err(Status::internal("Unexpected EOF decoding stream."));
        }
        Ok(())
    }
}

fn prefix(frames: &[Bytes]) -> [u8; HEADER_LEN] {
    let mut header = [0; HEADER_LEN];
    let mut at = 0;
    for frame in frames {
        let n = (HEADER_LEN - at).min(frame.len());
        header[at..at + n].copy_from_slice(&frame[..n]);
        at += n;
        if at == HEADER_LEN {
            break;
        }
    }
    header
}

/// The `len` message bytes after the frame header, still in their frames.
fn take_message(frames: Vec<Bytes>, len: usize) -> Payload {
    let mut skip = HEADER_LEN;
    let mut left = len;
    let mut parts = Vec::with_capacity(frames.len());
    for frame in frames {
        if left == 0 {
            break;
        }
        if skip >= frame.len() {
            skip -= frame.len();
            continue;
        }
        let end = frame.len().min(skip + left);
        parts.push(frame.slice(skip..end));
        left -= end - skip;
        skip = 0;
    }
    match parts.len() {
        0 => Payload::One(Bytes::new()),
        1 => Payload::One(parts.pop().expect("one part")),
        _ => Payload::Frames(parts, len),
    }
}

struct UnaryBody {
    // Counted when the body is first polled, as tonic's encoder counts it.
    sent: Option<(Arc<CallSizes>, u64)>,
    first: Option<Bytes>,
    rest: Option<Bytes>,
    trailers: Option<http::HeaderMap>,
}

impl Body for UnaryBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        let this = &mut *self;
        if let Some((sizes, len)) = this.sent.take() {
            sizes.sent.fetch_add(len, Ordering::Relaxed);
        }
        if let Some(data) = this.first.take().or_else(|| this.rest.take()) {
            return Poll::Ready(Some(Ok(Frame::data(data))));
        }
        Poll::Ready(this.trailers.take().map(|t| Ok(Frame::trailers(t))))
    }

    fn is_end_stream(&self) -> bool {
        self.first.is_none() && self.rest.is_none() && self.trailers.is_none()
    }
}

struct StreamBody {
    stream: ResponseStream,
    buf: BytesMut,
    sizes: Option<Arc<CallSizes>>,
    // A large message whose header went out in the last frame.
    held: Option<Bytes>,
    trailers: Option<http::HeaderMap>,
    done: bool,
}

impl Body for StreamBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        let this = &mut *self;
        if let Some(message) = this.held.take() {
            return Poll::Ready(Some(Ok(Frame::data(message))));
        }
        if this.done {
            return Poll::Ready(this.trailers.take().map(|t| Ok(Frame::trailers(t))));
        }
        let batch = &mut this.buf;
        loop {
            match Pin::new(&mut this.stream).poll_next(cx) {
                Poll::Ready(Some(Ok(message))) => {
                    if let Some(status) = too_large(message.len()) {
                        // tonic ends the call here, dropping what it buffered.
                        this.done = true;
                        batch.clear();
                        return Poll::Ready(Some(status_trailers(&status).map(Frame::trailers)));
                    }
                    if let Some(sizes) = &this.sizes {
                        sizes
                            .sent
                            .fetch_add(message.len() as u64, Ordering::Relaxed);
                    }
                    this.held = put_message(batch, message);
                    if this.held.is_some() || batch.len() >= STREAM_BATCH {
                        return Poll::Ready(Some(Ok(Frame::data(batch.split().freeze()))));
                    }
                }
                Poll::Ready(end) => {
                    this.done = true;
                    let status = match end {
                        Some(Err(status)) => status,
                        _ => Status::ok(""),
                    };
                    let trailers = match status_trailers(&status) {
                        Ok(trailers) => trailers,
                        Err(err) => return Poll::Ready(Some(Err(err))),
                    };
                    if batch.is_empty() {
                        return Poll::Ready(Some(Ok(Frame::trailers(trailers))));
                    }
                    this.trailers = Some(trailers);
                    return Poll::Ready(Some(Ok(Frame::data(batch.split().freeze()))));
                }
                Poll::Pending if batch.is_empty() => return Poll::Pending,
                Poll::Pending => return Poll::Ready(Some(Ok(Frame::data(batch.split().freeze())))),
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done && self.held.is_none() && self.trailers.is_none()
    }
}
