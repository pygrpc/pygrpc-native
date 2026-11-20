// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes};
use tonic::codec::{Codec, CompressionEncoding, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::server::Grpc;
use tonic::Status;

use super::stream::CallSizes;

/// Passes message bytes through untouched; Python owns the protobuf codec.
#[derive(Clone, Default)]
pub(super) struct BytesCodec {
    sizes: Option<Arc<CallSizes>>,
}

impl Codec for BytesCodec {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = BytesCodec;
    type Decoder = BytesCodec;

    fn encoder(&mut self) -> Self::Encoder {
        self.clone()
    }

    fn decoder(&mut self) -> Self::Decoder {
        self.clone()
    }
}

impl Encoder for BytesCodec {
    type Item = Bytes;
    type Error = Status;

    fn encode(&mut self, item: Bytes, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        // Responses aren't compressed, so this is the size on the wire.
        if let Some(sizes) = &self.sizes {
            sizes.sent.fetch_add(item.len() as u64, Ordering::Relaxed);
        }
        dst.put(item);
        Ok(())
    }
}

impl Decoder for BytesCodec {
    type Item = Bytes;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        let len = src.remaining();
        Ok(Some(src.copy_to_bytes(len)))
    }
}

pub(super) fn grpc(sizes: Option<Arc<CallSizes>>, max_receive: Option<usize>) -> Grpc<BytesCodec> {
    // Like grpcio servers: take compressed requests, but answer uncompressed.
    Grpc::new(BytesCodec { sizes })
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Deflate)
        // Stops decompressing at the limit. Uncompressed messages over it
        // never get here: RequestBody refuses them from their length prefix.
        .max_decoding_message_size(max_receive.unwrap_or(usize::MAX))
        .max_encoding_message_size(usize::MAX)
}
