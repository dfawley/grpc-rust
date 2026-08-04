/*
 * Copyright 2025 gRPC authors.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */

use bytes::{Buf, Bytes, BytesMut};
use grpc::core::{RecvMessage, SendMessage};

/// Wraps raw payload bytes to implement [`SendMessage`].
pub(crate) struct RawBytesSendMessage(pub(crate) Bytes);

impl SendMessage for RawBytesSendMessage {
    fn encode(&self) -> Result<Box<dyn Buf + Send + Sync>, String> {
        Ok(Box::new(self.0.clone()))
    }
}

/// Receives raw payload bytes from [`grpc::client::RecvStream::recv`].
#[derive(Default, Debug)]
pub(crate) struct RawBytesRecvMessage {
    pub(crate) bytes: Option<Bytes>,
}

impl RecvMessage for RawBytesRecvMessage {
    fn decode(&mut self, data: &mut dyn Buf) -> Result<(), String> {
        self.bytes = Some(data.copy_to_bytes(data.remaining()));
        Ok(())
    }
}

/// Accumulates incoming body bytes and extracts 5-byte length-prefixed uncompressed gRPC frames.
#[derive(Debug, Default)]
pub(crate) struct FrameDeframer {
    buffer: BytesMut,
}

impl FrameDeframer {
    pub(crate) fn new() -> Self {
        Self {
            buffer: BytesMut::new(),
        }
    }

    pub(crate) fn push(&mut self, chunk: Bytes) {
        self.buffer.extend_from_slice(&chunk);
    }

    /// Attempts to extract the next payload frame.
    ///
    /// Returns `Err` if the compression flag is non-zero (compressed incoming data is unsupported).
    pub(crate) fn pop_frame(&mut self) -> Result<Option<Bytes>, tonic::Status> {
        if self.buffer.len() < 5 {
            return Ok(None);
        }

        let compressed_flag = self.buffer[0];
        if compressed_flag != 0 {
            return Err(tonic::Status::unimplemented(
                "Compressed incoming request data is not supported by tonic-grpc adapter",
            ));
        }

        let len = u32::from_be_bytes([
            self.buffer[1],
            self.buffer[2],
            self.buffer[3],
            self.buffer[4],
        ]) as usize;

        if self.buffer.len() < 5 + len {
            return Ok(None);
        }

        self.buffer.advance(5);
        let payload = self.buffer.split_to(len).freeze();
        Ok(Some(payload))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deframer_valid_uncompressed() {
        let mut deframer = FrameDeframer::new();
        let payload = Bytes::from_static(b"hello world");

        let mut framed = BytesMut::new();
        framed.extend_from_slice(&[0x00]); // uncompressed
        framed.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        framed.extend_from_slice(&payload);

        deframer.push(framed.freeze());
        let popped = deframer.pop_frame().unwrap().unwrap();
        assert_eq!(popped, payload);
    }

    #[test]
    fn test_deframer_compressed_error() {
        let mut deframer = FrameDeframer::new();
        let payload = Bytes::from_static(b"compressed payload");

        let mut framed = BytesMut::new();
        framed.extend_from_slice(&[0x01]); // compressed flag = 1
        framed.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        framed.extend_from_slice(&payload);

        deframer.push(framed.freeze());
        let res = deframer.pop_frame();
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
    }
}

