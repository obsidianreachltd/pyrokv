use bytes::BytesMut;
use pyrokv_proto::error::RequestError;
use pyrokv_proto::{Frame, FrameType};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::kv_store::KvStore;

#[derive(Debug)]
pub struct ClientConn {
  pub id: u32,
  authenticated: bool,
  socket: TcpStream,
  kv_store: KvStore,
}

impl ClientConn {
  pub fn new(id: u32, authenticated: bool, socket: TcpStream, kv_store: KvStore) -> Self {
    Self {
      id,
      authenticated,
      socket,
      kv_store,
    }
  }

  /// Handle a single decoded frame and return a response frame.
  /// This keeps your business logic out of the TCP buffering/decoding loop.
  fn handle_frame(
    id: u32,
    authenticated: &mut bool,
    kv_store: &KvStore,
    frame: Frame,
  ) -> Frame {
    if !frame.header.ty.eq(&FrameType::Request) {
      return kv_store.build_error_response(
        frame.header.request_id,
        frame.header.op,
        RequestError::BadRequest,
      );
    }

    if frame.header.payload_length != frame.payload.len() as u32 {
      eprintln!("Client {} sent mismatched payload length", id);
      return kv_store.build_error_response(
        frame.header.request_id,
        frame.header.op,
        RequestError::BadRequest,
      );
    }

    match frame.header.op {
      pyrokv_proto::OpCode::Auth => {
        let res = kv_store.handle_auth_operation(&frame);

        if !res.header.flags.contains(pyrokv_proto::Flags::ERROR) {
          *authenticated = true;
        }

        res
      }

      pyrokv_proto::OpCode::Set => {
        if !*authenticated {
          return kv_store.build_error_response(
            frame.header.request_id,
            frame.header.op,
            RequestError::Unauthorized,
          );
        }
        kv_store.handle_set_operation(&frame)
      }

      pyrokv_proto::OpCode::Get => {
        if !*authenticated {
          return kv_store.build_error_response(
            frame.header.request_id,
            frame.header.op,
            RequestError::Unauthorized,
          );
        }
        kv_store.handle_get_operation(&frame)
      }

      pyrokv_proto::OpCode::Del => {
        if !*authenticated {
          return kv_store.build_error_response(
            frame.header.request_id,
            frame.header.op,
            RequestError::Unauthorized,
          );
        }
        kv_store.handle_delete_operation(&frame)
      }

      _ => {
        if !*authenticated {
          return kv_store.build_error_response(
            frame.header.request_id,
            frame.header.op,
            RequestError::Unauthorized,
          );
        }

        kv_store.build_error_response(
          frame.header.request_id,
          frame.header.op,
          RequestError::BadRequest,
        )
      }
    }
  }


  /// Try decode exactly one frame from the front of `buf`.
  /// - Ok(Some(frame)) => decoded and removed from buf
  /// - Ok(None)  => not enough bytes yet
  /// - Err(e)    => invalid protocol / frame
  fn try_decode_one(buf: &mut BytesMut) -> Result<Option<Frame>, pyrokv_proto::error::DecodeError> {
    // Keep these local so we don’t depend on internal proto module exports.
    // Your header is: version(u8) + op(u8) + expiry(u64) + payload_len(u32) = 14 bytes.
    const HEADER_LEN: usize = 14;
    const PAYLOAD_LEN_OFFSET: usize = 10; // payload_len starts after 1+1+8 bytes
    const MAX_PAYLOAD: usize = 1024 * 1024; // 1 MiB safety cap (tune as you like)

    if buf.len() < HEADER_LEN {
      return Ok(None);
    }

    let payload_len = u32::from_be_bytes([
      buf[PAYLOAD_LEN_OFFSET],
      buf[PAYLOAD_LEN_OFFSET + 1],
      buf[PAYLOAD_LEN_OFFSET + 2],
      buf[PAYLOAD_LEN_OFFSET + 3],
    ]) as usize;

    if payload_len > MAX_PAYLOAD {
      // Treat as invalid protocol; caller will close the connection.
      return Err(pyrokv_proto::error::DecodeError::Malformed(format!(
      "Payload length {payload_len} exceeds maximum allowed"
      )));
    }

    let frame_len = HEADER_LEN + payload_len;
    if buf.len() < frame_len {
      return Ok(None);
    }

    // Split off exactly one frame (no copy), then decode from that slice.
    let frame_bytes = buf.split_to(frame_len).freeze();
    let mut b = frame_bytes;
    Frame::decode_from(&mut b).map(Some)
  }

  pub async fn handle_connection(&mut self) -> std::io::Result<()> {
    let id = self.id;
    let kv_store = &self.kv_store;
    let authenticated = &mut self.authenticated;

    let (mut reader, mut writer) = self.socket.split();

    let mut buf = BytesMut::with_capacity(8 * 1024);

    loop {
      buf.reserve(8 * 1024);
      let n = reader.read_buf(&mut buf).await?;
      if n == 0 {
        break;
      }

      loop {
        match Self::try_decode_one(&mut buf) {
          Ok(Some(frame)) => {
            let res: Frame = Self::handle_frame(id, authenticated, kv_store, frame);

            let mut resp_buf = BytesMut::with_capacity(res.encoded_len());
            res.encode_into(&mut resp_buf);
            writer.write_all(&resp_buf).await?;
          }
          Ok(None) => break,
          Err(e) => {
            eprintln!("Client {} invalid frame stream: {}", id, e);
            return Ok(());
          }
        }
      }
    }

    Ok(())
  }
}
