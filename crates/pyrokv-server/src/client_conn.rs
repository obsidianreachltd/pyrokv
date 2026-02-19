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
  fn try_decode_one(
    buf: &mut BytesMut,
  ) -> Result<Option<Frame>, pyrokv_proto::error::DecodeError> {
    use pyrokv_proto::error::DecodeError;

    const MAX_PAYLOAD: usize = 1024 * 1024; // 1 MiB (tune)

    // Need at least a full header before we can know payload length
    if buf.len() < pyrokv_proto::Header::LEN {
      return Ok(None);
    }

    // Peek header without consuming the real buffer
    let mut header_bytes = buf[..pyrokv_proto::Header::LEN].to_vec();
    let mut b = bytes::Bytes::from(std::mem::take(&mut header_bytes));

    let header = pyrokv_proto::Header::decode_from(&mut b)?;

    let payload_len = header.payload_length as usize;
    if payload_len > MAX_PAYLOAD {
      return Err(DecodeError::Malformed(format!(
        "Payload length {} exceeds maximum allowed ({})",
        payload_len, MAX_PAYLOAD
      )));
    }

    let frame_len = pyrokv_proto::Header::LEN + payload_len;
    if buf.len() < frame_len {
      return Ok(None);
    }

    // Now we have the full frame: split and decode for real (no copy)
    let frame_bytes = buf.split_to(frame_len).freeze();
    let mut fb = frame_bytes;
    Frame::decode_from(&mut fb).map(Some)
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
