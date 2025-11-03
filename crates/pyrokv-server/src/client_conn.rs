use pyrokv_proto::{Frame, FrameType};
use pyrokv_proto::error::RequestError;
use tokio::net::{TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use bytes::{BytesMut};
use std::io;

use crate::kv_store::KvStore;

#[derive(Debug)]
pub struct ClientConn {
  pub id: u32,
  socket: TcpStream,
  kv_store: KvStore,
}

impl ClientConn {
  pub fn new(id: u32, socket: TcpStream, kv_store: KvStore) -> Self {
    Self { id, socket, kv_store }
  }

  pub async fn handle_connection(&mut self) -> io::Result<()> {
    // Split into read/write halves so we can use both independently
    let (mut reader, mut writer) = self.socket.split();
    let mut buffer: [u8; 1024] = [0u8; 1024];

    loop {
      let n: usize = reader.read(&mut buffer).await?;
      if n == 0 {
        break;
      }
      // Decode the packet
      let mut bytes: bytes::Bytes = bytes::Bytes::copy_from_slice(&buffer[..n]);
      match Frame::decode_from(&mut bytes) {
        Ok(frame) => {
          if frame.header.ty.eq(&FrameType::Request) {
            if frame.header.payload_length != frame.payload.len() as u32 {
              eprintln!("Client {} sent frame with mismatched payload length", self.id);
              let res: Frame = self.kv_store.build_error_response(frame.header.request_id, frame.header.op, RequestError::BadRequest);
              let mut resp_buf: BytesMut = BytesMut::with_capacity(res.encoded_len());
              res.encode_into(&mut resp_buf);
              writer.write_all(&resp_buf).await?;
              continue;
            }
            match frame.header.op {
              pyrokv_proto::OpCode::Set => {
                // Handle Set operation
                let res: Frame = self.kv_store.handle_set_operation(&frame);
                let mut resp_buf: BytesMut = BytesMut::with_capacity(res.encoded_len());
                res.encode_into(&mut resp_buf);
                writer.write_all(&resp_buf).await?;
              }
              pyrokv_proto::OpCode::Get => {
                // Handle Get operation
                let res: Frame = self.kv_store.handle_get_operation(&frame);
                let mut resp_buf: BytesMut = BytesMut::with_capacity(res.encoded_len());
                res.encode_into(&mut resp_buf);
                writer.write_all(&resp_buf).await?;
              }
              pyrokv_proto::OpCode::Del => {
                // Respond to Delete
                let res: Frame = self.kv_store.handle_delete_operation(&frame);
                let mut resp_buf: BytesMut = BytesMut::with_capacity(res.encoded_len());
                res.encode_into(&mut resp_buf);
                writer.write_all(&resp_buf).await?;
              }
              _ => {
                // Handle other opcodes as needed
              }
            }
          } else {
            // Handle non-request frames if necessary
            let res: Frame = self.kv_store.build_error_response(frame.header.request_id, frame.header.op, RequestError::BadRequest);
            let mut resp_buf: BytesMut = BytesMut::with_capacity(res.encoded_len());
            res.encode_into(&mut resp_buf);
            writer.write_all(&resp_buf).await?;
          }
        }
        Err(e) => {
          eprintln!("Client {} sent invalid frame: {}", self.id, e);
        }
      }
    }
    Ok(())
  }
}