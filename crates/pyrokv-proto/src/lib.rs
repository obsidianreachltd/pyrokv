use bytes::{Bytes, Buf, BytesMut, BufMut};
use crate::error::DecodeError::*;

pub mod frame;
pub mod header;
pub mod kpacket;
pub mod kvpacket;
pub mod error;

pub use header::{Header, FrameType, OpCode, Flags};
pub use frame::Frame;
pub use kpacket::KPacket;
pub use kvpacket::KVPacket;

pub fn decode_header(buf: &mut Bytes) -> Result<Header, crate::error::DecodeError> {
  if buf.remaining() < 12 {
    return Err(Underflow);
  }

  let op: OpCode = match OpCode::from(buf[3]) {
    Some(op) => op,
    None => return Err(BadOpCode(buf[3]))
  };

  let ty: FrameType = match FrameType::from(buf[4]) {
    Some(ty) => ty,
    None => return Err(BadType(buf[4]))
  };

  let flags: Flags = Flags::from_bits_truncate(buf.get_u16());
  let request_id: u32 = buf.get_u32();
  let payload_length: u32 = buf.get_u32();

  Ok(Header {
    op,
    ty,
    flags,
    request_id,
    payload_length,
  })
}

pub fn encode_get_request(request_id: u32, key: &[u8]) -> Frame {
  let mut payload: BytesMut = BytesMut::with_capacity(4 + key.len());
  payload.put_u32(key.len() as u32);
  payload.put_slice(key);

  Frame {
    header: Header {
      op: OpCode::Get,
      ty: FrameType::Request,
      flags: Flags::empty(),
      request_id,
      payload_length: payload.len() as u32,
    },
    payload: payload.freeze(),
  }
}

pub fn encode_get_response(request_id: u32, key: &[u8], value: &[u8], expiry: u32) -> Frame {
  // Build the KV packet first
  let pkt: KVPacket = KVPacket {
    expiry,
    key: Bytes::copy_from_slice(key),
    value: Bytes::copy_from_slice(value),
  };

  // expiry (4) + key_len (4) + key + val_len (4) + value
  let mut payload: BytesMut = BytesMut::with_capacity(4 + 4 + key.len() + 4 + value.len());
  pkt.encode_into(&mut payload);

  Frame {
    header: Header {
      op: OpCode::Get,
      ty: FrameType::Response,
      flags: Flags::empty(),
      request_id,
      payload_length: payload.len() as u32,
    },
    payload: payload.freeze(),
  }
}

pub fn decode_get_response(frame: &Frame) -> Result<Option<KVPacket>, crate::error::DecodeError> {
  let mut b: Bytes = frame.payload.clone();

  // expiry
  if b.remaining() < 4 { return Err(Underflow); }
  let expiry: u32 = b.get_u32();

  // key
  if b.remaining() < 4 { return Err(Underflow); }
  let key_len: u32 = b.get_u32();
  if b.remaining() < key_len as usize { return Err(Underflow); }
  let key: Bytes = b.copy_to_bytes(key_len as usize);

  // value
  if b.remaining() < 4 { return Err(Underflow); }
  let value_len: u32 = b.get_u32();
  // convention: value_len == 0 => not found
  if value_len == 0 {
      return Ok(None);
  }
  if b.remaining() < value_len as usize { return Err(Underflow); }
  let value = b.copy_to_bytes(value_len as usize);

  Ok(Some(KVPacket { expiry, key, value }))
}


pub fn encode_set_request(request_id: u32, key: &[u8], value: &[u8], expiry: u32) -> Frame {
  // Build the KV packet first
  let pkt: KVPacket = KVPacket {
    expiry,
    key: Bytes::copy_from_slice(key),
    value: Bytes::copy_from_slice(value),
  };

  // expiry (4) + key_len (4) + key + val_len (4) + value
  let mut payload: BytesMut = BytesMut::with_capacity(4 + 4 + key.len() + 4 + value.len());
  pkt.encode_into(&mut payload);

  Frame {
    header: Header {
      op: OpCode::Set,
      ty: FrameType::Request,
      flags: Flags::empty(),
      request_id,
      payload_length: payload.len() as u32,
    },
    payload: payload.freeze(),
  }
}
