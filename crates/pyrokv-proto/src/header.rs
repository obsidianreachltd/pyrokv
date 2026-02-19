use bytes::{Buf, BufMut, Bytes, BytesMut};

pub const MAGIC: u16 = 0x4D51; // 'MQ' in ASCII
pub const VERSION: u8 = 1;

#[repr(u8)]
#[derive(Copy, Clone, Debug)]
pub enum FrameType {
  Request = 0,
  Response = 1,
  Notification = 2
}

impl FrameType {
  pub fn from(byte: u8) -> Option<Self> {
    match byte {
      0 => Some(FrameType::Request),
      1 => Some(FrameType::Response),
      2 => Some(FrameType::Notification),
      _ => None,
    }
  }
  pub fn eq(&self, other: &FrameType) -> bool {
    match (self, other) {
      (FrameType::Request, FrameType::Request) => true,
      (FrameType::Response, FrameType::Response) => true,
      (FrameType::Notification, FrameType::Notification) => true,
      _ => false,
    }
  }
}

#[repr(u8)]
#[derive(Copy, Clone, Debug)]
pub enum OpCode {
  Set=0x01,
  Get=0x02,
  Del=0x03,
  MSet=0x04,
  MGet=0x05,
  Exists=0x06,
  Ping=0x10,
  Info=0x11,
  Auth=0x20,
}

impl OpCode {
  pub fn from(byte: u8) -> Option<Self> {
    match byte {
      0x01 => Some(OpCode::Set),
      0x02 => Some(OpCode::Get),
      0x03 => Some(OpCode::Del),
      0x04 => Some(OpCode::MSet),
      0x05 => Some(OpCode::MGet),
      0x06 => Some(OpCode::Exists),
      0x10 => Some(OpCode::Ping),
      0x11 => Some(OpCode::Info),
      0x20 => Some(OpCode::Auth),
      _ => None,
    }
  }
}

bitflags::bitflags! {
  #[derive(Clone, Debug)]
  pub struct Flags: u16 {
    const ERROR = 0b0000_0001;
    const BATCH = 0b0000_0010;
    const COMPRESSED = 0b0000_0100;
  }
}

#[derive(Clone, Debug)]
pub struct Header {
  pub op: OpCode,
  pub ty: FrameType,
  pub flags: Flags,
  pub request_id: u32,
  pub payload_length: u32,
}

impl Header {
  pub const LEN: usize = 15;

  pub fn debug(&self) -> String {
    format!(
      "Header {{ magic: 0x{:04X}, op: {:?}, ty: {:?}, flags: {:?}, request_id: {}, payload_length: {} }}",
      MAGIC,
      self.op,
      self.ty,
      self.flags,
      self.request_id,
      self.payload_length
    )
  }
  
  pub fn encode_into(&self, buf: &mut BytesMut) {
    buf.put_u16(MAGIC);
    buf.put_u8(VERSION);
    buf.put_u8(self.op as u8);
    buf.put_u8(self.ty as u8);
    buf.put_u16(self.flags.bits());
    buf.put_u32(self.request_id);
    buf.put_u32(self.payload_length);
  }

  pub fn decode_from(b: &mut Bytes) -> Result<Self, crate::error::DecodeError> {
    use crate::error::DecodeError::*;
    if b.remaining() < Self::LEN { return Err(Underflow); }
    let magic: u16 = b.get_u16();
    if magic != MAGIC { return Err(BadMagic(magic)); }
    let ver: u8 = b.get_u8();
    if ver != VERSION { return Err(BadVersion(ver)); }
    let op: OpCode = match b.get_u8() {
      0x01 => OpCode::Set,
      0x02 => OpCode::Get,
      0x03 => OpCode::Del,
      0x04 => OpCode::MSet,
      0x05 => OpCode::MGet,
      0x06 => OpCode::Exists,
      0x10 => OpCode::Ping,
      0x11 => OpCode::Info,
      0x20 => OpCode::Auth,
      x => return Err(BadOpCode(x)),
    };
    let ty: FrameType = match b.get_u8() { 0 => FrameType::Request, 1 => FrameType::Response, 2 => FrameType::Notification, x => return Err(BadType(x)) };
    let flags: Flags = Flags::from_bits_truncate(b.get_u16());
    let request_id: u32 = b.get_u32();
    let payload_length: u32 = b.get_u32();
    Ok(Header { op, ty, flags, request_id, payload_length })
  }

  pub fn payload_too_large(&self, max_size: u32) -> bool {
    self.payload_length > max_size
  }
}
