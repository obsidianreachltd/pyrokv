
use bytes::{Bytes, Buf, BytesMut, BufMut};

#[derive(Clone, Debug)]
pub struct KPacket {
  pub key: Bytes,
}
impl KPacket {
  pub const LEN: usize = 4;

  pub fn debug(&self) -> String {
    format!(
      "KPacket {{ key: {:?} }}",
      self.key,
    )
  }

  pub fn encode_into(&self, buf: &mut BytesMut) {
    buf.put_u32(self.key.len() as u32);
    buf.extend_from_slice(&self.key);
  }

  pub fn decode_from(b: &mut Bytes) -> Result<Self, crate::error::DecodeError> {
    use crate::error::DecodeError::*;
    if b.remaining() < Self::LEN { return Err(Underflow); }
    let key_len: usize = b.get_u32() as usize;
    let key: Bytes = b.copy_to_bytes(key_len);
    Ok(KPacket { key })
  }
}