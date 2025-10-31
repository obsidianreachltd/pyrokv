use std::time;

use bytes::{Bytes, Buf, BytesMut, BufMut};

#[derive(Clone, Debug)]
pub struct KVPacket {
  pub expiry: u32,
  pub key: Bytes,
  pub value: Bytes,
}

impl KVPacket {
  pub const LEN: usize = 4;

  pub fn debug(&self) -> String {
    format!(
      "KVPacket {{ expiry: {}, key: {:?}, value: {:?} }}",
      self.expiry,
      self.key,
      self.value
    )
  }

  pub fn encode_into(&self, buf: &mut BytesMut) {
    buf.put_u32(self.expiry);
    buf.put_u32(self.key.len() as u32);
    buf.extend_from_slice(&self.key);
    buf.put_u32(self.value.len() as u32);
    buf.extend_from_slice(&self.value);
  }

  pub fn decode_from(b: &mut Bytes) -> Result<Self, crate::error::DecodeError> {
    use crate::error::DecodeError::*;
    if b.remaining() < Self::LEN { return Err(Underflow); }
    let expiry: u32 = b.get_u32();
    let key_len: usize = b.get_u32() as usize;
    let key: Bytes = b.copy_to_bytes(key_len);
    let value_len: usize = b.get_u32() as usize;
    let value: Bytes = b.copy_to_bytes(value_len);
    Ok(KVPacket { expiry, key, value })
  }

  pub fn expired(&self) -> bool {
    let current_unix_ts: u32 = time::SystemTime::now()
      .duration_since(time::UNIX_EPOCH)
      .unwrap()
      .as_secs() as u32; 
    if self.expiry == 0 {
      return false;
    }
    return current_unix_ts >= self.expiry;
  }

  pub fn encoded_len(&self) -> usize {
    4 + 4 + self.key.len() + 4 + self.value.len()
  }
}
