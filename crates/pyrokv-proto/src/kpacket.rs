
use bytes::{Bytes, Buf, BytesMut, BufMut};
use crate::error::{DecodeError};

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

  pub fn decode_from(b: &mut Bytes) -> Result<Self, DecodeError> {
    // Minimum bytes needed: key_len (4) + at least 0 key
    const MIN_LEN: usize = 4;

    // Tune this to your protocol constraints
    const MAX_KEY_LEN: usize = 16 * 1024; // 16 KiB

    if b.remaining() < MIN_LEN {
        return Err(DecodeError::Underflow);
    }

    let key_len = b.get_u32() as usize;
    if key_len > MAX_KEY_LEN {
      return Err(DecodeError::Malformed(format!("Key length {key_len} exceeds maximum of {MAX_KEY_LEN}")));
    }
    if b.remaining() < key_len {
        return Err(DecodeError::Underflow);
    }
    let key: Bytes = b.copy_to_bytes(key_len);

    Ok(KPacket { key })
  }
}