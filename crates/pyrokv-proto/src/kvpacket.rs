use std::time;

use bytes::{Bytes, Buf, BytesMut, BufMut};

use crate::error::DecodeError;

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

  pub fn decode_from(b: &mut Bytes) -> Result<Self, DecodeError> {
    // Minimum bytes needed: expiry (4) + key_len (4) + value_len (4) + at least 0 key/value
    const MIN_LEN: usize = 4 + 4 + 4;

    // Tune these to your protocol constraints
    const MAX_KEY_LEN: usize = 16 * 1024;         // 16 KiB
    const MAX_VALUE_LEN: usize = 1 * 1024 * 1024; // 1 MiB

    if b.remaining() < MIN_LEN {
        return Err(DecodeError::Underflow);
    }

    let expiry: u32 = b.get_u32();

    let key_len = b.get_u32() as usize;
    if key_len > MAX_KEY_LEN {
      return Err(DecodeError::Malformed(format!("Key length {key_len} exceeds maximum of {MAX_KEY_LEN}")));
    }
    if b.remaining() < key_len + 4 {
        // +4 because we still need value_len u32 after the key
        return Err(DecodeError::Underflow);
    }
    let key: Bytes = b.copy_to_bytes(key_len);

    let value_len = b.get_u32() as usize;
    if value_len > MAX_VALUE_LEN {
      return Err(DecodeError::Malformed(format!("Value length {value_len} exceeds maximum of {MAX_VALUE_LEN}")));
    }
    if b.remaining() < value_len {
        return Err(DecodeError::Underflow);
    }
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
