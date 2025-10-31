use bytes::{Buf, Bytes, BytesMut};
use crate::header::Header;

#[derive(Clone, Debug)]
pub struct Frame {
  pub header: Header,
  pub payload: Bytes,
}

impl Frame {
  pub fn debug(&self) -> String {
    if self.payload.is_empty() {
      return format!(
        "Frame {{ header: {}, payload: <empty> }}",
        self.header.debug(),
      );
    } else if self.payload.len() < 4 {
      return format!(
        "Frame {{ header: {}, payload: {:x} }}",
        self.header.debug(),
        self.payload,
      );
    } else {
      return format!(
        "Frame {{ header: {}, payload: {:?} }}",
        self.header.debug(),
        &self.payload[4..],
      );
    }
  }

  pub fn encode_into(&self, buf: &mut BytesMut) {
    self.header.encode_into(buf);
    buf.extend_from_slice(&self.payload);
  }

  pub fn decode_from(b: &mut Bytes) -> Result<Self, crate::error::DecodeError> {
    // use crate::error::DecodeError::*;
    let header: Header = match Header::decode_from(b) {
      Ok(h) => h,
      Err(e) => return Err(e),
    };
    let payload: Bytes = b.copy_to_bytes(header.payload_length as usize);
    Ok(Frame { header, payload })
  }

  pub fn encoded_len(&self) -> usize {
    Header::LEN + self.payload.len()
  }
}