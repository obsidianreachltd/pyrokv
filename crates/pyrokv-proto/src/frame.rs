use bytes::{Buf, Bytes, BytesMut};
use crate::{error::DecodeError, header::Header};

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
    if b.remaining() < header.payload_length as usize {
      return Err(DecodeError::Underflow);
    }
    let payload: Bytes = b.copy_to_bytes(header.payload_length as usize);
    Ok(Frame { header, payload })
  }

  pub fn encoded_len(&self) -> usize {
    Header::LEN + self.payload.len()
  }

    /// If enough bytes exist, peek the payload length without consuming anything.
  pub fn peek_payload_len(buf: &BytesMut) -> Option<usize> {
    if buf.len() < Header::LEN {
      return None;
    }
    // header layout in your code:
    // version u8, op u8, expiry u64, payload_len u32  => 1+1+8+4 = 14
    // payload_len starts at offset 10
    let payload_len = u32::from_be_bytes([
      buf[10], buf[11], buf[12], buf[13],
    ]) as usize;
    Some(payload_len)
  }

  /// Try decode a single frame from the front of the buffer.
  /// Returns:
  /// - Ok(Some(frame)) if a full frame was decoded and removed from buf
  /// - Ok(None) if not enough bytes are available yet
  /// - Err(e) if protocol is invalid
  pub fn try_decode_from_bytesmut(
    buf: &mut BytesMut,
    max_payload: usize,
  ) -> Result<Option<Frame>, DecodeError> {
    if buf.len() < Header::LEN {
      return Ok(None);
    }

    let payload_len = Self::peek_payload_len(buf).ok_or(DecodeError::Underflow)?;

    if payload_len > max_payload {
      return Err(DecodeError::InvalidLength {
        expected: max_payload as u32,
        got: payload_len as u32,
      });
    }

    let frame_len = Header::LEN + payload_len;
    if buf.len() < frame_len {
      return Ok(None);
    }

    // Split off exactly this frame; advance the original buffer.
    let frame_bytes = buf.split_to(frame_len).freeze();

    // Now decode from immutable bytes
    let mut b = frame_bytes.clone();
    Frame::decode_from(&mut b).map(Some)
  }
}