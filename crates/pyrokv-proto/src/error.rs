use std::fmt::{Display, Formatter};
use bytes::Bytes;

#[derive(Debug)]
pub enum DecodeError {
  Underflow,
  BadMagic(u16),
  BadVersion(u8),
  BadType(u8),
  BadOpCode(u8),
}

impl Display for DecodeError { fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result { write!(f, "{self:?}") } }
impl std::error::Error for DecodeError {}

#[derive(Copy, Clone, Debug)]
pub enum RequestError {
  KeyNotFound = 0x01,
  KeyValueExpired = 0x02,
  KeyValueTooLarge = 0x03,
  BadRequest = 0x04,
  InternalError = 0x05,
}

impl Display for RequestError { fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result { write!(f, "{self:?}") } }
impl std::error::Error for RequestError {}
impl RequestError {
  pub fn to_bytes(&self) -> Bytes {
    Bytes::from(vec![*self as u8])
  }
}