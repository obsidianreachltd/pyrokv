use dashmap::DashMap;
use pyrokv_proto::{Frame, Header, KPacket, KVPacket, error::{DecodeError, RequestError}};
use bytes::{Bytes, BytesMut};
use once_cell::sync::Lazy;

#[derive(Clone, Debug)]
struct ValueEntry {
  value: Bytes,
  expiry: u32,
}

static KV_STORE: Lazy<DashMap<Bytes, ValueEntry, ahash::RandomState>> =
    Lazy::new(|| DashMap::with_capacity_and_hasher(1_000_000, ahash::RandomState::default()));

pub fn build_error_response(request_id: u32, op: pyrokv_proto::OpCode, error: RequestError) -> Frame {
  Frame {
    header: Header{
      request_id,
      ty: pyrokv_proto::FrameType::Response,
      op,
      flags: pyrokv_proto::Flags::ERROR,
      payload_length: 1,
    },
    payload: error.to_bytes(),
  }
}

fn store_kv_packet(kv: &KVPacket) {
  KV_STORE.insert(
    kv.key.clone(),
    ValueEntry {
      value: kv.value.clone(),
      expiry: kv.expiry,
    },
  );
}

fn retrieve_kv_packet(key: &Bytes) -> Result<KVPacket, RequestError> {
  match KV_STORE.get(key) {
    Some(entry) => {
      let kv: KVPacket = KVPacket{
        expiry: entry.expiry,
        key: key.clone(),
        value: entry.value.clone(),
      };
      if kv.expired() {
        delete_kv_packet(key);
        Err(RequestError::KeyValueExpired)
      } else {
        Ok(kv)
      }
    },
    None => Err(RequestError::KeyNotFound),
  }
}

fn delete_kv_packet(key: &Bytes) {
  KV_STORE.remove(key);
}

pub fn handle_set_operation(frame: &Frame) -> Frame {
  // TODO: Implement Set operation handling
  let mut bytes: Bytes = frame.payload.clone();
  let kv: Result<KVPacket, DecodeError> = KVPacket::decode_from(&mut bytes);
  match kv {
    Ok(kv_packet) => {
      // Check if the packet is expired
      if kv_packet.expired() {
        return build_error_response(frame.header.request_id, frame.header.op, RequestError::KeyValueExpired);
      } else if frame.header.payload_too_large(4_000_000) {
        return build_error_response(frame.header.request_id, frame.header.op, RequestError::KeyValueTooLarge);
      }
      // Store in the KV store
      store_kv_packet(&kv_packet);
      // Build success response
      return Frame{
        header: Header{
          request_id: frame.header.request_id,
          ty: pyrokv_proto::FrameType::Response,
          op: frame.header.op,
          flags: pyrokv_proto::Flags::empty(),
          payload_length: 0,
        },
        payload: Bytes::new(),
      };
    }
    Err(e) => {
      eprintln!("Client {} sent invalid KVPacket: {}", frame.header.request_id, e);
      return build_error_response(frame.header.request_id, frame.header.op, RequestError::BadRequest);
    }
  }
}

pub fn handle_get_operation(frame: &Frame) -> Frame {
  // TODO: Implement Get operation handling
  let mut bytes: Bytes = frame.payload.clone();
  let kpacket: Result<KPacket, DecodeError> = KPacket::decode_from(&mut bytes);
  match kpacket {
    Ok(kpkt) => {
      // Retrieve from the KV store
      match retrieve_kv_packet(&kpkt.key) {
        Ok(kv_packet) => {
          // Build success response
          let mut payload: BytesMut = BytesMut::with_capacity(kv_packet.encoded_len());
          kv_packet.encode_into(&mut payload);
          return Frame{
            header: Header{
              request_id: frame.header.request_id,
              ty: pyrokv_proto::FrameType::Response,
              op: frame.header.op,
              flags: pyrokv_proto::Flags::empty(),
              payload_length: payload.len() as u32,
            },
            payload: payload.freeze(),
          };
        }
        Err(err) => {
          return build_error_response(frame.header.request_id, frame.header.op, err);
        }
      }
    }
    Err(e) => {
      eprintln!("Client {} sent invalid KPacket: {}", frame.header.request_id, e);
      return build_error_response(frame.header.request_id, frame.header.op, RequestError::BadRequest);
    }
  }
}

pub fn handle_delete_operation(frame: &Frame) -> Frame {
  let mut bytes: Bytes = frame.payload.clone();
  let kpacket: Result<KPacket, DecodeError> = KPacket::decode_from(&mut bytes);
  match kpacket {
    Ok(kpkt) => {
      // Delete from the KV store
      delete_kv_packet(&kpkt.key);
      // Build success response
      return Frame{
        header: Header{
          request_id: frame.header.request_id,
          ty: pyrokv_proto::FrameType::Response,
          op: frame.header.op,
          flags: pyrokv_proto::Flags::empty(),
          payload_length: 0,
        },
        payload: Bytes::new(),
      };
    }
    Err(e) => {
      eprintln!("Client {} sent invalid KPacket for delete: {}", frame.header.request_id, e);
      return build_error_response(frame.header.request_id, frame.header.op, RequestError::BadRequest);
    }
  }
}