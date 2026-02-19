use dashmap::DashMap;
use once_cell::sync::Lazy;
use ahash::RandomState;
use pyrokv_proto::{Frame, Header, KPacket, KVPacket, error::{DecodeError, RequestError}};
use bytes::{Bytes, BytesMut};
use std::sync::mpsc::{Sender};

use crate::file_manager::{FMOpCode, FMPacket};

#[derive(Clone, Debug)]
struct ValueEntry {
  value: Bytes,
  expiry: u32,
}

#[derive(Clone, Debug)]
pub struct KvStore {
  storage_enabled: bool,
  tx: Sender<FMPacket>,
}

/// A single, global KV store shared by all connections.
/// DashMap is concurrent & sharded, so no external Mutex/RwLock required.
static KV: Lazy<DashMap<Bytes, ValueEntry, RandomState>> = Lazy::new(|| {
  // Presize to reduce shard growth under write-heavy load
  DashMap::with_capacity_and_hasher(1_000_000, RandomState::default())
});

impl KvStore {
  pub fn new(storage_enabled: bool, tx: Sender<FMPacket>) -> Self {
    std::thread::spawn({
      let kv_store: KvStore = Self {
        storage_enabled,
        tx: tx.clone(),
      };
      move || {
        kv_store.garbage_collector();
      }
    });
    Self {
      storage_enabled,
      tx,
    }
  }

  pub fn load_data(&self, data: Vec<KVPacket>) {
    for kv in data {
      KV.insert(
        kv.key.clone(),
        ValueEntry {
          value: kv.value.clone(),
          expiry: kv.expiry,
        },
      );
    }
  }
  
  pub fn build_error_response(&self, request_id: u32, op: pyrokv_proto::OpCode, error: RequestError) -> Frame {
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

  fn store_kv_packet(&self,kv: &KVPacket) -> Result<bool, RequestError> {
    if self.storage_enabled {
      let kv_clone = kv.clone();
      self.tx.send(FMPacket { op: FMOpCode::Set, pkt: kv_clone }).expect("Failed to send KVPacket to FileManager");
    }
    KV.insert(
      kv.key.clone(),
      ValueEntry {
        value: kv.value.clone(),
        expiry: kv.expiry,
      },
    );
    Ok(true)
  }

  fn retrieve_kv_packet(&self,key: &Bytes) -> Result<KVPacket, RequestError> {
    match KV.get(key) {
      Some(entry) => {
        let kv: KVPacket = KVPacket{
          expiry: entry.expiry,
          key: key.clone(),
          value: entry.value.clone(),
        };
        if kv.expired() {
          match self.delete_kv_packet(key) {
            Ok(_) => {},
            Err(e) => {
              eprintln!("Failed to delete expired KV key {:?}: {}", key, e);
            }
          }
          Err(RequestError::KeyValueExpired)
        } else {
          Ok(kv)
        }
      },
      None => Err(RequestError::KeyNotFound),
    }
  }

  fn delete_kv_packet(&self, key: &Bytes) -> Result<bool, RequestError> {
    if self.storage_enabled {
      self.tx.send(FMPacket { op: FMOpCode::Delete, pkt: KVPacket { expiry: 0, key: key.clone(), value: Bytes::new() } }).expect("Failed to send delete packet to FileManager");
    }
    KV.remove(key);
    Ok(true)
  }

  fn garbage_collector(&self) {
    loop {
      std::thread::sleep(std::time::Duration::from_secs(10));
      let keys_to_delete: Vec<Bytes> = KV.iter()
        .filter_map(|entry| {
          let kv_packet = KVPacket {
            expiry: entry.expiry,
            key: entry.key().clone(),
            value: entry.value().value.clone(),
          };
          if kv_packet.expired() {
            Some(entry.key().clone())
          } else {
            None
          }
        })
        .collect();
      for key in keys_to_delete {
        match self.delete_kv_packet(&key) {
          Ok(_) => {},
          Err(e) => {
            eprintln!("Failed to delete expired KV key {:?}: {}", key, e);
          }
        }
      }
    }
  }

  pub fn handle_auth_operation(&self, frame: &Frame) -> Frame {
    let mut bytes: Bytes = frame.payload.clone();
    let password: Result<KPacket, DecodeError> = KPacket::decode_from(&mut bytes);
    // Check if we have a password set, and if not, return an error
    let expected_password: Bytes = std::env::var("PYROKV_AUTH_PASSWORD").unwrap_or_else(|_| "".to_string()).into();
    match password {
      Ok(password) => {
        if password.key.eq(&expected_password) {
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
        } else {
          return self.build_error_response(frame.header.request_id, frame.header.op, RequestError::Unauthorized);
        }
      }
      Err(e) => {
        eprintln!("Client {} sent invalid KPacket: {}", frame.header.request_id, e);
        return self.build_error_response(frame.header.request_id, frame.header.op, RequestError::BadRequest);
      }
    }
  }

  pub fn handle_set_operation(&self, frame: &Frame) -> Frame {
    let mut bytes: Bytes = frame.payload.clone();
    let kv: Result<KVPacket, DecodeError> = KVPacket::decode_from(&mut bytes);
    match kv {
      Ok(kv_packet) => {
        // Check if the packet is expired
        if kv_packet.expired() {
          return self.build_error_response(frame.header.request_id, frame.header.op, RequestError::KeyValueExpired);
        } else if frame.header.payload_too_large(4_000_000) {
          return self.build_error_response(frame.header.request_id, frame.header.op, RequestError::KeyValueTooLarge);
        }
        // Store in the KV store
        match self.store_kv_packet(&kv_packet) {
          Ok(_) => {
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
            return self.build_error_response(frame.header.request_id, frame.header.op, e);
          }
        }
      }
      Err(e) => {
        eprintln!("Client {} sent invalid KVPacket: {}", frame.header.request_id, e);
        return self.build_error_response(frame.header.request_id, frame.header.op, RequestError::BadRequest);
      }
    }
  }

  pub fn handle_get_operation(&self, frame: &Frame) -> Frame {
    let mut bytes: Bytes = frame.payload.clone();
    let kpacket: Result<KPacket, DecodeError> = KPacket::decode_from(&mut bytes);
    match kpacket {
      Ok(kpkt) => {
        // Retrieve from the KV store
        match self.retrieve_kv_packet(&kpkt.key) {
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
            return self.build_error_response(frame.header.request_id, frame.header.op, err);
          }
        }
      }
      Err(e) => {
        eprintln!("Client {} sent invalid KPacket: {}", frame.header.request_id, e);
        return self.build_error_response(frame.header.request_id, frame.header.op, RequestError::BadRequest);
      }
    }
  }

  pub fn handle_delete_operation(&self, frame: &Frame) -> Frame {
    let mut bytes: Bytes = frame.payload.clone();
    let kpacket: Result<KPacket, DecodeError> = KPacket::decode_from(&mut bytes);
    match kpacket {
      Ok(kpkt) => {
        // Delete from the KV store
        match self.delete_kv_packet(&kpkt.key) {
          Ok(_) => {
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
          },
          Err(e) => {
            return self.build_error_response(frame.header.request_id, frame.header.op, e);
          }
        }
      }
      Err(e) => {
        eprintln!("Client {} sent invalid KPacket for delete: {}", frame.header.request_id, e);
        return self.build_error_response(frame.header.request_id, frame.header.op, RequestError::BadRequest);
      }
    }
  }
}