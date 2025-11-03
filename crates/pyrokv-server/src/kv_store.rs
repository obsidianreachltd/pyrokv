use dashmap::DashMap;
use once_cell::sync::Lazy;
use ahash::RandomState;
use pyrokv_proto::{Frame, Header, KPacket, KVPacket, error::{DecodeError, RequestError}};
use bytes::{Bytes, BytesMut};
use std::io::{Read, Write};
use std::fs::{File, DirEntry, ReadDir, read_dir};

#[derive(Clone, Debug)]
struct ValueEntry {
  value: Bytes,
  expiry: u32,
}

#[derive(Clone, Debug)]
pub struct KvStore {
  storage_enabled: bool,
}

/// A single, global KV store shared by all connections.
/// DashMap is concurrent & sharded, so no external Mutex/RwLock required.
static KV: Lazy<DashMap<Bytes, ValueEntry, RandomState>> = Lazy::new(|| {
  // Presize to reduce shard growth under write-heavy load
  DashMap::with_capacity_and_hasher(1_000_000, RandomState::default())
});

impl KvStore {
  const DATA_DIR: &'static str = "/var/lib/data/pyrokv/";

  pub fn new(storage_enabled: bool) -> Self {
    let kv_store: Self = Self {
      storage_enabled,
    };
    if storage_enabled {
      kv_store.load_from_disk();
    }
    kv_store
  }

  fn load_from_disk(&self) {
    // Load all existing KV files from disk into the in-memory store
    if !self.storage_enabled {
      return;
    }
    let paths: ReadDir = match read_dir(Self::DATA_DIR) {
      Ok(p) => p,
      Err(e) => {
        eprintln!("Failed to read data directory {}: {}", Self::DATA_DIR, e);
        return;
      }
    };
    for entry in paths {
      let entry: DirEntry = entry.expect("Failed to read dir entry");
      let path: std::path::PathBuf = entry.path();
      if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("dat") {
        let mut file: File = File::open(&path).expect("Failed to open KV file");
        let mut buf: Vec<u8> = Vec::new();
        file.read_to_end(&mut buf).expect("Failed to read KV file");
        let mut bytes: Bytes = Bytes::from(buf);
        match KVPacket::decode_from(&mut bytes) {
          Ok(kv_packet) => {
            match self.store_kv_packet(&kv_packet) {
              Ok(_) => {},
              Err(e) => {
                eprintln!("Failed to store KV packet from file {:?}: {}", path, e);
              }
            }
          }
          Err(e) => {
            eprintln!("Failed to decode KVPacket from file {:?}: {}", path, e);
          }
        }
      }
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
      std::thread::spawn(move || {
        let filename: String = format!("{}/{}.dat", Self::DATA_DIR, hex::encode(&kv_clone.key));
        let mut file: File = File::create(&filename).expect("Failed to create KV file");
        // Encode the KVPacket to bytes
        let mut buf: BytesMut = BytesMut::with_capacity(kv_clone.encoded_len());
        kv_clone.encode_into(&mut buf);
        file.write_all(&buf).expect("Failed to write KV value to file");
      });
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
      let key_clone = key.clone();
      std::thread::spawn(move || {
        let filename: String = format!("{}/{}.dat", Self::DATA_DIR, hex::encode(&key_clone));
        match std::fs::remove_file(&filename) {
          Ok(_) => {},
          Err(e) => {
            eprintln!("Failed to delete KV file {}: {}", filename, e);
          }
        }
      });
    }
    KV.remove(key);
    Ok(true)
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