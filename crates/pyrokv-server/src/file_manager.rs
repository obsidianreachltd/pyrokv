use std::sync::mpsc::{Receiver};
use pyrokv_proto::KVPacket;
use bytes::{Bytes, BytesMut};
use std::io::{Read, Write, Error};
use std::fs::{File, DirEntry, ReadDir, read_dir};

#[derive(PartialEq, Debug)]
pub enum FMOpCode {
  Delete = 0x01,
  Set = 0x02,
}

pub struct FMPacket {
  pub op: FMOpCode,
  pub pkt: KVPacket,
}

#[derive(Debug)]
pub struct FileManager {
  rx: Receiver<FMPacket>,
}

impl FileManager {
  const DATA_DIR: &'static str = "/var/lib/data/pyrokv/";

  pub fn new(rx: Receiver<FMPacket>) -> Self {
    Self { rx }
  }

  pub fn load_from_disk(&self) -> Result<Vec<KVPacket>, Error> {
    // Load all existing KV files from disk into the in-memory store
    let paths: ReadDir = match read_dir(Self::DATA_DIR) {
      Ok(paths) => paths,
      Err(e) => {
        eprintln!("Failed to read data directory {:?}: {}", Self::DATA_DIR, e);
        return Err(e);
      }
    };
    let mut packets: Vec<KVPacket> = Vec::new();
    for entry in paths {
      let entry: DirEntry = entry?;
      let path: std::path::PathBuf = entry.path();
      if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("dat") {
        let mut file: File = File::open(&path)?;
        let mut buf: Vec<u8> = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut bytes: Bytes = Bytes::from(buf);
        match KVPacket::decode_from(&mut bytes) {
          Ok(kv_packet) => {
            packets.push(kv_packet);
          },
          Err(e) => {
            eprintln!("Failed to decode KVPacket from file {:?}: {}", path, e);
          }
        }
      }
    }
    Ok(packets)
  }

  pub fn start_listener(&self) {
    while let Ok(packet) = self.rx.recv() {
      let filename: String = format!("{}/{}.dat", Self::DATA_DIR, hex::encode(&packet.pkt.key));
      if packet.op == FMOpCode::Delete {
        // Delete operation
        match std::fs::remove_file(&filename) {
          Ok(_) => {},
          Err(e) => {
            eprintln!("Failed to delete KV file {}: {}", filename, e);
          }
        }
        continue;
      } else if packet.op == FMOpCode::Set {
        // Set operation
        let mut file: File = File::create(&filename).expect("Failed to create KV file");
        // Encode the KVPacket to bytes
        let mut buf: BytesMut = BytesMut::with_capacity(packet.pkt.encoded_len());
        packet.pkt.encode_into(&mut buf);
        file.write_all(&buf).expect("Failed to write KV value to file");
        continue;
      }
    }
  }
}