use bytes::{Bytes, BytesMut};
use crossbeam_channel::Receiver;
use pyrokv_proto::KVPacket;
use std::fs::{read_dir, DirEntry, File};
use std::io::{Error, Read, Write};
use std::io::BufWriter;
use std::path::PathBuf;

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum FMOpCode {
  Delete = 0x01,
  Set = 0x02,
}

#[derive(Clone)]
pub struct FMPacket {
  pub op: FMOpCode,
  pub pkt: KVPacket,
}

#[derive(Debug)]
pub struct FileManager {
  rx: Receiver<FMPacket>,
}

impl FileManager {
  const DATA_DIR: &'static str = "/var/lib/pyrokv/data";

  pub fn new(rx: Receiver<FMPacket>) -> Result<Self, Error> {
    // Ensure persistence directory exists.
    std::fs::create_dir_all(Self::DATA_DIR)?;
    Ok(Self { rx })
  }

  fn key_path(key: &Bytes) -> (String, String) {
    let hex_key: String = hex::encode(key);

    // shard = first byte => first 2 hex chars
    let shard: &str = &hex_key[..2];

    let dir: String = format!("{}/{}", Self::DATA_DIR, shard);
    let file: String = format!("{}/{}.dat", dir, hex_key);

    (dir, file)
  }

  pub fn load_from_disk(&self) -> Result<Vec<KVPacket>, Error> {
    let mut packets: Vec<KVPacket> = Vec::new();
    let mut num_loaded: i32 = 0;

    for shard_entry in read_dir(Self::DATA_DIR)? {
      let shard_entry: DirEntry = shard_entry?;
      let shard_path: PathBuf = shard_entry.path();

      if !shard_path.is_dir() {
        continue;
      }

      for entry in read_dir(&shard_path)? {
        let entry: DirEntry = entry?;
        let path: PathBuf = entry.path();

        if !(path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("dat")) {
          continue;
        }

        let mut file: File = match File::open(&path) {
            Ok(f) => f,
            Err(e) => {
              eprintln!("Failed to open KV file {:?}: {}", path, e);
              continue;
            }
        };

        let mut buf: Vec<u8> = Vec::new();
        if let Err(e) = file.read_to_end(&mut buf) {
          eprintln!("Failed to read KV file {:?}: {}", path, e);
          continue;
        }

        let mut bytes: Bytes = Bytes::from(buf);
        match KVPacket::decode_from(&mut bytes) {
          Ok(pkt) => {
              packets.push(pkt);
              num_loaded += 1;
          }
          Err(e) => eprintln!("Failed to decode KVPacket from {:?}: {}", path, e),
        }
      }
    }

    println!("Loaded {} KV files from disk", num_loaded);
    Ok(packets)
  }

  pub fn start_listener(&self) {
    // Reusable encode buffer to reduce allocations on every SET
    let mut encode_buf: BytesMut = BytesMut::with_capacity(64 * 1024);

    // Optional batching vector (helps throughput under bursts)
    let mut batch: Vec<FMPacket> = Vec::with_capacity(256);

    while let Ok(first) = self.rx.recv() {
      batch.clear();
      batch.push(first);

      // Drain a burst without blocking
      while batch.len() < 256 {
        match self.rx.try_recv() {
          Ok(pkt) => batch.push(pkt),
          Err(_) => break,
        }
      }

      for packet in &batch {
        let (dir, filename) = Self::key_path(&packet.pkt.key);

        match packet.op {
          FMOpCode::Delete => {
            if let Err(e) = std::fs::remove_file(&filename) {
              // It's okay if the file doesn't exist; log others
              if e.kind() != std::io::ErrorKind::NotFound {
                eprintln!("Failed to delete KV file {}: {}", filename, e);
              }
            }
          }
          FMOpCode::Set => {
            // Ensure shard directory exists
            if let Err(e) = std::fs::create_dir_all(&dir) {
              eprintln!("Failed to create shard dir {}: {}", dir, e);
              continue;
            }
            // Encode packet
            encode_buf.clear();
            encode_buf.reserve(packet.pkt.encoded_len());
            packet.pkt.encode_into(&mut encode_buf);

            // Write packet
            match File::create(&filename) {
              Ok(file) => {
                let mut writer = BufWriter::new(file);
                if let Err(e) = writer.write_all(&encode_buf) {
                  eprintln!("Failed to write KV file {}: {}", filename, e);
                }
                // BufWriter flush on drop is usually fine; explicit flush if you want durability guarantees:
                let _ = writer.flush();
              }
              Err(e) => {
                eprintln!("Failed to create KV file {}: {}", filename, e);
              }
            }
          }
        }
      }
    }

    // Channel disconnected: exit listener cleanly.
    println!("FileManager listener exiting (channel closed).");
  }
}
