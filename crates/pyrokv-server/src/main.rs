use tokio::net::{TcpListener};
use std::{sync::{Arc, Mutex}, error::Error};

pub mod client_conn;
mod kv_store;
mod file_manager;
use client_conn::ClientConn;

use crate::file_manager::FMPacket;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
  // Check if storage enabled
  let storage_enabled: bool = std::env::var("PYROKV_STORAGE_ENABLED")
    .unwrap_or_else(|_| "false".into())
    .to_lowercase()
    .eq("true");

  // Check if auth enabled (if PYROKV_AUTH_PASSWORD is set)
  let auth_enabled: bool = std::env::var("PYROKV_AUTH_PASSWORD")
    .is_ok();

  let (tx, rx) = crossbeam_channel::bounded::<FMPacket>(10_000);

  // Initialize the KV store
  let kv_store: kv_store::KvStore = kv_store::KvStore::new(storage_enabled, tx);

    // Initialize and start the file manager
    if storage_enabled {
      let file_manager: file_manager::FileManager = match file_manager::FileManager::new(rx) {
        Ok(fm) => fm,
        Err(e) => {
          eprintln!("Failed to initialize FileManager: {}", e);
          return Err(e.into());
        }
      };

      match file_manager.load_from_disk() {
        Ok(d) => kv_store.load_data(d),
        Err(e) => eprintln!("Failed to load data from disk: {}", e),
      };

      std::thread::spawn(move || {
        file_manager.start_listener();
      });
    }

  // Get the port from environment variable or default to 8001
  let port: String = std::env::var("PYROKV_PORT").unwrap_or_else(|_| "8001".into());
  let listener: TcpListener = TcpListener::bind(format!("[::]:{}", port)).await?;
  println!("Server listening on port {}", port);

  // Shared connection registry
  let connections: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));
  let mut next_id: u32 = 1;

  loop {
    let (socket, _) = listener.accept().await?;
    let id: u32 = next_id;
    next_id += 1;
    let kv_store: kv_store::KvStore = kv_store.clone();

    let conn_list: Arc<Mutex<Vec<u32>>> = Arc::clone(&connections);

    tokio::spawn(async move {
      // Track connection
      {
        let mut list: std::sync::MutexGuard<'_, Vec<u32>> = conn_list.lock().unwrap();
        list.push(id);
      }

      // Handle connection
      let mut authenticated: bool = true;
      if auth_enabled {
        authenticated = false;
      }
      let mut conn: ClientConn = ClientConn::new(id, authenticated, socket, kv_store);
      if let Err(e) = conn.handle_connection().await {
        eprintln!("Client {} error: {}", id, e);
      }

      // Remove on disconnect
      {
        let mut list: std::sync::MutexGuard<'_, Vec<u32>> = conn_list.lock().unwrap();
        if let Some(pos) = list.iter().position(|x: &u32| *x == id) {
            list.remove(pos);
        }
      }
    });
  }
}
