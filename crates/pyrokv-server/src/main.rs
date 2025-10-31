use tokio::net::{TcpListener};
use std::{sync::{Arc, Mutex}, error::Error};

pub mod client_conn;
mod kv_store;
use client_conn::ClientConn;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
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

    let conn_list: Arc<Mutex<Vec<u32>>> = Arc::clone(&connections);

    tokio::spawn(async move {
      // Track connection
      {
        let mut list: std::sync::MutexGuard<'_, Vec<u32>> = conn_list.lock().unwrap();
        list.push(id);
        // println!("Active clients: {:?}", *list);
      }

      // Handle connection
      let mut conn: ClientConn = ClientConn::new(id, socket);
      if let Err(e) = conn.handle_connection().await {
        eprintln!("Client {} error: {}", id, e);
      }

      // Remove on disconnect
      {
        let mut list: std::sync::MutexGuard<'_, Vec<u32>> = conn_list.lock().unwrap();
        if let Some(pos) = list.iter().position(|x: &u32| *x == id) {
            list.remove(pos);
        }
        // println!("Client {} disconnected, remaining: {:?}", id, *list);
      }
    });
  }
}
