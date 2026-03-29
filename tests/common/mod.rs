#![allow(dead_code)]

use std::net::TcpListener;
use std::time::{Duration, Instant};

pub fn reserve_local_addr() -> (String, TcpListener) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    (addr, listener)
}

pub async fn wait_for_server(addr: &str) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "server did not start listening on {addr} before timeout"
        );

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
