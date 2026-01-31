use std::sync::Arc;

use tokio::sync::Mutex;
use wrongodb::{start_server, WrongoDB};

fn server_addr() -> String {
    if let Some(arg) = std::env::args().nth(1) {
        return arg;
    }
    if let Ok(addr) = std::env::var("WRONGO_ADDR") {
        if !addr.is_empty() {
            return addr;
        }
    }
    if let Ok(port) = std::env::var("WRONGO_PORT") {
        if !port.is_empty() {
            return format!("127.0.0.1:{port}");
        }
    }
    "127.0.0.1:27017".to_string()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = WrongoDB::open("test.db", ["name"])?;
    let db = Arc::new(Mutex::new(db));
    let addr = server_addr();
    start_server(&addr, db).await?;
    Ok(())
}
