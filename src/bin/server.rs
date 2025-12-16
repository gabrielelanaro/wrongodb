use std::sync::Arc;

use tokio::sync::Mutex;
use wrongodb::{start_server, WrongoDB};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = WrongoDB::open("test.db", ["name"], false)?;
    let db = Arc::new(Mutex::new(db));
    start_server("127.0.0.1:27017", db).await?;
    Ok(())
}
