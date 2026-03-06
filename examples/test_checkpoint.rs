use serde_json::json;
use wrongodb::{Connection, ConnectionConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing session-level document flow...\n");

    let conn = Connection::open("test_checkpoint", ConnectionConfig::default())?;
    let mut session = conn.open_session();

    for i in 0..10 {
        session.insert_one(
            "test",
            json!({"_id": format!("doc:{i}"), "value": format!("value:{i}")}),
        )?;
    }

    let count = session.find("test", None)?.len();

    println!("Inserted and scanned {} records", count);
    Ok(())
}
