use wrongodb::{Connection, ConnectionConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing session-level document flow...\n");

    let conn = Connection::open("test_checkpoint", ConnectionConfig::default())?;
    let mut session = conn.open_session();
    session.create("table:test")?;

    for i in 0..10 {
        let mut cursor = session.open_cursor("table:test")?;
        let key = format!("doc:{i}");
        let value = format!("value:{i}");
        cursor.insert(key.as_bytes(), value.as_bytes(), 0)?;
    }

    let mut cursor = session.open_cursor("table:test")?;
    let mut count = 0;
    while cursor.next(0)?.is_some() {
        count += 1;
    }

    println!("Inserted and scanned {} records", count);
    Ok(())
}
