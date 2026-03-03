use wrongodb::{Connection, ConnectionConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing low-level Connection/Session/Cursor flow...\n");

    let conn = Connection::open("test_checkpoint", ConnectionConfig::default())?;
    let mut session = conn.open_session();

    let mut txn = session.transaction()?;
    let txn_id = txn.as_ref().id();
    let mut cursor = txn.session_mut().open_cursor("table:test")?;

    for i in 0..10 {
        let key = format!("doc:{i}");
        let value = format!("value:{i}");
        cursor.insert(key.as_bytes(), value.as_bytes(), txn_id)?;
    }

    txn.commit()?;

    let mut read_cursor = session.open_cursor("table:test")?;
    let mut count = 0usize;
    while let Some((_k, _v)) = read_cursor.next(0)? {
        count += 1;
    }

    println!("Inserted and scanned {} records", count);
    Ok(())
}
