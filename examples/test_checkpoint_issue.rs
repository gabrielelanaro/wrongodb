use tempfile::tempdir;
use wrongodb::{Connection, ConnectionConfig};

fn main() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("testdb");

    let conn = Connection::open(&db_path, ConnectionConfig::default()).unwrap();

    // Phase 1: Create and insert initial data
    println!("=== Phase 1: Create and insert 0-4 ===");
    {
        let mut session = conn.open_session();
        session.create_table("table:test").unwrap();

        session
            .with_transaction(|session| {
                let mut cursor = session.open_table_cursor("table:test")?;
                for i in 0..5 {
                    println!("Inserting: key{}", i);
                    let key = format!("key{i}");
                    let value = format!("value{i}");
                    cursor.insert(key.as_bytes(), value.as_bytes())?;
                }
                Ok(())
            })
            .unwrap();
    }

    // Phase 2: Open and insert more data
    println!("\n=== Phase 2: Open and insert 5-9 ===");
    {
        let mut session = conn.open_session();

        session
            .with_transaction(|session| {
                let mut cursor = session.open_table_cursor("table:test")?;
                for i in 5..10 {
                    println!("Inserting: key{}", i);
                    let key = format!("key{i}");
                    let value = format!("value{i}");
                    cursor.insert(key.as_bytes(), value.as_bytes())?;
                }
                Ok(())
            })
            .unwrap();
        // Don't checkpoint - simulate crash
    }

    drop(conn);

    // Phase 3: Reopen and check
    println!("\n=== Phase 3: Reopen and check ===");
    {
        let conn = Connection::open(&db_path, ConnectionConfig::default()).unwrap();
        let session = conn.open_session();
        let mut cursor = session.open_table_cursor("table:test").unwrap();

        for i in 0..10 {
            let key = format!("key{}", i);
            let result = cursor.get(key.as_bytes()).unwrap();
            match &result {
                Some(value) => println!("{} -> {:?}", key, String::from_utf8_lossy(value)),
                None => println!("{} -> MISSING", key),
            }
        }
    }
}
