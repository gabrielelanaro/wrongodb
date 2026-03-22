use std::path::PathBuf;

use wrongodb::{Connection, ConnectionConfig};

fn main() {
    let db_path: PathBuf = "/tmp/test_recovery.db".into();

    let _ = std::fs::remove_dir_all(&db_path);

    println!("=== Creating database ===");
    {
        let conn = Connection::open(&db_path, ConnectionConfig::default()).unwrap();
        let mut session = conn.open_session();
        session.create_table("table:test").unwrap();
        session
            .with_transaction(|session| {
                println!("Inserting key0");
                let mut cursor = session.open_table_cursor("table:test")?;
                cursor.insert(b"key0", b"value0")?;
                Ok(())
            })
            .unwrap();
    }

    println!("\n=== Reopening database (recovery) ===");
    {
        let conn = Connection::open(&db_path, ConnectionConfig::default()).unwrap();
        let session = conn.open_session();

        println!("Trying to get key0");
        let mut cursor = session.open_table_cursor("table:test").unwrap();
        let found = cursor.get(b"key0").unwrap();
        println!("Found: {:?}", found);
    }
}
