use std::path::PathBuf;
use wrongodb::{Connection, ConnectionConfig};

fn main() {
    let db_path: PathBuf = "/tmp/test_recovery".into();

    // Clean up
    let _ = std::fs::remove_dir_all(&db_path);
    let _ = std::fs::remove_file(db_path.join("wrongo.wal"));

    println!("=== Creating database ===");
    {
        let conn = Connection::open(&db_path, ConnectionConfig::default().disable_auto_checkpoint())
            .unwrap();
        let mut session = conn.open_session();
        session.create("table:test").unwrap();

        let mut txn = session.transaction().unwrap();
        let txn_id = txn.as_mut().id();
        let mut cursor = txn.session_mut().open_cursor("table:test").unwrap();

        println!("Inserting key0");
        cursor.insert(b"key0", b"value0", txn_id).unwrap();
        txn.commit().unwrap();
    }

    println!("\n=== Reopening database (recovery) ===");
    {
        let conn = Connection::open(&db_path, ConnectionConfig::default().disable_auto_checkpoint())
            .unwrap();
        let mut session = conn.open_session();
        let mut cursor = session.open_cursor("table:test").unwrap();

        let mut txn = session.transaction().unwrap();
        let txn_id = txn.as_mut().id();

        println!("Trying to get key0");
        match cursor.get(b"key0", txn_id) {
            Ok(Some(v)) => println!("Found: {:?}", String::from_utf8_lossy(&v)),
            Ok(None) => println!("Not found (None)"),
            Err(e) => println!("Error: {}", e),
        }
    }
}
