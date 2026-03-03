use std::path::PathBuf;

use wrongodb::{Connection, ConnectionConfig};

fn main() {
    let db_path: PathBuf = "/tmp/test_recovery.db".into();

    let _ = std::fs::remove_dir_all(&db_path);

    println!("=== Creating database ===");
    {
        let conn = Connection::open(&db_path, ConnectionConfig::default()).unwrap();
        let mut session = conn.open_session();
        let mut txn = session.transaction().unwrap();
        let txn_id = txn.as_ref().id();
        let mut cursor = txn.session_mut().open_cursor("table:test").unwrap();

        println!("Inserting key0");
        cursor.insert(b"key0", b"value0", txn_id).unwrap();
        txn.commit().unwrap();
    }

    println!("\n=== Reopening database (recovery) ===");
    {
        let conn = Connection::open(&db_path, ConnectionConfig::default()).unwrap();
        let mut session = conn.open_session();
        let mut cursor = session.open_cursor("table:test").unwrap();

        println!("Trying to get key0");
        let found = cursor.get(b"key0", 0).unwrap();
        println!("Found: {:?}", found);
    }
}
