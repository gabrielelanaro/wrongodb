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

        println!("Inserting key0");
        txn.session_mut().create("table:test").unwrap();
        let mut cursor = txn.session_mut().open_cursor("table:test").unwrap();
        cursor
            .insert(b"key0", b"value0", txn.as_ref().id())
            .unwrap();
        txn.commit().unwrap();
    }

    println!("\n=== Reopening database (recovery) ===");
    {
        let conn = Connection::open(&db_path, ConnectionConfig::default()).unwrap();
        let session = conn.open_session();

        println!("Trying to get key0");
        let mut cursor = session.open_cursor("table:test").unwrap();
        let found = cursor.get(b"key0", 0).unwrap();
        println!("Found: {:?}", found);
    }
}
