use tempfile::tempdir;
use wrongodb::{Connection, ConnectionConfig};

fn main() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("testdb");

    let conn = Connection::open(&db_path, ConnectionConfig::default().disable_auto_checkpoint())
        .unwrap();

    // Phase 1: Create and insert initial data
    println!("=== Phase 1: Create and insert 0-4 ===");
    {
        let mut session = conn.open_session();
        session.create("table:test").unwrap();

        let mut txn = session.transaction().unwrap();
        let txn_id = txn.as_mut().id();
        let mut cursor = txn.session_mut().open_cursor("table:test").unwrap();

        for i in 0..5 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            println!("Inserting: {}", key);
            cursor.insert(key.as_bytes(), value.as_bytes(), txn_id).unwrap();
        }

        txn.commit().unwrap();
    }

    println!("Checkpointing...");
    conn.checkpoint_all().unwrap();
    println!("Checkpoint done");

    // Phase 2: Open and insert more data
    println!("\n=== Phase 2: Open and insert 5-9 ===");
    {
        let mut session = conn.open_session();

        let mut txn = session.transaction().unwrap();
        let txn_id = txn.as_mut().id();
        let mut cursor = txn.session_mut().open_cursor("table:test").unwrap();

        for i in 5..10 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            println!("Inserting: {}", key);
            cursor.insert(key.as_bytes(), value.as_bytes(), txn_id).unwrap();
        }

        txn.commit().unwrap();
        // Don't checkpoint - simulate crash
    }

    drop(conn);

    // Phase 3: Reopen and check
    println!("\n=== Phase 3: Reopen and check ===");
    {
        let conn = Connection::open(&db_path, ConnectionConfig::default().disable_auto_checkpoint())
            .unwrap();
        let mut session = conn.open_session();
        let mut cursor = session.open_cursor("table:test").unwrap();

        let mut txn = session.transaction().unwrap();
        let txn_id = txn.as_mut().id();

        for i in 0..10 {
            let key = format!("key{}", i);
            let result = cursor.get(key.as_bytes(), txn_id).unwrap();
            match &result {
                Some(v) => println!("{} -> {:?}", key, String::from_utf8_lossy(v)),
                None => println!("{} -> MISSING", key),
            }
        }
    }
}
