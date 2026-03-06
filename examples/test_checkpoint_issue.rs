use serde_json::json;
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
        session.create("table:test").unwrap();

        let mut txn = session.transaction().unwrap();

        for i in 0..5 {
            println!("Inserting: key{}", i);
            txn.session_mut()
                .insert_one(
                    "test",
                    json!({"_id": format!("key{i}"), "value": format!("value{i}")}),
                )
                .unwrap();
        }

        txn.commit().unwrap();
    }

    // Phase 2: Open and insert more data
    println!("\n=== Phase 2: Open and insert 5-9 ===");
    {
        let mut session = conn.open_session();

        let mut txn = session.transaction().unwrap();

        for i in 5..10 {
            println!("Inserting: key{}", i);
            txn.session_mut()
                .insert_one(
                    "test",
                    json!({"_id": format!("key{i}"), "value": format!("value{i}")}),
                )
                .unwrap();
        }

        txn.commit().unwrap();
        // Don't checkpoint - simulate crash
    }

    drop(conn);

    // Phase 3: Reopen and check
    println!("\n=== Phase 3: Reopen and check ===");
    {
        let conn = Connection::open(&db_path, ConnectionConfig::default()).unwrap();
        let mut session = conn.open_session();

        for i in 0..10 {
            let key = format!("key{}", i);
            let result = session
                .find_one("test", Some(json!({"_id": key.clone()})))
                .unwrap();
            match &result {
                Some(doc) => println!("{} -> {:?}", key, doc.get("value")),
                None => println!("{} -> MISSING", key),
            }
        }
    }
}
