//! Integration tests for connection-level global WAL behavior.

use std::fs;

use serde_json::json;
use tempfile::tempdir;

use wrongodb::{Connection, ConnectionConfig};

fn global_wal_path(db_dir: &std::path::Path) -> std::path::PathBuf {
    db_dir.join("global.wal")
}

fn insert_kv(conn: &Connection, table: &str, key: &[u8], value: &[u8]) {
    let mut session = conn.open_session();
    session
        .insert_one(
            table,
            json!({
                "_id": String::from_utf8_lossy(key).to_string(),
                "value": String::from_utf8_lossy(value).to_string(),
            }),
        )
        .unwrap();
}

#[test]
fn global_wal_created_when_enabled() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    let conn = Connection::open(&db_path, ConnectionConfig::default()).unwrap();
    insert_kv(&conn, "test", b"k1", b"v1");

    assert!(global_wal_path(&db_path).exists());
}

#[test]
fn global_wal_not_created_when_disabled() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");
    let cfg = ConnectionConfig::new().wal_enabled(false);

    let conn = Connection::open(&db_path, cfg).unwrap();
    insert_kv(&conn, "test", b"k1", b"v1");

    assert!(!global_wal_path(&db_path).exists());
}

#[test]
fn global_wal_grows_after_committed_writes() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    let conn = Connection::open(&db_path, ConnectionConfig::default()).unwrap();

    for i in 0..20 {
        let key = format!("k{i}");
        let value = format!("v{i}");
        insert_kv(&conn, "test", key.as_bytes(), value.as_bytes());
    }

    let metadata = fs::metadata(global_wal_path(&db_path)).unwrap();
    assert!(metadata.len() > 512);
}
