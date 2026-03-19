//! Integration tests for connection-level global WAL behavior.

use std::fs;

use crate::common::kv::insert_kv;
use tempfile::tempdir;

use wrongodb::{Connection, ConnectionConfig, RaftMode};

fn global_wal_path(db_dir: &std::path::Path) -> std::path::PathBuf {
    db_dir.join("global.wal")
}

#[test]
fn global_wal_created_when_enabled() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    let conn = Connection::open(&db_path, ConnectionConfig::default()).unwrap();
    insert_kv(&conn, "test", b"k1", b"v1").unwrap();

    assert!(global_wal_path(&db_path).exists());
}

#[test]
fn global_wal_not_created_when_disabled() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");
    let cfg = ConnectionConfig::new(false, RaftMode::Standalone);

    let conn = Connection::open(&db_path, cfg).unwrap();
    insert_kv(&conn, "test", b"k1", b"v1").unwrap();

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
        insert_kv(&conn, "test", key.as_bytes(), value.as_bytes()).unwrap();
    }

    let metadata = fs::metadata(global_wal_path(&db_path)).unwrap();
    assert!(metadata.len() > 512);
}
