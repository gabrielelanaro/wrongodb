//! Integration tests for connection-level global WAL recovery.

use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};

use tempfile::tempdir;

use wrongodb::{Connection, ConnectionConfig};

fn global_wal_path(db_dir: &std::path::Path) -> std::path::PathBuf {
    db_dir.join("global.wal")
}

fn insert_kv(conn: &Connection, table: &str, key: &[u8], value: &[u8]) {
    let mut session = conn.open_session();
    let mut txn = session.transaction().unwrap();
    let txn_id = txn.as_ref().id();
    let mut cursor = txn
        .session_mut()
        .open_cursor(&format!("table:{table}"))
        .unwrap();
    cursor.insert(key, value, txn_id).unwrap();
    txn.commit().unwrap();
}

fn exists(conn: &Connection, table: &str, key: &[u8]) -> bool {
    let mut session = conn.open_session();
    let mut cursor = session.open_cursor(&format!("table:{table}")).unwrap();
    cursor.get(key, 0).unwrap().is_some()
}

#[test]
fn recovery_replays_committed_transaction_writes() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let conn = Connection::open(&db_path, ConnectionConfig::default()).unwrap();
        insert_kv(&conn, "test", b"k1", b"a");
        insert_kv(&conn, "test", b"k2", b"b");
    }

    {
        let conn = Connection::open(&db_path, ConnectionConfig::default()).unwrap();
        assert!(exists(&conn, "test", b"k1"));
        assert!(exists(&conn, "test", b"k2"));
    }
}

#[test]
fn recovery_replays_records_appended_after_restart() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let conn = Connection::open(&db_path, ConnectionConfig::default()).unwrap();
        insert_kv(&conn, "test", b"k1", b"a");
    }

    {
        let conn = Connection::open(&db_path, ConnectionConfig::default()).unwrap();
        insert_kv(&conn, "test", b"k2", b"b");
    }

    {
        let conn = Connection::open(&db_path, ConnectionConfig::default()).unwrap();
        assert!(exists(&conn, "test", b"k1"));
        assert!(exists(&conn, "test", b"k2"));
    }
}

#[test]
fn recovery_skips_uncommitted_transaction_writes() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let conn = Connection::open(&db_path, ConnectionConfig::default()).unwrap();
        let mut session = conn.open_session();

        let mut txn = session.transaction().unwrap();
        let txn_id = txn.as_ref().id();
        let mut cursor = txn.session_mut().open_cursor("table:test").unwrap();
        cursor.insert(b"k1", b"a", txn_id).unwrap();
        cursor.insert(b"k2", b"b", txn_id).unwrap();

        std::mem::forget(txn);
    }

    {
        let conn = Connection::open(&db_path, ConnectionConfig::default()).unwrap();
        assert!(!exists(&conn, "test", b"k1"));
        assert!(!exists(&conn, "test", b"k2"));
    }
}

#[test]
fn recovery_handles_corrupted_global_wal_tail() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let conn = Connection::open(&db_path, ConnectionConfig::default()).unwrap();
        insert_kv(&conn, "test", b"k1", b"a");
    }

    let wal_path = global_wal_path(&db_path);
    {
        let mut file = OpenOptions::new().write(true).open(&wal_path).unwrap();
        file.seek(SeekFrom::Start(600)).unwrap();
        file.write_all(b"CORRUPTED_TAIL").unwrap();
    }

    let _ = Connection::open(&db_path, ConnectionConfig::default());
}

#[test]
fn recovery_replay_does_not_append_new_wal_records() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let conn = Connection::open(&db_path, ConnectionConfig::default()).unwrap();
        insert_kv(&conn, "test", b"k1", b"a");
    }

    let wal_path = global_wal_path(&db_path);
    let before = std::fs::metadata(&wal_path).unwrap().len();
    assert!(before > 512);

    {
        let _conn = Connection::open(&db_path, ConnectionConfig::default()).unwrap();
    }

    let after = std::fs::metadata(&wal_path).unwrap().len();
    assert_eq!(after, before);
}

#[test]
fn wal_disabled_mode_opens_without_global_wal() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");
    let cfg = ConnectionConfig::new().wal_enabled(false);

    {
        let conn = Connection::open(&db_path, cfg).unwrap();
        insert_kv(&conn, "test", b"k1", b"a");
    }

    let reopened = Connection::open(&db_path, ConnectionConfig::new().wal_enabled(false)).unwrap();
    assert!(!global_wal_path(&db_path).exists());
    let _ = reopened.base_path();
}
