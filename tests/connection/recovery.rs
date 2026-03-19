//! Integration tests for connection-level global WAL recovery.

use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};

use crate::common::kv::{get_kv, insert_kv_in_write_unit, update_kv_in_write_unit};
use tempfile::tempdir;

use wrongodb::{Connection, ConnectionConfig, RaftMode};

fn global_wal_path(db_dir: &std::path::Path) -> std::path::PathBuf {
    db_dir.join("global.wal")
}

fn insert_kv(conn: &Connection, table: &str, key: &[u8], value: &[u8]) {
    let mut session = conn.open_session();
    let mut txn = session.transaction().unwrap();
    insert_kv_in_write_unit(&mut txn, table, key, value).unwrap();
    txn.commit().unwrap();
}

fn insert_kv_non_transactional(conn: &Connection, table: &str, key: &[u8], value: &[u8]) {
    insert_kv(conn, table, key, value);
}

fn exists(conn: &Connection, table: &str, key: &[u8]) -> bool {
    get_kv(conn, table, key).unwrap().is_some()
}

fn read_value(conn: &Connection, table: &str, key: &[u8]) -> Option<Vec<u8>> {
    get_kv(conn, table, key).unwrap()
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
        insert_kv_in_write_unit(&mut txn, "test", b"k1", b"a").unwrap();
        insert_kv_in_write_unit(&mut txn, "test", b"k2", b"b").unwrap();

        std::mem::forget(txn);
    }

    {
        let conn = Connection::open(&db_path, ConnectionConfig::default()).unwrap();
        assert!(!exists(&conn, "test", b"k1"));
        assert!(!exists(&conn, "test", b"k2"));
    }
}

#[test]
fn recovery_skips_explicitly_aborted_transaction_writes() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let conn = Connection::open(&db_path, ConnectionConfig::default()).unwrap();
        let mut session = conn.open_session();

        let mut txn = session.transaction().unwrap();
        insert_kv_in_write_unit(&mut txn, "test", b"k1", b"a").unwrap();
        txn.abort().unwrap();
    }

    {
        let conn = Connection::open(&db_path, ConnectionConfig::default()).unwrap();
        assert!(!exists(&conn, "test", b"k1"));
    }
}

#[test]
fn recovery_replays_transactional_writes_at_commit_time() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let conn = Connection::open(&db_path, ConnectionConfig::default()).unwrap();
        insert_kv_non_transactional(&conn, "test", b"shared", b"plain");

        let mut session = conn.open_session();

        let mut txn = session.transaction().unwrap();
        update_kv_in_write_unit(&mut txn, "test", b"shared", b"txn").unwrap();
        txn.commit().unwrap();
    }

    {
        let conn = Connection::open(&db_path, ConnectionConfig::default()).unwrap();
        assert_eq!(
            read_value(&conn, "test", b"shared").as_deref(),
            Some(b"txn".as_slice())
        );
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
fn recovery_handles_empty_committed_transaction() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let conn = Connection::open(&db_path, ConnectionConfig::default()).unwrap();
        let mut session = conn.open_session();
        let txn = session.transaction().unwrap();
        txn.commit().unwrap();
    }

    let _reopened = Connection::open(&db_path, ConnectionConfig::default()).unwrap();
}

#[test]
fn wal_disabled_mode_opens_without_global_wal() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");
    let cfg = ConnectionConfig::new(false, RaftMode::Standalone);

    {
        let conn = Connection::open(&db_path, cfg).unwrap();
        insert_kv(&conn, "test", b"k1", b"a");
    }

    let _reopened =
        Connection::open(&db_path, ConnectionConfig::new(false, RaftMode::Standalone)).unwrap();
    assert!(!global_wal_path(&db_path).exists());
}
