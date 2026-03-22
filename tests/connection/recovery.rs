//! Integration tests for connection-level global WAL recovery.

use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};

use crate::common::kv::{get_kv, insert_kv_in_transaction, update_kv_in_transaction};
use tempfile::tempdir;

use wrongodb::{Connection, ConnectionConfig};

fn global_wal_path(db_dir: &std::path::Path) -> std::path::PathBuf {
    db_dir.join("global.wal")
}

fn insert_kv(conn: &Connection, table: &str, key: &[u8], value: &[u8]) {
    let mut session = conn.open_session();
    session.create_table(&format!("table:{table}")).unwrap();
    session
        .with_transaction(|session| insert_kv_in_transaction(session, table, key, value))
        .unwrap();
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
        let conn = Connection::open(&db_path, ConnectionConfig::new()).unwrap();
        insert_kv(&conn, "test", b"k1", b"a");
        insert_kv(&conn, "test", b"k2", b"b");
    }

    {
        let conn = Connection::open(&db_path, ConnectionConfig::new()).unwrap();
        assert!(exists(&conn, "test", b"k1"));
        assert!(exists(&conn, "test", b"k2"));
    }
}

#[test]
fn recovery_replays_records_appended_after_restart() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let conn = Connection::open(&db_path, ConnectionConfig::new()).unwrap();
        insert_kv(&conn, "test", b"k1", b"a");
    }

    {
        let conn = Connection::open(&db_path, ConnectionConfig::new()).unwrap();
        insert_kv(&conn, "test", b"k2", b"b");
    }

    {
        let conn = Connection::open(&db_path, ConnectionConfig::new()).unwrap();
        assert!(exists(&conn, "test", b"k1"));
        assert!(exists(&conn, "test", b"k2"));
    }
}

#[test]
fn recovery_skips_incomplete_transaction_commit_record() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let conn = Connection::open(&db_path, ConnectionConfig::new()).unwrap();
        insert_kv(&conn, "test", b"k1", b"a");
        insert_kv(&conn, "test", b"k2", b"b");
    }

    let wal_path = global_wal_path(&db_path);
    let len = std::fs::metadata(&wal_path).unwrap().len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&wal_path)
        .unwrap()
        .set_len(len.saturating_sub(8))
        .unwrap();

    {
        let conn = Connection::open(&db_path, ConnectionConfig::new()).unwrap();
        assert!(exists(&conn, "test", b"k1"));
        assert!(!exists(&conn, "test", b"k2"));
    }
}

#[test]
fn recovery_skips_explicitly_aborted_transaction_writes() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let conn = Connection::open(&db_path, ConnectionConfig::new()).unwrap();
        let mut session = conn.open_session();
        session.create_table("table:test").unwrap();

        let _ = session.with_transaction(|session| {
            insert_kv_in_transaction(session, "test", b"k1", b"a")?;
            Err::<(), wrongodb::WrongoDBError>(wrongodb::StorageError("abort".into()).into())
        });
    }

    {
        let conn = Connection::open(&db_path, ConnectionConfig::new()).unwrap();
        assert!(!exists(&conn, "test", b"k1"));
    }
}

#[test]
fn recovery_replays_transactional_writes_at_commit_time() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let conn = Connection::open(&db_path, ConnectionConfig::new()).unwrap();
        insert_kv_non_transactional(&conn, "test", b"shared", b"plain");

        let mut session = conn.open_session();

        session
            .with_transaction(|session| {
                update_kv_in_transaction(session, "test", b"shared", b"txn")
            })
            .unwrap();
    }

    {
        let conn = Connection::open(&db_path, ConnectionConfig::new()).unwrap();
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
        let conn = Connection::open(&db_path, ConnectionConfig::new()).unwrap();
        insert_kv(&conn, "test", b"k1", b"a");
    }

    let wal_path = global_wal_path(&db_path);
    {
        let mut file = OpenOptions::new().write(true).open(&wal_path).unwrap();
        file.seek(SeekFrom::Start(600)).unwrap();
        file.write_all(b"CORRUPTED_TAIL").unwrap();
    }

    let _ = Connection::open(&db_path, ConnectionConfig::new());
}

#[test]
fn recovery_replay_does_not_append_new_wal_records() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let conn = Connection::open(&db_path, ConnectionConfig::new()).unwrap();
        insert_kv(&conn, "test", b"k1", b"a");
    }

    let wal_path = global_wal_path(&db_path);
    let before = std::fs::metadata(&wal_path).unwrap().len();
    assert!(before > 512);

    {
        let _conn = Connection::open(&db_path, ConnectionConfig::new()).unwrap();
    }

    let after = std::fs::metadata(&wal_path).unwrap().len();
    assert_eq!(after, before);
}

#[test]
fn recovery_handles_empty_committed_transaction() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let conn = Connection::open(&db_path, ConnectionConfig::new()).unwrap();
        let mut session = conn.open_session();
        session.with_transaction(|_| Ok(())).unwrap();
    }

    let _reopened = Connection::open(&db_path, ConnectionConfig::new()).unwrap();
}

#[test]
fn wal_disabled_mode_opens_without_global_wal() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");
    let cfg = ConnectionConfig::new().logging_enabled(false);

    {
        let conn = Connection::open(&db_path, cfg).unwrap();
        insert_kv(&conn, "test", b"k1", b"a");
    }

    let _reopened =
        Connection::open(&db_path, ConnectionConfig::new().logging_enabled(false)).unwrap();
    assert!(!global_wal_path(&db_path).exists());
}
