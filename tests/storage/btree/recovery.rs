//! Integration tests for DB-level WAL recovery.

use std::path::{Path, PathBuf};

use tempfile::{tempdir, TempDir};
use wrongodb::{Connection, ConnectionConfig};

const TABLE_URI: &str = "table:test";
const READ_TXN_ID: u64 = u64::MAX - 1;

fn test_db_dir(name: &str) -> (TempDir, PathBuf) {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join(name);
    (tmp, path)
}

fn open_conn(path: &Path, wal_enabled: bool) -> Connection {
    Connection::open(
        path,
        ConnectionConfig::default()
            .wal_enabled(wal_enabled)
            .disable_auto_checkpoint(),
    )
    .unwrap()
}

fn insert_range(conn: &Connection, start: usize, end: usize) {
    let mut session = conn.open_session();
    session.create(TABLE_URI).unwrap();

    let mut txn = session.transaction().unwrap();
    let txn_id = txn.as_mut().id();
    let mut cursor = txn.session_mut().open_cursor(TABLE_URI).unwrap();

    for i in start..end {
        let key = format!("key{}", i);
        let value = format!("value{}", i);
        cursor.insert(key.as_bytes(), value.as_bytes(), txn_id).unwrap();
    }

    txn.commit().unwrap();
}

fn assert_range(conn: &Connection, start: usize, end: usize) {
    let mut session = conn.open_session();
    let mut cursor = session.open_cursor(TABLE_URI).unwrap();

    for i in start..end {
        let key = format!("key{}", i);
        let value = format!("value{}", i);
        let result = cursor.get(key.as_bytes(), READ_TXN_ID).unwrap();
        assert_eq!(result, Some(value.as_bytes().to_vec()));
    }
}

#[test]
fn recover_from_wal_after_crash() {
    let (_tmp, db_path) = test_db_dir("recover_crash");

    {
        let conn = open_conn(&db_path, true);
        insert_range(&conn, 0, 10);
    }

    {
        let conn = open_conn(&db_path, true);
        assert_range(&conn, 0, 10);
    }
}

#[test]
fn recover_single_insert_no_split() {
    let (_tmp, db_path) = test_db_dir("recover_single");

    {
        let conn = open_conn(&db_path, true);
        insert_range(&conn, 0, 1);
    }

    {
        let conn = open_conn(&db_path, true);
        assert_range(&conn, 0, 1);
    }
}

#[test]
fn recovery_after_checkpoint() {
    let (_tmp, db_path) = test_db_dir("recover_checkpoint");

    let conn = open_conn(&db_path, true);
    insert_range(&conn, 0, 5);
    conn.checkpoint_all().unwrap();
    insert_range(&conn, 5, 10);
    drop(conn);

    let conn = open_conn(&db_path, true);
    assert_range(&conn, 0, 10);
}

#[test]
fn recovery_idempotent() {
    let (_tmp, db_path) = test_db_dir("recover_idempotent");

    {
        let conn = open_conn(&db_path, true);
        insert_range(&conn, 0, 5);
    }

    {
        let conn = open_conn(&db_path, true);
        assert_range(&conn, 0, 5);
    }

    {
        let conn = open_conn(&db_path, true);
        assert_range(&conn, 0, 5);
    }
}

#[test]
fn recovery_with_multiple_splits() {
    let (_tmp, db_path) = test_db_dir("recover_splits");

    {
        let conn = open_conn(&db_path, true);
        let mut session = conn.open_session();
        session.create(TABLE_URI).unwrap();

        let mut txn = session.transaction().unwrap();
        let txn_id = txn.as_mut().id();
        let mut cursor = txn.session_mut().open_cursor(TABLE_URI).unwrap();

        for i in 0..200 {
            let key = format!("key{:010}", i);
            let value = format!("value{}", i);
            cursor.insert(key.as_bytes(), value.as_bytes(), txn_id).unwrap();
        }

        txn.commit().unwrap();
    }

    {
        let conn = open_conn(&db_path, true);
        let mut session = conn.open_session();
        let mut cursor = session.open_cursor(TABLE_URI).unwrap();

        cursor.set_range(
            Some(b"key0000000000".to_vec()),
            Some(b"key0000000200".to_vec()),
        );

        let mut count = 0;
        while let Some((k, v)) = cursor.next(READ_TXN_ID).unwrap() {
            assert!(k.starts_with(b"key"));
            assert!(v.starts_with(b"value"));
            count += 1;
        }
        assert_eq!(count, 200);
    }
}

#[test]
fn recovery_with_corrupted_wal() {
    let (_tmp, db_path) = test_db_dir("recover_corrupt");
    let wal_path = db_path.join("wrongo.wal");

    {
        let conn = open_conn(&db_path, true);
        insert_range(&conn, 0, 20);
    }

    {
        use std::fs::OpenOptions;
        use std::io::{Seek, SeekFrom, Write};

        let len = std::fs::metadata(&wal_path).unwrap().len();
        let offset = if len > 600 { 600 } else { len / 2 };

        let mut file = OpenOptions::new().write(true).open(&wal_path).unwrap();
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(b"CORRUPT_DATA_HERE").unwrap();
    }

    let conn = open_conn(&db_path, true);
    let mut session = conn.open_session();
    let mut cursor = session.open_cursor(TABLE_URI).unwrap();
    let _ = cursor.get(b"key0", READ_TXN_ID);
}

#[test]
fn recovery_empty_database() {
    let (_tmp, db_path) = test_db_dir("recover_empty");

    {
        let conn = open_conn(&db_path, true);
        let mut session = conn.open_session();
        session.create(TABLE_URI).unwrap();
    }

    {
        let conn = open_conn(&db_path, true);
        let mut session = conn.open_session();
        let mut cursor = session.open_cursor(TABLE_URI).unwrap();
        let result = cursor.get(b"nonexistent", READ_TXN_ID).unwrap();
        assert_eq!(result, None);
    }
}

#[test]
fn recovery_large_keys_and_values() {
    let (_tmp, db_path) = test_db_dir("recover_large");

    {
        let conn = open_conn(&db_path, true);
        let mut session = conn.open_session();
        session.create(TABLE_URI).unwrap();

        let mut txn = session.transaction().unwrap();
        let txn_id = txn.as_mut().id();
        let mut cursor = txn.session_mut().open_cursor(TABLE_URI).unwrap();

        for i in 0..10 {
            let key = format!("key_{}_with_lots_of_padding_data_{}", i, i);
            let value = format!("value_{}_with_even_more_padding_data_{}", i, i);
            cursor.insert(key.as_bytes(), value.as_bytes(), txn_id).unwrap();
        }

        txn.commit().unwrap();
    }

    {
        let conn = open_conn(&db_path, true);
        let mut session = conn.open_session();
        let mut cursor = session.open_cursor(TABLE_URI).unwrap();
        for i in 0..10 {
            let key = format!("key_{}_with_lots_of_padding_data_{}", i, i);
            let value = format!("value_{}_with_even_more_padding_data_{}", i, i);
            let result = cursor.get(key.as_bytes(), READ_TXN_ID).unwrap();
            assert_eq!(result, Some(value.as_bytes().to_vec()));
        }
    }
}

#[test]
fn recovery_with_wal_disabled() {
    let (_tmp, db_path) = test_db_dir("recover_no_wal");

    {
        let conn = open_conn(&db_path, false);
        insert_range(&conn, 0, 5);
        conn.checkpoint_all().unwrap();
    }

    {
        let conn = open_conn(&db_path, false);
        assert_range(&conn, 0, 5);
    }
}
