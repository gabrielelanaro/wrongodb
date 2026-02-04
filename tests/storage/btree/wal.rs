//! WAL integration tests for DB-level operations.

use std::fs;
use std::path::{Path, PathBuf};

use tempfile::tempdir;
use wrongodb::{Connection, ConnectionConfig};

fn wal_path(base_path: &Path) -> PathBuf {
    base_path.join("wrongo.wal")
}

#[test]
fn wal_created_when_enabled() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("wal-enabled");

    let conn = Connection::open(&db_path, ConnectionConfig::default().disable_auto_checkpoint())
        .unwrap();
    drop(conn);

    assert!(wal_path(&db_path).exists());
}

#[test]
fn wal_disabled_no_file_created() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("wal-disabled");

    let conn = Connection::open(
        &db_path,
        ConnectionConfig::default()
            .wal_enabled(false)
            .disable_auto_checkpoint(),
    )
    .unwrap();

    {
        let mut session = conn.open_session();
        session.create("table:test").unwrap();
    }

    drop(conn);

    assert!(!wal_path(&db_path).exists());
}

#[test]
fn wal_records_written_for_committed_write() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("wal-write");

    let conn = Connection::open(&db_path, ConnectionConfig::default().disable_auto_checkpoint())
        .unwrap();

    {
        let mut session = conn.open_session();
        session.create("table:test").unwrap();

        let mut txn = session.transaction().unwrap();
        let txn_id = txn.as_mut().id();
        let mut cursor = txn.session_mut().open_cursor("table:test").unwrap();
        cursor.insert(b"key1", b"value1", txn_id).unwrap();
        txn.commit().unwrap();
    }

    drop(conn);

    let metadata = fs::metadata(wal_path(&db_path)).unwrap();
    assert!(metadata.len() > 512);
}
