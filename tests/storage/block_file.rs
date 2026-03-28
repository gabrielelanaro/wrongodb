use tempfile::tempdir;

use super::btree::{
    checkpoint, create_file, file_store_path, get_file_row, insert_file_row, open_connection,
    scan_file_range, update_file_row, FILE_URI,
};

#[test]
fn file_cursor_checkpoint_persists_rows_after_reopen_without_wal() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let conn = open_connection(&db_path, false);
        let mut session = conn.open_session();
        create_file(&mut session);
        checkpoint(&mut session);
        insert_file_row(&session, b"alpha", b"one");
        insert_file_row(&session, b"beta", b"two");
        checkpoint(&mut session);
    }

    let conn = open_connection(&db_path, false);
    let session = conn.open_session();
    assert_eq!(get_file_row(&session, b"alpha"), Some(b"one".to_vec()));
    assert_eq!(get_file_row(&session, b"beta"), Some(b"two".to_vec()));
    assert!(file_store_path(&db_path).exists());
}

#[test]
fn file_cursor_uncheckpointed_writes_are_lost_after_reopen_without_wal() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let conn = open_connection(&db_path, false);
        let mut session = conn.open_session();
        create_file(&mut session);
        checkpoint(&mut session);
        insert_file_row(&session, b"alpha", b"one");
    }

    let conn = open_connection(&db_path, false);
    let mut session = conn.open_session();
    session.create_file(FILE_URI).unwrap();
    assert_eq!(get_file_row(&session, b"alpha"), None);
}

#[test]
fn file_cursor_range_scan_returns_exact_entries_after_checkpoint() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let conn = open_connection(&db_path, false);
        let mut session = conn.open_session();
        create_file(&mut session);
        checkpoint(&mut session);

        for i in 0..8 {
            let key = format!("k{i:02}");
            let value = format!("v{i:02}");
            insert_file_row(&session, key.as_bytes(), value.as_bytes());
        }
        checkpoint(&mut session);
    }

    let conn = open_connection(&db_path, false);
    let session = conn.open_session();
    let entries = scan_file_range(&session, Some(b"k02"), Some(b"k06"));
    let expected = vec![
        (b"k02".to_vec(), b"v02".to_vec()),
        (b"k03".to_vec(), b"v03".to_vec()),
        (b"k04".to_vec(), b"v04".to_vec()),
        (b"k05".to_vec(), b"v05".to_vec()),
    ];
    assert_eq!(entries, expected);
}

#[test]
fn file_cursor_checkpoint_persists_updates() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let conn = open_connection(&db_path, false);
        let mut session = conn.open_session();
        create_file(&mut session);
        checkpoint(&mut session);
        insert_file_row(&session, b"alpha", b"one");
        checkpoint(&mut session);
        update_file_row(&session, b"alpha", b"two");
        checkpoint(&mut session);
    }

    let conn = open_connection(&db_path, false);
    let session = conn.open_session();
    assert_eq!(get_file_row(&session, b"alpha"), Some(b"two".to_vec()));
}
