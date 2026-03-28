use tempfile::tempdir;

use super::{
    checkpoint, create_table, delete_table_row, file_len, get_table_row, insert_table_row,
    open_connection, table_store_path,
};

#[test]
fn checkpoint_persists_rows_after_reopen_without_wal() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let conn = open_connection(&db_path, false);
        let mut session = conn.open_session();
        create_table(&mut session);
        checkpoint(&mut session);
        insert_table_row(&session, b"alpha", b"one");
        insert_table_row(&session, b"beta", b"two");
        checkpoint(&mut session);
    }

    let conn = open_connection(&db_path, false);
    let session = conn.open_session();
    assert_eq!(get_table_row(&session, b"alpha"), Some(b"one".to_vec()));
    assert_eq!(get_table_row(&session, b"beta"), Some(b"two".to_vec()));
}

#[test]
fn uncheckpointed_rows_are_discarded_after_reopen_without_wal() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let conn = open_connection(&db_path, false);
        let mut session = conn.open_session();
        create_table(&mut session);
        checkpoint(&mut session);
        insert_table_row(&session, b"alpha", b"one");
    }

    let conn = open_connection(&db_path, false);
    let mut session = conn.open_session();
    session.create_table(super::TABLE_URI, Vec::new()).unwrap();
    assert_eq!(get_table_row(&session, b"alpha"), None);
}

#[test]
fn updates_between_checkpoints_stop_growing_the_store_file() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");
    let store_path = table_store_path(&db_path);

    let conn = open_connection(&db_path, false);
    let mut session = conn.open_session();
    create_table(&mut session);
    checkpoint(&mut session);

    let size_before = file_len(&store_path);
    insert_table_row(&session, b"k1", &vec![b'v'; 512]);
    let size_after_first = file_len(&store_path);
    insert_table_row(&session, b"k2", &vec![b'w'; 512]);
    let size_after_second = file_len(&store_path);

    assert!(size_after_first >= size_before);
    assert_eq!(size_after_second, size_after_first);
}

#[test]
fn freed_space_is_reused_after_checkpoint() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");
    let store_path = table_store_path(&db_path);

    {
        let conn = open_connection(&db_path, false);
        let mut session = conn.open_session();
        create_table(&mut session);
        checkpoint(&mut session);

        for i in 0..48 {
            let key = format!("key{i:02}");
            insert_table_row(&session, key.as_bytes(), &vec![b'v'; 256]);
        }
        checkpoint(&mut session);

        for i in 0..48 {
            let key = format!("key{i:02}");
            delete_table_row(&session, key.as_bytes());
        }
        checkpoint(&mut session);
        let size_after_delete = file_len(&store_path);

        for i in 0..48 {
            let key = format!("new{i:02}");
            insert_table_row(&session, key.as_bytes(), &vec![b'w'; 256]);
        }
        checkpoint(&mut session);

        assert_eq!(file_len(&store_path), size_after_delete);
    }
}
