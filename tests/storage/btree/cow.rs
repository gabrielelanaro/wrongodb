use tempfile::tempdir;

use super::{
    checkpoint, create_table, get_table_row, insert_table_row, open_connection, update_table_row,
};

#[test]
fn uncheckpointed_update_does_not_replace_the_stable_value_on_reopen_without_wal() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let conn = open_connection(&db_path, false);
        let mut session = conn.open_session();
        create_table(&mut session);
        checkpoint(&mut session);
        insert_table_row(&session, b"alpha", b"one");
        checkpoint(&mut session);

        update_table_row(&session, b"alpha", b"two");
        assert_eq!(get_table_row(&session, b"alpha"), Some(b"two".to_vec()));
    }

    let conn = open_connection(&db_path, false);
    let session = conn.open_session();
    assert_eq!(get_table_row(&session, b"alpha"), Some(b"one".to_vec()));
}

#[test]
fn checkpointed_update_replaces_the_stable_value_on_reopen_without_wal() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let conn = open_connection(&db_path, false);
        let mut session = conn.open_session();
        create_table(&mut session);
        checkpoint(&mut session);
        insert_table_row(&session, b"alpha", b"one");
        checkpoint(&mut session);

        update_table_row(&session, b"alpha", b"two");
        checkpoint(&mut session);
    }

    let conn = open_connection(&db_path, false);
    let session = conn.open_session();
    assert_eq!(get_table_row(&session, b"alpha"), Some(b"two".to_vec()));
}
