use tempfile::tempdir;

use super::btree::{
    checkpoint, create_table, insert_table_row, open_connection, scan_table_range, TABLE_URI,
};

#[test]
fn table_range_scan_returns_exact_sorted_entries_after_reopen() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let conn = open_connection(&db_path, false);
        let mut session = conn.open_session();
        create_table(&mut session);
        checkpoint(&mut session);

        for i in 0..100 {
            let key = format!("key{i:03}");
            let value = format!("value{i:03}");
            insert_table_row(&session, key.as_bytes(), value.as_bytes());
        }
        checkpoint(&mut session);
    }

    let conn = open_connection(&db_path, false);
    let session = conn.open_session();
    let entries = scan_table_range(&session, Some(b"key010"), Some(b"key015"));
    let expected = vec![
        (b"key010".to_vec(), b"value010".to_vec()),
        (b"key011".to_vec(), b"value011".to_vec()),
        (b"key012".to_vec(), b"value012".to_vec()),
        (b"key013".to_vec(), b"value013".to_vec()),
        (b"key014".to_vec(), b"value014".to_vec()),
    ];
    assert_eq!(entries, expected);
}

#[test]
fn table_range_scan_returns_no_rows_for_empty_ranges() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    let conn = open_connection(&db_path, false);
    let mut session = conn.open_session();
    create_table(&mut session);
    checkpoint(&mut session);

    assert!(scan_table_range(&session, Some(b"z"), Some(b"zz")).is_empty());

    insert_table_row(&session, b"key1", b"value1");
    assert!(scan_table_range(&session, Some(b"z"), Some(b"zz")).is_empty());
}

#[test]
fn table_cursor_reset_rescans_the_same_range_from_start() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    let conn = open_connection(&db_path, false);
    let mut session = conn.open_session();
    create_table(&mut session);
    checkpoint(&mut session);

    for i in 0..5 {
        let key = format!("key{i}");
        let value = format!("value{i}");
        insert_table_row(&session, key.as_bytes(), value.as_bytes());
    }

    let mut cursor = session.open_table_cursor(TABLE_URI).unwrap();
    cursor.set_range(Some(b"key1".to_vec()), Some(b"key4".to_vec()));
    assert_eq!(
        cursor.next().unwrap(),
        Some((b"key1".to_vec(), b"value1".to_vec()))
    );

    cursor.reset();

    let mut rescanned = Vec::new();
    while let Some(entry) = cursor.next().unwrap() {
        rescanned.push(entry);
    }

    assert_eq!(
        rescanned,
        vec![
            (b"key1".to_vec(), b"value1".to_vec()),
            (b"key2".to_vec(), b"value2".to_vec()),
            (b"key3".to_vec(), b"value3".to_vec()),
        ]
    );
}
