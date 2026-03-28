use tempfile::tempdir;

use super::{checkpoint, create_table, insert_table_row, open_connection, scan_table_range};

#[test]
fn ordered_range_scan_is_sorted_and_respects_bounds_after_reopen() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let conn = open_connection(&db_path, false);
        let mut session = conn.open_session();
        create_table(&mut session);
        checkpoint(&mut session);

        for i in 0..500u32 {
            let key = format!("k{i:04}");
            let value = format!("v{i:04}");
            insert_table_row(&session, key.as_bytes(), value.as_bytes());
        }
        checkpoint(&mut session);
    }

    let conn = open_connection(&db_path, false);
    let session = conn.open_session();
    let slice = scan_table_range(&session, Some(b"k0100"), Some(b"k0200"));

    assert_eq!(slice.len(), 100);
    assert_eq!(
        slice.first().unwrap(),
        &(b"k0100".to_vec(), b"v0100".to_vec())
    );
    assert_eq!(
        slice.last().unwrap(),
        &(b"k0199".to_vec(), b"v0199".to_vec())
    );
    assert!(slice.windows(2).all(|window| window[0].0 < window[1].0));
}

#[test]
fn large_insert_batch_remains_readable_after_reopen() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let conn = open_connection(&db_path, false);
        let mut session = conn.open_session();
        create_table(&mut session);
        checkpoint(&mut session);

        for i in 0..800u32 {
            let key = format!("k{i:04}");
            insert_table_row(&session, key.as_bytes(), &vec![b'v'; 64]);
        }
        checkpoint(&mut session);
    }

    let conn = open_connection(&db_path, false);
    let session = conn.open_session();
    let all = scan_table_range(&session, None, None);
    assert_eq!(all.len(), 800);
    assert_eq!(all.first().unwrap().0, b"k0000".to_vec());
    assert_eq!(all.last().unwrap().0, b"k0799".to_vec());
}
