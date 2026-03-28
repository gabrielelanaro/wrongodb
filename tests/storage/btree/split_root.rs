use tempfile::tempdir;

use super::{checkpoint, create_table, insert_table_row, open_connection, scan_table_range};

#[test]
fn large_insert_set_survives_reopen_with_all_rows_present() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let conn = open_connection(&db_path, false);
        let mut session = conn.open_session();
        create_table(&mut session);
        checkpoint(&mut session);

        for i in 0..200u32 {
            let key = format!("k{i:04}");
            insert_table_row(&session, key.as_bytes(), &vec![b'v'; 24]);
        }
        checkpoint(&mut session);
    }

    let conn = open_connection(&db_path, false);
    let session = conn.open_session();
    let entries = scan_table_range(&session, None, None);
    assert_eq!(entries.len(), 200);
    assert!(entries.windows(2).all(|window| window[0].0 < window[1].0));
}

#[test]
fn multiple_growth_rounds_preserve_range_visibility() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("db");

    {
        let conn = open_connection(&db_path, false);
        let mut session = conn.open_session();
        create_table(&mut session);
        checkpoint(&mut session);

        for i in 0..60u32 {
            let key = format!("k{i:04}");
            insert_table_row(&session, key.as_bytes(), &vec![b'x'; 16]);
        }
        checkpoint(&mut session);

        for i in 60..120u32 {
            let key = format!("k{i:04}");
            insert_table_row(&session, key.as_bytes(), &vec![b'y'; 16]);
        }
        checkpoint(&mut session);
    }

    let conn = open_connection(&db_path, false);
    let session = conn.open_session();
    let slice = scan_table_range(&session, Some(b"k0058"), Some(b"k0063"));
    assert_eq!(
        slice,
        vec![
            (b"k0058".to_vec(), vec![b'x'; 16]),
            (b"k0059".to_vec(), vec![b'x'; 16]),
            (b"k0060".to_vec(), vec![b'y'; 16]),
            (b"k0061".to_vec(), vec![b'y'; 16]),
            (b"k0062".to_vec(), vec![b'y'; 16]),
        ]
    );
}
