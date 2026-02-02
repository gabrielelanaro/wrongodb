use wrongodb::{Connection, ConnectionConfig};

#[test]
fn test_connection_basic() {
    let tmp = tempfile::tempdir().unwrap();
    let conn = Connection::open(tmp.path().join("test"), ConnectionConfig::default()).unwrap();

    let mut session = conn.open_session();
    session.create("table:test").unwrap();

    let mut cursor = session.open_cursor("table:test").unwrap();
    cursor.insert(b"key1", b"value1").unwrap();

    let value = cursor.get(b"key1").unwrap();
    assert_eq!(value, Some(b"value1".to_vec()));
}

#[test]
fn test_connection_with_config() {
    let tmp = tempfile::tempdir().unwrap();
    let config = ConnectionConfig::new()
        .wal_enabled(false)
        .checkpoint_after_updates(100);
    let conn = Connection::open(tmp.path().join("test"), config).unwrap();

    let mut session = conn.open_session();
    session.create("table:users").unwrap();

    let mut cursor = session.open_cursor("table:users").unwrap();
    cursor.insert(b"user1", b"alice").unwrap();
    cursor.insert(b"user2", b"bob").unwrap();

    let value1 = cursor.get(b"user1").unwrap();
    let value2 = cursor.get(b"user2").unwrap();
    assert_eq!(value1, Some(b"alice".to_vec()));
    assert_eq!(value2, Some(b"bob".to_vec()));
}

#[test]
fn test_cursor_delete() {
    let tmp = tempfile::tempdir().unwrap();
    let conn = Connection::open(tmp.path().join("test"), ConnectionConfig::default()).unwrap();

    let mut session = conn.open_session();
    session.create("table:items").unwrap();

    let mut cursor = session.open_cursor("table:items").unwrap();
    cursor.insert(b"item1", b"apple").unwrap();
    cursor.insert(b"item2", b"banana").unwrap();

    let value1 = cursor.get(b"item1").unwrap();
    assert_eq!(value1, Some(b"apple".to_vec()));

    let deleted = cursor.delete(b"item1").unwrap();
    assert_eq!(deleted, true);

    let value1_after = cursor.get(b"item1").unwrap();
    assert_eq!(value1_after, None);

    let value2_after = cursor.get(b"item2").unwrap();
    assert_eq!(value2_after, Some(b"banana".to_vec()));
}
