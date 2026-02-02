use wrongodb::{Connection, ConnectionConfig};

#[test]
fn test_connection_basic() {
    let tmp = tempfile::tempdir().unwrap();
    let conn = Connection::open(tmp.path().join("test"), ConnectionConfig::default()).unwrap();

    let mut session = conn.open_session();
    session.create("table:test").unwrap();

    let mut cursor = session.open_cursor("table:test").unwrap();
    cursor.insert(b"key1", b"value1").unwrap();

    let value = cursor.get(b"key1").unwrap().unwrap();
    assert_eq!(value, b"value1");
}

#[test]
fn test_connection_with_config() {
    let tmp = tempfile::tempdir().unwrap();
    let config = ConnectionConfig::new()
        .wal_enabled(false);
    let conn = Connection::open(tmp.path().join("test"), config).unwrap();

    let mut session = conn.open_session();
    session.create("table:users").unwrap();

    let mut cursor = session.open_cursor("table:users").unwrap();
    cursor.insert(b"user1", b"alice").unwrap();
    cursor.insert(b"user2", b"bob").unwrap();

    let value = cursor.get(b"user1").unwrap().unwrap();
    assert_eq!(value, b"alice");

    let value = cursor.get(b"user2").unwrap().unwrap();
    assert_eq!(value, b"bob");
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

    let value = cursor.get(b"item1").unwrap().unwrap();
    assert_eq!(value, b"apple");

    cursor.delete(b"item1").unwrap();

    assert!(cursor.get(b"item1").unwrap().is_none());

    let value = cursor.get(b"item2").unwrap().unwrap();
    assert_eq!(value, b"banana");
}
