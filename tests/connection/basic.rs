use wrongodb::{Connection, ConnectionConfig};

#[test]
fn test_connection_basic() {
    let tmp = tempfile::tempdir().unwrap();
    let conn = Connection::open(tmp.path().join("test"), ConnectionConfig::default()).unwrap();

    let mut session = conn.open_session();
    session.create("table:test").unwrap();

    // Open cursor first, then transaction
    let mut cursor = session.open_cursor("table:test").unwrap();
    {
        let txn = session.transaction().unwrap();
        let txn_id = txn.as_ref().id();
        cursor.insert(b"key1", b"value1", txn_id).unwrap();

        let value = cursor.get(b"key1", txn_id).unwrap().unwrap();
        assert_eq!(value, b"value1");

        txn.commit().unwrap();
    }
}

#[test]
fn test_connection_with_config() {
    let tmp = tempfile::tempdir().unwrap();
    let config = ConnectionConfig::new().wal_enabled(false);
    let conn = Connection::open(tmp.path().join("test"), config).unwrap();

    let mut session = conn.open_session();
    session.create("table:users").unwrap();

    // Open cursor first, then transaction
    let mut cursor = session.open_cursor("table:users").unwrap();
    {
        let txn = session.transaction().unwrap();
        let txn_id = txn.as_ref().id();
        cursor.insert(b"user1", b"alice", txn_id).unwrap();
        cursor.insert(b"user2", b"bob", txn_id).unwrap();

        let value = cursor.get(b"user1", txn_id).unwrap().unwrap();
        assert_eq!(value, b"alice");

        let value = cursor.get(b"user2", txn_id).unwrap().unwrap();
        assert_eq!(value, b"bob");

        txn.commit().unwrap();
    }
}

#[test]
fn test_cursor_delete() {
    let tmp = tempfile::tempdir().unwrap();
    let conn = Connection::open(tmp.path().join("test"), ConnectionConfig::default()).unwrap();

    let mut session = conn.open_session();
    session.create("table:items").unwrap();

    // Open cursor first, then transaction
    let mut cursor = session.open_cursor("table:items").unwrap();
    {
        let txn = session.transaction().unwrap();
        let txn_id = txn.as_ref().id();
        cursor.insert(b"item1", b"apple", txn_id).unwrap();
        cursor.insert(b"item2", b"banana", txn_id).unwrap();

        let value = cursor.get(b"item1", txn_id).unwrap().unwrap();
        assert_eq!(value, b"apple");

        cursor.delete(b"item1", txn_id).unwrap();

        assert!(cursor.get(b"item1", txn_id).unwrap().is_none());

        let value = cursor.get(b"item2", txn_id).unwrap().unwrap();
        assert_eq!(value, b"banana");

        txn.commit().unwrap();
    }
}
