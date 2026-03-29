use wrongodb::{Connection, ConnectionConfig, WrongoDBError};

fn table_uri(collection: &str) -> String {
    format!("table:{collection}")
}

fn get_kv(
    conn: &Connection,
    collection: &str,
    key: &[u8],
) -> Result<Option<Vec<u8>>, WrongoDBError> {
    let mut session = conn.open_session();
    session.create_table(&table_uri(collection), Vec::new())?;
    let mut cursor = session.open_table_cursor(&table_uri(collection))?;
    cursor.get(key)
}

#[test]
fn test_connection_basic() {
    let tmp = tempfile::tempdir().unwrap();
    let conn = Connection::open(tmp.path().join("test"), ConnectionConfig::new()).unwrap();

    let mut session = conn.open_session();
    session.create_table("table:test", Vec::new()).unwrap();

    session
        .with_transaction(|session| {
            session
                .open_table_cursor("table:test")?
                .insert(b"key1", b"value1")?;
            let value = session
                .open_table_cursor("table:test")?
                .get(b"key1")?
                .unwrap();
            assert_eq!(value, b"value1".to_vec());
            Ok(())
        })
        .unwrap();
}

#[test]
fn test_connection_with_config() {
    let tmp = tempfile::tempdir().unwrap();
    let config = ConnectionConfig::new().logging_enabled(false);
    let conn = Connection::open(tmp.path().join("test"), config).unwrap();

    let mut session = conn.open_session();
    session.create_table("table:users", Vec::new()).unwrap();

    session
        .with_transaction(|session| {
            session
                .open_table_cursor("table:users")?
                .insert(b"user1", b"alice")?;
            session
                .open_table_cursor("table:users")?
                .insert(b"user2", b"bob")?;

            let value = session
                .open_table_cursor("table:users")?
                .get(b"user1")?
                .unwrap();
            assert_eq!(value, b"alice".to_vec());

            let value = session
                .open_table_cursor("table:users")?
                .get(b"user2")?
                .unwrap();
            assert_eq!(value, b"bob".to_vec());
            Ok(())
        })
        .unwrap();
}

#[test]
fn test_session_delete() {
    let tmp = tempfile::tempdir().unwrap();
    let conn = Connection::open(tmp.path().join("test"), ConnectionConfig::new()).unwrap();

    let mut session = conn.open_session();
    session.create_table("table:items", Vec::new()).unwrap();

    session
        .with_transaction(|session| {
            session
                .open_table_cursor("table:items")?
                .insert(b"item1", b"apple")?;
            session
                .open_table_cursor("table:items")?
                .insert(b"item2", b"banana")?;

            let value = session
                .open_table_cursor("table:items")?
                .get(b"item1")?
                .unwrap();
            assert_eq!(value, b"apple".to_vec());

            session.open_table_cursor("table:items")?.delete(b"item1")?;

            assert!(session
                .open_table_cursor("table:items")?
                .get(b"item1")?
                .is_none());

            let value = session
                .open_table_cursor("table:items")?
                .get(b"item2")?
                .unwrap();
            assert_eq!(value, b"banana".to_vec());
            Ok(())
        })
        .unwrap();
}

#[test]
fn test_session_autocommit_visibility_with_wal() {
    let tmp = tempfile::tempdir().unwrap();
    let conn = Connection::open(tmp.path().join("test"), ConnectionConfig::new()).unwrap();

    let mut session = conn.open_session();
    session.create_table("table:kv", Vec::new()).unwrap();

    session
        .open_table_cursor("table:kv")
        .unwrap()
        .insert(b"k1", b"v1")
        .unwrap();
    assert_eq!(get_kv(&conn, "kv", b"k1").unwrap().unwrap(), b"v1".to_vec());

    session
        .open_table_cursor("table:kv")
        .unwrap()
        .delete(b"k1")
        .unwrap();
    assert!(get_kv(&conn, "kv", b"k1").unwrap().is_none());
}
