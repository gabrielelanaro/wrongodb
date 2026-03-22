use tempfile::tempdir;

use wrongodb::{Connection, ConnectionConfig, WrongoDBError};

fn insert_kv(conn: &Connection, key: &[u8], value: &[u8]) -> Result<(), WrongoDBError> {
    let mut session = conn.open_session();
    session.create_table("table:test")?;
    let mut cursor = session.open_table_cursor("table:test")?;
    cursor.insert(key, value)?;
    Ok(())
}

#[test]
fn wal_enabled_connection_allows_public_cursor_writes() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("db");
    let conn = Connection::open(&path, ConnectionConfig::new(true)).unwrap();

    insert_kv(&conn, b"alice", b"value").unwrap();

    let session = conn.open_session();
    let mut cursor = session.open_table_cursor("table:test").unwrap();
    let value = cursor.get(b"alice").unwrap().unwrap();
    assert_eq!(value, b"value".to_vec());
}

#[test]
fn wal_disabled_connection_allows_public_cursor_writes() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("db");
    let conn = Connection::open(&path, ConnectionConfig::new(false)).unwrap();

    insert_kv(&conn, b"alice", b"value").unwrap();

    let session = conn.open_session();
    let mut cursor = session.open_table_cursor("table:test").unwrap();
    let value = cursor.get(b"alice").unwrap().unwrap();
    assert_eq!(value, b"value".to_vec());
}
