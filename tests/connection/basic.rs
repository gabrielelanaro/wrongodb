use crate::common::kv::{
    delete_kv_in_session, delete_kv_in_transaction, get_kv, get_kv_in_transaction,
    insert_kv_in_session, insert_kv_in_transaction,
};
use wrongodb::{Connection, ConnectionConfig};

#[test]
fn test_connection_basic() {
    let tmp = tempfile::tempdir().unwrap();
    let conn = Connection::open(tmp.path().join("test"), ConnectionConfig::default()).unwrap();

    let mut session = conn.open_session();
    session.create_table("table:test").unwrap();

    {
        session
            .with_transaction(|session| {
                insert_kv_in_transaction(session, "test", b"key1", b"value1")?;
                let value = get_kv_in_transaction(session, "test", b"key1")?.unwrap();
                assert_eq!(value, b"value1".to_vec());
                Ok(())
            })
            .unwrap();
    }
}

#[test]
fn test_connection_with_config() {
    let tmp = tempfile::tempdir().unwrap();
    let config = ConnectionConfig::new(false);
    let conn = Connection::open(tmp.path().join("test"), config).unwrap();

    let mut session = conn.open_session();
    session.create_table("table:users").unwrap();

    {
        session
            .with_transaction(|session| {
                insert_kv_in_transaction(session, "users", b"user1", b"alice")?;
                insert_kv_in_transaction(session, "users", b"user2", b"bob")?;

                let value = get_kv_in_transaction(session, "users", b"user1")?.unwrap();
                assert_eq!(value, b"alice".to_vec());

                let value = get_kv_in_transaction(session, "users", b"user2")?.unwrap();
                assert_eq!(value, b"bob".to_vec());
                Ok(())
            })
            .unwrap();
    }
}

#[test]
fn test_session_delete() {
    let tmp = tempfile::tempdir().unwrap();
    let conn = Connection::open(tmp.path().join("test"), ConnectionConfig::default()).unwrap();

    let mut session = conn.open_session();
    session.create_table("table:items").unwrap();

    {
        session
            .with_transaction(|session| {
                insert_kv_in_transaction(session, "items", b"item1", b"apple")?;
                insert_kv_in_transaction(session, "items", b"item2", b"banana")?;

                let value = get_kv_in_transaction(session, "items", b"item1")?.unwrap();
                assert_eq!(value, b"apple".to_vec());

                delete_kv_in_transaction(session, "items", b"item1")?;

                assert!(get_kv_in_transaction(session, "items", b"item1")?.is_none());

                let value = get_kv_in_transaction(session, "items", b"item2")?.unwrap();
                assert_eq!(value, b"banana".to_vec());
                Ok(())
            })
            .unwrap();
    }
}

#[test]
fn test_session_autocommit_visibility_with_wal() {
    let tmp = tempfile::tempdir().unwrap();
    let conn = Connection::open(tmp.path().join("test"), ConnectionConfig::default()).unwrap();

    let mut session = conn.open_session();
    session.create_table("table:kv").unwrap();

    insert_kv_in_session(&mut session, "kv", b"k1", b"v1").unwrap();
    assert_eq!(get_kv(&conn, "kv", b"k1").unwrap().unwrap(), b"v1".to_vec());

    delete_kv_in_session(&mut session, "kv", b"k1").unwrap();
    assert!(get_kv(&conn, "kv", b"k1").unwrap().is_none());
}
