use crate::common::kv::{
    delete_kv_in_session, delete_kv_in_write_unit, get_kv, get_kv_in_write_unit,
    insert_kv_in_session, insert_kv_in_write_unit,
};
use wrongodb::{Connection, ConnectionConfig};

#[test]
fn test_connection_basic() {
    let tmp = tempfile::tempdir().unwrap();
    let conn = Connection::open(tmp.path().join("test"), ConnectionConfig::default()).unwrap();

    let mut session = conn.open_session();
    session.create("table:test").unwrap();

    {
        let mut txn = session.transaction().unwrap();
        insert_kv_in_write_unit(&mut txn, "test", b"key1", b"value1").unwrap();

        let value = get_kv_in_write_unit(&mut txn, "test", b"key1")
            .unwrap()
            .unwrap();
        assert_eq!(value, b"value1".to_vec());

        txn.commit().unwrap();
    }
}

#[test]
fn test_connection_with_config() {
    let tmp = tempfile::tempdir().unwrap();
    let config = ConnectionConfig::new(false);
    let conn = Connection::open(tmp.path().join("test"), config).unwrap();

    let mut session = conn.open_session();
    session.create("table:users").unwrap();

    {
        let mut txn = session.transaction().unwrap();
        insert_kv_in_write_unit(&mut txn, "users", b"user1", b"alice").unwrap();
        insert_kv_in_write_unit(&mut txn, "users", b"user2", b"bob").unwrap();

        let value = get_kv_in_write_unit(&mut txn, "users", b"user1")
            .unwrap()
            .unwrap();
        assert_eq!(value, b"alice".to_vec());

        let value = get_kv_in_write_unit(&mut txn, "users", b"user2")
            .unwrap()
            .unwrap();
        assert_eq!(value, b"bob".to_vec());

        txn.commit().unwrap();
    }
}

#[test]
fn test_session_delete() {
    let tmp = tempfile::tempdir().unwrap();
    let conn = Connection::open(tmp.path().join("test"), ConnectionConfig::default()).unwrap();

    let mut session = conn.open_session();
    session.create("table:items").unwrap();

    {
        let mut txn = session.transaction().unwrap();
        insert_kv_in_write_unit(&mut txn, "items", b"item1", b"apple").unwrap();
        insert_kv_in_write_unit(&mut txn, "items", b"item2", b"banana").unwrap();

        let value = get_kv_in_write_unit(&mut txn, "items", b"item1")
            .unwrap()
            .unwrap();
        assert_eq!(value, b"apple".to_vec());

        delete_kv_in_write_unit(&mut txn, "items", b"item1").unwrap();

        assert!(get_kv_in_write_unit(&mut txn, "items", b"item1")
            .unwrap()
            .is_none());

        let value = get_kv_in_write_unit(&mut txn, "items", b"item2")
            .unwrap()
            .unwrap();
        assert_eq!(value, b"banana".to_vec());

        txn.commit().unwrap();
    }
}

#[test]
fn test_session_autocommit_visibility_with_wal() {
    let tmp = tempfile::tempdir().unwrap();
    let conn = Connection::open(tmp.path().join("test"), ConnectionConfig::default()).unwrap();

    let mut session = conn.open_session();
    session.create("table:kv").unwrap();

    insert_kv_in_session(&mut session, "kv", b"k1", b"v1").unwrap();
    assert_eq!(get_kv(&conn, "kv", b"k1").unwrap().unwrap(), b"v1".to_vec());

    delete_kv_in_session(&mut session, "kv", b"k1").unwrap();
    assert!(get_kv(&conn, "kv", b"k1").unwrap().is_none());
}
