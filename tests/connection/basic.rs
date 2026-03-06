use crate::common::kv::{delete_kv_in_session, get_kv, get_kv_in_session, insert_kv_in_session};
use wrongodb::{Connection, ConnectionConfig};

#[test]
fn test_connection_basic() {
    let tmp = tempfile::tempdir().unwrap();
    let conn = Connection::open(tmp.path().join("test"), ConnectionConfig::default()).unwrap();

    let mut session = conn.open_session();
    session.create("table:test").unwrap();

    {
        let mut txn = session.transaction().unwrap();
        let txn_id = txn.as_ref().id();
        insert_kv_in_session(txn.session_mut(), "test", b"key1", b"value1", txn_id).unwrap();

        let value = get_kv_in_session(txn.session_mut(), "test", b"key1", txn_id)
            .unwrap()
            .unwrap();
        assert_eq!(value, b"value1".to_vec());

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

    {
        let mut txn = session.transaction().unwrap();
        let txn_id = txn.as_ref().id();
        insert_kv_in_session(txn.session_mut(), "users", b"user1", b"alice", txn_id).unwrap();
        insert_kv_in_session(txn.session_mut(), "users", b"user2", b"bob", txn_id).unwrap();

        let value = get_kv_in_session(txn.session_mut(), "users", b"user1", txn_id)
            .unwrap()
            .unwrap();
        assert_eq!(value, b"alice".to_vec());

        let value = get_kv_in_session(txn.session_mut(), "users", b"user2", txn_id)
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
        let txn_id = txn.as_ref().id();
        insert_kv_in_session(txn.session_mut(), "items", b"item1", b"apple", txn_id).unwrap();
        insert_kv_in_session(txn.session_mut(), "items", b"item2", b"banana", txn_id).unwrap();

        let value = get_kv_in_session(txn.session_mut(), "items", b"item1", txn_id)
            .unwrap()
            .unwrap();
        assert_eq!(value, b"apple".to_vec());

        delete_kv_in_session(txn.session_mut(), "items", b"item1", txn_id).unwrap();

        assert!(
            get_kv_in_session(txn.session_mut(), "items", b"item1", txn_id)
                .unwrap()
                .is_none()
        );

        let value = get_kv_in_session(txn.session_mut(), "items", b"item2", txn_id)
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

    insert_kv_in_session(&mut session, "kv", b"k1", b"v1", 0).unwrap();
    assert_eq!(get_kv(&conn, "kv", b"k1").unwrap().unwrap(), b"v1".to_vec());

    delete_kv_in_session(&mut session, "kv", b"k1", 0).unwrap();
    assert!(get_kv(&conn, "kv", b"k1").unwrap().is_none());
}
