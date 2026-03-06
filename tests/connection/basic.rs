use serde_json::json;
use wrongodb::{Connection, ConnectionConfig};

#[test]
fn test_connection_basic() {
    let tmp = tempfile::tempdir().unwrap();
    let conn = Connection::open(tmp.path().join("test"), ConnectionConfig::default()).unwrap();

    let mut session = conn.open_session();
    session.create("table:test").unwrap();

    {
        let mut txn = session.transaction().unwrap();
        txn.session_mut()
            .insert_one("test", json!({"_id": "key1", "value": "value1"}))
            .unwrap();

        let value = txn
            .session_mut()
            .find_one("test", Some(json!({"_id": "key1"})))
            .unwrap()
            .unwrap();
        assert_eq!(value.get("value"), Some(&json!("value1")));

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
        txn.session_mut()
            .insert_one("users", json!({"_id": "user1", "name": "alice"}))
            .unwrap();
        txn.session_mut()
            .insert_one("users", json!({"_id": "user2", "name": "bob"}))
            .unwrap();

        let value = txn
            .session_mut()
            .find_one("users", Some(json!({"_id": "user1"})))
            .unwrap()
            .unwrap();
        assert_eq!(value.get("name"), Some(&json!("alice")));

        let value = txn
            .session_mut()
            .find_one("users", Some(json!({"_id": "user2"})))
            .unwrap()
            .unwrap();
        assert_eq!(value.get("name"), Some(&json!("bob")));

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
        txn.session_mut()
            .insert_one("items", json!({"_id": "item1", "value": "apple"}))
            .unwrap();
        txn.session_mut()
            .insert_one("items", json!({"_id": "item2", "value": "banana"}))
            .unwrap();

        let value = txn
            .session_mut()
            .find_one("items", Some(json!({"_id": "item1"})))
            .unwrap()
            .unwrap();
        assert_eq!(value.get("value"), Some(&json!("apple")));

        txn.session_mut()
            .delete_one("items", Some(json!({"_id": "item1"})))
            .unwrap();

        assert!(txn
            .session_mut()
            .find_one("items", Some(json!({"_id": "item1"})))
            .unwrap()
            .is_none());

        let value = txn
            .session_mut()
            .find_one("items", Some(json!({"_id": "item2"})))
            .unwrap()
            .unwrap();
        assert_eq!(value.get("value"), Some(&json!("banana")));

        txn.commit().unwrap();
    }
}

#[test]
fn test_session_autocommit_visibility_with_wal() {
    let tmp = tempfile::tempdir().unwrap();
    let conn = Connection::open(tmp.path().join("test"), ConnectionConfig::default()).unwrap();

    let mut session = conn.open_session();
    session.create("table:kv").unwrap();

    session
        .insert_one("kv", json!({"_id": "k1", "value": "v1"}))
        .unwrap();
    assert_eq!(
        session
            .find_one("kv", Some(json!({"_id": "k1"})))
            .unwrap()
            .unwrap()
            .get("value"),
        Some(&json!("v1"))
    );

    session
        .delete_one("kv", Some(json!({"_id": "k1"})))
        .unwrap();
    assert!(session
        .find_one("kv", Some(json!({"_id": "k1"})))
        .unwrap()
        .is_none());
}
