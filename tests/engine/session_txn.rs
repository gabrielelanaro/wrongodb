//! Session-scoped transaction behavior tests.

use serde_json::json;
use tempfile::tempdir;

use wrongodb::WrongoDB;

#[test]
fn session_txn_insert_and_commit() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("db");

    let db = WrongoDB::open(&path).unwrap();
    let coll = db.collection("test");

    let mut session = db.open_session();
    let mut txn = session.transaction().unwrap();
    let doc = coll
        .insert_one(txn.session_mut(), json!({"name": "alice"}))
        .unwrap();
    let id = doc.get("_id").unwrap().clone();
    txn.commit().unwrap();

    let mut verify = db.open_session();
    let found = coll
        .find_one(&mut verify, Some(json!({"_id": id})))
        .unwrap();
    assert!(found.is_some());
}

#[test]
fn session_txn_insert_and_abort() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("db");

    let db = WrongoDB::open(&path).unwrap();
    let coll = db.collection("test");

    let mut session = db.open_session();
    let mut txn = session.transaction().unwrap();
    let doc = coll
        .insert_one(txn.session_mut(), json!({"name": "alice"}))
        .unwrap();
    let id = doc.get("_id").unwrap().clone();
    txn.abort().unwrap();

    let mut verify = db.open_session();
    let found = coll
        .find_one(&mut verify, Some(json!({"_id": id})))
        .unwrap();
    assert!(found.is_none());
}

#[test]
fn session_txn_read_your_own_writes() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("db");

    let db = WrongoDB::open(&path).unwrap();
    let coll = db.collection("test");

    let mut session = db.open_session();
    let mut txn = session.transaction().unwrap();
    let doc = coll
        .insert_one(txn.session_mut(), json!({"name": "alice"}))
        .unwrap();
    let id = doc.get("_id").unwrap().clone();

    let found = coll
        .find_one(txn.session_mut(), Some(json!({"_id": id})))
        .unwrap();
    assert!(found.is_some());

    txn.commit().unwrap();
}

#[test]
fn session_txn_update_and_commit() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("db");

    let db = WrongoDB::open(&path).unwrap();
    let coll = db.collection("test");

    let mut session = db.open_session();
    let doc = coll
        .insert_one(&mut session, json!({"name": "alice", "age": 30}))
        .unwrap();
    let id = doc.get("_id").unwrap().clone();

    let mut txn = session.transaction().unwrap();
    let result = coll
        .update_one(
            txn.session_mut(),
            Some(json!({"_id": id.clone()})),
            json!({"$set": {"age": 31}}),
        )
        .unwrap();
    assert_eq!(result.modified, 1);
    txn.commit().unwrap();

    let found = coll
        .find_one(&mut session, Some(json!({"_id": id})))
        .unwrap()
        .unwrap();
    assert_eq!(found.get("age").unwrap().as_i64().unwrap(), 31);
}

#[test]
fn session_txn_delete_and_commit() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("db");

    let db = WrongoDB::open(&path).unwrap();
    let coll = db.collection("test");

    let mut session = db.open_session();
    let doc = coll
        .insert_one(&mut session, json!({"name": "alice"}))
        .unwrap();
    let id = doc.get("_id").unwrap().clone();

    let mut txn = session.transaction().unwrap();
    let deleted = coll
        .delete_one(txn.session_mut(), Some(json!({"_id": id.clone()})))
        .unwrap();
    assert_eq!(deleted, 1);
    txn.commit().unwrap();

    let found = coll
        .find_one(&mut session, Some(json!({"_id": id})))
        .unwrap();
    assert!(found.is_none());
}

#[test]
fn session_txn_duplicate_key_error() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("db");

    let db = WrongoDB::open(&path).unwrap();
    let coll = db.collection("test");

    let mut session = db.open_session();
    coll.insert_one(&mut session, json!({"_id": "dup", "name": "alice"}))
        .unwrap();

    let mut txn = session.transaction().unwrap();
    let result = coll.insert_one(txn.session_mut(), json!({"_id": "dup", "name": "bob"}));
    assert!(result.is_err());
    txn.abort().unwrap();
}

#[test]
fn session_txn_multi_collection_commit_and_abort() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("db");

    let db = WrongoDB::open(&path).unwrap();
    let coll_a = db.collection("coll_a");
    let coll_b = db.collection("coll_b");

    let mut session = db.open_session();
    let mut txn = session.transaction().unwrap();
    let doc_a = coll_a
        .insert_one(txn.session_mut(), json!({"name": "alice"}))
        .unwrap();
    let doc_b = coll_b
        .insert_one(txn.session_mut(), json!({"name": "bob"}))
        .unwrap();
    let id_a = doc_a.get("_id").unwrap().clone();
    let id_b = doc_b.get("_id").unwrap().clone();
    txn.commit().unwrap();

    let mut verify = db.open_session();
    assert!(coll_a
        .find_one(&mut verify, Some(json!({"_id": id_a.clone()})))
        .unwrap()
        .is_some());
    assert!(coll_b
        .find_one(&mut verify, Some(json!({"_id": id_b.clone()})))
        .unwrap()
        .is_some());

    let mut txn2 = session.transaction().unwrap();
    coll_a
        .insert_one(txn2.session_mut(), json!({"name": "carol"}))
        .unwrap();
    coll_b
        .insert_one(txn2.session_mut(), json!({"name": "dave"}))
        .unwrap();
    txn2.abort().unwrap();

    let mut verify2 = db.open_session();
    let carols = coll_a
        .find(&mut verify2, Some(json!({"name": "carol"})))
        .unwrap();
    let daves = coll_b
        .find(&mut verify2, Some(json!({"name": "dave"})))
        .unwrap();
    assert!(carols.is_empty());
    assert!(daves.is_empty());
}

#[test]
fn session_txn_index_rollback_on_abort() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("db");

    let db = WrongoDB::open(&path).unwrap();
    let coll = db.collection("test");
    let mut session = db.open_session();

    coll.create_index(&mut session, "name").unwrap();

    let mut txn = session.transaction().unwrap();
    coll.insert_one(txn.session_mut(), json!({"name": "alice"}))
        .unwrap();
    txn.abort().unwrap();

    let found = coll
        .find(&mut session, Some(json!({"name": "alice"})))
        .unwrap();
    assert!(found.is_empty());
}

#[test]
fn session_txn_index_query_sees_uncommitted_write() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("db");

    let db = WrongoDB::open(&path).unwrap();
    let coll = db.collection("test");
    let mut session = db.open_session();

    coll.create_index(&mut session, "name").unwrap();

    let mut txn = session.transaction().unwrap();
    coll.insert_one(txn.session_mut(), json!({"name": "alice"}))
        .unwrap();

    let found = coll
        .find(txn.session_mut(), Some(json!({"name": "alice"})))
        .unwrap();
    assert_eq!(found.len(), 1);

    txn.commit().unwrap();
}

#[test]
fn crash_before_commit_marker_skips_multi_collection_writes() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("db");

    {
        let db = WrongoDB::open(&path).unwrap();
        let coll_a = db.collection("coll_a");
        let coll_b = db.collection("coll_b");
        let mut session = db.open_session();

        let mut txn = session.transaction().unwrap();
        coll_a
            .insert_one(txn.session_mut(), json!({"_id": 1, "name": "alice"}))
            .unwrap();
        coll_b
            .insert_one(txn.session_mut(), json!({"_id": 1, "name": "bob"}))
            .unwrap();

        // Simulate process crash before commit/abort marker emission.
        std::mem::forget(txn);
    }

    let db = WrongoDB::open(&path).unwrap();
    let coll_a = db.collection("coll_a");
    let coll_b = db.collection("coll_b");
    let mut session = db.open_session();

    assert!(coll_a
        .find_one(&mut session, Some(json!({"_id": 1})))
        .unwrap()
        .is_none());
    assert!(coll_b
        .find_one(&mut session, Some(json!({"_id": 1})))
        .unwrap()
        .is_none());
}

#[test]
fn crash_after_commit_marker_recovers_multi_collection_writes() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("db");

    {
        let db = WrongoDB::open(&path).unwrap();
        let coll_a = db.collection("coll_a");
        let coll_b = db.collection("coll_b");
        let mut session = db.open_session();

        let mut txn = session.transaction().unwrap();
        coll_a
            .insert_one(txn.session_mut(), json!({"_id": 1, "name": "alice"}))
            .unwrap();
        coll_b
            .insert_one(txn.session_mut(), json!({"_id": 1, "name": "bob"}))
            .unwrap();
        txn.commit().unwrap();
    }

    let db = WrongoDB::open(&path).unwrap();
    let coll_a = db.collection("coll_a");
    let coll_b = db.collection("coll_b");
    let mut session = db.open_session();

    assert!(coll_a
        .find_one(&mut session, Some(json!({"_id": 1})))
        .unwrap()
        .is_some());
    assert!(coll_b
        .find_one(&mut session, Some(json!({"_id": 1})))
        .unwrap()
        .is_some());
}
