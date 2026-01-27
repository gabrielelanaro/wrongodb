use serde_json::json;
use tempfile::tempdir;

use wrongodb::WrongoDB;

#[test]
fn insert_and_find_roundtrip() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("db.log");

    let mut db = WrongoDB::open(&path, ["name"], false).unwrap();

    let alice = db.insert_one(json!({"name": "alice", "age": 30})).unwrap();
    let bob = db.insert_one(json!({"name": "bob", "age": 25})).unwrap();

    assert!(alice.get("_id").unwrap().is_string());
    assert!(bob.get("_id").unwrap().is_string());

    let all = db.find(None).unwrap();
    assert_eq!(all.len(), 2);

    let bobs = db.find(Some(json!({"name": "bob"}))).unwrap();
    assert_eq!(bobs.len(), 1);
    assert_eq!(bobs[0].get("age").unwrap().as_i64().unwrap(), 25);

    let missing = db.find_one(Some(json!({"name": "carol"}))).unwrap();
    assert!(missing.is_none());
}

#[test]
fn rebuilds_index_from_disk_on_open() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("db.log");

    {
        let mut db = WrongoDB::open(&path, ["name"], true).unwrap();
        db.insert_one(json!({"name": "alice"})).unwrap();
        db.insert_one(json!({"name": "bob"})).unwrap();
    }

    let mut db2 = WrongoDB::open(&path, ["name"], false).unwrap();
    let bobs = db2.find(Some(json!({"name": "bob"}))).unwrap();
    assert_eq!(bobs.len(), 1);
}

#[test]
fn find_one_by_id_hits_id_index() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("db.log");

    let mut db = WrongoDB::open(&path, ["name"], false).unwrap();
    let alice = db.insert_one(json!({"name": "alice"})).unwrap();
    let bob = db.insert_one(json!({"name": "bob"})).unwrap();

    let bob_id = bob.get("_id").unwrap().clone();
    let got = db.find_one(Some(json!({"_id": bob_id}))).unwrap().unwrap();
    assert_eq!(got.get("_id").unwrap(), bob.get("_id").unwrap());
    assert_eq!(got.get("name").unwrap().as_str().unwrap(), "bob");

    // Re-open and ensure the id index is rebuilt from the append-only log.
    drop(db);
    let mut db2 = WrongoDB::open(&path, ["name"], false).unwrap();
    let got2 = db2
        .find_one(Some(json!({"_id": alice.get("_id").unwrap().clone()})))
        .unwrap()
        .unwrap();
    assert_eq!(got2.get("name").unwrap().as_str().unwrap(), "alice");
}
