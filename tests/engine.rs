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

    let db2 = WrongoDB::open(&path, ["name"], false).unwrap();
    let bobs = db2.find(Some(json!({"name": "bob"}))).unwrap();
    assert_eq!(bobs.len(), 1);
}

