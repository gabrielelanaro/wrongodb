use serde_json::json;
use tempfile::tempdir;

use wrongodb::WrongoDB;

mod persistent_secondary_index;

#[test]
fn insert_and_find_roundtrip() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("db.log");

    let mut db = WrongoDB::open(&path, ["name"]).unwrap();

    let coll = db.collection("test").unwrap();
    let alice = coll.insert_one(json!({"name": "alice", "age": 30})).unwrap();
    let bob = coll.insert_one(json!({"name": "bob", "age": 25})).unwrap();

    assert!(alice.get("_id").unwrap().is_string());
    assert!(bob.get("_id").unwrap().is_string());

    let all = coll.find(None).unwrap();
    assert_eq!(all.len(), 2);

    let bobs = coll.find(Some(json!({"name": "bob"}))).unwrap();
    assert_eq!(bobs.len(), 1);
    assert_eq!(bobs[0].get("age").unwrap().as_i64().unwrap(), 25);

    let missing = coll.find_one(Some(json!({"name": "carol"}))).unwrap();
    assert!(missing.is_none());
}

#[test]
fn rebuilds_index_from_disk_on_open() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("db.log");

    {
        let mut db = WrongoDB::open(&path, ["name"]).unwrap();
        let coll = db.collection("test").unwrap();
        coll.insert_one(json!({"name": "alice"})).unwrap();
        coll.insert_one(json!({"name": "bob"})).unwrap();
    }

    let mut db2 = WrongoDB::open(&path, ["name"]).unwrap();
    let coll = db2.collection("test").unwrap();
    let bobs = coll.find(Some(json!({"name": "bob"}))).unwrap();
    assert_eq!(bobs.len(), 1);
}

#[test]
fn find_one_by_id_uses_main_table() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("db.log");

    let mut db = WrongoDB::open(&path, ["name"]).unwrap();
    let alice_id = {
        let coll = db.collection("test").unwrap();
        let alice = coll.insert_one(json!({"name": "alice"})).unwrap();
        let bob = coll.insert_one(json!({"name": "bob"})).unwrap();

        let bob_id = bob.get("_id").unwrap().clone();
        let got = coll
            .find_one(Some(json!({"_id": bob_id})))
            .unwrap()
            .unwrap();
        assert_eq!(got.get("_id").unwrap(), bob.get("_id").unwrap());
        assert_eq!(got.get("name").unwrap().as_str().unwrap(), "bob");

        alice.get("_id").unwrap().clone()
    };

    // Re-open and ensure the main table persists _id lookups.
    drop(db);
    let mut db2 = WrongoDB::open(&path, ["name"]).unwrap();
    let coll2 = db2.collection("test").unwrap();
    let got2 = coll2
        .find_one(Some(json!({"_id": alice_id})))
        .unwrap()
        .unwrap();
    assert_eq!(got2.get("name").unwrap().as_str().unwrap(), "alice");
}

#[test]
fn document_crud_roundtrip() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("db.log");

    let mut db = WrongoDB::open(&path, ["name"]).unwrap();
    let coll = db.collection("test").unwrap();
    let doc = coll
        .insert_one(json!({"name": "alice", "age": 30}))
        .unwrap();
    let id = doc.get("_id").unwrap().clone();

    let fetched = coll
        .find_one(Some(json!({"_id": id.clone()})))
        .unwrap()
        .unwrap();
    assert_eq!(fetched.get("name").unwrap().as_str().unwrap(), "alice");

    coll.update_one(
        Some(json!({"_id": id.clone()})),
        json!({"$set": {"age": 31}}),
    )
    .unwrap();

    let updated = coll
        .find_one(Some(json!({"_id": id.clone()})))
        .unwrap()
        .unwrap();
    assert_eq!(updated.get("age").unwrap().as_i64().unwrap(), 31);

    coll.delete_one(Some(json!({"_id": id.clone()}))).unwrap();
    let missing = coll.find_one(Some(json!({"_id": id}))).unwrap();
    assert!(missing.is_none());
}
