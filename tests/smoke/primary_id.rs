use serde_json::json;
use tempfile::tempdir;

use wrongodb::WrongoDB;

fn is_lower_hex_24(s: &str) -> bool {
    if s.len() != 24 {
        return false;
    }
    s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

#[test]
fn auto_id_generation() {
    let tmp = tempdir().unwrap();
    let db_dir = tmp.path().join("db");

    let db = WrongoDB::open(&db_dir).unwrap();
    let coll = db.collection("test");
    let mut session = db.open_session();

    // Default `_id` is ObjectId-like (24 lowercase hex chars).
    let auto = coll
        .insert_one(&mut session, json!({"name": "auto"}))
        .unwrap();
    let auto_id = auto.get("_id").unwrap().as_str().unwrap();
    assert!(is_lower_hex_24(auto_id));
}

#[test]
fn primary_btree_file_created() {
    let tmp = tempdir().unwrap();
    let db_dir = tmp.path().join("db");
    let main_table_path = db_dir.join("test.main.wt");

    let db = WrongoDB::open(&db_dir).unwrap();
    let coll = db.collection("test");
    let mut session = db.open_session();

    coll.insert_one(&mut session, json!({"name": "auto"}))
        .unwrap();

    // Primary B+tree file is created and non-empty.
    let meta = std::fs::metadata(&main_table_path).unwrap();
    assert!(meta.len() > 0);
}

#[test]
fn duplicate_key_rejection() {
    let tmp = tempdir().unwrap();
    let db_dir = tmp.path().join("db");

    let db = WrongoDB::open(&db_dir).unwrap();
    let coll = db.collection("test");
    let mut session = db.open_session();

    // Uniqueness enforcement: duplicate `_id` insert fails.
    let first = coll
        .insert_one(&mut session, json!({"_id": "dup", "name": "first"}))
        .unwrap();
    let dup_id = first.get("_id").unwrap().clone();
    let dup_attempt = coll.insert_one(&mut session, json!({"_id": dup_id, "name": "second"}));
    assert!(dup_attempt.is_err());
    let msg = dup_attempt.err().unwrap().to_string();
    assert!(
        msg.contains("duplicate key error"),
        "expected duplicate key error, got: {msg}"
    );
}

#[test]
fn embedded_id_field_ordering() {
    let tmp = tempdir().unwrap();
    let db_dir = tmp.path().join("db");

    let db = WrongoDB::open(&db_dir).unwrap();
    let coll = db.collection("test");
    let mut session = db.open_session();

    // Embedded-doc field order matters for `_id` (Mongo-like):
    // `{a:1,b:2}` and `{b:2,a:1}` should be treated as distinct ids.
    coll.insert_one(
        &mut session,
        json!({"_id": {"a": 1, "b": 2}, "name": "order1"}),
    )
    .unwrap();
    coll.insert_one(
        &mut session,
        json!({"_id": {"b": 2, "a": 1}, "name": "order2"}),
    )
    .unwrap();

    let got_order1 = coll
        .find_one(&mut session, Some(json!({"_id": {"a": 1, "b": 2}})))
        .unwrap()
        .unwrap();
    assert_eq!(got_order1.get("name").unwrap().as_str().unwrap(), "order1");

    let got_order2 = coll
        .find_one(&mut session, Some(json!({"_id": {"b": 2, "a": 1}})))
        .unwrap()
        .unwrap();
    assert_eq!(got_order2.get("name").unwrap().as_str().unwrap(), "order2");
}

#[test]
fn persistence_across_reopen() {
    let tmp = tempdir().unwrap();
    let db_dir = tmp.path().join("db");

    let db = WrongoDB::open(&db_dir).unwrap();
    {
        let coll = db.collection("test");
        let mut session = db.open_session();
        coll.insert_one(
            &mut session,
            json!({"_id": {"b": 2, "a": 1}, "name": "order2"}),
        )
        .unwrap();
    }

    // Re-open and verify primary lookups still work.
    drop(db);
    let db2 = WrongoDB::open(&db_dir).unwrap();
    let coll = db2.collection("test");
    let mut session = db2.open_session();
    let got = coll
        .find_one(&mut session, Some(json!({"_id": {"b": 2, "a": 1}})))
        .unwrap()
        .unwrap();
    assert_eq!(got.get("name").unwrap().as_str().unwrap(), "order2");
}
