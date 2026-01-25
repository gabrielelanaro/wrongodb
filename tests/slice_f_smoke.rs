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
fn slice_f_primary_id_smoke() {
    let tmp = tempdir().unwrap();
    let log_path = tmp.path().join("db.log");
    let id_index_path = tmp.path().join("db.log.id_index.wt");

    let mut db = WrongoDB::open(&log_path, ["name"], true).unwrap();

    // 1) Default `_id` is ObjectId-like (24 lowercase hex chars).
    let auto = db.insert_one(json!({"name": "auto"})).unwrap();
    let auto_id = auto.get("_id").unwrap().as_str().unwrap();
    assert!(is_lower_hex_24(auto_id));

    // 2) Primary B+tree file is created and non-empty.
    let meta = std::fs::metadata(&id_index_path).unwrap();
    assert!(meta.len() > 0);

    // 3) Uniqueness enforcement: duplicate `_id` insert fails.
    let err = db
        .insert_one(json!({"_id": "dup", "name": "first"}))
        .unwrap();
    let dup_id = err.get("_id").unwrap().clone();
    let dup_attempt = db.insert_one(json!({"_id": dup_id, "name": "second"}));
    assert!(dup_attempt.is_err());
    let msg = dup_attempt.err().unwrap().to_string();
    assert!(
        msg.contains("duplicate key error"),
        "expected duplicate key error, got: {msg}"
    );

    // 4) Embedded-doc field order matters for `_id` (Mongo-like):
    // `{a:1,b:2}` and `{b:2,a:1}` should be treated as distinct ids.
    db.insert_one(json!({"_id": {"a": 1, "b": 2}, "name": "order1"}))
        .unwrap();
    db.insert_one(json!({"_id": {"b": 2, "a": 1}, "name": "order2"}))
        .unwrap();

    let got_order1 = db
        .find_one(Some(json!({"_id": {"a": 1, "b": 2}})))
        .unwrap()
        .unwrap();
    assert_eq!(got_order1.get("name").unwrap().as_str().unwrap(), "order1");

    let got_order2 = db
        .find_one(Some(json!({"_id": {"b": 2, "a": 1}})))
        .unwrap()
        .unwrap();
    assert_eq!(got_order2.get("name").unwrap().as_str().unwrap(), "order2");

    // 5) Re-open and verify primary lookups still work (index rebuild on open).
    drop(db);
    let db2 = WrongoDB::open(&log_path, ["name"], false).unwrap();
    let got = db2
        .find_one(Some(json!({"_id": {"b": 2, "a": 1}})))
        .unwrap()
        .unwrap();
    assert_eq!(got.get("name").unwrap().as_str().unwrap(), "order2");
}
