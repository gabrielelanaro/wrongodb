use std::collections::HashSet;

use serde_json::Value;

use crate::api::cursor::Cursor;
use crate::api::session::Session;
use crate::core::bson::{decode_document, encode_document, encode_id_value};
use crate::core::document::{normalize_document_in_place, validate_is_object};
use crate::core::errors::StorageError;
use crate::index::{decode_index_id, encode_range_bounds};
use crate::txn::TxnId;
use crate::{Document, WrongoDBError};

mod update;

use self::update::apply_update;

#[derive(Debug, Clone, Copy)]
pub(crate) struct UpdateResult {
    pub(crate) matched: usize,
    pub(crate) modified: usize,
}

pub(crate) fn insert_one(
    session: &mut Session,
    collection: &str,
    doc: Value,
) -> Result<Document, WrongoDBError> {
    with_txn(session, |session| {
        insert_one_in_txn(session, collection, doc)
    })
}

pub(crate) fn find(
    session: &mut Session,
    collection: &str,
    filter: Option<Value>,
) -> Result<Vec<Document>, WrongoDBError> {
    with_txn(session, |session| {
        let txn_id = require_txn_id(session)?;
        find_with_txn(session, collection, filter, txn_id)
    })
}

#[allow(dead_code)]
pub(crate) fn find_one(
    session: &mut Session,
    collection: &str,
    filter: Option<Value>,
) -> Result<Option<Document>, WrongoDBError> {
    Ok(find(session, collection, filter)?.into_iter().next())
}

pub(crate) fn count(
    session: &mut Session,
    collection: &str,
    filter: Option<Value>,
) -> Result<usize, WrongoDBError> {
    Ok(find(session, collection, filter)?.len())
}

pub(crate) fn distinct(
    session: &mut Session,
    collection: &str,
    key: &str,
    filter: Option<Value>,
) -> Result<Vec<Value>, WrongoDBError> {
    let docs = find(session, collection, filter)?;
    let mut seen = HashSet::new();
    let mut values = Vec::new();

    for doc in docs {
        if let Some(val) = doc.get(key) {
            let key_str = serde_json::to_string(val).unwrap_or_default();
            if seen.insert(key_str) {
                values.push(val.clone());
            }
        }
    }

    Ok(values)
}

pub(crate) fn update_one(
    session: &mut Session,
    collection: &str,
    filter: Option<Value>,
    update: Value,
) -> Result<UpdateResult, WrongoDBError> {
    with_txn(session, |session| {
        let txn_id = require_txn_id(session)?;
        let docs = find_with_txn(session, collection, filter, txn_id)?;
        if docs.is_empty() {
            return Ok(UpdateResult {
                matched: 0,
                modified: 0,
            });
        }

        let doc = &docs[0];
        let updated_doc = apply_update(doc, &update)?;

        let id = doc
            .get("_id")
            .ok_or_else(|| crate::core::errors::DocumentValidationError("missing _id".into()))?;
        let key = encode_id_value(id)?;
        let value = encode_document(&updated_doc)?;

        let txn_id = require_txn_id(session)?;
        let mut cursor = session.open_cursor(&format!("table:{}", collection))?;
        cursor.update(&key, &value, txn_id)?;

        apply_index_remove(session, collection, doc, txn_id)?;
        apply_index_add(session, collection, &updated_doc, txn_id)?;

        Ok(UpdateResult {
            matched: 1,
            modified: 1,
        })
    })
}

pub(crate) fn update_many(
    session: &mut Session,
    collection: &str,
    filter: Option<Value>,
    update: Value,
) -> Result<UpdateResult, WrongoDBError> {
    with_txn(session, |session| {
        let txn_id = require_txn_id(session)?;
        let docs = find_with_txn(session, collection, filter, txn_id)?;
        if docs.is_empty() {
            return Ok(UpdateResult {
                matched: 0,
                modified: 0,
            });
        }

        let mut modified = 0;
        for doc in docs {
            let updated_doc = apply_update(&doc, &update)?;

            let id = doc.get("_id").ok_or_else(|| {
                crate::core::errors::DocumentValidationError("missing _id".into())
            })?;
            let key = encode_id_value(id)?;
            let value = encode_document(&updated_doc)?;

            let txn_id = require_txn_id(session)?;
            let mut cursor = session.open_cursor(&format!("table:{}", collection))?;
            cursor.update(&key, &value, txn_id)?;

            apply_index_remove(session, collection, &doc, txn_id)?;
            apply_index_add(session, collection, &updated_doc, txn_id)?;
            modified += 1;
        }

        Ok(UpdateResult {
            matched: modified,
            modified,
        })
    })
}

pub(crate) fn delete_one(
    session: &mut Session,
    collection: &str,
    filter: Option<Value>,
) -> Result<usize, WrongoDBError> {
    with_txn(session, |session| {
        let txn_id = require_txn_id(session)?;
        let docs = find_with_txn(session, collection, filter, txn_id)?;
        if docs.is_empty() {
            return Ok(0);
        }

        let doc = &docs[0];
        let Some(id) = doc.get("_id") else {
            return Ok(0);
        };
        let key = encode_id_value(id)?;

        let txn_id = require_txn_id(session)?;
        let mut cursor = session.open_cursor(&format!("table:{}", collection))?;
        cursor.delete(&key, txn_id)?;

        apply_index_remove(session, collection, doc, txn_id)?;
        Ok(1)
    })
}

pub(crate) fn delete_many(
    session: &mut Session,
    collection: &str,
    filter: Option<Value>,
) -> Result<usize, WrongoDBError> {
    with_txn(session, |session| {
        let txn_id = require_txn_id(session)?;
        let docs = find_with_txn(session, collection, filter, txn_id)?;
        if docs.is_empty() {
            return Ok(0);
        }

        let mut deleted = 0;
        for doc in docs {
            let Some(id) = doc.get("_id") else {
                continue;
            };
            let key = encode_id_value(id)?;

            let txn_id = require_txn_id(session)?;
            let mut cursor = session.open_cursor(&format!("table:{}", collection))?;
            cursor.delete(&key, txn_id)?;

            apply_index_remove(session, collection, &doc, txn_id)?;
            deleted += 1;
        }

        Ok(deleted)
    })
}

pub(crate) fn list_indexes(
    session: &mut Session,
    collection: &str,
) -> Result<Vec<String>, WrongoDBError> {
    let table = session.table_handle(collection, false)?;
    let table_guard = table.read();
    let catalog = table_guard
        .index_catalog()
        .ok_or_else(|| StorageError("missing index catalog".into()))?;
    Ok(catalog.index_names())
}

pub(crate) fn create_index(
    session: &mut Session,
    collection: &str,
    field: &str,
) -> Result<(), WrongoDBError> {
    with_txn(session, |session| {
        let txn_id = require_txn_id(session)?;
        let docs = find_with_txn(session, collection, None, txn_id)?;
        let table = session.table_handle(collection, false)?;
        let mut table_guard = table.write();
        let catalog = table_guard
            .index_catalog_mut()
            .ok_or_else(|| StorageError("missing index catalog".into()))?;
        catalog.add_index(field, vec![field.to_string()], &docs, txn_id)?;
        Ok(())
    })
}

#[allow(dead_code)]
pub(crate) fn checkpoint(session: &mut Session, collection: &str) -> Result<(), WrongoDBError> {
    let _ = session.table_handle(collection, false)?;
    session.checkpoint_all()
}

fn with_txn<R, F>(session: &mut Session, f: F) -> Result<R, WrongoDBError>
where
    F: FnOnce(&mut Session) -> Result<R, WrongoDBError>,
{
    if session.current_txn().is_some() {
        return f(session);
    }

    let mut txn = session.transaction()?;
    let result = f(txn.session_mut());
    match result {
        Ok(value) => {
            txn.commit()?;
            Ok(value)
        }
        Err(err) => {
            let _ = txn.abort();
            Err(err)
        }
    }
}

fn require_txn_id(session: &Session) -> Result<TxnId, WrongoDBError> {
    session
        .current_txn()
        .map(|txn| txn.id())
        .ok_or(WrongoDBError::NoActiveTransaction)
}

fn insert_one_in_txn(
    session: &mut Session,
    collection: &str,
    doc: Value,
) -> Result<Document, WrongoDBError> {
    validate_is_object(&doc)?;
    let mut obj = doc.as_object().expect("validated object").clone();
    normalize_document_in_place(&mut obj)?;

    let id = obj
        .get("_id")
        .ok_or_else(|| crate::core::errors::DocumentValidationError("missing _id".into()))?;
    let key = encode_id_value(id)?;
    let value = encode_document(&obj)?;

    let txn_id = require_txn_id(session)?;
    let mut cursor = session.open_cursor(&format!("table:{}", collection))?;
    cursor.insert(&key, &value, txn_id)?;

    apply_index_add(session, collection, &obj, txn_id)?;
    Ok(obj)
}

fn find_with_txn(
    session: &mut Session,
    collection: &str,
    filter: Option<Value>,
    txn_id: TxnId,
) -> Result<Vec<Document>, WrongoDBError> {
    let filter_doc = match filter {
        None => Document::new(),
        Some(v) => {
            validate_is_object(&v)?;
            v.as_object().expect("validated object").clone()
        }
    };

    let mut table_cursor = session.open_cursor(&format!("table:{}", collection))?;

    if filter_doc.is_empty() {
        let mut results = Vec::new();
        while let Some((_, bytes)) = table_cursor.next(txn_id)? {
            results.push(decode_document(&bytes)?);
        }
        return Ok(results);
    }

    let matches_filter = |doc: &Document| {
        filter_doc.iter().all(|(k, v)| {
            if k == "_id" {
                serde_json::to_string(doc.get(k).unwrap()).unwrap()
                    == serde_json::to_string(v).unwrap()
            } else {
                doc.get(k) == Some(v)
            }
        })
    };

    if let Some(id_value) = filter_doc.get("_id") {
        let key = encode_id_value(id_value)?;
        let doc_bytes = table_cursor.get(&key, txn_id)?;
        return Ok(match doc_bytes {
            Some(bytes) => {
                let doc = decode_document(&bytes)?;
                if matches_filter(&doc) {
                    vec![doc]
                } else {
                    Vec::new()
                }
            }
            _ => Vec::new(),
        });
    }

    let indexed_field = {
        let table_handle = session.table_handle(collection, false)?;
        let table_guard = table_handle.read();
        let catalog = match table_guard.index_catalog() {
            Some(c) => c,
            None => {
                return scan_with_cursor(table_cursor, txn_id, &matches_filter);
            }
        };
        filter_doc.keys().find(|k| catalog.has_index(k)).cloned()
    };

    if let Some(field) = indexed_field {
        let value = filter_doc.get(&field).unwrap();
        let Some((start_key, end_key)) = encode_range_bounds(value) else {
            return Ok(Vec::new());
        };
        let mut index_cursor = session.open_cursor(&format!("index:{}:{}", collection, field))?;
        index_cursor.set_range(Some(start_key), Some(end_key));

        let mut results = Vec::new();
        while let Some((key, _)) = index_cursor.next(txn_id)? {
            let Some(id) = decode_index_id(&key)? else {
                continue;
            };
            let key = encode_id_value(&id)?;
            if let Some(bytes) = table_cursor.get(&key, txn_id)? {
                let doc = decode_document(&bytes)?;
                if matches_filter(&doc) {
                    results.push(doc);
                }
            }
        }
        return Ok(results);
    }

    scan_with_cursor(table_cursor, txn_id, &matches_filter)
}

fn scan_with_cursor<F>(
    mut cursor: Cursor,
    txn_id: TxnId,
    matches_filter: &F,
) -> Result<Vec<Document>, WrongoDBError>
where
    F: Fn(&Document) -> bool,
{
    let mut results = Vec::new();
    while let Some((_, bytes)) = cursor.next(txn_id)? {
        let doc = decode_document(&bytes)?;
        if matches_filter(&doc) {
            results.push(doc);
        }
    }
    Ok(results)
}

fn apply_index_add(
    session: &mut Session,
    collection: &str,
    doc: &Document,
    txn_id: TxnId,
) -> Result<(), WrongoDBError> {
    let table = session.table_handle(collection, false)?;
    let mut table_guard = table.write();
    let catalog = table_guard
        .index_catalog_mut()
        .ok_or_else(|| StorageError("missing index catalog".into()))?;
    catalog.add_doc(doc, txn_id)?;
    Ok(())
}

fn apply_index_remove(
    session: &mut Session,
    collection: &str,
    doc: &Document,
    txn_id: TxnId,
) -> Result<(), WrongoDBError> {
    let table = session.table_handle(collection, false)?;
    let mut table_guard = table.write();
    let catalog = table_guard
        .index_catalog_mut()
        .ok_or_else(|| StorageError("missing index catalog".into()))?;
    catalog.remove_doc(doc, txn_id)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use serde_json::json;
    use tempfile::tempdir;

    use super::*;
    use crate::{Connection, ConnectionConfig};

    fn run_with_timeout<F>(timeout: Duration, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
            let _ = tx.send(result);
        });

        match rx.recv_timeout(timeout) {
            Ok(Ok(())) => {}
            Ok(Err(payload)) => std::panic::resume_unwind(payload),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                panic!("test timed out after {:?}", timeout)
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("test worker disconnected before reporting result")
            }
        }
    }

    #[test]
    fn insert_find_update_delete_roundtrip() {
        run_with_timeout(Duration::from_secs(20), || {
            let tmp = tempdir().unwrap();
            let conn =
                Connection::open(tmp.path().join("db"), ConnectionConfig::default()).unwrap();
            let mut session = conn.open_session();

            let inserted =
                insert_one(&mut session, "test", json!({"name": "alice", "age": 30})).unwrap();
            let id = inserted.get("_id").unwrap().clone();

            let fetched = find_one(&mut session, "test", Some(json!({"_id": id.clone()})))
                .unwrap()
                .unwrap();
            assert_eq!(fetched.get("name").unwrap().as_str().unwrap(), "alice");

            let updated = update_one(
                &mut session,
                "test",
                Some(json!({"_id": id.clone()})),
                json!({"$set": {"age": 31}}),
            )
            .unwrap();
            assert_eq!(updated.matched, 1);
            assert_eq!(updated.modified, 1);

            let fetched = find_one(&mut session, "test", Some(json!({"_id": id.clone()})))
                .unwrap()
                .unwrap();
            assert_eq!(fetched.get("age").unwrap().as_i64().unwrap(), 31);

            let deleted = delete_one(&mut session, "test", Some(json!({"_id": id}))).unwrap();
            assert_eq!(deleted, 1);
            let missing = find_one(&mut session, "test", Some(json!({"name": "alice"}))).unwrap();
            assert!(missing.is_none());
        });
    }

    #[test]
    fn create_and_list_indexes() {
        run_with_timeout(Duration::from_secs(20), || {
            let tmp = tempdir().unwrap();
            let conn =
                Connection::open(tmp.path().join("db"), ConnectionConfig::default()).unwrap();
            let mut session = conn.open_session();

            insert_one(&mut session, "test", json!({"name": "alice"})).unwrap();
            create_index(&mut session, "test", "name").unwrap();

            let indexes = list_indexes(&mut session, "test").unwrap();
            assert!(indexes.iter().any(|idx| idx == "name"));
        });
    }
}
