use serde_json::Value;

use crate::api::Session;
use crate::core::bson::{encode_document, encode_id_value};
use crate::core::document::{normalize_document_in_place, validate_is_object};
use crate::core::errors::DocumentValidationError;
use crate::document_query::find_with_txn;
use crate::index::encode_index_key;
use crate::{Document, WrongoDBError};

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
    session.with_txn(|session| insert_one_in_txn(session, collection, doc))
}

pub(crate) fn update_one(
    session: &mut Session,
    collection: &str,
    filter: Option<Value>,
    update: Value,
) -> Result<UpdateResult, WrongoDBError> {
    session.with_txn(|session| {
        let txn_id = session.require_txn_id()?;
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
            .ok_or_else(|| DocumentValidationError("missing _id".into()))?;
        let key = encode_id_value(id)?;
        let value = encode_document(&updated_doc)?;
        let primary_store = session
            .collection_schema(collection)?
            .primary_store()
            .to_string();
        session.update_store_value(&primary_store, &key, &value, txn_id)?;

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
    session.with_txn(|session| {
        let txn_id = session.require_txn_id()?;
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
            let id = doc
                .get("_id")
                .ok_or_else(|| DocumentValidationError("missing _id".into()))?;
            let key = encode_id_value(id)?;
            let value = encode_document(&updated_doc)?;
            let primary_store = session
                .collection_schema(collection)?
                .primary_store()
                .to_string();
            session.update_store_value(&primary_store, &key, &value, txn_id)?;

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
    session.with_txn(|session| {
        let txn_id = session.require_txn_id()?;
        let docs = find_with_txn(session, collection, filter, txn_id)?;
        if docs.is_empty() {
            return Ok(0);
        }

        let doc = &docs[0];
        let Some(id) = doc.get("_id") else {
            return Ok(0);
        };
        let key = encode_id_value(id)?;
        let primary_store = session
            .collection_schema(collection)?
            .primary_store()
            .to_string();
        session.delete_store_value(&primary_store, &key, txn_id)?;
        apply_index_remove(session, collection, doc, txn_id)?;
        Ok(1)
    })
}

pub(crate) fn delete_many(
    session: &mut Session,
    collection: &str,
    filter: Option<Value>,
) -> Result<usize, WrongoDBError> {
    session.with_txn(|session| {
        let txn_id = session.require_txn_id()?;
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
            let primary_store = session
                .collection_schema(collection)?
                .primary_store()
                .to_string();
            session.delete_store_value(&primary_store, &key, txn_id)?;
            apply_index_remove(session, collection, &doc, txn_id)?;
            deleted += 1;
        }

        Ok(deleted)
    })
}

pub(crate) fn create_index(
    session: &mut Session,
    collection: &str,
    field: &str,
) -> Result<(), WrongoDBError> {
    session.create(&format!("index:{collection}:{field}"))
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
        .ok_or_else(|| DocumentValidationError("missing _id".into()))?;
    let key = encode_id_value(id)?;
    let value = encode_document(&obj)?;
    let txn_id = session.require_txn_id()?;

    let primary_store = session
        .collection_schema(collection)?
        .primary_store()
        .to_string();
    session.insert_store_value(&primary_store, &key, &value, txn_id)?;
    apply_index_add(session, collection, &obj, txn_id)?;
    Ok(obj)
}

fn apply_update(doc: &Document, update: &Value) -> Result<Document, WrongoDBError> {
    let update_obj = match update.as_object() {
        Some(obj) => obj,
        None => return Ok(doc.clone()),
    };

    let is_update_operators = update_obj.keys().any(|k| k.starts_with('$'));

    if !is_update_operators {
        let mut new_doc = update_obj.clone();
        if let Some(id) = doc.get("_id") {
            new_doc.insert("_id".to_string(), id.clone());
        }
        return Ok(new_doc);
    }

    let mut new_doc = doc.clone();

    if let Some(Value::Object(set_fields)) = update_obj.get("$set") {
        for (k, v) in set_fields {
            new_doc.insert(k.clone(), v.clone());
        }
    }

    if let Some(Value::Object(unset_fields)) = update_obj.get("$unset") {
        for k in unset_fields.keys() {
            new_doc.remove(k);
        }
    }

    if let Some(Value::Object(inc_fields)) = update_obj.get("$inc") {
        for (k, v) in inc_fields {
            if let Some(inc_val) = v.as_f64() {
                let current = new_doc
                    .get(k)
                    .and_then(|value| value.as_f64())
                    .unwrap_or(0.0);
                new_doc.insert(
                    k.clone(),
                    Value::Number(
                        serde_json::Number::from_f64(current + inc_val)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ),
                );
            }
        }
    }

    if let Some(Value::Object(push_fields)) = update_obj.get("$push") {
        for (k, v) in push_fields {
            let arr = new_doc
                .entry(k.clone())
                .or_insert_with(|| Value::Array(vec![]));
            if let Value::Array(arr_vec) = arr {
                arr_vec.push(v.clone());
            }
        }
    }

    if let Some(Value::Object(pull_fields)) = update_obj.get("$pull") {
        for (k, v) in pull_fields {
            if let Some(Value::Array(arr)) = new_doc.get_mut(k) {
                arr.retain(|item| item != v);
            }
        }
    }

    Ok(new_doc)
}

fn apply_index_add(
    session: &mut Session,
    collection: &str,
    doc: &Document,
    txn_id: crate::txn::TxnId,
) -> Result<(), WrongoDBError> {
    apply_index_doc(session, collection, doc, txn_id, true)
}

fn apply_index_remove(
    session: &mut Session,
    collection: &str,
    doc: &Document,
    txn_id: crate::txn::TxnId,
) -> Result<(), WrongoDBError> {
    apply_index_doc(session, collection, doc, txn_id, false)
}

fn apply_index_doc(
    session: &mut Session,
    collection: &str,
    doc: &Document,
    txn_id: crate::txn::TxnId,
    is_add: bool,
) -> Result<(), WrongoDBError> {
    let Some(id) = doc.get("_id") else {
        return Ok(());
    };
    let schema = session.collection_schema(collection)?;
    for def in schema.index_definitions() {
        let Some(field) = def.columns.first() else {
            continue;
        };
        let Some(value) = doc.get(field) else {
            continue;
        };
        let Some(key) = encode_index_key(value, id)? else {
            continue;
        };
        if is_add {
            session.insert_store_value(&def.source, &key, &[], txn_id)?;
        } else {
            session.delete_store_value(&def.source, &key, txn_id)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;

    use serde_json::json;
    use tempfile::tempdir;

    use super::apply_update;
    use super::insert_one;
    use crate::api::connection::{Connection, ConnectionConfig, RaftMode, RaftPeerConfig};
    use crate::WrongoDBError;

    fn free_local_addr() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        addr.to_string()
    }

    #[test]
    fn test_apply_update_set() {
        let doc = serde_json::from_value(json!({"_id": 1, "name": "alice", "age": 30})).unwrap();
        let update = json!({"$set": {"age": 31}});
        let result = apply_update(&doc, &update).unwrap();
        assert_eq!(result.get("age").unwrap().as_i64().unwrap(), 31);
        assert_eq!(result.get("name").unwrap().as_str().unwrap(), "alice");
    }

    #[test]
    fn test_apply_update_unset() {
        let doc = serde_json::from_value(json!({"_id": 1, "name": "alice", "age": 30})).unwrap();
        let update = json!({"$unset": {"age": ""}});
        let result = apply_update(&doc, &update).unwrap();
        assert!(!result.contains_key("age"));
        assert!(result.contains_key("name"));
    }

    #[test]
    fn test_apply_update_inc() {
        let doc = serde_json::from_value(json!({"_id": 1, "counter": 10})).unwrap();
        let update = json!({"$inc": {"counter": 5}});
        let result = apply_update(&doc, &update).unwrap();
        assert_eq!(result.get("counter").unwrap().as_f64().unwrap(), 15.0);
    }

    #[test]
    fn test_apply_update_push() {
        let doc = serde_json::from_value(json!({"_id": 1, "tags": ["a", "b"]})).unwrap();
        let update = json!({"$push": {"tags": "c"}});
        let result = apply_update(&doc, &update).unwrap();
        let tags = result.get("tags").unwrap().as_array().unwrap();
        assert_eq!(tags.len(), 3);
        assert_eq!(tags[2].as_str().unwrap(), "c");
    }

    #[test]
    fn test_apply_update_pull() {
        let doc = serde_json::from_value(json!({"_id": 1, "tags": ["a", "b", "c"]})).unwrap();
        let update = json!({"$pull": {"tags": "b"}});
        let result = apply_update(&doc, &update).unwrap();
        let tags = result.get("tags").unwrap().as_array().unwrap();
        assert_eq!(tags.len(), 2);
        assert!(!tags.iter().any(|t| t.as_str() == Some("b")));
    }

    #[test]
    fn test_apply_update_replacement() {
        let doc = serde_json::from_value(json!({"_id": 1, "name": "alice", "age": 30})).unwrap();
        let update = json!({"name": "bob", "status": "active"});
        let result = apply_update(&doc, &update).unwrap();
        assert_eq!(result.get("name").unwrap().as_str().unwrap(), "bob");
        assert_eq!(result.get("status").unwrap().as_str().unwrap(), "active");
        assert!(!result.contains_key("age"));
        assert!(result.contains_key("_id"));
    }

    #[test]
    fn clustered_collection_write_path_rejects_writes_when_not_leader() {
        let dir = tempdir().unwrap();
        let conn = Connection::open(
            dir.path(),
            ConnectionConfig::new().raft_mode(RaftMode::Cluster {
                local_node_id: "n1".to_string(),
                local_raft_addr: free_local_addr(),
                peers: vec![RaftPeerConfig {
                    node_id: "n2".to_string(),
                    raft_addr: free_local_addr(),
                }],
            }),
        )
        .unwrap();
        let mut session = conn.open_session();

        let err =
            insert_one(&mut session, "items", json!({"_id": "k1", "value": "v1"})).unwrap_err();
        match err {
            WrongoDBError::NotLeader { leader_hint } => assert_eq!(leader_hint, None),
            other => panic!("unexpected error: {other}"),
        }
    }
}
