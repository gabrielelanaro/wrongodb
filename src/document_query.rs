use serde_json::Value;

use crate::api::{Cursor, Session};
use crate::core::bson::{decode_document, encode_id_value};
use crate::core::document::validate_is_object;
use crate::index::{decode_index_id, encode_range_bounds};
use crate::{Document, WrongoDBError};

pub(crate) fn find(
    session: &mut Session,
    collection: &str,
    filter: Option<Value>,
) -> Result<Vec<Document>, WrongoDBError> {
    session.with_txn(|session| {
        let txn_id = session.require_txn_id()?;
        find_with_txn(session, collection, filter, txn_id)
    })
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
    let mut seen = std::collections::HashSet::new();
    let mut values = Vec::new();

    for doc in docs {
        if let Some(value) = doc.get(key) {
            let encoded = serde_json::to_string(value).unwrap_or_default();
            if seen.insert(encoded) {
                values.push(value.clone());
            }
        }
    }

    Ok(values)
}

pub(crate) fn list_indexes(
    session: &Session,
    collection: &str,
) -> Result<Vec<String>, WrongoDBError> {
    session.schema_catalog().list_indexes(collection)
}

pub(crate) fn find_with_txn(
    session: &mut Session,
    collection: &str,
    filter: Option<Value>,
    txn_id: crate::txn::TxnId,
) -> Result<Vec<Document>, WrongoDBError> {
    let filter_doc = match filter {
        None => Document::new(),
        Some(value) => {
            validate_is_object(&value)?;
            value.as_object().expect("validated object").clone()
        }
    };

    let mut table_cursor = session.open_cursor(&format!("table:{collection}"))?;

    if filter_doc.is_empty() {
        return scan_with_cursor(&mut table_cursor, txn_id, |doc| {
            let _ = doc;
            true
        });
    }

    let matches_filter = |doc: &Document| {
        filter_doc.iter().all(|(key, value)| {
            if key == "_id" {
                serde_json::to_string(doc.get(key).unwrap()).unwrap()
                    == serde_json::to_string(value).unwrap()
            } else {
                doc.get(key) == Some(value)
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
            None => Vec::new(),
        });
    }

    let schema = session.collection_schema(collection)?;
    let indexed_field = filter_doc.keys().find(|key| schema.has_index(key)).cloned();
    if let Some(field) = indexed_field {
        let value = filter_doc.get(&field).expect("field selected from filter");
        let Some((start_key, end_key)) = encode_range_bounds(value) else {
            return Ok(Vec::new());
        };
        let mut index_cursor = session.open_cursor(&format!("index:{collection}:{field}"))?;
        index_cursor.set_range(Some(start_key), Some(end_key));

        let mut results = Vec::new();
        while let Some((key, _)) = index_cursor.next(txn_id)? {
            let Some(id) = decode_index_id(&key)? else {
                continue;
            };
            let primary_key = encode_id_value(&id)?;
            if let Some(bytes) = table_cursor.get(&primary_key, txn_id)? {
                let doc = decode_document(&bytes)?;
                if matches_filter(&doc) {
                    results.push(doc);
                }
            }
        }
        return Ok(results);
    }

    scan_with_cursor(&mut table_cursor, txn_id, matches_filter)
}

fn scan_with_cursor<F>(
    cursor: &mut Cursor,
    txn_id: crate::txn::TxnId,
    matches_filter: F,
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
