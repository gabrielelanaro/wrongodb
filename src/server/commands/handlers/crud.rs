use crate::commands::Command;
use crate::{WrongoDB, WrongoDBError};
use bson::{doc, oid::ObjectId, Bson, Document};
use serde_json::{Map, Value};

// ============================================================================
// BSON <-> Value conversion utilities
// ============================================================================

pub fn bson_to_json_document(doc: &Document) -> Map<String, Value> {
    let mut map = Map::new();
    for (k, v) in doc {
        map.insert(k.clone(), bson_value_to_value(v));
    }
    map
}

pub fn bson_to_value(doc: &Document) -> Value {
    Value::Object(bson_to_json_document(doc))
}

pub fn bson_value_to_value(bson: &Bson) -> Value {
    match bson {
        Bson::Double(d) => Value::Number(
            serde_json::Number::from_f64(*d).unwrap_or_else(|| serde_json::Number::from(0)),
        ),
        Bson::String(s) => Value::String(s.clone()),
        Bson::Document(d) => bson_to_value(d),
        Bson::Array(a) => Value::Array(a.iter().map(bson_value_to_value).collect()),
        Bson::Boolean(b) => Value::Bool(*b),
        Bson::Null => Value::Null,
        Bson::Int32(i) => Value::Number((*i).into()),
        Bson::Int64(i) => Value::Number((*i).into()),
        Bson::ObjectId(oid) => Value::String(oid.to_hex()),
        _ => Value::Null,
    }
}

pub fn value_to_bson(value: &Value) -> Document {
    match value {
        Value::Object(map) => {
            let mut doc = Document::new();
            for (k, v) in map {
                doc.insert(k.clone(), value_to_bson_value(v));
            }
            doc
        }
        _ => Document::new(),
    }
}

pub fn value_to_bson_value(value: &Value) -> Bson {
    match value {
        Value::Null => Bson::Null,
        Value::Bool(b) => Bson::Boolean(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Bson::Int64(i)
            } else if let Some(f) = n.as_f64() {
                Bson::Double(f)
            } else {
                Bson::Null
            }
        }
        Value::String(s) => Bson::String(s.clone()),
        Value::Array(a) => Bson::Array(a.iter().map(value_to_bson_value).collect()),
        Value::Object(_) => Bson::Document(value_to_bson(value)),
    }
}

// ============================================================================
// Insert Command
// ============================================================================

pub struct InsertCommand;

impl Command for InsertCommand {
    fn names(&self) -> &[&str] {
        &["insert"]
    }

    fn execute(&self, doc: &Document, db: &mut WrongoDB) -> Result<Document, WrongoDBError> {
        let coll_name = doc.get_str("insert").unwrap_or("test");

        let inserted_ids: Vec<ObjectId> = if let Ok(docs) = doc.get_array("documents") {
            let mut ids = Vec::new();
            for doc_bson in docs {
                if let Bson::Document(ref d) = doc_bson {
                    let mut doc_with_id = d.clone();
                    doc_with_id
                        .entry("_id".to_string())
                        .or_insert_with(|| Bson::ObjectId(ObjectId::new()));
                    let id = match doc_with_id.get("_id") {
                        Some(Bson::ObjectId(id)) => *id,
                        _ => ObjectId::new(),
                    };
                    let json_doc = bson_to_json_document(&doc_with_id);
                    if db.insert_one_doc_into(coll_name, json_doc).is_ok() {
                        ids.push(id);
                    }
                }
            }
            ids
        } else {
            Vec::new()
        };

        let mut inserted_ids_doc = Document::new();
        for (i, id) in inserted_ids.iter().enumerate() {
            inserted_ids_doc.insert(i.to_string(), Bson::ObjectId(*id));
        }

        Ok(doc! {
            "ok": Bson::Double(1.0),
            "n": Bson::Int32(inserted_ids.len() as i32),
            "insertedIds": Bson::Document(inserted_ids_doc),
        })
    }
}

// ============================================================================
// Find Command
// ============================================================================

pub struct FindCommand;

impl Command for FindCommand {
    fn names(&self) -> &[&str] {
        &["find"]
    }

    fn execute(&self, doc: &Document, db: &mut WrongoDB) -> Result<Document, WrongoDBError> {
        let coll_name = doc.get_str("find").unwrap_or("test");

        let filter = doc.get("filter").and_then(|f| f.as_document()).cloned();
        let filter_json = filter.map(|d| bson_to_value(&d));

        let mut results = db.find_in(coll_name, filter_json)?;

        // Handle skip
        let skip = doc.get("skip").and_then(|v| v.as_i64()).unwrap_or(0);
        if skip > 0 {
            results = results.into_iter().skip(skip as usize).collect();
        }

        // Handle limit and batchSize
        let limit = doc
            .get("limit")
            .and_then(|v| v.as_i64())
            .unwrap_or(i64::MAX);
        let batch_size = doc
            .get("batchSize")
            .or_else(|| {
                doc.get("cursor")
                    .and_then(|c| c.as_document())
                    .and_then(|d| d.get("batchSize"))
            })
            .and_then(|v| v.as_i64())
            .unwrap_or(i64::MAX);
        let take = limit.min(batch_size);
        if take >= 0 && take != i64::MAX {
            results.truncate(take as usize);
        }

        let results_bson: Vec<Bson> = results
            .into_iter()
            .map(|d| Bson::Document(value_to_bson(&Value::Object(d))))
            .collect();

        Ok(doc! {
            "ok": Bson::Double(1.0),
            "cursor": {
                "id": Bson::Int64(0),
                "ns": format!("test.{}", coll_name),
                "firstBatch": Bson::Array(results_bson),
            },
        })
    }
}

// ============================================================================
// Update Command
// ============================================================================

pub struct UpdateCommand;

impl Command for UpdateCommand {
    fn names(&self) -> &[&str] {
        &["update"]
    }

    fn execute(&self, doc: &Document, db: &mut WrongoDB) -> Result<Document, WrongoDBError> {
        let coll_name = doc.get_str("update").unwrap_or("test");
        let mut n_matched = 0i32;
        let mut n_modified = 0i32;

        if let Ok(updates) = doc.get_array("updates") {
            for update_spec in updates {
                if let Bson::Document(spec) = update_spec {
                    let filter = spec.get_document("q").ok().cloned();
                    let update_doc = spec.get_document("u").ok().cloned();
                    let multi = spec.get_bool("multi").unwrap_or(false);

                    if let (Some(filter), Some(update)) = (filter, update_doc) {
                        let filter_json = bson_to_value(&filter);
                        let update_json = bson_to_value(&update);

                        let result = if multi {
                            db.update_many_in(coll_name, Some(filter_json), update_json)?
                        } else {
                            db.update_one_in(coll_name, Some(filter_json), update_json)?
                        };

                        n_matched += result.matched as i32;
                        n_modified += result.modified as i32;
                    }
                }
            }
        }

        Ok(doc! {
            "ok": Bson::Double(1.0),
            "n": Bson::Int32(n_matched),
            "nModified": Bson::Int32(n_modified),
            "writeErrors": Bson::Array(vec![]),
            "writeConcernErrors": Bson::Array(vec![]),
        })
    }
}

// ============================================================================
// Delete Command
// ============================================================================

pub struct DeleteCommand;

impl Command for DeleteCommand {
    fn names(&self) -> &[&str] {
        &["delete"]
    }

    fn execute(&self, doc: &Document, db: &mut WrongoDB) -> Result<Document, WrongoDBError> {
        let coll_name = doc.get_str("delete").unwrap_or("test");
        let mut n_deleted = 0i32;

        if let Ok(deletes) = doc.get_array("deletes") {
            for delete_spec in deletes {
                if let Bson::Document(spec) = delete_spec {
                    let filter = spec.get_document("q").ok().cloned();
                    let limit = spec.get_i32("limit").unwrap_or(0);

                    if let Some(filter) = filter {
                        let filter_json = bson_to_value(&filter);
                        let count = if limit == 1 {
                            db.delete_one_in(coll_name, Some(filter_json))?
                        } else {
                            db.delete_many_in(coll_name, Some(filter_json))?
                        };
                        n_deleted += count as i32;
                    }
                }
            }
        }

        Ok(doc! {
            "ok": Bson::Double(1.0),
            "n": Bson::Int32(n_deleted),
            "writeErrors": Bson::Array(vec![]),
            "writeConcernErrors": Bson::Array(vec![]),
        })
    }
}

// ============================================================================
// DeleteMany Command (mongosh helper)
// ============================================================================

pub struct DeleteManyCommand;

impl Command for DeleteManyCommand {
    fn names(&self) -> &[&str] {
        &["deleteMany"]
    }

    fn execute(&self, doc: &Document, db: &mut WrongoDB) -> Result<Document, WrongoDBError> {
        // deleteMany format: {deleteMany: "collection", deletes: [{q: filter, limit: 1}]}
        let coll_name = doc.get_str("deleteMany").unwrap_or("test");
        let filter = doc.get_document("filter").ok();

        let count = if let Some(filter_doc) = filter {
            let filter_json = bson_to_value(&filter_doc);
            db.delete_many_in(coll_name, Some(filter_json))?
        } else {
            db.delete_many_in(coll_name, None)?
        };

        Ok(doc! {
            "acknowledged": Bson::Boolean(true),
            "deletedCount": Bson::Int32(count as i32),
        })
    }
}
