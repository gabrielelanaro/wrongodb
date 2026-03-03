use super::crud::{bson_to_value, value_to_bson_value};
use crate::commands::Command;
use crate::{document_ops, Connection, WrongoDBError};
use bson::{doc, Bson, Document};
use serde_json::Value;

/// Handles: count (deprecated but still used)
pub struct CountCommand;

impl Command for CountCommand {
    fn names(&self) -> &[&str] {
        &["count"]
    }

    fn execute(&self, doc: &Document, conn: &Connection) -> Result<Document, WrongoDBError> {
        let coll_name = doc.get_str("count").unwrap_or("test");
        let mut session = conn.open_session();
        let query = doc.get("query").and_then(|q| q.as_document()).cloned();
        let filter_json = query.map(|d| bson_to_value(&d));

        let count = document_ops::count(&mut session, coll_name, filter_json)?;

        Ok(doc! {
            "ok": Bson::Double(1.0),
            "n": Bson::Int64(count as i64),
        })
    }
}

/// Handles: distinct
pub struct DistinctCommand;

impl Command for DistinctCommand {
    fn names(&self) -> &[&str] {
        &["distinct"]
    }

    fn execute(&self, doc: &Document, conn: &Connection) -> Result<Document, WrongoDBError> {
        let coll_name = doc.get_str("distinct").unwrap_or("test");
        let mut session = conn.open_session();
        let key = doc.get_str("key").unwrap_or("_id");
        let query = doc.get("query").and_then(|q| q.as_document()).cloned();
        let filter_json = query.map(|d| bson_to_value(&d));

        let values = document_ops::distinct(&mut session, coll_name, key, filter_json)?;
        let values_bson: Vec<Bson> = values
            .into_iter()
            .map(|v| value_to_bson_value(&v))
            .collect();

        Ok(doc! {
            "ok": Bson::Double(1.0),
            "values": Bson::Array(values_bson),
        })
    }
}

/// Handles: aggregate
/// Note: Currently only supports basic pipeline stages ($match, $limit, $skip, $count)
pub struct AggregateCommand;

impl Command for AggregateCommand {
    fn names(&self) -> &[&str] {
        &["aggregate"]
    }

    fn execute(&self, doc: &Document, conn: &Connection) -> Result<Document, WrongoDBError> {
        let coll_name = doc.get_str("aggregate").unwrap_or("test");
        let mut session = conn.open_session();
        let pipeline = doc.get_array("pipeline").cloned().unwrap_or_default();

        let mut results = document_ops::find(&mut session, coll_name, None)?;

        for stage in pipeline {
            if let Bson::Document(stage_doc) = stage {
                results = apply_pipeline_stage(&stage_doc, results)?;
            }
        }

        let results_bson: Vec<Bson> = results
            .into_iter()
            .map(|d| Bson::Document(super::crud::value_to_bson(&Value::Object(d))))
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

fn apply_pipeline_stage(
    stage: &Document,
    mut results: Vec<serde_json::Map<String, Value>>,
) -> Result<Vec<serde_json::Map<String, Value>>, WrongoDBError> {
    if let Ok(match_doc) = stage.get_document("$match") {
        let filter = bson_to_value(match_doc);
        if let Value::Object(filter_obj) = filter {
            results.retain(|doc| filter_obj.iter().all(|(k, v)| doc.get(k) == Some(v)));
        }
    }

    if let Ok(limit) = stage.get_i64("$limit") {
        results.truncate(limit as usize);
    } else if let Ok(limit) = stage.get_i32("$limit") {
        results.truncate(limit as usize);
    }

    if let Ok(skip) = stage.get_i64("$skip") {
        results = results.into_iter().skip(skip as usize).collect();
    } else if let Ok(skip) = stage.get_i32("$skip") {
        results = results.into_iter().skip(skip as usize).collect();
    }

    if let Ok(count_field) = stage.get_str("$count") {
        let count = results.len();
        let mut count_doc = serde_json::Map::new();
        count_doc.insert(count_field.to_string(), Value::Number(count.into()));
        return Ok(vec![count_doc]);
    }

    if let Ok(project_doc) = stage.get_document("$project") {
        let project_filter = bson_to_value(project_doc);
        if let Value::Object(proj_obj) = project_filter {
            results = results
                .into_iter()
                .map(|doc| {
                    let mut new_doc = serde_json::Map::new();
                    for (k, v) in &proj_obj {
                        let include = match v {
                            Value::Number(n) => n.as_i64() == Some(1),
                            Value::Bool(b) => *b,
                            _ => false,
                        };
                        if include {
                            if let Some(val) = doc.get(k) {
                                new_doc.insert(k.clone(), val.clone());
                            }
                        }
                    }
                    if !proj_obj.contains_key("_id")
                        || proj_obj.get("_id") != Some(&Value::Number(0.into()))
                    {
                        if let Some(id) = doc.get("_id") {
                            new_doc.insert("_id".to_string(), id.clone());
                        }
                    }
                    new_doc
                })
                .collect();
        }
    }

    Ok(results)
}
