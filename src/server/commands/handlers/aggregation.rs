use super::crud::namespace_from_field;
use super::cursor::{create_materialized_cursor_response, default_batch_size};
use crate::api::DatabaseContext;
use crate::server::commands::{
    bson_document_to_json_value, json_value_to_bson_document, json_value_to_bson_value, Command,
    CommandContext,
};
use crate::WrongoDBError;
use async_trait::async_trait;
use bson::{doc, Bson, Document};
use serde_json::Value;

/// Handles: count (deprecated but still used)
pub struct CountCommand;

#[async_trait]
impl Command for CountCommand {
    fn names(&self) -> &[&str] {
        &["count"]
    }

    async fn execute(
        &self,
        ctx: &CommandContext,
        doc: &Document,
        db: &DatabaseContext,
    ) -> Result<Document, WrongoDBError> {
        let namespace = namespace_from_field(ctx, doc, "count")?;
        let mut session = db.connection().open_session();
        let query = doc.get("query").and_then(|q| q.as_document()).cloned();
        let filter_json = query.map(|d| bson_document_to_json_value(&d));

        let count = db
            .document_query()
            .count(&mut session, &namespace, filter_json)?;

        Ok(doc! {
            "ok": Bson::Double(1.0),
            "n": Bson::Int64(count as i64),
        })
    }
}

/// Handles: distinct
pub struct DistinctCommand;

#[async_trait]
impl Command for DistinctCommand {
    fn names(&self) -> &[&str] {
        &["distinct"]
    }

    async fn execute(
        &self,
        ctx: &CommandContext,
        doc: &Document,
        db: &DatabaseContext,
    ) -> Result<Document, WrongoDBError> {
        let namespace = namespace_from_field(ctx, doc, "distinct")?;
        let mut session = db.connection().open_session();
        let key = doc.get_str("key").unwrap_or("_id");
        let query = doc.get("query").and_then(|q| q.as_document()).cloned();
        let filter_json = query.map(|d| bson_document_to_json_value(&d));

        let values = db
            .document_query()
            .distinct(&mut session, &namespace, key, filter_json)?;
        let values_bson: Vec<Bson> = values
            .into_iter()
            .map(|v| json_value_to_bson_value(&v))
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

#[async_trait]
impl Command for AggregateCommand {
    fn names(&self) -> &[&str] {
        &["aggregate"]
    }

    async fn execute(
        &self,
        ctx: &CommandContext,
        doc: &Document,
        db: &DatabaseContext,
    ) -> Result<Document, WrongoDBError> {
        let namespace = namespace_from_field(ctx, doc, "aggregate")?;
        let mut session = db.connection().open_session();
        let pipeline = doc.get_array("pipeline").cloned().unwrap_or_default();

        let mut results = db.document_query().find(&mut session, &namespace, None)?;

        for stage in pipeline {
            if let Bson::Document(stage_doc) = stage {
                results = apply_pipeline_stage(&stage_doc, results)?;
            }
        }

        let results_docs: Vec<Document> = results
            .into_iter()
            .map(|d| json_value_to_bson_document(&Value::Object(d)))
            .collect();

        Ok(create_materialized_cursor_response(
            ctx,
            namespace,
            results_docs,
            default_batch_size(doc),
        ))
    }
}

fn apply_pipeline_stage(
    stage: &Document,
    mut results: Vec<serde_json::Map<String, Value>>,
) -> Result<Vec<serde_json::Map<String, Value>>, WrongoDBError> {
    if let Ok(match_doc) = stage.get_document("$match") {
        let filter = bson_document_to_json_value(match_doc);
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
        let project_filter = bson_document_to_json_value(project_doc);
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
