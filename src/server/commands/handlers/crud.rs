use super::cursor::default_batch_size;
use crate::api::DatabaseContext;
use crate::core::Namespace;
use crate::document_query::FindRequest;
use crate::replication::oplog_namespace;
use crate::server::commands::{
    bson_document_to_json_document, bson_document_to_json_value, json_value_to_bson_document,
    Command, CommandContext, CursorState, FindCursorState,
};
use crate::WrongoDBError;
use async_trait::async_trait;
use bson::{doc, oid::ObjectId, Bson, Document};
use serde_json::Value;

pub struct InsertCommand;

#[async_trait]
impl Command for InsertCommand {
    fn names(&self) -> &[&str] {
        &["insert"]
    }

    async fn execute(
        &self,
        ctx: &CommandContext,
        doc: &Document,
        db: &DatabaseContext,
    ) -> Result<Document, WrongoDBError> {
        let namespace = namespace_from_field(ctx, doc, "insert")?;

        let inserted_ids: Vec<ObjectId> = if let Ok(docs) = doc.get_array("documents") {
            let mut ids = Vec::new();
            for doc_bson in docs {
                if let Bson::Document(d) = doc_bson {
                    let mut doc_with_id = d.clone();
                    doc_with_id
                        .entry("_id".to_string())
                        .or_insert_with(|| Bson::ObjectId(ObjectId::new()));
                    let id = match doc_with_id.get("_id") {
                        Some(Bson::ObjectId(id)) => *id,
                        _ => ObjectId::new(),
                    };
                    let json_doc = bson_document_to_json_document(&doc_with_id);
                    db.write_ops()
                        .insert_one(&namespace, Value::Object(json_doc))?;
                    ids.push(id);
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

pub struct FindCommand;

#[async_trait]
impl Command for FindCommand {
    fn names(&self) -> &[&str] {
        &["find"]
    }

    async fn execute(
        &self,
        ctx: &CommandContext,
        doc: &Document,
        db: &DatabaseContext,
    ) -> Result<Document, WrongoDBError> {
        let namespace = namespace_from_field(ctx, doc, "find")?;
        let filter = doc.get("filter").and_then(|f| f.as_document()).cloned();
        let filter_json = filter.map(|d| bson_document_to_json_value(&d));
        let batch_size = default_batch_size(doc);
        let skip = parse_non_negative_i64(doc.get("skip")) as usize;
        let limit = parse_positive_limit(doc.get("limit"));
        let single_batch = doc.get_bool("singleBatch").unwrap_or(false);
        let tailable = doc.get_bool("tailable").unwrap_or(false);
        let await_data = doc.get_bool("awaitData").unwrap_or(false);
        validate_tailable_flags(&namespace, tailable, await_data)?;

        let request = FindRequest {
            namespace: namespace.clone(),
            filter: filter_json.clone(),
            skip,
            batch_size,
            limit,
            resume: None,
        };

        let mut session = db.connection().open_session();
        let planned = db.document_query().find_page(&mut session, &request)?;
        let remaining_limit = limit.map(|limit| limit.saturating_sub(planned.page.docs.len()));
        let keep_alive = if single_batch || remaining_limit == Some(0) {
            false
        } else if tailable {
            true
        } else {
            !planned.page.reached_end
        };
        let cursor_id = if keep_alive {
            ctx.cursor_manager()
                .create(CursorState::Find(Box::new(FindCursorState {
                    namespace: namespace.clone(),
                    filter: filter_json,
                    plan: planned.plan,
                    resume: planned.page.next_resume.clone(),
                    batch_size,
                    remaining_limit,
                    tailable,
                    await_data,
                    single_batch,
                }))) as i64
        } else {
            0
        };

        let results_bson: Vec<Bson> = planned
            .page
            .docs
            .into_iter()
            .map(|d| Bson::Document(json_value_to_bson_document(&Value::Object(d))))
            .collect();

        Ok(doc! {
            "ok": Bson::Double(1.0),
            "cursor": {
                "id": Bson::Int64(cursor_id),
                "ns": namespace.full_name(),
                "firstBatch": Bson::Array(results_bson),
            },
        })
    }
}

pub struct UpdateCommand;

#[async_trait]
impl Command for UpdateCommand {
    fn names(&self) -> &[&str] {
        &["update"]
    }

    async fn execute(
        &self,
        ctx: &CommandContext,
        doc: &Document,
        db: &DatabaseContext,
    ) -> Result<Document, WrongoDBError> {
        let namespace = namespace_from_field(ctx, doc, "update")?;
        let mut n_matched = 0i32;
        let mut n_modified = 0i32;

        if let Ok(updates) = doc.get_array("updates") {
            for update in updates {
                if let Bson::Document(update_doc) = update {
                    let filter = update_doc.get("q").and_then(|q| q.as_document()).cloned();
                    let filter_json = filter.map(|d| bson_document_to_json_value(&d));

                    let update_spec = update_doc.get("u").and_then(|u| u.as_document()).cloned();
                    let update_json = update_spec
                        .map(|d| bson_document_to_json_value(&d))
                        .unwrap_or(Value::Null);

                    let multi = update_doc.get_bool("multi").unwrap_or(false);

                    let result = if multi {
                        db.write_ops()
                            .update_many(&namespace, filter_json, update_json)?
                    } else {
                        db.write_ops()
                            .update_one(&namespace, filter_json, update_json)?
                    };

                    n_matched += result.matched as i32;
                    n_modified += result.modified as i32;
                }
            }
        }

        Ok(doc! {
            "ok": Bson::Double(1.0),
            "n": Bson::Int32(n_matched),
            "nModified": Bson::Int32(n_modified),
        })
    }
}

pub struct DeleteCommand;

#[async_trait]
impl Command for DeleteCommand {
    fn names(&self) -> &[&str] {
        &["delete"]
    }

    async fn execute(
        &self,
        ctx: &CommandContext,
        doc: &Document,
        db: &DatabaseContext,
    ) -> Result<Document, WrongoDBError> {
        let namespace = namespace_from_field(ctx, doc, "delete")?;
        let mut n_deleted = 0i32;

        if let Ok(deletes) = doc.get_array("deletes") {
            for delete in deletes {
                if let Bson::Document(delete_doc) = delete {
                    let filter = delete_doc.get("q").and_then(|q| q.as_document()).cloned();
                    let filter_json = filter.map(|d| bson_document_to_json_value(&d));

                    let limit = delete_doc.get_i32("limit").unwrap_or(0);

                    if limit == 1 {
                        n_deleted += db.write_ops().delete_one(&namespace, filter_json)? as i32;
                    } else {
                        n_deleted += db.write_ops().delete_many(&namespace, filter_json)? as i32;
                    }
                }
            }
        }

        Ok(doc! {
            "ok": Bson::Double(1.0),
            "n": Bson::Int32(n_deleted),
        })
    }
}

pub(crate) fn namespace_from_field(
    ctx: &CommandContext,
    doc: &Document,
    field: &str,
) -> Result<Namespace, WrongoDBError> {
    let collection = required_string_field(doc, field)?;
    Namespace::new(ctx.db_name().clone(), collection)
}

fn required_string_field(doc: &Document, field: &str) -> Result<String, WrongoDBError> {
    doc.get_str(field)
        .map(str::to_string)
        .map_err(|_| WrongoDBError::Protocol(format!("command requires string field {field}")))
}

fn parse_non_negative_i64(value: Option<&Bson>) -> i64 {
    match value {
        Some(Bson::Int32(value)) if *value > 0 => i64::from(*value),
        Some(Bson::Int64(value)) if *value > 0 => *value,
        _ => 0,
    }
}

fn parse_positive_limit(value: Option<&Bson>) -> Option<usize> {
    match value {
        Some(Bson::Int32(value)) if *value > 0 => Some(*value as usize),
        Some(Bson::Int64(value)) if *value > 0 => Some(*value as usize),
        _ => None,
    }
}

fn validate_tailable_flags(
    namespace: &Namespace,
    tailable: bool,
    await_data: bool,
) -> Result<(), WrongoDBError> {
    if await_data && !tailable {
        return Err(WrongoDBError::Protocol(
            "awaitData requires tailable=true".into(),
        ));
    }

    if (tailable || await_data) && *namespace != oplog_namespace() {
        return Err(WrongoDBError::Protocol(
            "tailable and awaitData are only supported on local.oplog.rs".into(),
        ));
    }

    Ok(())
}
