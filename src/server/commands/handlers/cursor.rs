use std::time::Duration;

use async_trait::async_trait;
use bson::{doc, Bson, Document};
use tokio::time::timeout;

use super::crud::namespace_from_field;
use crate::api::DatabaseContext;
use crate::core::bson::decode_id_value;
use crate::core::Namespace;
use crate::document_query::{FindRequest, FindResumeToken};
use crate::replication::oplog_namespace;
use crate::server::commands::{
    json_value_to_bson_document, Command, CommandContext, CursorState, FindCursorState,
    MaterializedCursorState,
};
use crate::WrongoDBError;

const DEFAULT_AWAIT_DATA_TIMEOUT_MS: u64 = 1_000;

/// Handles: getMore
pub struct GetMoreCommand;

#[async_trait]
impl Command for GetMoreCommand {
    fn names(&self) -> &[&str] {
        &["getMore"]
    }

    async fn execute(
        &self,
        ctx: &CommandContext,
        doc: &Document,
        db: &DatabaseContext,
    ) -> Result<Document, WrongoDBError> {
        let Some(cursor_id) = parse_cursor_id(doc.get("getMore")) else {
            let namespace = namespace_from_field(ctx, doc, "collection")?;
            return Ok(empty_batch_response(0, &namespace, "nextBatch"));
        };
        if cursor_id == 0 {
            let namespace = namespace_from_field(ctx, doc, "collection")?;
            return Ok(empty_batch_response(0, &namespace, "nextBatch"));
        }

        let Some(cursor_state) = ctx.cursor_manager().take(cursor_id) else {
            return Err(WrongoDBError::Protocol(format!(
                "cursor not found: {cursor_id}"
            )));
        };

        let (next_state, response) = match cursor_state {
            CursorState::Find(find_cursor_state) => {
                get_more_find_cursor(ctx, doc, db, cursor_id, *find_cursor_state).await?
            }
            CursorState::Materialized(materialized_cursor_state) => {
                get_more_materialized_cursor(doc, cursor_id, materialized_cursor_state)?
            }
        };

        if let Some(next_state) = next_state {
            ctx.cursor_manager().restore(cursor_id, next_state);
        }

        Ok(response)
    }
}

/// Handles: killCursors
pub struct KillCursorsCommand;

#[async_trait]
impl Command for KillCursorsCommand {
    fn names(&self) -> &[&str] {
        &["killCursors"]
    }

    async fn execute(
        &self,
        ctx: &CommandContext,
        doc: &Document,
        _db: &DatabaseContext,
    ) -> Result<Document, WrongoDBError> {
        let mut killed = Vec::new();
        let mut not_found = Vec::new();
        let mut unknown = Vec::new();

        for cursor in doc.get_array("cursors").cloned().unwrap_or_default() {
            let Some(cursor_id) = parse_cursor_id(Some(&cursor)) else {
                unknown.push(cursor);
                continue;
            };

            if cursor_id == 0 {
                not_found.push(Bson::Int64(0));
                continue;
            }

            if ctx.cursor_manager().kill(cursor_id) {
                killed.push(Bson::Int64(cursor_id as i64));
            } else {
                not_found.push(Bson::Int64(cursor_id as i64));
            }
        }

        Ok(doc! {
            "ok": Bson::Double(1.0),
            "cursorsKilled": Bson::Array(killed),
            "cursorsNotFound": Bson::Array(not_found),
            "cursorsAlive": Bson::Array(vec![]),
            "cursorsUnknown": Bson::Array(unknown),
        })
    }
}

pub(crate) fn create_materialized_cursor_response(
    ctx: &CommandContext,
    namespace: Namespace,
    docs: Vec<Document>,
    batch_size: usize,
) -> Document {
    let take = docs.len().min(batch_size);
    let first_batch_docs = docs[..take].to_vec();
    let cursor_id = if take < docs.len() {
        ctx.cursor_manager()
            .create(CursorState::Materialized(MaterializedCursorState {
                namespace: namespace.clone(),
                docs,
                next_offset: take,
            })) as i64
    } else {
        0
    };

    cursor_response(
        cursor_id,
        &namespace,
        "firstBatch",
        Bson::Array(first_batch_docs.into_iter().map(Bson::Document).collect()),
    )
}

pub(crate) fn default_batch_size(doc: &Document) -> usize {
    parse_non_zero_usize(doc.get("batchSize").or_else(|| {
        doc.get("cursor")
            .and_then(|cursor| cursor.as_document())
            .and_then(|cursor| cursor.get("batchSize"))
    }))
    .unwrap_or(usize::MAX)
}

fn get_more_materialized_cursor(
    doc: &Document,
    cursor_id: u64,
    mut state: MaterializedCursorState,
) -> Result<(Option<CursorState>, Document), WrongoDBError> {
    let batch_size = parse_non_zero_usize(doc.get("batchSize")).unwrap_or(usize::MAX);
    let end = state
        .next_offset
        .saturating_add(batch_size)
        .min(state.docs.len());
    let next_batch = state.docs[state.next_offset..end].to_vec();
    state.next_offset = end;

    let keep_alive = state.next_offset < state.docs.len();
    let response_id = if keep_alive { cursor_id as i64 } else { 0 };
    let response = cursor_response(
        response_id,
        &state.namespace,
        "nextBatch",
        Bson::Array(next_batch.into_iter().map(Bson::Document).collect()),
    );

    let next_state = keep_alive.then_some(CursorState::Materialized(state));
    Ok((next_state, response))
}

async fn get_more_find_cursor(
    ctx: &CommandContext,
    doc: &Document,
    db: &DatabaseContext,
    cursor_id: u64,
    mut state: FindCursorState,
) -> Result<(Option<CursorState>, Document), WrongoDBError> {
    let batch_size = parse_non_zero_usize(doc.get("batchSize")).unwrap_or(state.batch_size);
    let max_time_ms = parse_max_time_ms(doc).unwrap_or(DEFAULT_AWAIT_DATA_TIMEOUT_MS);

    let mut page = fetch_find_page(db, &state, batch_size)?;
    if should_wait_for_await_data(&state, &page) {
        await_oplog_advance(ctx, &state, max_time_ms).await;
        page = fetch_find_page(db, &state, batch_size)?;
    }

    if let Some(next_resume) = page.next_resume.clone() {
        state.resume = Some(next_resume);
    }

    let remaining_limit = state
        .remaining_limit
        .map(|remaining| remaining.saturating_sub(page.docs.len()));
    state.remaining_limit = remaining_limit;

    let keep_alive = if state.single_batch || remaining_limit == Some(0) {
        false
    } else if state.tailable {
        true
    } else {
        !page.reached_end
    };

    let next_batch = Bson::Array(
        page.docs
            .into_iter()
            .map(|document| {
                Bson::Document(json_value_to_bson_document(&serde_json::Value::Object(
                    document,
                )))
            })
            .collect(),
    );
    let response_id = if keep_alive { cursor_id as i64 } else { 0 };
    let response = cursor_response(response_id, &state.namespace, "nextBatch", next_batch);

    let next_state = keep_alive.then_some(CursorState::Find(Box::new(state)));
    Ok((next_state, response))
}

fn fetch_find_page(
    db: &DatabaseContext,
    state: &FindCursorState,
    batch_size: usize,
) -> Result<crate::document_query::FindPage, WrongoDBError> {
    let mut session = db.connection().open_session();
    db.document_query().find_page_with_plan(
        &mut session,
        &state.plan,
        &FindRequest {
            namespace: state.namespace.clone(),
            filter: state.filter.clone(),
            skip: 0,
            batch_size,
            limit: state.remaining_limit,
            resume: state.resume.clone(),
        },
    )
}

fn should_wait_for_await_data(
    state: &FindCursorState,
    page: &crate::document_query::FindPage,
) -> bool {
    state.tailable
        && state.await_data
        && state.namespace == oplog_namespace()
        && page.docs.is_empty()
        && page.reached_end
        && state.remaining_limit != Some(0)
}

async fn await_oplog_advance(ctx: &CommandContext, state: &FindCursorState, max_time_ms: u64) {
    let Some(resume_index) = oplog_resume_index(state.resume.as_ref()) else {
        return;
    };

    if ctx.oplog_await_service().latest_index() > resume_index {
        return;
    }

    let mut receiver = ctx.oplog_await_service().subscribe();
    if *receiver.borrow() > resume_index {
        return;
    }

    let _ = timeout(Duration::from_millis(max_time_ms), receiver.changed()).await;
}

fn oplog_resume_index(resume: Option<&FindResumeToken>) -> Option<u64> {
    let encoded = match resume {
        Some(FindResumeToken::TableScan { last_primary_key })
        | Some(FindResumeToken::IdRangeScan { last_primary_key }) => last_primary_key,
        Some(FindResumeToken::ReadyIndexEquality { .. }) | None => return None,
    };

    decode_id_value(encoded).ok()?.as_u64()
}

fn empty_batch_response(cursor_id: i64, namespace: &Namespace, batch_field: &str) -> Document {
    cursor_response(cursor_id, namespace, batch_field, Bson::Array(vec![]))
}

fn cursor_response(
    cursor_id: i64,
    namespace: &Namespace,
    batch_field: &str,
    batch: Bson,
) -> Document {
    let mut cursor = doc! {
        "id": Bson::Int64(cursor_id),
        "ns": namespace.full_name(),
    };
    cursor.insert(batch_field, batch);

    doc! {
        "ok": Bson::Double(1.0),
        "cursor": Bson::Document(cursor),
    }
}

fn parse_cursor_id(value: Option<&Bson>) -> Option<u64> {
    match value {
        Some(Bson::Int64(value)) => u64::try_from(*value).ok(),
        Some(Bson::Int32(value)) => u64::try_from(*value).ok(),
        _ => None,
    }
}

fn parse_non_zero_usize(value: Option<&Bson>) -> Option<usize> {
    let value = match value {
        Some(Bson::Int32(value)) => i64::from(*value),
        Some(Bson::Int64(value)) => *value,
        _ => return None,
    };

    (value > 0).then_some(value as usize)
}

fn parse_max_time_ms(doc: &Document) -> Option<u64> {
    match doc.get("maxTimeMS") {
        Some(Bson::Int32(value)) if *value > 0 => Some(*value as u64),
        Some(Bson::Int64(value)) if *value > 0 => Some(*value as u64),
        _ => None,
    }
}
