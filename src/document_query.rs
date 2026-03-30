use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;

use serde_json::{Map, Value};

use crate::catalog::{CollectionCatalog, CollectionDefinition};
use crate::core::bson::encode_id_value;
use crate::core::document::validate_is_object;
use crate::core::errors::DocumentValidationError;
use crate::core::Namespace;
use crate::index::{decode_index_id, encode_range_bounds};
use crate::storage::api::{FileCursor, Session, TableCursor};
use crate::storage::row::decode_row_value;
use crate::{Document, WrongoDBError};

/// One command-layer find request that may represent an initial `find` or a `getMore`.
#[derive(Debug, Clone)]
pub(crate) struct FindRequest {
    /// Namespace being read.
    pub(crate) namespace: Namespace,
    /// Normalized command filter expressed in the internal JSON document model.
    pub(crate) filter: Option<Value>,
    /// Number of matching rows to skip on the initial `find`.
    pub(crate) skip: usize,
    /// Maximum number of documents this batch may return.
    pub(crate) batch_size: usize,
    /// Remaining total result budget across the cursor lifetime.
    pub(crate) limit: Option<usize>,
    /// Saved bookmark used by `getMore`.
    pub(crate) resume: Option<FindResumeToken>,
}

/// One page of find results plus the bookmark needed to continue later.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FindPage {
    /// Documents returned in this page.
    pub(crate) docs: Vec<Document>,
    /// Bookmark for the next `getMore`, if continuation is possible.
    pub(crate) next_resume: Option<FindResumeToken>,
    /// Whether this page reached the current end of the chosen scan.
    pub(crate) reached_end: bool,
}

/// One planned page result returned by the initial `find` path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedFindPage {
    /// Plan chosen for the initial `find`.
    pub(crate) plan: FindPlan,
    /// First page produced by that plan.
    pub(crate) page: FindPage,
}

/// Query plan families supported by WrongoDB's current read path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FindPlan {
    /// Full primary-table scan in `_id` order.
    TableScan,
    /// Direct primary-key lookup by exact `_id`.
    IdPointLookup {
        /// Encoded `_id` key used by the storage layer.
        encoded_id: Vec<u8>,
    },
    /// Primary-key range scan starting at one `_id` bound.
    IdRangeScan {
        /// Whether the initial `_id` bound is inclusive.
        inclusive: bool,
        /// Encoded lower bound for the first scan.
        encoded_start: Vec<u8>,
    },
    /// Equality scan over one ready secondary index.
    ReadyIndexEquality {
        /// Backing index ident to open on each page read.
        index_uri: String,
        /// Encoded lower bound for the index equality range.
        encoded_start: Vec<u8>,
        /// Encoded upper bound for the index equality range.
        encoded_end: Vec<u8>,
    },
}

/// Bookmark used by server-side cursors to resume a later `getMore`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FindResumeToken {
    /// Bookmark after a full table scan page.
    TableScan {
        /// Last emitted primary key.
        last_primary_key: Vec<u8>,
    },
    /// Bookmark after an `_id` range scan page.
    IdRangeScan {
        /// Last emitted primary key.
        last_primary_key: Vec<u8>,
    },
    /// Bookmark after an index equality scan page.
    ReadyIndexEquality {
        /// Backing index ident the cursor must reopen.
        index_uri: String,
        /// Last emitted raw index key.
        last_index_key: Vec<u8>,
    },
}

/// Read path for collection queries over the WT-like storage API.
///
/// `DocumentQuery` resolves collection definitions from the durable catalog,
/// opens the storage-layer cursor primitives, and builds a small set of
/// Mongo-like query plans on top of them.
#[derive(Clone)]
pub(crate) struct DocumentQuery {
    catalog: Arc<CollectionCatalog>,
}

impl DocumentQuery {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    /// Creates the document query service.
    pub(crate) fn new(catalog: Arc<CollectionCatalog>) -> Self {
        Self { catalog }
    }

    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------

    pub(crate) fn find(
        &self,
        session: &mut Session,
        namespace: &Namespace,
        filter: Option<Value>,
    ) -> Result<Vec<Document>, WrongoDBError> {
        session.with_transaction(|session| self.find_in_transaction(session, namespace, filter))
    }

    /// Counts the documents matching `filter`.
    pub(crate) fn count(
        &self,
        session: &mut Session,
        namespace: &Namespace,
        filter: Option<Value>,
    ) -> Result<usize, WrongoDBError> {
        Ok(self.find(session, namespace, filter)?.len())
    }

    /// Returns the distinct values of `key` among the matching documents.
    pub(crate) fn distinct(
        &self,
        session: &mut Session,
        namespace: &Namespace,
        key: &str,
        filter: Option<Value>,
    ) -> Result<Vec<Value>, WrongoDBError> {
        let docs = self.find(session, namespace, filter)?;
        let mut seen = HashSet::new();
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

    /// Execute one `find` page and return both the plan and the page.
    pub(crate) fn find_page(
        &self,
        session: &mut Session,
        request: &FindRequest,
    ) -> Result<PlannedFindPage, WrongoDBError> {
        let filter_doc = normalize_filter(request.filter.clone())?;
        let collection_definition = self.collection_definition(session, &request.namespace)?;
        let plan = plan_find(collection_definition.as_ref(), &filter_doc)?;
        let page = self.find_page_with_plan(session, &plan, request)?;

        Ok(PlannedFindPage { plan, page })
    }

    /// Execute one page using an already chosen plan.
    pub(crate) fn find_page_with_plan(
        &self,
        session: &mut Session,
        plan: &FindPlan,
        request: &FindRequest,
    ) -> Result<FindPage, WrongoDBError> {
        let filter_doc = normalize_filter(request.filter.clone())?;
        let collection_definition = self.collection_definition(session, &request.namespace)?;
        let target_docs = request
            .limit
            .map(|remaining| remaining.min(request.batch_size))
            .unwrap_or(request.batch_size);

        match plan {
            FindPlan::TableScan => {
                let Some(definition) = collection_definition else {
                    return Ok(FindPage {
                        docs: Vec::new(),
                        next_resume: None,
                        reached_end: true,
                    });
                };
                let mut table_cursor = session.open_table_cursor(definition.table_uri())?;
                self.scan_table_page(
                    &mut table_cursor,
                    TableScanSpec {
                        filter_doc: &filter_doc,
                        target_docs,
                        skip: request.skip,
                        initial_start: None,
                        skip_initial_start: false,
                        resume: request.resume.clone(),
                        resume_kind: ResumeKind::TableScan,
                    },
                )
            }
            FindPlan::IdPointLookup { encoded_id } => {
                let Some(definition) = collection_definition else {
                    return Ok(FindPage {
                        docs: Vec::new(),
                        next_resume: None,
                        reached_end: true,
                    });
                };
                self.lookup_id_point(
                    session,
                    definition,
                    encoded_id,
                    &filter_doc,
                    target_docs,
                    request.resume.clone(),
                )
            }
            FindPlan::IdRangeScan {
                inclusive,
                encoded_start,
            } => {
                let Some(definition) = collection_definition else {
                    return Ok(FindPage {
                        docs: Vec::new(),
                        next_resume: Some(FindResumeToken::IdRangeScan {
                            last_primary_key: encoded_start.clone(),
                        }),
                        reached_end: true,
                    });
                };
                let mut table_cursor = session.open_table_cursor(definition.table_uri())?;
                self.scan_table_page(
                    &mut table_cursor,
                    TableScanSpec {
                        filter_doc: &filter_doc,
                        target_docs,
                        skip: request.skip,
                        initial_start: Some(encoded_start.clone()),
                        skip_initial_start: !inclusive,
                        resume: request.resume.clone(),
                        resume_kind: ResumeKind::IdRangeScan,
                    },
                )
            }
            FindPlan::ReadyIndexEquality {
                index_uri,
                encoded_start,
                encoded_end,
                ..
            } => {
                let Some(definition) = collection_definition else {
                    return Ok(FindPage {
                        docs: Vec::new(),
                        next_resume: None,
                        reached_end: true,
                    });
                };
                let mut table_cursor = session.open_table_cursor(definition.table_uri())?;
                let mut index_cursor = session.open_index_cursor(index_uri)?;
                self.scan_index_page(
                    &mut table_cursor,
                    &mut index_cursor,
                    IndexScanSpec {
                        filter_doc: &filter_doc,
                        target_docs,
                        skip: request.skip,
                        index_uri,
                        encoded_start,
                        encoded_end,
                        resume: request.resume.clone(),
                    },
                )
            }
        }
    }

    /// Executes a full read inside the caller's active transaction.
    pub(crate) fn find_in_transaction(
        &self,
        session: &mut Session,
        namespace: &Namespace,
        filter: Option<Value>,
    ) -> Result<Vec<Document>, WrongoDBError> {
        let filter_doc = normalize_filter(filter.clone())?;
        let collection_definition = self.collection_definition(session, namespace)?;
        let plan = plan_find(collection_definition.as_ref(), &filter_doc)?;
        let page = self.find_page_with_plan(
            session,
            &plan,
            &FindRequest {
                namespace: namespace.clone(),
                filter,
                skip: 0,
                batch_size: usize::MAX,
                limit: None,
                resume: None,
            },
        )?;
        Ok(page.docs)
    }

    // ------------------------------------------------------------------------
    // Table scans and lookups
    // ------------------------------------------------------------------------

    fn lookup_id_point(
        &self,
        session: &mut Session,
        definition: CollectionDefinition,
        encoded_id: &[u8],
        filter_doc: &Document,
        target_docs: usize,
        resume: Option<FindResumeToken>,
    ) -> Result<FindPage, WrongoDBError> {
        if target_docs == 0 || resume.is_some() {
            return Ok(FindPage {
                docs: Vec::new(),
                next_resume: None,
                reached_end: true,
            });
        }

        let mut table_cursor = session.open_table_cursor(definition.table_uri())?;
        let docs = match table_cursor.get(encoded_id)? {
            Some(bytes) => {
                let doc = decode_row_value(table_cursor.table(), encoded_id, &bytes)?;
                if matches_filter(&doc, filter_doc)? {
                    vec![doc]
                } else {
                    Vec::new()
                }
            }
            None => Vec::new(),
        };

        Ok(FindPage {
            docs,
            next_resume: None,
            reached_end: true,
        })
    }

    fn scan_table_page(
        &self,
        cursor: &mut TableCursor<'_>,
        spec: TableScanSpec<'_>,
    ) -> Result<FindPage, WrongoDBError> {
        let (range_start, skip_key, empty_resume) = match spec.resume {
            Some(FindResumeToken::TableScan { last_primary_key }) => (
                Some(last_primary_key.clone()),
                Some(last_primary_key.clone()),
                Some(FindResumeToken::TableScan { last_primary_key }),
            ),
            Some(FindResumeToken::IdRangeScan { last_primary_key }) => (
                Some(last_primary_key.clone()),
                Some(last_primary_key.clone()),
                Some(FindResumeToken::IdRangeScan { last_primary_key }),
            ),
            Some(FindResumeToken::ReadyIndexEquality { .. }) => {
                return Err(WrongoDBError::Protocol(
                    "index resume token cannot continue a table scan".into(),
                ));
            }
            None => {
                let empty_resume = spec
                    .initial_start
                    .clone()
                    .map(|start| resume_token_for_table_scan(spec.resume_kind, start));
                let skip_key = if spec.skip_initial_start {
                    spec.initial_start.clone()
                } else {
                    None
                };
                (spec.initial_start, skip_key, empty_resume)
            }
        };

        cursor.set_range(range_start, None);
        self.collect_table_page(
            cursor,
            TableCollectSpec {
                filter_doc: spec.filter_doc,
                target_docs: spec.target_docs,
                skip: spec.skip,
                resume_kind: spec.resume_kind,
            },
            skip_key,
            empty_resume,
        )
    }

    fn collect_table_page(
        &self,
        cursor: &mut TableCursor<'_>,
        spec: TableCollectSpec<'_>,
        skip_key: Option<Vec<u8>>,
        empty_resume: Option<FindResumeToken>,
    ) -> Result<FindPage, WrongoDBError> {
        let mut docs = Vec::new();
        let mut skipped_matches = 0usize;
        let mut last_emitted_key = None;
        let mut reached_end = true;

        while let Some((key, bytes)) = cursor.next()? {
            if should_skip_key(skip_key.as_deref(), &key) {
                continue;
            }

            let doc = decode_row_value(cursor.table(), &key, &bytes)?;
            if !matches_filter(&doc, spec.filter_doc)? {
                continue;
            }

            if skipped_matches < spec.skip {
                skipped_matches += 1;
                continue;
            }

            if docs.len() >= spec.target_docs {
                reached_end =
                    !table_cursor_has_more_matches(cursor, spec.filter_doc, skip_key.as_deref())?;
                break;
            }

            last_emitted_key = Some(key.clone());
            docs.push(doc);
            if docs.len() == spec.target_docs {
                reached_end =
                    !table_cursor_has_more_matches(cursor, spec.filter_doc, skip_key.as_deref())?;
                break;
            }
        }

        let next_resume = last_emitted_key
            .map(|key| resume_token_for_table_scan(spec.resume_kind, key))
            .or(empty_resume);

        Ok(FindPage {
            docs,
            next_resume,
            reached_end,
        })
    }

    fn scan_index_page(
        &self,
        table_cursor: &mut TableCursor<'_>,
        index_cursor: &mut FileCursor<'_>,
        spec: IndexScanSpec<'_>,
    ) -> Result<FindPage, WrongoDBError> {
        let (range_start, skip_key, empty_resume) = match spec.resume {
            Some(FindResumeToken::ReadyIndexEquality {
                index_uri,
                last_index_key,
            }) => (
                Some(last_index_key.clone()),
                Some(last_index_key.clone()),
                Some(FindResumeToken::ReadyIndexEquality {
                    index_uri,
                    last_index_key,
                }),
            ),
            Some(FindResumeToken::TableScan { .. }) | Some(FindResumeToken::IdRangeScan { .. }) => {
                return Err(WrongoDBError::Protocol(
                    "table resume token cannot continue an index scan".into(),
                ));
            }
            None => (Some(spec.encoded_start.to_vec()), None, None),
        };

        index_cursor.set_range(range_start, Some(spec.encoded_end.to_vec()));

        let mut docs = Vec::new();
        let mut skipped_matches = 0usize;
        let mut last_index_key = None;
        let mut reached_end = true;

        while let Some((key, _)) = index_cursor.next()? {
            if key.as_slice() > spec.encoded_end {
                break;
            }
            if should_skip_key(skip_key.as_deref(), &key) {
                continue;
            }

            let Some(id) = decode_index_id(&key)? else {
                continue;
            };
            let primary_key = encode_id_value(&id)?;
            let Some(bytes) = table_cursor.get(&primary_key)? else {
                continue;
            };
            let doc = decode_row_value(table_cursor.table(), &primary_key, &bytes)?;
            if !matches_filter(&doc, spec.filter_doc)? {
                continue;
            }

            if skipped_matches < spec.skip {
                skipped_matches += 1;
                continue;
            }

            last_index_key = Some(key.clone());
            docs.push(doc);
            if docs.len() == spec.target_docs {
                reached_end = !index_cursor_has_more_matches(
                    table_cursor,
                    index_cursor,
                    spec.filter_doc,
                    spec.encoded_end,
                    skip_key.as_deref(),
                )?;
                break;
            }
        }

        let next_resume = last_index_key
            .map(|last_index_key| FindResumeToken::ReadyIndexEquality {
                index_uri: spec.index_uri.to_string(),
                last_index_key,
            })
            .or(empty_resume);

        Ok(FindPage {
            docs,
            next_resume,
            reached_end,
        })
    }

    // ------------------------------------------------------------------------
    // Catalog helpers
    // ------------------------------------------------------------------------

    fn collection_definition(
        &self,
        session: &mut Session,
        namespace: &Namespace,
    ) -> Result<Option<CollectionDefinition>, WrongoDBError> {
        self.catalog.get_collection(session, namespace)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResumeKind {
    TableScan,
    IdRangeScan,
}

struct TableScanSpec<'a> {
    filter_doc: &'a Document,
    target_docs: usize,
    skip: usize,
    initial_start: Option<Vec<u8>>,
    skip_initial_start: bool,
    resume: Option<FindResumeToken>,
    resume_kind: ResumeKind,
}

struct TableCollectSpec<'a> {
    filter_doc: &'a Document,
    target_docs: usize,
    skip: usize,
    resume_kind: ResumeKind,
}

struct IndexScanSpec<'a> {
    filter_doc: &'a Document,
    target_docs: usize,
    skip: usize,
    index_uri: &'a str,
    encoded_start: &'a [u8],
    encoded_end: &'a [u8],
    resume: Option<FindResumeToken>,
}

fn normalize_filter(filter: Option<Value>) -> Result<Document, WrongoDBError> {
    match filter {
        None => Ok(Document::new()),
        Some(value) => {
            validate_is_object(&value)?;
            match value {
                Value::Object(document) => Ok(document),
                _ => unreachable!("validate_is_object accepted a non-object filter"),
            }
        }
    }
}

fn plan_find(
    collection_definition: Option<&CollectionDefinition>,
    filter_doc: &Document,
) -> Result<FindPlan, WrongoDBError> {
    if filter_doc.is_empty() {
        return Ok(FindPlan::TableScan);
    }

    if let Some(id_filter) = filter_doc.get("_id") {
        return plan_id_filter(id_filter);
    }

    let Some(definition) = collection_definition else {
        return Ok(FindPlan::TableScan);
    };

    if let Some(index) = definition.indexes().values().find(|index| {
        index.ready()
            && index
                .indexed_field()
                .map(|field| filter_doc.contains_key(&field))
                .unwrap_or(false)
    }) {
        let field = index.indexed_field()?;
        let Some(value) = filter_doc.get(&field) else {
            return Err(WrongoDBError::Protocol(format!(
                "planned index scan without filter field {field}"
            )));
        };
        if let Some((encoded_start, encoded_end)) = encode_range_bounds(value) {
            return Ok(FindPlan::ReadyIndexEquality {
                index_uri: index.uri().to_string(),
                encoded_start,
                encoded_end,
            });
        }
    }

    Ok(FindPlan::TableScan)
}

fn plan_id_filter(id_filter: &Value) -> Result<FindPlan, WrongoDBError> {
    match id_filter {
        Value::Object(operators) => {
            if let Some(value) = operators.get("$gt") {
                ensure_only_supported_id_range_operators(operators)?;
                return Ok(FindPlan::IdRangeScan {
                    inclusive: false,
                    encoded_start: encode_id_value(value)?,
                });
            }
            if let Some(value) = operators.get("$gte") {
                ensure_only_supported_id_range_operators(operators)?;
                return Ok(FindPlan::IdRangeScan {
                    inclusive: true,
                    encoded_start: encode_id_value(value)?,
                });
            }

            Err(DocumentValidationError("only _id.$gt and _id.$gte are supported".into()).into())
        }
        _ => Ok(FindPlan::IdPointLookup {
            encoded_id: encode_id_value(id_filter)?,
        }),
    }
}

fn ensure_only_supported_id_range_operators(
    operators: &Map<String, Value>,
) -> Result<(), WrongoDBError> {
    if operators.len() == 1 && (operators.contains_key("$gt") || operators.contains_key("$gte")) {
        return Ok(());
    }

    Err(
        DocumentValidationError("only a single _id.$gt or _id.$gte operator is supported".into())
            .into(),
    )
}

fn matches_filter(doc: &Document, filter_doc: &Document) -> Result<bool, WrongoDBError> {
    for (key, value) in filter_doc {
        if key == "_id" {
            if !matches_id_filter(doc, value)? {
                return Ok(false);
            }
            continue;
        }

        if doc.get(key) != Some(value) {
            return Ok(false);
        }
    }

    Ok(true)
}

fn matches_id_filter(doc: &Document, value: &Value) -> Result<bool, WrongoDBError> {
    let Some(doc_id) = doc.get("_id") else {
        return Ok(false);
    };

    match value {
        Value::Object(operators) => {
            if let Some(bound) = operators.get("$gt") {
                ensure_only_supported_id_range_operators(operators)?;
                return Ok(compare_id_values(doc_id, bound)? == Ordering::Greater);
            }
            if let Some(bound) = operators.get("$gte") {
                ensure_only_supported_id_range_operators(operators)?;
                let ordering = compare_id_values(doc_id, bound)?;
                return Ok(matches!(ordering, Ordering::Equal | Ordering::Greater));
            }

            Err(DocumentValidationError("only _id.$gt and _id.$gte are supported".into()).into())
        }
        _ => Ok(doc_id == value),
    }
}

fn compare_id_values(left: &Value, right: &Value) -> Result<Ordering, WrongoDBError> {
    Ok(encode_id_value(left)?.cmp(&encode_id_value(right)?))
}

fn should_skip_key(skip_key: Option<&[u8]>, key: &[u8]) -> bool {
    skip_key.map(|skip_key| skip_key == key).unwrap_or(false)
}

fn resume_token_for_table_scan(resume_kind: ResumeKind, key: Vec<u8>) -> FindResumeToken {
    match resume_kind {
        ResumeKind::TableScan => FindResumeToken::TableScan {
            last_primary_key: key,
        },
        ResumeKind::IdRangeScan => FindResumeToken::IdRangeScan {
            last_primary_key: key,
        },
    }
}

fn table_cursor_has_more_matches(
    cursor: &mut TableCursor<'_>,
    filter_doc: &Document,
    skip_key: Option<&[u8]>,
) -> Result<bool, WrongoDBError> {
    while let Some((key, bytes)) = cursor.next()? {
        if should_skip_key(skip_key, &key) {
            continue;
        }

        let doc = decode_row_value(cursor.table(), &key, &bytes)?;
        if matches_filter(&doc, filter_doc)? {
            return Ok(true);
        }
    }

    Ok(false)
}

fn index_cursor_has_more_matches(
    table_cursor: &mut TableCursor<'_>,
    index_cursor: &mut FileCursor<'_>,
    filter_doc: &Document,
    encoded_end: &[u8],
    skip_key: Option<&[u8]>,
) -> Result<bool, WrongoDBError> {
    while let Some((key, _)) = index_cursor.next()? {
        if key.as_slice() > encoded_end {
            break;
        }
        if should_skip_key(skip_key, &key) {
            continue;
        }

        let Some(id) = decode_index_id(&key)? else {
            continue;
        };
        let primary_key = encode_id_value(&id)?;
        let Some(bytes) = table_cursor.get(&primary_key)? else {
            continue;
        };
        let doc = decode_row_value(table_cursor.table(), &primary_key, &bytes)?;
        if matches_filter(&doc, filter_doc)? {
            return Ok(true);
        }
    }

    Ok(false)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;
    use tempfile::tempdir;

    use super::*;
    use crate::api::DdlPath;
    use crate::catalog::{CatalogStore, CollectionCatalog, CreateIndexRequest};
    use crate::collection_write_path::CollectionWritePath;
    use crate::core::DatabaseName;
    use crate::replication::{
        OplogMode, OplogStore, ReplicationConfig, ReplicationCoordinator, ReplicationObserver,
    };
    use crate::storage::api::{Connection, ConnectionConfig};

    const TEST_OPLOG_TABLE_URI: &str = "table:test_oplog";

    struct DocumentQueryFixture {
        _dir: tempfile::TempDir,
        connection: Arc<Connection>,
        ddl_path: DdlPath,
        query: DocumentQuery,
        write_path: CollectionWritePath,
    }

    impl DocumentQueryFixture {
        fn new() -> Self {
            let dir = tempdir().unwrap();

            let config = ConnectionConfig::new().logging_enabled(false);
            let connection = Arc::new(Connection::open(dir.path(), config).unwrap());
            let metadata_store = connection.metadata_store();
            let oplog_store = OplogStore::new(metadata_store.clone(), TEST_OPLOG_TABLE_URI);
            let catalog = Arc::new(CollectionCatalog::new(CatalogStore::new()));
            let replication = ReplicationCoordinator::new(ReplicationConfig::default());
            {
                let mut session = connection.open_session();
                oplog_store.ensure_table_exists(&mut session).unwrap();
                let next_op_index = oplog_store
                    .load_last_op_time(&mut session)
                    .unwrap()
                    .map(|op_time| op_time.index + 1)
                    .unwrap_or(1);
                replication.seed_next_op_index(next_op_index);
                catalog.ensure_store_exists(&mut session).unwrap();
                catalog.load_cache(&session).unwrap();
            }
            let query = DocumentQuery::new(catalog.clone());
            let write_path = CollectionWritePath::new(
                metadata_store.clone(),
                catalog.clone(),
                query.clone(),
                ReplicationObserver::new(replication.clone(), oplog_store),
            );
            let ddl_path = DdlPath::new(connection.clone(), metadata_store, catalog, replication);

            Self {
                _dir: dir,
                connection,
                ddl_path,
                query,
                write_path,
            }
        }
    }

    fn namespace(collection: &str) -> Namespace {
        Namespace::new(DatabaseName::new("test").unwrap(), collection).unwrap()
    }

    // EARS: When an indexed lookup runs after checkpoint reconciliation, the
    // query layer shall still find the matching document through the durable
    // index definition.
    #[test]
    fn indexed_lookup_survives_checkpoint_reconciliation() {
        let fixture = DocumentQueryFixture::new();
        let mut session = fixture.connection.open_session();

        fixture
            .ddl_path
            .create_collection(&namespace("test"), vec!["name".to_string()])
            .unwrap();
        fixture
            .ddl_path
            .create_index(
                &namespace("test"),
                CreateIndexRequest::single_field_ascending("name"),
            )
            .unwrap();
        session
            .with_transaction(|session| {
                fixture.write_path.insert_one_in_transaction(
                    session,
                    &namespace("test"),
                    json!({"_id": 1, "name": "alice"}),
                    OplogMode::GenerateOplog,
                )?;
                Ok(())
            })
            .unwrap();

        session.checkpoint().unwrap();

        let docs = fixture
            .query
            .find(
                &mut session,
                &namespace("test"),
                Some(json!({"name": "alice"})),
            )
            .unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].get("name"), Some(&json!("alice")));
    }

    // EARS: When a find uses `_id.$gt`, the query layer shall read only later
    // rows in `_id` order.
    #[test]
    fn id_gt_filter_reads_only_later_rows() {
        let fixture = DocumentQueryFixture::new();
        fixture
            .ddl_path
            .create_collection(&namespace("users"), vec!["name".to_string()])
            .unwrap();

        let mut session = fixture.connection.open_session();
        session
            .with_transaction(|session| {
                fixture.write_path.insert_one_in_transaction(
                    session,
                    &namespace("users"),
                    json!({"_id": 1, "name": "alice"}),
                    OplogMode::GenerateOplog,
                )?;
                fixture.write_path.insert_one_in_transaction(
                    session,
                    &namespace("users"),
                    json!({"_id": 2, "name": "bob"}),
                    OplogMode::GenerateOplog,
                )?;
                Ok(())
            })
            .unwrap();

        let docs = fixture
            .query
            .find(
                &mut session,
                &namespace("users"),
                Some(json!({"_id": {"$gt": 1}})),
            )
            .unwrap();

        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].get("_id"), Some(&json!(2)));
    }

    // EARS: When a find uses `_id.$gte`, the query layer shall include the
    // bound row and every later row in `_id` order.
    #[test]
    fn id_gte_filter_includes_the_bound_row() {
        let fixture = DocumentQueryFixture::new();
        fixture
            .ddl_path
            .create_collection(&namespace("users"), vec!["name".to_string()])
            .unwrap();

        let mut session = fixture.connection.open_session();
        session
            .with_transaction(|session| {
                fixture.write_path.insert_one_in_transaction(
                    session,
                    &namespace("users"),
                    json!({"_id": 1, "name": "alice"}),
                    OplogMode::GenerateOplog,
                )?;
                fixture.write_path.insert_one_in_transaction(
                    session,
                    &namespace("users"),
                    json!({"_id": 2, "name": "bob"}),
                    OplogMode::GenerateOplog,
                )?;
                Ok(())
            })
            .unwrap();

        let docs = fixture
            .query
            .find(
                &mut session,
                &namespace("users"),
                Some(json!({"_id": {"$gte": 1}})),
            )
            .unwrap();

        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].get("_id"), Some(&json!(1)));
        assert_eq!(docs[1].get("_id"), Some(&json!(2)));
    }

    // EARS: When a paged find resumes from a bookmark, it shall continue from
    // the last emitted raw key instead of restarting from the beginning.
    #[test]
    fn paged_find_resumes_after_last_primary_key() {
        let fixture = DocumentQueryFixture::new();
        fixture
            .ddl_path
            .create_collection(&namespace("users"), vec!["name".to_string()])
            .unwrap();

        let mut session = fixture.connection.open_session();
        session
            .with_transaction(|session| {
                fixture.write_path.insert_one_in_transaction(
                    session,
                    &namespace("users"),
                    json!({"_id": 1, "name": "alice"}),
                    OplogMode::GenerateOplog,
                )?;
                fixture.write_path.insert_one_in_transaction(
                    session,
                    &namespace("users"),
                    json!({"_id": 2, "name": "bob"}),
                    OplogMode::GenerateOplog,
                )?;
                Ok(())
            })
            .unwrap();

        let request = FindRequest {
            namespace: namespace("users"),
            filter: None,
            skip: 0,
            batch_size: 1,
            limit: None,
            resume: None,
        };
        let first_page = fixture.query.find_page(&mut session, &request).unwrap();
        assert_eq!(first_page.page.docs.len(), 1);
        assert_eq!(first_page.page.docs[0].get("_id"), Some(&json!(1)));

        let second_page = fixture
            .query
            .find_page_with_plan(
                &mut session,
                &first_page.plan,
                &FindRequest {
                    namespace: request.namespace.clone(),
                    filter: None,
                    skip: 0,
                    batch_size: 1,
                    limit: None,
                    resume: first_page.page.next_resume.clone(),
                },
            )
            .unwrap();
        assert_eq!(second_page.docs.len(), 1);
        assert_eq!(second_page.docs[0].get("_id"), Some(&json!(2)));
    }
}
