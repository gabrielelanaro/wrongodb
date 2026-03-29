use std::sync::Arc;

use serde_json::{Number, Value};

use crate::core::bson::encode_id_value;
use crate::storage::api::Session;
use crate::storage::metadata_store::{MetadataEntry, MetadataStore};
use crate::storage::row::{decode_row_value_from_metadata, encode_row_value};
use crate::{Document, StorageError, WrongoDBError};

pub(crate) const OPLOG_COLLECTION: &str = "__oplog";
pub(crate) const OPLOG_TABLE_URI: &str = "table:__oplog";
const OPLOG_COLUMNS: [&str; 5] = ["term", "op", "ns", "o", "o2"];

/// Replication position assigned to one oplog entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OpTime {
    pub(crate) term: u64,
    pub(crate) index: u64,
}

/// Whether a write should emit oplog rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OplogMode {
    GenerateOplog,
    SuppressOplog,
}

/// One logical oplog operation persisted in the replication-owned oplog table.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum OplogOperation {
    Insert {
        ns: String,
        document: Document,
    },
    Update {
        ns: String,
        document: Document,
        document_key: Document,
    },
    Delete {
        ns: String,
        document_key: Document,
    },
}

/// One oplog row with its assigned replication position.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct OplogEntry {
    pub(crate) op_time: OpTime,
    pub(crate) operation: OplogOperation,
}

/// Storage-backed internal oplog table managed by the replication layer.
#[derive(Debug, Clone)]
pub(crate) struct OplogStore {
    metadata_store: Arc<MetadataStore>,
}

impl OplogStore {
    /// Build an oplog store handle over the storage metadata plane.
    pub(crate) fn new(metadata_store: Arc<MetadataStore>) -> Self {
        Self { metadata_store }
    }

    /// Ensure the internal oplog table exists.
    pub(crate) fn ensure_table_exists(&self, session: &mut Session) -> Result<(), WrongoDBError> {
        session.create_table(
            OPLOG_TABLE_URI,
            OPLOG_COLUMNS
                .iter()
                .map(|column| (*column).to_string())
                .collect(),
        )
    }

    /// Load the durable tail so startup can reseed the next oplog index.
    pub(crate) fn load_last_op_time(
        &self,
        session: &mut Session,
    ) -> Result<Option<OpTime>, WrongoDBError> {
        let mut cursor = session.open_table_cursor(OPLOG_TABLE_URI)?;
        let oplog_metadata = self.oplog_metadata()?;
        let mut last_op_time = None;

        while let Some((key, value)) = cursor.next()? {
            let entry = decode_oplog_entry(&oplog_metadata, &key, &value)?;
            last_op_time = Some(entry.op_time);
        }

        Ok(last_op_time)
    }

    /// Append one logical oplog entry inside the caller's active transaction.
    pub(crate) fn append(
        &self,
        session: &mut Session,
        entry: &OplogEntry,
    ) -> Result<(), WrongoDBError> {
        let key = encode_id_value(&Value::Number(Number::from(entry.op_time.index)))?;
        let value = self.encode_row(entry)?;
        let mut cursor = session.open_table_cursor(OPLOG_TABLE_URI)?;
        cursor.insert(&key, &value)
    }

    // TODO: why dead code? if this is truly dead code we should get rid of it.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn list_entries(
        &self,
        session: &mut Session,
    ) -> Result<Vec<OplogEntry>, WrongoDBError> {
        let mut cursor = session.open_table_cursor(OPLOG_TABLE_URI)?;
        let oplog_metadata = self.oplog_metadata()?;
        let mut entries = Vec::new();

        while let Some((key, value)) = cursor.next()? {
            entries.push(decode_oplog_entry(&oplog_metadata, &key, &value)?);
        }

        Ok(entries)
    }

    fn encode_row(&self, entry: &OplogEntry) -> Result<Vec<u8>, WrongoDBError> {
        let oplog_metadata = self.oplog_metadata()?;
        let row_format = oplog_metadata.row_format().ok_or_else(|| {
            StorageError(format!(
                "table metadata row is missing row_format: {OPLOG_TABLE_URI}"
            ))
        })?;
        let row = oplog_row_from_entry(entry)?;
        encode_row_value(row_format, oplog_metadata.value_columns(), &row)
    }

    fn oplog_metadata(&self) -> Result<MetadataEntry, WrongoDBError> {
        self.metadata_store
            .get(OPLOG_TABLE_URI)?
            .ok_or_else(|| StorageError(format!("unknown table URI: {OPLOG_TABLE_URI}")).into())
    }
}

fn oplog_row_from_entry(entry: &OplogEntry) -> Result<Document, WrongoDBError> {
    let mut row = Document::new();
    row.insert(
        "_id".to_string(),
        Value::Number(Number::from(entry.op_time.index)),
    );
    row.insert(
        "term".to_string(),
        Value::Number(Number::from(entry.op_time.term)),
    );

    match &entry.operation {
        OplogOperation::Insert { ns, document } => {
            row.insert("op".to_string(), Value::String("i".to_string()));
            row.insert("ns".to_string(), Value::String(ns.clone()));
            row.insert(
                "o".to_string(),
                // `wt_row_v1` currently stores only scalar cells, so logical
                // document payloads are encoded as JSON strings.
                Value::String(serde_json::to_string(document)?),
            );
        }
        OplogOperation::Update {
            ns,
            document,
            document_key,
        } => {
            row.insert("op".to_string(), Value::String("u".to_string()));
            row.insert("ns".to_string(), Value::String(ns.clone()));
            row.insert(
                "o".to_string(),
                Value::String(serde_json::to_string(document)?),
            );
            row.insert(
                "o2".to_string(),
                Value::String(serde_json::to_string(document_key)?),
            );
        }
        OplogOperation::Delete { ns, document_key } => {
            row.insert("op".to_string(), Value::String("d".to_string()));
            row.insert("ns".to_string(), Value::String(ns.clone()));
            row.insert(
                "o".to_string(),
                Value::String(serde_json::to_string(document_key)?),
            );
        }
    }

    Ok(row)
}

fn decode_oplog_entry(
    oplog_metadata: &MetadataEntry,
    key: &[u8],
    value: &[u8],
) -> Result<OplogEntry, WrongoDBError> {
    let row_format = oplog_metadata.row_format().ok_or_else(|| {
        StorageError(format!(
            "table metadata row is missing row_format: {}",
            oplog_metadata.uri()
        ))
    })?;
    let row =
        decode_row_value_from_metadata(row_format, oplog_metadata.value_columns(), key, value)?;
    let index = required_u64_field(&row, "_id")?;
    let term = required_u64_field(&row, "term")?;
    let op = required_string_field(&row, "op")?;
    let ns = required_string_field(&row, "ns")?;

    let operation = match op.as_str() {
        "i" => OplogOperation::Insert {
            ns,
            document: required_json_document_field(&row, "o")?,
        },
        "u" => OplogOperation::Update {
            ns,
            document: required_json_document_field(&row, "o")?,
            document_key: required_json_document_field(&row, "o2")?,
        },
        "d" => OplogOperation::Delete {
            ns,
            document_key: required_json_document_field(&row, "o")?,
        },
        _ => return Err(StorageError(format!("unknown oplog op type: {op}")).into()),
    };

    Ok(OplogEntry {
        op_time: OpTime { term, index },
        operation,
    })
}

fn required_u64_field(doc: &Document, field: &str) -> Result<u64, WrongoDBError> {
    doc.get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| StorageError(format!("oplog row is missing numeric field {field}")).into())
}

fn required_string_field(doc: &Document, field: &str) -> Result<String, WrongoDBError> {
    doc.get(field)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| StorageError(format!("oplog row is missing string field {field}")).into())
}

fn required_json_document_field(doc: &Document, field: &str) -> Result<Document, WrongoDBError> {
    let raw = required_string_field(doc, field)?;
    let value: Value = serde_json::from_str(&raw)?;
    let object = value
        .as_object()
        .cloned()
        .ok_or_else(|| StorageError(format!("oplog field {field} is not a JSON document")))?;
    Ok(object)
}
