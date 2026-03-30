use std::sync::Arc;

use serde_json::{Number, Value};

use crate::core::bson::encode_id_value;
use crate::core::{DatabaseName, Namespace};
use crate::storage::api::Session;
use crate::storage::metadata_store::{MetadataEntry, MetadataStore};
use crate::storage::row::{decode_row_value_from_metadata, encode_row_value};
use crate::{Document, StorageError, WrongoDBError};

const OPLOG_DATABASE: &str = "local";
const OPLOG_COLLECTION: &str = "oplog.rs";
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
    table_uri: String,
}

impl OplogStore {
    /// Build an oplog store handle over the storage metadata plane.
    pub(crate) fn new(metadata_store: Arc<MetadataStore>, table_uri: impl Into<String>) -> Self {
        Self {
            metadata_store,
            table_uri: table_uri.into(),
        }
    }

    /// Ensure the internal oplog table exists.
    #[cfg(test)]
    pub(crate) fn ensure_table_exists(&self, session: &mut Session) -> Result<(), WrongoDBError> {
        session.create_table(
            &self.table_uri,
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
        let mut cursor = session.open_table_cursor(&self.table_uri)?;
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
        let mut cursor = session.open_table_cursor(&self.table_uri)?;
        cursor.insert(&key, &value)
    }

    #[cfg(test)]
    pub(crate) fn list_entries(
        &self,
        session: &mut Session,
    ) -> Result<Vec<OplogEntry>, WrongoDBError> {
        let mut cursor = session.open_table_cursor(&self.table_uri)?;
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
                "table metadata row is missing row_format: {}",
                self.table_uri
            ))
        })?;
        let row = oplog_row_from_entry(entry)?;
        encode_row_value(row_format, oplog_metadata.value_columns(), &row)
    }

    fn oplog_metadata(&self) -> Result<MetadataEntry, WrongoDBError> {
        self.metadata_store
            .get(&self.table_uri)?
            .ok_or_else(|| StorageError(format!("unknown table URI: {}", self.table_uri)).into())
    }
}

/// Return the reserved Mongo-style oplog namespace.
pub(crate) fn oplog_namespace() -> Namespace {
    Namespace::new(
        DatabaseName::new(OPLOG_DATABASE).expect("local is a valid database name"),
        OPLOG_COLLECTION,
    )
    .expect("local.oplog.rs is a valid namespace")
}

/// Return whether a namespace is reserved for replication-owned oplog state.
pub(crate) fn is_reserved_namespace(namespace: &Namespace) -> bool {
    namespace.db_name().is_local() && namespace.collection_name() == OPLOG_COLLECTION
}

pub(in crate::replication) fn oplog_storage_columns() -> Vec<String> {
    OPLOG_COLUMNS
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>()
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

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::tempdir;

    use super::{OpTime, OplogEntry, OplogOperation, OplogStore};
    use crate::core::{DatabaseName, Namespace};
    use crate::storage::api::{Connection, ConnectionConfig};

    const TEST_OPLOG_TABLE_URI: &str = "table:test_oplog";

    fn open_store() -> (Connection, OplogStore) {
        let dir = tempdir().unwrap();
        let base_path = dir.path().to_path_buf();
        std::mem::forget(dir);

        let connection = Connection::open(&base_path, ConnectionConfig::new()).unwrap();
        let oplog_store = OplogStore::new(connection.metadata_store(), TEST_OPLOG_TABLE_URI);
        let mut session = connection.open_session();
        oplog_store.ensure_table_exists(&mut session).unwrap();

        (connection, oplog_store)
    }

    fn namespace(collection: &str) -> Namespace {
        Namespace::new(DatabaseName::new("test").unwrap(), collection).unwrap()
    }

    // EARS: When an oplog entry is appended and later reloaded, the oplog store
    // shall preserve both the logical operation and its replication position.
    #[test]
    fn oplog_store_roundtrips_entries() {
        let (connection, oplog_store) = open_store();
        let entry = OplogEntry {
            op_time: OpTime { term: 3, index: 42 },
            operation: OplogOperation::Update {
                ns: namespace("users").full_name(),
                document: json!({"_id": 7, "name": "alice"})
                    .as_object()
                    .unwrap()
                    .clone(),
                document_key: json!({"_id": 7}).as_object().unwrap().clone(),
            },
        };

        let mut session = connection.open_session();
        session
            .with_transaction(|session| oplog_store.append(session, &entry))
            .unwrap();

        let entries = oplog_store.list_entries(&mut session).unwrap();
        assert_eq!(entries, vec![entry]);
        assert_eq!(
            oplog_store.load_last_op_time(&mut session).unwrap(),
            Some(OpTime { term: 3, index: 42 })
        );
    }
}
