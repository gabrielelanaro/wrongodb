use std::sync::Arc;

use serde_json::{Number, Value};

use crate::core::bson::encode_id_value;
use crate::core::{DatabaseName, Namespace};
use crate::replication::OpTime;
use crate::storage::api::Session;
use crate::storage::metadata_store::{MetadataEntry, MetadataStore};
use crate::storage::row::RowFormat;
use crate::storage::row::{decode_row_value_from_metadata, encode_row_value};
use crate::{Document, StorageError, WrongoDBError};

const REPL_STATE_DATABASE: &str = "local";
const REPL_STATE_COLLECTION: &str = "repl_state";
const REPL_STATE_COLUMNS: [&str; 2] = ["term", "index"];
const LAST_APPLIED_KEY: &str = "lastApplied";

/// Storage-backed durable replication markers owned by the secondary.
#[derive(Debug, Clone)]
pub(crate) struct LocalReplicationStateStore {
    metadata_store: Arc<MetadataStore>,
    table_uri: String,
}

impl LocalReplicationStateStore {
    /// Build the replication marker store over one metadata-managed table.
    pub(crate) fn new(metadata_store: Arc<MetadataStore>, table_uri: impl Into<String>) -> Self {
        Self {
            metadata_store,
            table_uri: table_uri.into(),
        }
    }

    /// Read the last oplog entry that the secondary finished applying.
    pub(crate) fn load_last_applied(
        &self,
        session: &mut Session,
    ) -> Result<Option<OpTime>, WrongoDBError> {
        let mut cursor = session.open_table_cursor(&self.table_uri)?;
        let key = encode_id_value(&Value::String(LAST_APPLIED_KEY.to_string()))?;
        let Some(value) = cursor.get(&key)? else {
            return Ok(None);
        };

        let metadata = self.table_metadata()?;
        let row_format = self.required_row_format(&metadata)?;
        let row =
            decode_row_value_from_metadata(row_format, metadata.value_columns(), &key, &value)?;
        Ok(Some(OpTime {
            term: required_u64_field(&row, "term")?,
            index: required_u64_field(&row, "index")?,
        }))
    }

    /// Persist the last oplog entry that the secondary finished applying.
    pub(crate) fn save_last_applied(
        &self,
        session: &mut Session,
        op_time: OpTime,
    ) -> Result<(), WrongoDBError> {
        let key = encode_id_value(&Value::String(LAST_APPLIED_KEY.to_string()))?;
        let value = self.encode_row(op_time)?;
        let mut cursor = session.open_table_cursor(&self.table_uri)?;
        if cursor.get(&key)?.is_some() {
            cursor.update(&key, &value)
        } else {
            cursor.insert(&key, &value)
        }
    }

    fn encode_row(&self, op_time: OpTime) -> Result<Vec<u8>, WrongoDBError> {
        let metadata = self.table_metadata()?;
        let row_format = self.required_row_format(&metadata)?;

        let mut row = Document::new();
        row.insert(
            "_id".to_string(),
            Value::String(LAST_APPLIED_KEY.to_string()),
        );
        row.insert(
            "term".to_string(),
            Value::Number(Number::from(op_time.term)),
        );
        row.insert(
            "index".to_string(),
            Value::Number(Number::from(op_time.index)),
        );
        encode_row_value(row_format, metadata.value_columns(), &row)
    }

    fn table_metadata(&self) -> Result<MetadataEntry, WrongoDBError> {
        self.metadata_store
            .get(&self.table_uri)?
            .ok_or_else(|| StorageError(format!("unknown table URI: {}", self.table_uri)).into())
    }

    fn required_row_format(&self, metadata: &MetadataEntry) -> Result<RowFormat, WrongoDBError> {
        metadata.row_format().ok_or_else(|| {
            StorageError(format!(
                "table metadata row is missing row_format: {}",
                self.table_uri
            ))
            .into()
        })
    }

    /// Ensure the internal replication-state table exists.
    #[cfg(test)]
    pub(crate) fn ensure_table_exists(&self, session: &mut Session) -> Result<(), WrongoDBError> {
        session.create_table(
            &self.table_uri,
            REPL_STATE_COLUMNS
                .iter()
                .map(|column| (*column).to_string())
                .collect(),
        )
    }
}

/// Return the reserved namespace used for replication restart markers.
pub(crate) fn replication_state_namespace() -> Namespace {
    Namespace::new(
        DatabaseName::new(REPL_STATE_DATABASE).expect("local is a valid database name"),
        REPL_STATE_COLLECTION,
    )
    .expect("local.repl_state is a valid namespace")
}

pub(in crate::replication) fn replication_state_storage_columns() -> Vec<String> {
    REPL_STATE_COLUMNS
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>()
}

fn required_u64_field(doc: &Document, field: &str) -> Result<u64, WrongoDBError> {
    doc.get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| StorageError(format!("replication state is missing field {field}")).into())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{replication_state_namespace, LocalReplicationStateStore};
    use crate::catalog::{CatalogStore, CollectionCatalog};
    use crate::replication::{bootstrap_replication_state, OpTime};
    use crate::storage::api::{Connection, ConnectionConfig};

    const TEST_REPL_STATE_TABLE_URI: &str = "table:test_repl_state";

    // EARS: When the secondary persists its last-applied marker and restarts,
    // the replication state store shall reload the same durable opTime.
    #[test]
    fn last_applied_roundtrips() {
        let dir = tempdir().unwrap();
        let connection = Connection::open(dir.path(), ConnectionConfig::default()).unwrap();
        let store =
            LocalReplicationStateStore::new(connection.metadata_store(), TEST_REPL_STATE_TABLE_URI);
        let mut session = connection.open_session();
        store.ensure_table_exists(&mut session).unwrap();

        session
            .with_transaction(|session| {
                store.save_last_applied(session, OpTime { term: 3, index: 99 })
            })
            .unwrap();

        assert_eq!(
            store.load_last_applied(&mut session).unwrap(),
            Some(OpTime { term: 3, index: 99 })
        );
    }

    // EARS: When the replication bootstrap runs, it shall create the reserved
    // `local.repl_state` namespace used for durable apply markers.
    #[test]
    fn bootstrap_creates_reserved_replication_state_namespace() {
        let dir = tempdir().unwrap();
        let connection = Connection::open(dir.path(), ConnectionConfig::default()).unwrap();
        let catalog = CollectionCatalog::new(CatalogStore::new());
        let mut session = connection.open_session();
        catalog.ensure_store_exists(&mut session).unwrap();

        let _store = bootstrap_replication_state(&connection, &catalog).unwrap();

        let definition = catalog
            .get_collection(&session, &replication_state_namespace())
            .unwrap();
        assert!(definition.is_some());
    }
}
