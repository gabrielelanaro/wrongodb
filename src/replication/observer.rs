use serde_json::Value;

use crate::core::Namespace;
use crate::{Document, StorageError, WrongoDBError};

use super::{OpTime, OplogEntry, OplogMode, OplogOperation, OplogStore, ReplicationCoordinator};
use crate::storage::api::Session;

/// Mongo-style logical write observer that appends oplog rows inside the write transaction.
#[derive(Debug, Clone)]
pub(crate) struct ReplicationObserver {
    coordinator: ReplicationCoordinator,
    oplog_store: OplogStore,
}

impl ReplicationObserver {
    /// Build a write observer over the current coordinator and oplog store.
    pub(crate) fn new(coordinator: ReplicationCoordinator, oplog_store: OplogStore) -> Self {
        Self {
            coordinator,
            oplog_store,
        }
    }

    /// Append an insert oplog row for one inserted document.
    pub(crate) fn on_insert(
        &self,
        session: &mut Session,
        namespace: &Namespace,
        document: &Document,
        oplog_mode: OplogMode,
    ) -> Result<Option<OpTime>, WrongoDBError> {
        self.append_entry_for_write(
            session,
            oplog_mode,
            OplogOperation::Insert {
                ns: namespace.full_name(),
                document: document.clone(),
            },
        )
    }

    /// Append an update oplog row for one updated document.
    pub(crate) fn on_update(
        &self,
        session: &mut Session,
        namespace: &Namespace,
        document: &Document,
        oplog_mode: OplogMode,
    ) -> Result<Option<OpTime>, WrongoDBError> {
        let id = document
            .get("_id")
            .ok_or_else(|| StorageError("updated document is missing _id".into()))?;

        self.append_entry_for_write(
            session,
            oplog_mode,
            OplogOperation::Update {
                ns: namespace.full_name(),
                document: document.clone(),
                document_key: document_key(id),
            },
        )
    }

    /// Append a delete oplog row for one deleted document.
    pub(crate) fn on_delete(
        &self,
        session: &mut Session,
        namespace: &Namespace,
        id: &Value,
        oplog_mode: OplogMode,
    ) -> Result<Option<OpTime>, WrongoDBError> {
        self.append_entry_for_write(
            session,
            oplog_mode,
            OplogOperation::Delete {
                ns: namespace.full_name(),
                document_key: document_key(id),
            },
        )
    }

    fn append_entry_for_write(
        &self,
        session: &mut Session,
        oplog_mode: OplogMode,
        operation: OplogOperation,
    ) -> Result<Option<OpTime>, WrongoDBError> {
        if oplog_mode == OplogMode::SuppressOplog {
            return Ok(None);
        }

        // The oplog row must land in the same local transaction as the user
        // data change so both commit or abort together.
        let op_time = OpTime {
            term: self.coordinator.current_term(),
            index: self.coordinator.reserve_next_op_index(),
        };
        self.oplog_store
            .append(session, &OplogEntry { op_time, operation })?;
        Ok(Some(op_time))
    }
}

fn document_key(id: &Value) -> Document {
    let mut key = Document::new();
    key.insert("_id".to_string(), id.clone());
    key
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::tempdir;

    use super::ReplicationObserver;
    use crate::core::{DatabaseName, Namespace};
    use crate::replication::{OplogMode, OplogStore, ReplicationConfig, ReplicationCoordinator};
    use crate::storage::api::{Connection, ConnectionConfig};

    const TEST_OPLOG_TABLE_URI: &str = "table:test_oplog";

    fn namespace(collection: &str) -> Namespace {
        Namespace::new(DatabaseName::new("test").unwrap(), collection).unwrap()
    }

    // EARS: When the observer runs in suppress-oplog mode, it shall not append any
    // oplog rows.
    #[test]
    fn observer_skips_writes_in_suppress_mode() {
        let dir = tempdir().unwrap();
        let base_path = dir.path().to_path_buf();
        std::mem::forget(dir);

        let connection = Connection::open(&base_path, ConnectionConfig::new()).unwrap();
        let oplog_store = OplogStore::new(connection.metadata_store(), TEST_OPLOG_TABLE_URI);
        let coordinator = ReplicationCoordinator::new(ReplicationConfig::default());
        let observer = ReplicationObserver::new(coordinator, oplog_store.clone());
        let document = json!({"_id": 1, "name": "alice"})
            .as_object()
            .unwrap()
            .clone();

        let mut session = connection.open_session();
        oplog_store.ensure_table_exists(&mut session).unwrap();
        session
            .with_transaction(|session| {
                observer.on_insert(
                    session,
                    &namespace("users"),
                    &document,
                    OplogMode::SuppressOplog,
                )
            })
            .unwrap();

        assert!(oplog_store.list_entries(&mut session).unwrap().is_empty());
    }
}
