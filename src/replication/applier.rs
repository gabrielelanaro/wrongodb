use std::sync::Arc;

use crate::api::DdlPath;
use crate::collection_write_path::CollectionWritePath;
use crate::core::Namespace;
use crate::document_query::DocumentQuery;
use crate::replication::{LocalReplicationStateStore, OplogEntry, OplogOperation};
use crate::StorageError;
use crate::{Connection, Document, WrongoDBError};

/// Applies persisted oplog entries to local data on a secondary.
///
/// CRUD entries are applied in one transaction together with the durable
/// `lastApplied` marker. DDL entries are replayed through [`DdlPath`], which
/// keeps its own idempotent orchestration, and the marker is updated after the
/// DDL call succeeds.
#[derive(Clone)]
pub(crate) struct OplogApplier {
    connection: Arc<Connection>,
    collection_write_path: CollectionWritePath,
    document_query: DocumentQuery,
    ddl_path: DdlPath,
    state_store: LocalReplicationStateStore,
}

impl OplogApplier {
    /// Builds the oplog applier over the existing document and DDL write paths.
    pub(crate) fn new(
        connection: Arc<Connection>,
        collection_write_path: CollectionWritePath,
        document_query: DocumentQuery,
        ddl_path: DdlPath,
        state_store: LocalReplicationStateStore,
    ) -> Self {
        Self {
            connection,
            collection_write_path,
            document_query,
            ddl_path,
            state_store,
        }
    }

    /// Apply one locally persisted oplog entry.
    pub(crate) fn apply(&self, entry: &OplogEntry) -> Result<(), WrongoDBError> {
        let result = match &entry.operation {
            OplogOperation::Insert { ns, document } => {
                self.apply_insert(entry.op_time, ns, document)
            }
            OplogOperation::Update {
                ns,
                document,
                document_key,
            } => self.apply_update(entry.op_time, ns, document, document_key),
            OplogOperation::Delete { ns, document_key } => {
                self.apply_delete(entry.op_time, ns, document_key)
            }
            OplogOperation::CreateCollection {
                ns,
                storage_columns,
                collection_uuid,
            } => self.apply_create_collection(entry.op_time, ns, storage_columns, collection_uuid),
            OplogOperation::CreateIndex {
                ns,
                name,
                indexed_field,
            } => self.apply_create_index(entry.op_time, ns, name, indexed_field),
        };

        result.map_err(|err| {
            StorageError(format!(
                "failed to apply oplog entry {:?}: {err}",
                entry.operation
            ))
            .into()
        })
    }

    fn apply_insert(
        &self,
        op_time: crate::replication::OpTime,
        ns: &str,
        document: &Document,
    ) -> Result<(), WrongoDBError> {
        let namespace = Namespace::parse(ns)?;
        let mut session = self.connection.open_session();
        session.with_transaction(|session| {
            let existing = self.find_by_document_key(
                session,
                &namespace,
                &document_key(document_id(document)?),
            )?;
            if let Some(current) = existing.first() {
                if current != document {
                    return Err(StorageError(format!(
                        "replicated insert collides with different existing document in {}",
                        namespace.full_name()
                    ))
                    .into());
                }
            } else {
                let _ = self.collection_write_path.insert_one(
                    session,
                    &namespace,
                    serde_json::Value::Object(document.clone()),
                )?;
            }
            self.state_store.save_last_applied(session, op_time)?;
            Ok(())
        })
    }

    fn apply_update(
        &self,
        op_time: crate::replication::OpTime,
        ns: &str,
        document: &Document,
        document_key: &Document,
    ) -> Result<(), WrongoDBError> {
        let namespace = Namespace::parse(ns)?;
        let mut session = self.connection.open_session();
        session.with_transaction(|session| {
            let existing = self.find_by_document_key(session, &namespace, document_key)?;
            let Some(current) = existing.first() else {
                return Err(StorageError(format!(
                    "replicated update could not find target document in {}",
                    namespace.full_name()
                ))
                .into());
            };
            if current != document {
                self.collection_write_path
                    .replace_one(session, &namespace, document)?;
            }
            self.state_store.save_last_applied(session, op_time)?;
            Ok(())
        })
    }

    fn apply_delete(
        &self,
        op_time: crate::replication::OpTime,
        ns: &str,
        document_key: &Document,
    ) -> Result<(), WrongoDBError> {
        let namespace = Namespace::parse(ns)?;
        let mut session = self.connection.open_session();
        session.with_transaction(|session| {
            let existing = self.find_by_document_key(session, &namespace, document_key)?;
            if !existing.is_empty() {
                self.collection_write_path.delete_by_id(
                    session,
                    &namespace,
                    document_key_id(document_key)?,
                )?;
            }
            self.state_store.save_last_applied(session, op_time)?;
            Ok(())
        })
    }

    fn apply_create_collection(
        &self,
        op_time: crate::replication::OpTime,
        ns: &str,
        storage_columns: &[String],
        collection_uuid: &str,
    ) -> Result<(), WrongoDBError> {
        self.ddl_path.apply_create_collection(
            &Namespace::parse(ns)?,
            storage_columns.to_vec(),
            collection_uuid.to_string(),
        )?;
        self.persist_last_applied(op_time)
    }

    fn apply_create_index(
        &self,
        op_time: crate::replication::OpTime,
        ns: &str,
        name: &str,
        indexed_field: &str,
    ) -> Result<(), WrongoDBError> {
        self.ddl_path
            .apply_create_index(&Namespace::parse(ns)?, name, indexed_field)?;
        self.persist_last_applied(op_time)
    }

    fn persist_last_applied(
        &self,
        op_time: crate::replication::OpTime,
    ) -> Result<(), WrongoDBError> {
        let mut session = self.connection.open_session();
        session.with_transaction(|session| self.state_store.save_last_applied(session, op_time))
    }

    fn find_by_document_key(
        &self,
        session: &mut crate::storage::api::Session,
        namespace: &Namespace,
        document_key: &Document,
    ) -> Result<Vec<Document>, WrongoDBError> {
        self.document_query.find_in_transaction(
            session,
            namespace,
            Some(serde_json::Value::Object(document_key.clone())),
        )
    }
}

fn document_id(document: &Document) -> Result<&serde_json::Value, WrongoDBError> {
    document
        .get("_id")
        .ok_or_else(|| StorageError("replicated document is missing _id".into()).into())
}

fn document_key_id(document_key: &Document) -> Result<&serde_json::Value, WrongoDBError> {
    document_key
        .get("_id")
        .ok_or_else(|| StorageError("replicated documentKey is missing _id".into()).into())
}

fn document_key(id: &serde_json::Value) -> Document {
    let mut key = Document::new();
    key.insert("_id".to_string(), id.clone());
    key
}
