use std::sync::Arc;

use serde_json::Value;

use crate::collection_write_path::{CollectionWritePath, UpdateOneOutcome, UpdateResult};
use crate::core::Namespace;
use crate::replication::{OpTime, OplogAwaitService, ReplicationCoordinator, ReplicationObserver};
use crate::storage::api::Session;
use crate::{Connection, Document, WrongoDBError};

/// Top-level server write executor modeled after MongoDB's `write_ops_exec`.
#[derive(Clone)]
pub(crate) struct WriteOps {
    connection: Arc<Connection>,
    collection_write_path: CollectionWritePath,
    replication_observer: ReplicationObserver,
    replication: ReplicationCoordinator,
    oplog_await_service: OplogAwaitService,
}

impl WriteOps {
    /// Create the top-level primary write executor.
    pub(crate) fn new(
        connection: Arc<Connection>,
        collection_write_path: CollectionWritePath,
        replication_observer: ReplicationObserver,
        replication: ReplicationCoordinator,
        oplog_await_service: OplogAwaitService,
    ) -> Self {
        Self {
            connection,
            collection_write_path,
            replication_observer,
            replication,
            oplog_await_service,
        }
    }

    /// Insert one document through the primary-side write path.
    pub(crate) fn insert_one(
        &self,
        namespace: &Namespace,
        doc: Value,
    ) -> Result<Document, WrongoDBError> {
        let (document, op_time) =
            self.run_on_primary(|collection_write_path, observer, session| {
                let document = collection_write_path.insert_one(session, namespace, doc)?;
                let op_time = observer.on_insert(
                    session,
                    namespace,
                    &document,
                    crate::replication::OplogMode::GenerateOplog,
                )?;
                Ok((document, op_time))
            })?;
        self.notify_oplog_waiters(op_time);
        Ok(document)
    }

    /// Update the first matching document through the primary-side write path.
    pub(crate) fn update_one(
        &self,
        namespace: &Namespace,
        filter: Option<Value>,
        update: Value,
    ) -> Result<UpdateResult, WrongoDBError> {
        let (result, op_time) =
            self.run_on_primary(|collection_write_path, observer, session| {
                let outcome =
                    collection_write_path.update_one(session, namespace, filter, update)?;
                let op_time = append_update_outcome(observer, session, namespace, &outcome)?;
                Ok((outcome.result, op_time))
            })?;
        self.notify_oplog_waiters(op_time);
        Ok(result)
    }

    /// Update every matching document through the primary-side write path.
    pub(crate) fn update_many(
        &self,
        namespace: &Namespace,
        filter: Option<Value>,
        update: Value,
    ) -> Result<UpdateResult, WrongoDBError> {
        let (result, op_time) =
            self.run_on_primary(|collection_write_path, observer, session| {
                let outcome =
                    collection_write_path.update_many(session, namespace, filter, update)?;
                let op_time = append_updated_documents(
                    observer,
                    session,
                    namespace,
                    &outcome.updated_documents,
                )?;
                Ok((outcome.result, op_time))
            })?;
        self.notify_oplog_waiters(op_time);
        Ok(result)
    }

    /// Delete the first matching document through the primary-side write path.
    pub(crate) fn delete_one(
        &self,
        namespace: &Namespace,
        filter: Option<Value>,
    ) -> Result<usize, WrongoDBError> {
        let (deleted, op_time) =
            self.run_on_primary(|collection_write_path, observer, session| {
                let outcome = collection_write_path.delete_one(session, namespace, filter)?;
                let op_time =
                    append_deleted_ids(observer, session, namespace, &outcome.deleted_ids)?;
                Ok((outcome.deleted, op_time))
            })?;
        self.notify_oplog_waiters(op_time);
        Ok(deleted)
    }

    /// Delete every matching document through the primary-side write path.
    pub(crate) fn delete_many(
        &self,
        namespace: &Namespace,
        filter: Option<Value>,
    ) -> Result<usize, WrongoDBError> {
        let (deleted, op_time) =
            self.run_on_primary(|collection_write_path, observer, session| {
                let outcome = collection_write_path.delete_many(session, namespace, filter)?;
                let op_time =
                    append_deleted_ids(observer, session, namespace, &outcome.deleted_ids)?;
                Ok((outcome.deleted, op_time))
            })?;
        self.notify_oplog_waiters(op_time);
        Ok(deleted)
    }

    fn run_on_primary<R, F>(&self, write: F) -> Result<(R, Option<OpTime>), WrongoDBError>
    where
        F: FnOnce(
            &CollectionWritePath,
            &ReplicationObserver,
            &mut Session,
        ) -> Result<(R, Option<OpTime>), WrongoDBError>,
    {
        self.replication.require_writable_primary()?;
        let mut session = self.connection.open_session();
        session.with_transaction(|session| {
            write(
                &self.collection_write_path,
                &self.replication_observer,
                session,
            )
        })
    }

    fn notify_oplog_waiters(&self, op_time: Option<OpTime>) {
        if let Some(op_time) = op_time {
            self.oplog_await_service.notify_committed(op_time);
        }
    }
}

fn append_update_outcome(
    observer: &ReplicationObserver,
    session: &mut Session,
    namespace: &Namespace,
    outcome: &UpdateOneOutcome,
) -> Result<Option<OpTime>, WrongoDBError> {
    match &outcome.updated_document {
        Some(document) => observer.on_update(
            session,
            namespace,
            document,
            crate::replication::OplogMode::GenerateOplog,
        ),
        None => Ok(None),
    }
}

fn append_updated_documents(
    observer: &ReplicationObserver,
    session: &mut Session,
    namespace: &Namespace,
    documents: &[Document],
) -> Result<Option<OpTime>, WrongoDBError> {
    let mut latest_op_time = None;
    for document in documents {
        latest_op_time = observer.on_update(
            session,
            namespace,
            document,
            crate::replication::OplogMode::GenerateOplog,
        )?;
    }
    Ok(latest_op_time)
}

fn append_deleted_ids(
    observer: &ReplicationObserver,
    session: &mut Session,
    namespace: &Namespace,
    deleted_ids: &[Value],
) -> Result<Option<OpTime>, WrongoDBError> {
    let mut latest_op_time = None;
    for id in deleted_ids {
        latest_op_time = observer.on_delete(
            session,
            namespace,
            id,
            crate::replication::OplogMode::GenerateOplog,
        )?;
    }
    Ok(latest_op_time)
}
