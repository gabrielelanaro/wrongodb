use std::sync::Arc;

use serde_json::Value;

use crate::collection_write_path::{CollectionWritePath, UpdateResult};
use crate::core::Namespace;
use crate::replication::{OpTime, OplogAwaitService, OplogMode, ReplicationCoordinator};
use crate::storage::api::Session;
use crate::{Connection, Document, WrongoDBError};

/// Top-level server write executor modeled after MongoDB's `write_ops_exec`.
#[derive(Clone)]
pub(crate) struct WriteOps {
    connection: Arc<Connection>,
    collection_write_path: CollectionWritePath,
    replication: ReplicationCoordinator,
    oplog_await_service: OplogAwaitService,
}

impl WriteOps {
    /// Create the top-level primary write executor.
    pub(crate) fn new(
        connection: Arc<Connection>,
        collection_write_path: CollectionWritePath,
        replication: ReplicationCoordinator,
        oplog_await_service: OplogAwaitService,
    ) -> Self {
        Self {
            connection,
            collection_write_path,
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
        let (document, op_time) = self.run_on_primary(|collection_write_path, session| {
            collection_write_path.insert_one_with_op_time_in_transaction(
                session,
                namespace,
                doc,
                OplogMode::GenerateOplog,
            )
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
        let (result, op_time) = self.run_on_primary(|collection_write_path, session| {
            collection_write_path.update_one_with_op_time_in_transaction(
                session,
                namespace,
                filter,
                update,
                OplogMode::GenerateOplog,
            )
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
        let (result, op_time) = self.run_on_primary(|collection_write_path, session| {
            collection_write_path.update_many_with_op_time_in_transaction(
                session,
                namespace,
                filter,
                update,
                OplogMode::GenerateOplog,
            )
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
        let (deleted, op_time) = self.run_on_primary(|collection_write_path, session| {
            collection_write_path.delete_one_with_op_time_in_transaction(
                session,
                namespace,
                filter,
                OplogMode::GenerateOplog,
            )
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
        let (deleted, op_time) = self.run_on_primary(|collection_write_path, session| {
            collection_write_path.delete_many_with_op_time_in_transaction(
                session,
                namespace,
                filter,
                OplogMode::GenerateOplog,
            )
        })?;
        self.notify_oplog_waiters(op_time);
        Ok(deleted)
    }

    fn run_on_primary<R, F>(&self, write: F) -> Result<(R, Option<OpTime>), WrongoDBError>
    where
        F: FnOnce(&CollectionWritePath, &mut Session) -> Result<(R, Option<OpTime>), WrongoDBError>,
    {
        self.replication.require_writable_primary()?;
        let mut session = self.connection.open_session();
        session.with_transaction(|session| write(&self.collection_write_path, session))
    }

    fn notify_oplog_waiters(&self, op_time: Option<OpTime>) {
        if let Some(op_time) = op_time {
            self.oplog_await_service.notify_committed(op_time);
        }
    }
}
