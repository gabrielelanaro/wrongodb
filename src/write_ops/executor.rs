use std::sync::Arc;

use serde_json::Value;

use crate::collection_write_path::{CollectionWritePath, UpdateResult};
use crate::core::Namespace;
use crate::replication::{OplogMode, ReplicationCoordinator};
use crate::storage::api::Session;
use crate::{Connection, Document, WrongoDBError};

/// Top-level server write executor modeled after MongoDB's `write_ops_exec`.
#[derive(Clone)]
pub(crate) struct WriteOps {
    connection: Arc<Connection>,
    collection_write_path: CollectionWritePath,
    replication: ReplicationCoordinator,
}

impl WriteOps {
    /// Create the top-level primary write executor.
    pub(crate) fn new(
        connection: Arc<Connection>,
        collection_write_path: CollectionWritePath,
        replication: ReplicationCoordinator,
    ) -> Self {
        Self {
            connection,
            collection_write_path,
            replication,
        }
    }

    /// Insert one document through the primary-side write path.
    pub(crate) fn insert_one(
        &self,
        namespace: &Namespace,
        doc: Value,
    ) -> Result<Document, WrongoDBError> {
        self.run_on_primary(|collection_write_path, session| {
            collection_write_path.insert_one_in_transaction(
                session,
                namespace,
                doc,
                OplogMode::GenerateOplog,
            )
        })
    }

    /// Update the first matching document through the primary-side write path.
    pub(crate) fn update_one(
        &self,
        namespace: &Namespace,
        filter: Option<Value>,
        update: Value,
    ) -> Result<UpdateResult, WrongoDBError> {
        self.run_on_primary(|collection_write_path, session| {
            collection_write_path.update_one_in_transaction(
                session,
                namespace,
                filter,
                update,
                OplogMode::GenerateOplog,
            )
        })
    }

    /// Update every matching document through the primary-side write path.
    pub(crate) fn update_many(
        &self,
        namespace: &Namespace,
        filter: Option<Value>,
        update: Value,
    ) -> Result<UpdateResult, WrongoDBError> {
        self.run_on_primary(|collection_write_path, session| {
            collection_write_path.update_many_in_transaction(
                session,
                namespace,
                filter,
                update,
                OplogMode::GenerateOplog,
            )
        })
    }

    /// Delete the first matching document through the primary-side write path.
    pub(crate) fn delete_one(
        &self,
        namespace: &Namespace,
        filter: Option<Value>,
    ) -> Result<usize, WrongoDBError> {
        self.run_on_primary(|collection_write_path, session| {
            collection_write_path.delete_one_in_transaction(
                session,
                namespace,
                filter,
                OplogMode::GenerateOplog,
            )
        })
    }

    /// Delete every matching document through the primary-side write path.
    pub(crate) fn delete_many(
        &self,
        namespace: &Namespace,
        filter: Option<Value>,
    ) -> Result<usize, WrongoDBError> {
        self.run_on_primary(|collection_write_path, session| {
            collection_write_path.delete_many_in_transaction(
                session,
                namespace,
                filter,
                OplogMode::GenerateOplog,
            )
        })
    }

    fn run_on_primary<R, F>(&self, write: F) -> Result<R, WrongoDBError>
    where
        F: FnOnce(&CollectionWritePath, &mut Session) -> Result<R, WrongoDBError>,
    {
        self.replication.require_writable_primary()?;
        let mut session = self.connection.open_session();
        session.with_transaction(|session| write(&self.collection_write_path, session))
    }
}
