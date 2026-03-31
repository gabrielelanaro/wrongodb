use crate::catalog::{CollectionCatalog, CollectionDefinition};
use crate::replication::{
    oplog_namespace, replication_state_namespace, LocalReplicationStateStore, OplogStore,
    ReplicationCoordinator,
};
use crate::{Connection, WrongoDBError};

use super::oplog::oplog_storage_columns;
use super::state_store::replication_state_storage_columns;

/// Ensure the reserved oplog namespace exists and seed replication state from it.
///
/// This is replication bootstrap policy, not generic database-service wiring:
/// the replication layer owns the decision that `local.oplog.rs` must exist and
/// that the coordinator's next oplog position comes from its durable tail.
pub(crate) fn bootstrap_oplog(
    connection: &Connection,
    catalog: &CollectionCatalog,
    replication: &ReplicationCoordinator,
) -> Result<OplogStore, WrongoDBError> {
    let metadata_store = connection.metadata_store();
    let mut session = connection.open_session();
    let oplog_namespace = oplog_namespace();

    if catalog
        .get_collection(&session, &oplog_namespace)?
        .is_none()
    {
        // Bootstrap is creating an internal system namespace, not running
        // user-visible DDL. We intentionally bypass `DdlPath` here because
        // reserved namespaces must be initialized locally without writable-
        // primary checks or DDL oplog generation.
        let definition =
            CollectionDefinition::new_generated(oplog_namespace.clone(), oplog_storage_columns());
        session.create_table(
            definition.table_uri(),
            definition.storage_columns().to_vec(),
        )?;
        session.with_transaction(|session| catalog.create_collection(session, &definition))?;
    }

    let oplog_table_uri = catalog
        .get_collection(&session, &oplog_namespace)?
        .expect("oplog namespace is created during replication bootstrap")
        .table_uri()
        .to_string();
    let oplog_store = OplogStore::new(metadata_store, oplog_table_uri);
    let next_op_index = oplog_store
        .load_last_op_time(&mut session)?
        .map(|op_time| op_time.index + 1)
        .unwrap_or(1);
    replication.seed_next_op_index(next_op_index);

    Ok(oplog_store)
}

/// Ensure the reserved replication-state namespace exists.
pub(crate) fn bootstrap_replication_state(
    connection: &Connection,
    catalog: &CollectionCatalog,
) -> Result<LocalReplicationStateStore, WrongoDBError> {
    let metadata_store = connection.metadata_store();
    let mut session = connection.open_session();
    let state_namespace = replication_state_namespace();

    if catalog
        .get_collection(&session, &state_namespace)?
        .is_none()
    {
        // Like the oplog bootstrap above, this reserved marker store is an
        // internal local namespace. We create the backing table and catalog row
        // directly instead of going through `DdlPath` so startup can ensure the
        // namespace exists without emitting replicated DDL.
        let definition = CollectionDefinition::new_generated(
            state_namespace.clone(),
            replication_state_storage_columns(),
        );
        session.create_table(
            definition.table_uri(),
            definition.storage_columns().to_vec(),
        )?;
        session.with_transaction(|session| catalog.create_collection(session, &definition))?;
    }

    let table_uri = catalog
        .get_collection(&session, &state_namespace)?
        .expect("replication state namespace is created during replication bootstrap")
        .table_uri()
        .to_string();
    Ok(LocalReplicationStateStore::new(metadata_store, table_uri))
}
