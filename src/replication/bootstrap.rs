use crate::catalog::CollectionCatalog;
use crate::replication::{oplog_namespace, OplogStore, ReplicationCoordinator};
use crate::{Connection, WrongoDBError};

use super::oplog::oplog_storage_columns;

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
        let _ =
            catalog.create_collection(&mut session, &oplog_namespace, &oplog_storage_columns())?;
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
