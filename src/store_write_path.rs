use std::sync::Arc;

use crate::api::WriteUnitOfWork;
use crate::core::errors::{DocumentValidationError, StorageError};
use crate::durability::{DurabilityBackend, DurabilityGuarantee, DurableOp};
use crate::replication::{ReplicationCoordinator, WritePathMode};
use crate::storage::table_cache::TableCache;
use crate::txn::TxnId;
use crate::WrongoDBError;

#[derive(Clone)]
pub(crate) struct StoreWritePath {
    table_cache: Arc<TableCache>,
    durability_backend: Arc<DurabilityBackend>,
    replication_coordinator: Arc<ReplicationCoordinator>,
}

impl StoreWritePath {
    pub(crate) fn new(
        table_cache: Arc<TableCache>,
        durability_backend: Arc<DurabilityBackend>,
        replication_coordinator: Arc<ReplicationCoordinator>,
    ) -> Self {
        Self {
            table_cache,
            durability_backend,
            replication_coordinator,
        }
    }

    pub(crate) fn ensure_store(&self, store_name: &str) -> Result<(), WrongoDBError> {
        let _ = self.table_cache.get_or_open_store(store_name)?;
        Ok(())
    }

    pub(crate) fn write_path_mode(&self) -> WritePathMode {
        self.replication_coordinator.write_path_mode()
    }

    pub(crate) fn record_commit(&self, txn_id: TxnId) -> Result<(), WrongoDBError> {
        self.replication_coordinator.record(
            self.durability_backend.as_ref(),
            DurableOp::TxnCommit {
                txn_id,
                commit_ts: txn_id,
            },
            DurabilityGuarantee::Sync,
        )
    }

    pub(crate) fn record_abort(&self, txn_id: TxnId) -> Result<(), WrongoDBError> {
        self.replication_coordinator.record(
            self.durability_backend.as_ref(),
            DurableOp::TxnAbort { txn_id },
            DurabilityGuarantee::Buffered,
        )
    }

    pub(crate) fn insert(
        &self,
        write_unit: &mut WriteUnitOfWork<'_>,
        store_name: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), WrongoDBError> {
        let txn_id = write_unit.txn_id();
        match self.replication_coordinator.write_path_mode() {
            WritePathMode::LocalApply => {
                let mut cursor = write_unit.open_store_cursor_by_name(store_name)?;
                cursor.insert(key, value)
            }
            WritePathMode::DeferredReplication => {
                if self.contains_key(store_name, key, txn_id)? {
                    return Err(DocumentValidationError("duplicate key error".into()).into());
                }
                self.record_put(store_name, key, value, txn_id)
            }
        }
    }

    pub(crate) fn update(
        &self,
        write_unit: &mut WriteUnitOfWork<'_>,
        store_name: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), WrongoDBError> {
        let txn_id = write_unit.txn_id();
        match self.replication_coordinator.write_path_mode() {
            WritePathMode::LocalApply => {
                let mut cursor = write_unit.open_store_cursor_by_name(store_name)?;
                cursor.update(key, value)
            }
            WritePathMode::DeferredReplication => {
                if !self.contains_key(store_name, key, txn_id)? {
                    return Err(WrongoDBError::Storage(StorageError(
                        "key not found for update".to_string(),
                    )));
                }
                self.record_put(store_name, key, value, txn_id)
            }
        }
    }

    pub(crate) fn delete(
        &self,
        write_unit: &mut WriteUnitOfWork<'_>,
        store_name: &str,
        key: &[u8],
    ) -> Result<(), WrongoDBError> {
        let txn_id = write_unit.txn_id();
        match self.replication_coordinator.write_path_mode() {
            WritePathMode::LocalApply => {
                let mut cursor = write_unit.open_store_cursor_by_name(store_name)?;
                cursor.delete(key)
            }
            WritePathMode::DeferredReplication => {
                if !self.contains_key(store_name, key, txn_id)? {
                    return Err(WrongoDBError::Storage(StorageError(
                        "key not found for delete".to_string(),
                    )));
                }
                self.record_delete(store_name, key, txn_id)
            }
        }
    }

    fn contains_key(
        &self,
        store_name: &str,
        key: &[u8],
        txn_id: TxnId,
    ) -> Result<bool, WrongoDBError> {
        let table = self.table_cache.get_or_open_store(store_name)?;
        let mut table = table.write();
        table.contains_key(key, txn_id)
    }

    fn record_put(
        &self,
        store_name: &str,
        key: &[u8],
        value: &[u8],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        self.replication_coordinator.record(
            self.durability_backend.as_ref(),
            DurableOp::Put {
                store_name: store_name.to_string(),
                key: key.to_vec(),
                value: value.to_vec(),
                txn_id,
            },
            DurabilityGuarantee::Buffered,
        )
    }

    fn record_delete(
        &self,
        store_name: &str,
        key: &[u8],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        self.replication_coordinator.record(
            self.durability_backend.as_ref(),
            DurableOp::Delete {
                store_name: store_name.to_string(),
                key: key.to_vec(),
                txn_id,
            },
            DurabilityGuarantee::Buffered,
        )
    }
}
