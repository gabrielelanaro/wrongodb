use std::sync::Arc;

use crate::core::errors::{DocumentValidationError, StorageError};
use crate::durability::{DurabilityBackend, DurabilityGuarantee, DurableOp};
use crate::replication::{ReplicationCoordinator, WritePathMode};
use crate::storage::api::WriteUnitOfWork;
use crate::txn::TxnId;
use crate::WrongoDBError;

#[derive(Clone)]
pub(crate) struct StoreWritePath {
    durability_backend: Arc<DurabilityBackend>,
    replication_coordinator: Arc<ReplicationCoordinator>,
}

impl StoreWritePath {
    pub(crate) fn new(
        durability_backend: Arc<DurabilityBackend>,
        replication_coordinator: Arc<ReplicationCoordinator>,
    ) -> Self {
        Self {
            durability_backend,
            replication_coordinator,
        }
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
        uri: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), WrongoDBError> {
        let txn_id = write_unit.txn_id();
        match self.replication_coordinator.write_path_mode() {
            WritePathMode::LocalApply => {
                let mut cursor = write_unit.open_cursor(uri)?;
                cursor.insert(key, value)
            }
            WritePathMode::DeferredReplication => {
                if self.contains_key(write_unit, uri, key)? {
                    return Err(DocumentValidationError("duplicate key error".into()).into());
                }
                self.record_put(uri, key, value, txn_id)
            }
        }
    }

    pub(crate) fn update(
        &self,
        write_unit: &mut WriteUnitOfWork<'_>,
        uri: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), WrongoDBError> {
        let txn_id = write_unit.txn_id();
        match self.replication_coordinator.write_path_mode() {
            WritePathMode::LocalApply => {
                let mut cursor = write_unit.open_cursor(uri)?;
                cursor.update(key, value)
            }
            WritePathMode::DeferredReplication => {
                if !self.contains_key(write_unit, uri, key)? {
                    return Err(WrongoDBError::Storage(StorageError(
                        "key not found for update".to_string(),
                    )));
                }
                self.record_put(uri, key, value, txn_id)
            }
        }
    }

    pub(crate) fn delete(
        &self,
        write_unit: &mut WriteUnitOfWork<'_>,
        uri: &str,
        key: &[u8],
    ) -> Result<(), WrongoDBError> {
        let txn_id = write_unit.txn_id();
        match self.replication_coordinator.write_path_mode() {
            WritePathMode::LocalApply => {
                let mut cursor = write_unit.open_cursor(uri)?;
                cursor.delete(key)
            }
            WritePathMode::DeferredReplication => {
                if !self.contains_key(write_unit, uri, key)? {
                    return Err(WrongoDBError::Storage(StorageError(
                        "key not found for delete".to_string(),
                    )));
                }
                self.record_delete(uri, key, txn_id)
            }
        }
    }

    fn contains_key(
        &self,
        write_unit: &mut WriteUnitOfWork<'_>,
        uri: &str,
        key: &[u8],
    ) -> Result<bool, WrongoDBError> {
        let mut cursor = write_unit.open_cursor(uri)?;
        Ok(cursor.get(key)?.is_some())
    }

    fn record_put(
        &self,
        uri: &str,
        key: &[u8],
        value: &[u8],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        self.replication_coordinator.record(
            self.durability_backend.as_ref(),
            DurableOp::Put {
                uri: uri.to_string(),
                key: key.to_vec(),
                value: value.to_vec(),
                txn_id,
            },
            DurabilityGuarantee::Buffered,
        )
    }

    fn record_delete(&self, uri: &str, key: &[u8], txn_id: TxnId) -> Result<(), WrongoDBError> {
        self.replication_coordinator.record(
            self.durability_backend.as_ref(),
            DurableOp::Delete {
                uri: uri.to_string(),
                key: key.to_vec(),
                txn_id,
            },
            DurabilityGuarantee::Buffered,
        )
    }
}
