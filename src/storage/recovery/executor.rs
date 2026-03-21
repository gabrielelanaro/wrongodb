use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use crate::core::errors::StorageError;
use crate::storage::btree::BTreeCursor;
use crate::storage::command::StorageCommand;
use crate::storage::handle_cache::HandleCache;
use crate::storage::metadata_catalog::MetadataCatalog;
use crate::storage::table::{
    apply_delete_autocommit, apply_delete_in_txn, apply_put_autocommit, apply_put_in_txn,
    checkpoint_store, open_or_create_btree,
};
use crate::txn::{Transaction, TransactionManager, TxnId, TXN_NONE};
use crate::WrongoDBError;

pub(crate) trait RecoveryApplier: Send + Sync {
    fn apply(&self, command: StorageCommand) -> Result<(), WrongoDBError>;
    fn checkpoint_open_stores(&self) -> Result<(), WrongoDBError>;
}

#[derive(Debug)]
pub(crate) struct RecoveryExecutor {
    base_path: PathBuf,
    metadata_catalog: Arc<MetadataCatalog>,
    store_handles: Arc<HandleCache<String, RwLock<BTreeCursor>>>,
    transaction_manager: Arc<TransactionManager>,
    in_flight_txns: Mutex<HashMap<TxnId, Transaction>>,
}

impl RecoveryExecutor {
    pub(crate) fn new(
        base_path: PathBuf,
        metadata_catalog: Arc<MetadataCatalog>,
        store_handles: Arc<HandleCache<String, RwLock<BTreeCursor>>>,
        transaction_manager: Arc<TransactionManager>,
    ) -> Self {
        Self {
            base_path,
            metadata_catalog,
            store_handles,
            transaction_manager,
            in_flight_txns: Mutex::new(HashMap::new()),
        }
    }

    fn open_store_for_uri(
        &self,
        uri: &str,
        txn_id: TxnId,
    ) -> Result<Arc<RwLock<BTreeCursor>>, WrongoDBError> {
        let source = self
            .metadata_catalog
            .lookup_source_for_txn(uri, txn_id)?
            .ok_or_else(|| StorageError(format!("unknown URI during apply: {uri}")))?;
        self.store_handles.get_or_try_insert_with(source, |source| {
            let path = self.base_path.join(source);
            Ok(RwLock::new(open_or_create_btree(path)?))
        })
    }
}

impl RecoveryApplier for RecoveryExecutor {
    fn apply(&self, command: StorageCommand) -> Result<(), WrongoDBError> {
        match command {
            StorageCommand::Put {
                uri,
                key,
                value,
                txn_id,
            } => {
                let store = self.open_store_for_uri(&uri, txn_id)?;
                if txn_id == TXN_NONE {
                    apply_put_autocommit(
                        &mut store.write(),
                        self.transaction_manager.as_ref(),
                        &key,
                        &value,
                    )?;
                } else {
                    let mut txns = self.in_flight_txns.lock();
                    let txn = txns
                        .entry(txn_id)
                        .or_insert_with(|| self.transaction_manager.begin_replay_txn(txn_id));
                    apply_put_in_txn(&mut store.write(), &key, &value, txn)?;
                }
            }
            StorageCommand::Delete { uri, key, txn_id } => {
                let store = self.open_store_for_uri(&uri, txn_id)?;
                if txn_id == TXN_NONE {
                    let _ = apply_delete_autocommit(
                        &mut store.write(),
                        self.transaction_manager.as_ref(),
                        &key,
                    )?;
                } else {
                    let mut txns = self.in_flight_txns.lock();
                    let txn = txns
                        .entry(txn_id)
                        .or_insert_with(|| self.transaction_manager.begin_replay_txn(txn_id));
                    let _ = apply_delete_in_txn(&mut store.write(), &key, txn)?;
                }
            }
            StorageCommand::TxnCommit { txn_id, .. } => {
                if txn_id != TXN_NONE {
                    if let Some(mut txn) = self.in_flight_txns.lock().remove(&txn_id) {
                        self.transaction_manager.commit_txn_state(&mut txn)?;
                    }
                }
            }
            StorageCommand::TxnAbort { txn_id } => {
                if txn_id != TXN_NONE {
                    if let Some(mut txn) = self.in_flight_txns.lock().remove(&txn_id) {
                        self.transaction_manager.abort_txn_state(&mut txn)?;
                    }
                }
            }
            StorageCommand::Checkpoint => {}
        }
        Ok(())
    }

    fn checkpoint_open_stores(&self) -> Result<(), WrongoDBError> {
        for store in self.store_handles.all_handles() {
            checkpoint_store(&mut store.write(), self.transaction_manager.as_ref())?;
        }
        Ok(())
    }
}
