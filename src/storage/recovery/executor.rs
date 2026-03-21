use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::core::errors::StorageError;
use crate::storage::btree::BTreeCursor;
use crate::storage::handle_cache::HandleCache;
use crate::storage::metadata_catalog::MetadataCatalog;
use crate::storage::table::{checkpoint_store, open_or_create_btree};
use crate::txn::{GlobalTxnState, Transaction, TxnId, TxnLogOp};
use crate::WrongoDBError;

pub(crate) trait RecoveryApplier: Send + Sync {
    fn apply_committed_transaction(
        &self,
        txn_id: TxnId,
        ops: &[TxnLogOp],
    ) -> Result<(), WrongoDBError>;
    fn checkpoint_open_stores(&self) -> Result<(), WrongoDBError>;
}

#[derive(Debug)]
pub(crate) struct RecoveryExecutor {
    base_path: PathBuf,
    metadata_catalog: Arc<MetadataCatalog>,
    store_handles: Arc<HandleCache<String, RwLock<BTreeCursor>>>,
    global_txn: Arc<GlobalTxnState>,
}

impl RecoveryExecutor {
    pub(crate) fn new(
        base_path: PathBuf,
        metadata_catalog: Arc<MetadataCatalog>,
        store_handles: Arc<HandleCache<String, RwLock<BTreeCursor>>>,
        global_txn: Arc<GlobalTxnState>,
    ) -> Self {
        Self {
            base_path,
            metadata_catalog,
            store_handles,
            global_txn,
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
    fn apply_committed_transaction(
        &self,
        txn_id: TxnId,
        ops: &[TxnLogOp],
    ) -> Result<(), WrongoDBError> {
        let mut txn = Transaction::replay(txn_id);

        for op in ops {
            match op {
                TxnLogOp::Put { uri, key, value } => {
                    let store = self.open_store_for_uri(uri, txn_id)?;
                    store.write().put(uri, key, value, &mut txn)?;
                }
                TxnLogOp::Delete { uri, key } => {
                    let store = self.open_store_for_uri(uri, txn_id)?;
                    let _ = store.write().delete(uri, key, &mut txn)?;
                }
            }
        }

        txn.commit(self.global_txn.as_ref())?;
        Ok(())
    }

    fn checkpoint_open_stores(&self) -> Result<(), WrongoDBError> {
        for store in self.store_handles.all_handles() {
            checkpoint_store(&mut store.write(), self.global_txn.as_ref())?;
        }
        Ok(())
    }
}
