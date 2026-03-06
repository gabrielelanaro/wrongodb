use std::fmt::Debug;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::core::errors::WrongoDBError;
use crate::storage::wal::GlobalWal;
use crate::txn::TxnId;

pub(crate) trait RecoveryUnit: Send + Sync + Debug {
    fn record_put(
        &self,
        store_name: &str,
        key: &[u8],
        value: &[u8],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError>;

    fn record_delete(
        &self,
        store_name: &str,
        key: &[u8],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError>;

    fn record_commit(&self, txn_id: TxnId, commit_ts: TxnId) -> Result<(), WrongoDBError>;

    fn record_abort(&self, txn_id: TxnId) -> Result<(), WrongoDBError>;
}

#[derive(Debug, Default)]
pub(crate) struct NoopRecoveryUnit;

impl RecoveryUnit for NoopRecoveryUnit {
    fn record_put(
        &self,
        _store_name: &str,
        _key: &[u8],
        _value: &[u8],
        _txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        Ok(())
    }

    fn record_delete(
        &self,
        _store_name: &str,
        _key: &[u8],
        _txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        Ok(())
    }

    fn record_commit(&self, _txn_id: TxnId, _commit_ts: TxnId) -> Result<(), WrongoDBError> {
        Ok(())
    }

    fn record_abort(&self, _txn_id: TxnId) -> Result<(), WrongoDBError> {
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct WalRecoveryUnit {
    wal: Arc<Mutex<GlobalWal>>,
}

impl WalRecoveryUnit {
    pub(crate) fn new(wal: Arc<Mutex<GlobalWal>>) -> Self {
        Self { wal }
    }
}

impl RecoveryUnit for WalRecoveryUnit {
    fn record_put(
        &self,
        store_name: &str,
        key: &[u8],
        value: &[u8],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        self.wal.lock().log_put(store_name, key, value, txn_id, 0)?;
        Ok(())
    }

    fn record_delete(
        &self,
        store_name: &str,
        key: &[u8],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        self.wal.lock().log_delete(store_name, key, txn_id, 0)?;
        Ok(())
    }

    fn record_commit(&self, txn_id: TxnId, commit_ts: TxnId) -> Result<(), WrongoDBError> {
        let mut wal = self.wal.lock();
        wal.log_txn_commit(txn_id, commit_ts, 0)?;
        wal.sync()?;
        Ok(())
    }

    fn record_abort(&self, txn_id: TxnId) -> Result<(), WrongoDBError> {
        self.wal.lock().log_txn_abort(txn_id, 0)?;
        Ok(())
    }
}
