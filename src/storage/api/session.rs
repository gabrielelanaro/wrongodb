use std::path::PathBuf;
use std::sync::{Arc, Weak};

use parking_lot::RwLock;

use crate::core::errors::StorageError;
use crate::durability::DurabilityBackend;
use crate::schema::SchemaCatalog;
use crate::storage::api::cursor::{Cursor, CursorWriteAccess};
use crate::storage::handle_cache::HandleCache;
use crate::storage::table::Table;
use crate::txn::{ActiveWriteUnit, RecoveryUnit, TransactionManager, TxnId, TXN_NONE};
use crate::WrongoDBError;

/// A request-scoped execution context over shared connection infrastructure.
///
/// `Connection` owns long-lived shared components (storage handles, schema
/// metadata, global transaction state, and durability machinery). `Session`
/// exists to own mutable per-request state that must not be global.
///
/// `Session` is where callers:
/// - open cursors
/// - start transactions
/// - trigger checkpoint
///
/// It intentionally does not own document write orchestration or the replicated
/// write path. Those live in higher internal layers.
///
/// In practice this means `Session` answers "what can this request do right
/// now?" while `Connection` answers "what engine are we attached to?".
pub struct Session {
    base_path: PathBuf,
    table_handles: Arc<HandleCache<String, RwLock<Table>>>,
    schema_catalog: Arc<SchemaCatalog>,
    transaction_manager: Arc<TransactionManager>,
    durability_backend: Arc<DurabilityBackend>,
    recovery_unit: Arc<dyn RecoveryUnit>,
    active_txn: Option<ActiveWriteUnit>,
}

impl Session {
    pub(crate) fn new(
        base_path: PathBuf,
        table_handles: Arc<HandleCache<String, RwLock<Table>>>,
        schema_catalog: Arc<SchemaCatalog>,
        transaction_manager: Arc<TransactionManager>,
        durability_backend: Arc<DurabilityBackend>,
    ) -> Self {
        let recovery_unit = durability_backend.new_recovery_unit();
        Self {
            base_path,
            table_handles,
            schema_catalog,
            transaction_manager,
            recovery_unit,
            durability_backend,
            active_txn: None,
        }
    }

    /// Ensure the primary store for `table:<collection>` exists.
    ///
    /// This method exists so callers can bootstrap a table through the same
    /// session object they use to open cursors, instead of forcing schema
    /// creation through the higher-level document write path.
    ///
    /// Only `table:...` URIs are supported publicly. Index creation stays
    /// internal because it is not a single-store operation: it has to update
    /// schema metadata and backfill index entries from existing collection
    /// data.
    pub fn create(&mut self, uri: &str) -> Result<(), WrongoDBError> {
        if let Some(collection) = uri.strip_prefix("table:") {
            let store_name = self.schema_catalog.primary_store_name(collection);
            let _ = self
                .table_handles
                .get_or_try_insert_with(store_name, |store_name| {
                    let path = self.base_path.join(store_name);
                    Ok(RwLock::new(Table::open_or_create_store(
                        path,
                        self.transaction_manager.clone(),
                    )?))
                })?;
            return Ok(());
        }

        Err(WrongoDBError::Storage(StorageError(format!(
            "unsupported URI for Session::create: {uri}; only table:... is supported"
        ))))
    }

    /// Open a non-transactional cursor bound to `TXN_NONE`.
    ///
    /// This method intentionally has the same name as
    /// [`WriteUnitOfWork::open_cursor`]. The duplication is deliberate:
    /// transaction scope is expressed by the receiver instead of by exposing
    /// raw transaction ids in the public cursor API.
    ///
    /// `Session::open_cursor` means "open a cursor outside any transaction".
    /// [`WriteUnitOfWork::open_cursor`] means "open the same kind of cursor,
    /// but bound to the active transaction".
    ///
    /// The method lives on `Session` because non-transactional access is still
    /// a first-class storage API and should not require creating a transaction
    /// boundary object just to read or perform local writes.
    pub fn open_cursor(&self, uri: &str) -> Result<Cursor, WrongoDBError> {
        let store_name = self.schema_catalog.resolve_uri(uri)?;
        self.open_store_cursor_with_access(&store_name, TXN_NONE, CursorWriteAccess::ReadWrite)
    }

    /// Start a transactional write unit of work.
    ///
    /// This exists as a separate step so commit/abort ordering, local WAL
    /// markers, and transaction-bound cursor opening all flow through a single
    /// RAII boundary instead of being spread across the public API.
    ///
    /// The public API deliberately makes transaction scope explicit here rather
    /// than letting cursor methods take raw transaction ids.
    pub fn transaction(&mut self) -> Result<WriteUnitOfWork<'_>, WrongoDBError> {
        if self.active_txn.is_some() {
            return Err(WrongoDBError::TransactionAlreadyActive);
        }

        let txn = self.transaction_manager.begin_snapshot_txn();
        self.recovery_unit.begin_unit_of_work()?;
        self.active_txn = Some(ActiveWriteUnit::new(txn));
        Ok(WriteUnitOfWork::new(self))
    }

    /// Reconcile and checkpoint all known stores.
    ///
    /// Checkpoint is session-level because it coordinates many stores and, when
    /// durability is enabled, emits the matching durability marker only after
    /// the storage-level reconciliation pass has finished.
    ///
    /// It lives on `Session` instead of `Cursor` or `Table` because checkpoint
    /// is not a one-store concern.
    pub fn checkpoint(&mut self) -> Result<(), WrongoDBError> {
        for store_name in self.schema_catalog.all_store_names()? {
            let table = self
                .table_handles
                .get_or_try_insert_with(store_name, |store_name| {
                    let path = self.base_path.join(store_name);
                    Ok(RwLock::new(Table::open_or_create_store(
                        path,
                        self.transaction_manager.clone(),
                    )?))
                })?;
            table.write().checkpoint_store()?;
        }

        if self.transaction_manager.has_active_transactions()
            || !self.durability_backend.is_enabled()
        {
            return Ok(());
        }

        self.durability_backend.record(
            crate::durability::DurableOp::Checkpoint,
            crate::durability::DurabilityGuarantee::Sync,
        )?;
        self.durability_backend.truncate_to_checkpoint()
    }

    fn open_store_cursor_with_access(
        &self,
        store_name: &str,
        bound_txn_id: TxnId,
        write_access: CursorWriteAccess,
    ) -> Result<Cursor, WrongoDBError> {
        let table =
            self.table_handles
                .get_or_try_insert_with(store_name.to_string(), |store_name| {
                    let path = self.base_path.join(store_name);
                    Ok(RwLock::new(Table::open_or_create_store(
                        path,
                        self.transaction_manager.clone(),
                    )?))
                })?;
        let active_txn = self.bound_transaction_handle(bound_txn_id)?;
        Ok(Cursor::new(
            table,
            store_name.to_string(),
            bound_txn_id,
            active_txn,
            self.recovery_unit.clone(),
            write_access,
        ))
    }

    fn bound_transaction_handle(
        &self,
        bound_txn_id: TxnId,
    ) -> Result<Option<Weak<parking_lot::Mutex<crate::txn::Transaction>>>, WrongoDBError> {
        if bound_txn_id == TXN_NONE {
            return Ok(None);
        }

        let active = self.active_txn.as_ref().ok_or_else(|| {
            StorageError(format!(
                "transaction cursor requested for txn {bound_txn_id}, but no transaction is active"
            ))
        })?;
        Ok(Some(Arc::downgrade(&active.txn_handle())))
    }
}

/// Public handle for one active transaction started from [`Session::transaction`].
///
/// This type exists so the public API has an explicit transaction scope:
/// while a write unit of work is alive, callers open cursors from it and those
/// cursors are automatically bound to the active transaction.
///
/// That is why this type is separate from [`Session`]. It keeps commit/abort
/// ownership and transaction-bound cursor opening behind a single object,
/// instead of exposing raw transaction ids or letting callers keep reaching
/// back into `Session` during a transaction.
///
/// The type is intentionally narrow: it is the transaction boundary, not a
/// second general-purpose session object.
pub struct WriteUnitOfWork<'a> {
    session: &'a mut Session,
    committed: bool,
}

impl<'a> WriteUnitOfWork<'a> {
    /// Open a cursor bound to this transaction.
    ///
    /// This duplicates [`Session::open_cursor`] by design. The reason is API
    /// clarity: a caller should choose transaction scope by opening the cursor
    /// from the transaction boundary, not by reaching back into `Session` and
    /// not by threading transaction ids through every cursor method.
    ///
    /// The duplication is about binding, not behavior: both methods open the
    /// same kind of cursor, but they bind it to different transaction scopes.
    pub fn open_cursor(&mut self, uri: &str) -> Result<Cursor, WrongoDBError> {
        let store_name = self.session.schema_catalog.resolve_uri(uri)?;
        self.session.open_store_cursor_with_access(
            &store_name,
            self.txn_id(),
            CursorWriteAccess::ReadWrite,
        )
    }

    /// Commit the transaction and consume this write unit of work.
    ///
    /// Consuming `self` makes the transaction boundary single-use and avoids
    /// partially committed public states. This is also where durability ordering
    /// is enforced before local transaction state is advanced.
    ///
    /// The method lives here, not on `Session`, because commit is part of the
    /// transaction boundary's responsibility.
    pub fn commit(mut self) -> Result<(), WrongoDBError> {
        let Some(active) = self.session.active_txn.as_ref() else {
            self.committed = true;
            return Ok(());
        };
        let txn_id = active.txn_id();
        let txn_handle = active.txn_handle();

        self.session
            .recovery_unit
            .commit_unit_of_work(txn_id, txn_id)?;

        {
            let mut txn = txn_handle.lock();
            self.session
                .transaction_manager
                .commit_txn_state(&mut txn)?;
        }

        self.session.active_txn = None;
        self.committed = true;
        Ok(())
    }

    /// Abort the transaction and consume this write unit of work.
    ///
    /// Like [`commit`](Self::commit), this consumes the boundary so rollback is
    /// a terminal operation. The method exists separately from `Drop` so callers
    /// can observe an explicit abort result instead of relying on best-effort
    /// cleanup.
    ///
    /// It exists for the same reason as [`commit`](Self::commit): abort belongs
    /// to the transaction boundary, not to the general session object.
    pub fn abort(mut self) -> Result<(), WrongoDBError> {
        let Some(active) = self.session.active_txn.as_ref() else {
            self.committed = true;
            return Ok(());
        };
        let txn_id = active.txn_id();
        let txn_handle = active.txn_handle();

        self.session.recovery_unit.abort_unit_of_work(txn_id)?;

        {
            let mut txn = txn_handle.lock();
            self.session.transaction_manager.abort_txn_state(&mut txn)?;
        }

        self.session.active_txn = None;
        self.committed = true;
        Ok(())
    }

    fn new(session: &'a mut Session) -> Self {
        Self {
            session,
            committed: false,
        }
    }

    pub(crate) fn txn_id(&self) -> TxnId {
        self.session
            .active_txn
            .as_ref()
            .expect("transaction should exist")
            .txn_id()
    }

    pub(crate) fn open_store_cursor_by_name(
        &mut self,
        store_name: &str,
    ) -> Result<Cursor, WrongoDBError> {
        self.session.open_store_cursor_with_access(
            store_name,
            self.txn_id(),
            CursorWriteAccess::ReadWrite,
        )
    }
}

impl<'a> Drop for WriteUnitOfWork<'a> {
    fn drop(&mut self) {
        if self.committed {
            return;
        }

        if let Some(active) = self.session.active_txn.take() {
            let txn_id = active.txn_id();
            let txn_handle = active.txn_handle();
            let _ = self.session.recovery_unit.abort_unit_of_work(txn_id);
            let mut txn = txn_handle.lock();
            let _ = self.session.transaction_manager.abort_txn_state(&mut txn);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use tempfile::tempdir;

    use super::*;
    use crate::durability::{DurabilityBackend, StoreCommandApplier};
    use crate::recovery::recover_from_wal;
    use crate::schema::SchemaCatalog;
    use crate::storage::handle_cache::HandleCache;
    use crate::storage::table::Table;
    use crate::storage::wal::{GlobalWal, WalFileReader, WalReader, WalRecord};
    use crate::txn::{GlobalTxnState, TransactionManager};

    const TEST_URI: &str = "table:items";
    const TEST_STORE: &str = "items.main.wt";
    const TEST_KEY: &[u8] = b"k1";
    const TEST_VALUE: &[u8] = b"v1";

    struct SessionTestFixture {
        session: Session,
    }

    impl SessionTestFixture {
        fn with_backend(backend: DurabilityBackend) -> Self {
            let dir = tempdir().unwrap();
            let base_path = dir.path().to_path_buf();
            std::mem::forget(dir);
            Self::build(base_path, Arc::new(backend))
        }

        fn open_local_wal<P: AsRef<Path>>(path: P) -> Self {
            let base_path = path.as_ref().to_path_buf();
            std::fs::create_dir_all(&base_path).unwrap();
            let transaction_manager =
                Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())));
            let table_handles = Arc::new(HandleCache::<String, RwLock<Table>>::new());
            let applier = Arc::new(StoreCommandApplier::new(
                base_path.clone(),
                table_handles.clone(),
                transaction_manager,
            ));
            recover_existing_wal_if_present(&base_path, applier.clone());
            let backend = Arc::new(DurabilityBackend::open(&base_path, true).unwrap());
            Self::build(base_path, backend)
        }

        fn into_session(self) -> Session {
            self.session
        }

        fn build(base_path: PathBuf, backend: Arc<DurabilityBackend>) -> Self {
            let transaction_manager =
                Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())));
            let table_handles = Arc::new(HandleCache::<String, RwLock<Table>>::new());
            let schema_catalog = Arc::new(SchemaCatalog::new(base_path.clone()));
            let session = Session::new(
                base_path,
                table_handles,
                schema_catalog,
                transaction_manager,
                backend,
            );

            Self { session }
        }
    }

    fn recover_existing_wal_if_present(base_path: &Path, applier: Arc<StoreCommandApplier>) {
        let wal_path = GlobalWal::path_for_db(base_path);
        if !wal_path.exists() {
            return;
        }

        let reader = WalFileReader::open(&wal_path).unwrap();
        recover_from_wal(applier, reader).unwrap();
    }

    fn read_wal_records(db_dir: &std::path::Path) -> Vec<WalRecord> {
        let wal_path = db_dir.join("global.wal");
        let mut reader = WalFileReader::open(&wal_path).unwrap();
        let mut records = Vec::new();
        while let Some((_header, record)) = reader.read_record().unwrap() {
            records.push(record);
        }
        records
    }

    fn insert_in_write_unit(
        write_unit: &mut WriteUnitOfWork<'_>,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), WrongoDBError> {
        let mut cursor = write_unit.open_cursor(TEST_URI)?;
        cursor.insert(key, value)
    }

    #[test]
    fn create_table_allows_non_transactional_cursor_access() {
        let mut session =
            SessionTestFixture::with_backend(DurabilityBackend::Disabled).into_session();
        session.create(TEST_URI).unwrap();

        let mut cursor = session.open_cursor(TEST_URI).unwrap();
        cursor.insert(TEST_KEY, TEST_VALUE).unwrap();

        assert_eq!(cursor.get(TEST_KEY).unwrap(), Some(TEST_VALUE.to_vec()));
    }

    #[test]
    fn create_rejects_index_uris() {
        let mut session =
            SessionTestFixture::with_backend(DurabilityBackend::Disabled).into_session();

        let err = session.create("index:test:name").unwrap_err();
        assert!(err.to_string().contains("only table:... is supported"));
    }

    #[test]
    fn transaction_cursor_reads_its_uncommitted_write() {
        let mut session =
            SessionTestFixture::with_backend(DurabilityBackend::Disabled).into_session();
        session.create(TEST_URI).unwrap();

        let mut txn = session.transaction().unwrap();
        insert_in_write_unit(&mut txn, TEST_KEY, TEST_VALUE).unwrap();

        let mut cursor = txn.open_cursor(TEST_URI).unwrap();
        assert_eq!(cursor.get(TEST_KEY).unwrap(), Some(TEST_VALUE.to_vec()));
    }

    #[test]
    fn local_mode_commit_makes_transactional_cursor_write_visible() {
        let mut session =
            SessionTestFixture::with_backend(DurabilityBackend::Disabled).into_session();
        session.create(TEST_URI).unwrap();

        let mut txn = session.transaction().unwrap();
        insert_in_write_unit(&mut txn, TEST_KEY, TEST_VALUE).unwrap();
        txn.commit().unwrap();

        let mut cursor = session.open_cursor(TEST_URI).unwrap();
        assert_eq!(cursor.get(TEST_KEY).unwrap(), Some(TEST_VALUE.to_vec()));
    }

    #[test]
    fn local_mode_abort_discards_transactional_cursor_write() {
        let mut session =
            SessionTestFixture::with_backend(DurabilityBackend::Disabled).into_session();
        session.create(TEST_URI).unwrap();

        let mut txn = session.transaction().unwrap();
        insert_in_write_unit(&mut txn, TEST_KEY, TEST_VALUE).unwrap();
        txn.abort().unwrap();

        let mut cursor = session.open_cursor(TEST_URI).unwrap();
        assert_eq!(cursor.get(TEST_KEY).unwrap(), None);
    }

    #[test]
    fn dropped_write_unit_discards_transactional_cursor_write() {
        let mut session =
            SessionTestFixture::with_backend(DurabilityBackend::Disabled).into_session();
        session.create(TEST_URI).unwrap();

        {
            let mut txn = session.transaction().unwrap();
            insert_in_write_unit(&mut txn, TEST_KEY, TEST_VALUE).unwrap();
        }

        let mut cursor = session.open_cursor(TEST_URI).unwrap();
        assert_eq!(cursor.get(TEST_KEY).unwrap(), None);
    }

    #[test]
    fn checkpoint_skips_truncate_when_transaction_active() {
        let dir = tempdir().unwrap();
        let base_path = dir.path().to_path_buf();
        let transaction_manager =
            Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())));
        let table_handles = Arc::new(HandleCache::<String, RwLock<Table>>::new());
        let backend = Arc::new(DurabilityBackend::open(&base_path, true).unwrap());
        let schema_catalog = Arc::new(SchemaCatalog::new(base_path.clone()));
        let mut session = Session::new(
            base_path.clone(),
            table_handles.clone(),
            schema_catalog.clone(),
            transaction_manager.clone(),
            backend.clone(),
        );
        let mut checkpoint_session = Session::new(
            base_path,
            table_handles,
            schema_catalog,
            transaction_manager,
            backend,
        );
        session.create(TEST_URI).unwrap();

        let mut txn = session.transaction().unwrap();
        insert_in_write_unit(&mut txn, TEST_KEY, TEST_VALUE).unwrap();

        let wal_path = dir.path().join("global.wal");
        let before = std::fs::metadata(&wal_path).unwrap().len();
        checkpoint_session.checkpoint().unwrap();
        let after = std::fs::metadata(&wal_path).unwrap().len();

        assert!(after >= before);
        let records = read_wal_records(dir.path());
        assert!(records
            .iter()
            .all(|record| !matches!(record, WalRecord::Checkpoint)));
    }

    #[test]
    fn checkpoint_truncates_when_no_active_transactions() {
        let dir = tempdir().unwrap();
        let mut session = SessionTestFixture::open_local_wal(dir.path()).into_session();
        session.create(TEST_URI).unwrap();

        let mut txn = session.transaction().unwrap();
        insert_in_write_unit(&mut txn, TEST_KEY, TEST_VALUE).unwrap();
        txn.commit().unwrap();

        let wal_path = dir.path().join("global.wal");
        session.checkpoint().unwrap();
        let records = read_wal_records(dir.path());
        assert!(records.is_empty());

        let after = std::fs::metadata(&wal_path).unwrap().len();
        assert!(after <= 512);
    }

    #[test]
    fn local_wal_mode_records_transactional_cursor_write_and_commit_markers() {
        let dir = tempdir().unwrap();
        let mut session = SessionTestFixture::open_local_wal(dir.path()).into_session();
        session.create(TEST_URI).unwrap();

        let mut txn = session.transaction().unwrap();
        let txn_id = txn.txn_id();
        insert_in_write_unit(&mut txn, TEST_KEY, TEST_VALUE).unwrap();
        txn.commit().unwrap();

        let records = read_wal_records(dir.path());
        assert!(records.iter().any(|record| matches!(
            record,
            WalRecord::Put {
                store_name,
                key,
                value,
                txn_id: record_txn_id,
            } if store_name == TEST_STORE
                && key == TEST_KEY
                && value == TEST_VALUE
                && *record_txn_id == txn_id
        )));
        assert!(records.iter().any(|record| matches!(
            record,
            WalRecord::TxnCommit {
                txn_id: record_txn_id,
                commit_ts,
            } if *record_txn_id == txn_id && *commit_ts == txn_id
        )));
    }
}
