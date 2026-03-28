use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::core::errors::StorageError;
use crate::storage::btree::BTreeCursor;
use crate::storage::handle_cache::HandleCache;
use crate::storage::log_manager::LogManager;
use crate::storage::reserved_store::{
    StoreId, FIRST_DYNAMIC_STORE_ID, METADATA_STORE_ID, METADATA_STORE_NAME,
};
use crate::storage::row::RowFormat;
use crate::storage::table::{
    get_version, open_or_create_btree, scan_range, IndexMetadata, TableMetadata,
};
use crate::txn::{GlobalTxnState, TXN_NONE};
use crate::WrongoDBError;

// ============================================================================
// Constants
// ============================================================================

pub(crate) const FILE_URI_PREFIX: &str = "file:";
pub(crate) const TABLE_URI_PREFIX: &str = "table:";
pub(crate) const INDEX_URI_PREFIX: &str = "index:";

// ============================================================================
// URI Helpers
// ============================================================================

pub(crate) fn file_uri(file_name: &str) -> String {
    format!("{FILE_URI_PREFIX}{file_name}")
}

pub(crate) fn table_uri(collection: &str) -> String {
    format!("{TABLE_URI_PREFIX}{collection}")
}

pub(crate) fn index_uri(collection: &str, name: &str) -> String {
    format!("{INDEX_URI_PREFIX}{collection}:{name}")
}

pub(crate) fn table_file_uri(collection: &str) -> String {
    file_uri(&table_store_name(collection))
}

pub(crate) fn index_file_uri(collection: &str, name: &str) -> String {
    file_uri(&index_store_name(collection, name))
}

pub(crate) fn file_name_from_uri(uri: &str) -> Result<&str, WrongoDBError> {
    uri.strip_prefix(FILE_URI_PREFIX)
        .filter(|name| !name.is_empty())
        .ok_or_else(|| StorageError(format!("invalid file URI: {uri}")).into())
}

fn table_store_name(collection: &str) -> String {
    format!("{collection}.main.wt")
}

fn index_store_name(collection: &str, name: &str) -> String {
    format!("{collection}.{name}.idx.wt")
}

// ============================================================================
// Metadata Types
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MetadataEntry {
    uri: String,
    source: Option<String>,
    store_id: Option<StoreId>,
    row_format: Option<RowFormat>,
    key_columns: Vec<String>,
    value_columns: Vec<String>,
    columns: Vec<String>,
}

impl MetadataEntry {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    pub(crate) fn file(file_name: &str, store_id: StoreId) -> Self {
        Self {
            uri: file_uri(file_name),
            source: None,
            store_id: Some(store_id),
            row_format: None,
            key_columns: Vec::new(),
            value_columns: Vec::new(),
            columns: Vec::new(),
        }
    }

    pub(crate) fn table(collection: &str, value_columns: Vec<String>) -> Self {
        Self {
            uri: table_uri(collection),
            source: Some(table_file_uri(collection)),
            store_id: None,
            row_format: Some(RowFormat::WtRowV1),
            key_columns: vec!["_id".to_string()],
            value_columns,
            columns: Vec::new(),
        }
    }

    pub(crate) fn index(collection: &str, name: &str, columns: Vec<String>) -> Self {
        Self {
            uri: index_uri(collection, name),
            source: Some(index_file_uri(collection, name)),
            store_id: None,
            row_format: None,
            key_columns: Vec::new(),
            value_columns: Vec::new(),
            columns,
        }
    }

    fn from_record(uri: String, record: MetadataRecord) -> Result<Self, WrongoDBError> {
        let entry = Self {
            uri,
            source: record.source,
            store_id: record.store_id,
            row_format: record.row_format,
            key_columns: record.key_columns,
            value_columns: record.value_columns,
            columns: record.columns,
        };
        entry.validate()?;
        Ok(entry)
    }

    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------

    pub(crate) fn uri(&self) -> &str {
        &self.uri
    }

    pub(crate) fn source_uri(&self) -> Result<&str, WrongoDBError> {
        self.source.as_deref().ok_or_else(|| {
            StorageError(format!(
                "metadata row does not reference a source URI: {}",
                self.uri
            ))
            .into()
        })
    }

    pub(crate) fn file_name(&self) -> Result<&str, WrongoDBError> {
        file_name_from_uri(&self.uri)
    }

    pub(crate) fn store_id(&self) -> Result<StoreId, WrongoDBError> {
        self.store_id.ok_or_else(|| {
            StorageError(format!(
                "metadata row does not own a store id: {}",
                self.uri
            ))
            .into()
        })
    }

    pub(crate) fn row_format(&self) -> Option<RowFormat> {
        self.row_format
    }

    pub(crate) fn key_columns(&self) -> &[String] {
        &self.key_columns
    }

    pub(crate) fn value_columns(&self) -> &[String] {
        &self.value_columns
    }

    pub(crate) fn columns(&self) -> &[String] {
        &self.columns
    }

    pub(crate) fn is_file(&self) -> bool {
        self.uri.starts_with(FILE_URI_PREFIX)
    }

    pub(crate) fn is_table(&self) -> bool {
        self.uri.starts_with(TABLE_URI_PREFIX)
    }

    pub(crate) fn is_index(&self) -> bool {
        self.uri.starts_with(INDEX_URI_PREFIX)
    }

    pub(crate) fn into_index_metadata(
        self,
        store_id: StoreId,
    ) -> Result<IndexMetadata, WrongoDBError> {
        if !self.is_index() {
            return Err(StorageError(format!("metadata row is not an index: {}", self.uri)).into());
        }

        if self.columns.is_empty() {
            return Err(StorageError(format!(
                "index metadata row is missing columns: {}",
                self.uri
            ))
            .into());
        }

        let name = index_name_from_uri(&self.uri)?.to_string();
        Ok(IndexMetadata::new(name, self.uri, store_id, self.columns))
    }

    pub(crate) fn into_table_metadata(
        self,
        store_id: StoreId,
        indexes: Vec<IndexMetadata>,
    ) -> Result<TableMetadata, WrongoDBError> {
        if !self.is_table() {
            return Err(StorageError(format!("metadata row is not a table: {}", self.uri)).into());
        }

        let row_format = self.row_format.ok_or_else(|| {
            StorageError(format!(
                "table metadata row is missing row_format: {}",
                self.uri
            ))
        })?;
        if self.key_columns.is_empty() {
            return Err(StorageError(format!(
                "table metadata row is missing key_columns: {}",
                self.uri
            ))
            .into());
        }

        Ok(TableMetadata::with_indexes(
            self.uri,
            store_id,
            row_format,
            self.key_columns,
            self.value_columns,
            indexes,
        ))
    }

    fn validate(&self) -> Result<(), WrongoDBError> {
        if self.is_file() {
            return self.validate_file();
        }
        if self.is_table() {
            return self.validate_table();
        }
        if self.is_index() {
            return self.validate_index();
        }

        Err(StorageError(format!("unsupported metadata URI: {}", self.uri)).into())
    }

    fn validate_file(&self) -> Result<(), WrongoDBError> {
        let store_id = self.store_id.ok_or_else(|| {
            StorageError(format!(
                "file metadata row is missing store_id: {}",
                self.uri
            ))
        })?;
        validate_metadata_store_id(store_id, &self.uri)?;
        file_name_from_uri(&self.uri)?;

        if self.source.is_some() {
            return Err(StorageError(format!(
                "file metadata row must not reference a source URI: {}",
                self.uri
            ))
            .into());
        }
        if self.row_format.is_some()
            || !self.key_columns.is_empty()
            || !self.value_columns.is_empty()
            || !self.columns.is_empty()
        {
            return Err(StorageError(format!(
                "file metadata row contains table/index fields: {}",
                self.uri
            ))
            .into());
        }

        Ok(())
    }

    fn validate_table(&self) -> Result<(), WrongoDBError> {
        let source = self.source.as_deref().ok_or_else(|| {
            StorageError(format!(
                "table metadata row is missing source URI: {}",
                self.uri
            ))
        })?;
        if !source.starts_with(FILE_URI_PREFIX) {
            return Err(StorageError(format!(
                "table metadata row {} has invalid source URI {}",
                self.uri, source
            ))
            .into());
        }
        file_name_from_uri(source)?;
        if self.store_id.is_some() {
            return Err(StorageError(format!(
                "table metadata row must not own a store id: {}",
                self.uri
            ))
            .into());
        }
        if self.row_format.is_none() {
            return Err(StorageError(format!(
                "table metadata row is missing row_format: {}",
                self.uri
            ))
            .into());
        }
        if self.key_columns.is_empty() {
            return Err(StorageError(format!(
                "table metadata row is missing key_columns: {}",
                self.uri
            ))
            .into());
        }
        if !self.columns.is_empty() {
            return Err(StorageError(format!(
                "table metadata row must not declare index columns: {}",
                self.uri
            ))
            .into());
        }

        Ok(())
    }

    fn validate_index(&self) -> Result<(), WrongoDBError> {
        let source = self.source.as_deref().ok_or_else(|| {
            StorageError(format!(
                "index metadata row is missing source URI: {}",
                self.uri
            ))
        })?;
        if !source.starts_with(FILE_URI_PREFIX) {
            return Err(StorageError(format!(
                "index metadata row {} has invalid source URI {}",
                self.uri, source
            ))
            .into());
        }
        file_name_from_uri(source)?;
        if self.store_id.is_some() {
            return Err(StorageError(format!(
                "index metadata row must not own a store id: {}",
                self.uri
            ))
            .into());
        }
        if self.row_format.is_some()
            || !self.key_columns.is_empty()
            || !self.value_columns.is_empty()
        {
            return Err(StorageError(format!(
                "index metadata row contains table fields: {}",
                self.uri
            ))
            .into());
        }
        if self.columns.is_empty() {
            return Err(StorageError(format!(
                "index metadata row is missing columns: {}",
                self.uri
            ))
            .into());
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MetadataRecord {
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    store_id: Option<StoreId>,
    #[serde(default)]
    row_format: Option<RowFormat>,
    #[serde(default)]
    key_columns: Vec<String>,
    #[serde(default)]
    value_columns: Vec<String>,
    #[serde(default)]
    columns: Vec<String>,
}

impl From<&MetadataEntry> for MetadataRecord {
    fn from(entry: &MetadataEntry) -> Self {
        Self {
            source: entry.source.clone(),
            store_id: entry.store_id,
            row_format: entry.row_format,
            key_columns: entry.key_columns.clone(),
            value_columns: entry.value_columns.clone(),
            columns: entry.columns.clone(),
        }
    }
}

// ============================================================================
// MetadataStore
// ============================================================================

/// Storage-facing catalog of logical URI to physical file-object mappings.
///
/// `MetadataStore` owns the reserved `metadata.wt` B-tree used by the storage
/// engine to persist file, table, and index metadata rows. `file:` rows own
/// physical store ids, while `table:` and `index:` rows reference a `file:`
/// source URI.
#[derive(Debug, Clone)]
pub(crate) struct MetadataStore {
    base_path: PathBuf,
    store_handles: Arc<HandleCache<String, RwLock<BTreeCursor>>>,
    global_txn: Arc<GlobalTxnState>,
    log_manager: Arc<LogManager>,
    next_store_id: Arc<AtomicU64>,
}

impl MetadataStore {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    pub(crate) fn new(
        base_path: PathBuf,
        store_handles: Arc<HandleCache<String, RwLock<BTreeCursor>>>,
        global_txn: Arc<GlobalTxnState>,
        log_manager: Arc<LogManager>,
    ) -> Result<Self, WrongoDBError> {
        let store = Self {
            base_path,
            store_handles,
            global_txn,
            log_manager,
            next_store_id: Arc::new(AtomicU64::new(FIRST_DYNAMIC_STORE_ID)),
        };
        Ok(store)
    }

    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------

    pub(crate) fn allocate_store_id(&self) -> StoreId {
        self.next_store_id.fetch_add(1, Ordering::SeqCst)
    }

    pub(crate) fn get(&self, uri: &str) -> Result<Option<MetadataEntry>, WrongoDBError> {
        let value = get_version(
            &mut self.open_metadata_store()?.write(),
            uri.as_bytes(),
            TXN_NONE,
        )?;

        value
            .map(|value| decode_entry(uri.as_bytes().to_vec(), value))
            .transpose()
    }

    pub(crate) fn insert(&self, entry: &MetadataEntry) -> Result<(), WrongoDBError> {
        if self.get(entry.uri())?.is_some() {
            return Err(StorageError(format!("duplicate metadata URI: {}", entry.uri())).into());
        }

        self.write_entry(entry.uri(), WriteOp::Put(encode_entry(entry)?))
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn update(&self, entry: &MetadataEntry) -> Result<(), WrongoDBError> {
        if self.get(entry.uri())?.is_none() {
            return Err(StorageError(format!("unknown metadata URI: {}", entry.uri())).into());
        }

        self.write_entry(entry.uri(), WriteOp::Put(encode_entry(entry)?))
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn remove(&self, uri: &str) -> Result<bool, WrongoDBError> {
        if self.get(uri)?.is_none() {
            return Ok(false);
        }

        self.write_entry(uri, WriteOp::Delete)?;
        Ok(true)
    }

    pub(crate) fn scan_prefix(&self, prefix: &str) -> Result<Vec<MetadataEntry>, WrongoDBError> {
        let metadata_store = self.open_metadata_store()?;
        let start = (!prefix.is_empty()).then(|| prefix.as_bytes().to_vec());
        let end = prefix_upper_bound(prefix);
        let rows = scan_range(
            &mut metadata_store.write(),
            start.as_deref(),
            end.as_deref(),
            TXN_NONE,
        )?;

        rows.into_iter()
            .map(|(key, value)| decode_entry(key, value))
            .collect()
    }

    pub(crate) fn all_store_names(&self) -> Result<Vec<String>, WrongoDBError> {
        let mut store_names = self
            .all_stores_with_id()?
            .into_iter()
            .map(|(_, store_name)| store_name)
            .collect::<Vec<_>>();
        store_names.sort();
        store_names.dedup();
        Ok(store_names)
    }

    pub(crate) fn all_stores_with_id(&self) -> Result<Vec<(StoreId, String)>, WrongoDBError> {
        let mut stores = self
            .scan_prefix(FILE_URI_PREFIX)?
            .into_iter()
            .map(|entry| Ok((entry.store_id()?, entry.file_name()?.to_string())))
            .collect::<Result<Vec<_>, WrongoDBError>>()?;
        stores.sort_by(|left, right| left.1.cmp(&right.1).then(left.0.cmp(&right.0)));
        stores.dedup();
        Ok(stores)
    }

    // ------------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------------

    fn write_entry(&self, uri: &str, op: WriteOp) -> Result<(), WrongoDBError> {
        let metadata_store = self.open_metadata_store()?;
        let mut txn = self.global_txn.begin_snapshot_txn();

        let result = {
            let mut btree = metadata_store.write();
            match op {
                WriteOp::Put(value) => {
                    btree.put(METADATA_STORE_ID, uri.as_bytes(), &value, &mut txn)
                }
                WriteOp::Delete => btree
                    .delete(METADATA_STORE_ID, uri.as_bytes(), &mut txn)
                    .map(|_| ()),
            }
        };

        if let Err(err) = result {
            let _ = txn.abort(self.global_txn.as_ref());
            return Err(err);
        }

        let commit_ts = txn.id();
        if let Err(err) =
            self.log_manager
                .log_transaction_commit(txn.id(), commit_ts, txn.log_ops())
        {
            let _ = txn.abort(self.global_txn.as_ref());
            return Err(err);
        }

        txn.commit(self.global_txn.as_ref())?;
        Ok(())
    }

    fn open_metadata_store(&self) -> Result<Arc<RwLock<BTreeCursor>>, WrongoDBError> {
        self.open_or_create_store(METADATA_STORE_NAME)
    }

    fn open_or_create_store(
        &self,
        store_name: &str,
    ) -> Result<Arc<RwLock<BTreeCursor>>, WrongoDBError> {
        self.store_handles
            .get_or_try_insert_with(store_name.to_string(), |store_name| {
                let path = self.base_path.join(store_name);
                Ok(RwLock::new(open_or_create_btree(path)?))
            })
    }
}

/// Reseeds the next dynamic store id from committed file metadata.
pub(crate) fn reseed_next_store_id_from_metadata(
    store: &MetadataStore,
) -> Result<(), WrongoDBError> {
    let next_store_id = store
        .scan_prefix(FILE_URI_PREFIX)?
        .into_iter()
        .map(|entry| entry.store_id())
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .max()
        .map(|max_store_id| max_store_id + 1)
        .unwrap_or(FIRST_DYNAMIC_STORE_ID);
    store.next_store_id.store(next_store_id, Ordering::SeqCst);
    Ok(())
}

#[derive(Debug, Clone)]
#[cfg_attr(not(test), allow(dead_code))]
enum WriteOp {
    Put(Vec<u8>),
    Delete,
}

// ============================================================================
// Encoding Helpers
// ============================================================================

fn encode_entry(entry: &MetadataEntry) -> Result<Vec<u8>, WrongoDBError> {
    Ok(bson::to_vec(&MetadataRecord::from(entry))?)
}

fn decode_entry(key: Vec<u8>, value: Vec<u8>) -> Result<MetadataEntry, WrongoDBError> {
    let uri = String::from_utf8(key)
        .map_err(|err| StorageError(format!("metadata row key is not valid UTF-8: {err}")))?;
    let record: MetadataRecord = bson::from_slice(&value)?;
    MetadataEntry::from_record(uri, record)
}

fn validate_metadata_store_id(store_id: StoreId, uri: &str) -> Result<(), WrongoDBError> {
    if store_id < FIRST_DYNAMIC_STORE_ID {
        return Err(StorageError(format!(
            "metadata row {uri} uses reserved store id {store_id}"
        ))
        .into());
    }
    Ok(())
}

fn index_name_from_uri(uri: &str) -> Result<&str, WrongoDBError> {
    let stripped = uri.strip_prefix(INDEX_URI_PREFIX).ok_or_else(|| {
        StorageError(format!(
            "index metadata lookup requires index: URI, got: {uri}"
        ))
    })?;
    stripped
        .split_once(':')
        .map(|(_, name)| name)
        .ok_or_else(|| StorageError(format!("invalid index URI: {uri}")).into())
}

fn prefix_upper_bound(prefix: &str) -> Option<Vec<u8>> {
    if prefix.is_empty() {
        return None;
    }

    let mut bound = prefix.as_bytes().to_vec();
    for idx in (0..bound.len()).rev() {
        if bound[idx] != u8::MAX {
            bound[idx] += 1;
            bound.truncate(idx + 1);
            return Some(bound);
        }
    }
    None
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parking_lot::RwLock;
    use tempfile::tempdir;

    use super::*;
    use crate::storage::btree::BTreeCursor;
    use crate::storage::handle_cache::HandleCache;

    struct MetadataFixture {
        _tmp: tempfile::TempDir,
        store: MetadataStore,
    }

    impl MetadataFixture {
        fn new() -> Self {
            let tmp = tempdir().unwrap();
            let store_handles = Arc::new(HandleCache::<String, RwLock<BTreeCursor>>::new());
            let global_txn = Arc::new(GlobalTxnState::new());
            let log_manager = Arc::new(LogManager::disabled());
            let store = MetadataStore::new(
                tmp.path().to_path_buf(),
                store_handles,
                global_txn,
                log_manager,
            )
            .unwrap();

            Self { _tmp: tmp, store }
        }
    }

    #[test]
    fn metadata_store_crud_roundtrip() {
        let fixture = MetadataFixture::new();
        let entry = MetadataEntry::file("users.main.wt", 2);
        fixture.store.insert(&entry).unwrap();

        assert_eq!(fixture.store.get(entry.uri()).unwrap(), Some(entry.clone()));

        let updated = MetadataEntry::index("users", "name_1", vec!["name".to_string()]);
        fixture.store.update(&updated).unwrap_err();
    }

    #[test]
    fn metadata_store_scans_by_prefix() {
        let fixture = MetadataFixture::new();
        fixture
            .store
            .insert(&MetadataEntry::file("users.main.wt", 2))
            .unwrap();
        fixture
            .store
            .insert(&MetadataEntry::table("users", vec!["name".to_string()]))
            .unwrap();
        fixture
            .store
            .insert(&MetadataEntry::file("users.email_1.idx.wt", 3))
            .unwrap();
        fixture
            .store
            .insert(&MetadataEntry::index(
                "users",
                "email_1",
                vec!["email".to_string()],
            ))
            .unwrap();

        let files = fixture.store.scan_prefix(FILE_URI_PREFIX).unwrap();
        let tables = fixture.store.scan_prefix(TABLE_URI_PREFIX).unwrap();
        let indexes = fixture.store.scan_prefix("index:users:").unwrap();

        assert_eq!(files.len(), 2);
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].uri(), "table:users");
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].uri(), "index:users:email_1");
    }

    #[test]
    fn metadata_store_remove_deletes_entry() {
        let fixture = MetadataFixture::new();
        let entry = MetadataEntry::file("users.main.wt", 2);
        fixture.store.insert(&entry).unwrap();

        assert!(fixture.store.remove(entry.uri()).unwrap());
        assert_eq!(fixture.store.get(entry.uri()).unwrap(), None);
        assert!(!fixture.store.remove(entry.uri()).unwrap());
    }

    #[test]
    fn metadata_store_persists_store_id_and_reseeds_allocator() {
        let tmp = tempdir().unwrap();
        let base_path = tmp.path().to_path_buf();
        let store_handles = Arc::new(HandleCache::<String, RwLock<BTreeCursor>>::new());
        let global_txn = Arc::new(GlobalTxnState::new());
        let log_manager = Arc::new(LogManager::disabled());
        let store = MetadataStore::new(
            base_path.clone(),
            store_handles.clone(),
            global_txn.clone(),
            log_manager.clone(),
        )
        .unwrap();

        store
            .insert(&MetadataEntry::file("users.main.wt", 7))
            .unwrap();

        let reopened =
            MetadataStore::new(base_path, store_handles, global_txn, log_manager).unwrap();
        assert_eq!(
            reopened
                .get("file:users.main.wt")
                .unwrap()
                .unwrap()
                .store_id()
                .unwrap(),
            7
        );
        reseed_next_store_id_from_metadata(&reopened).unwrap();
        assert_eq!(reopened.allocate_store_id(), 8);
    }

    #[test]
    fn metadata_entry_builds_index_metadata_with_store_id() {
        let metadata = MetadataEntry::index("users", "name_1", vec!["name".to_string()]);
        let index = metadata.into_index_metadata(9).unwrap();

        assert_eq!(index.name(), "name_1");
        assert_eq!(index.uri(), "index:users:name_1");
        assert_eq!(index.store_id(), 9);
        assert_eq!(index.columns(), &["name".to_string()]);
    }

    #[test]
    fn all_store_names_returns_only_file_rows() {
        let fixture = MetadataFixture::new();
        fixture
            .store
            .insert(&MetadataEntry::file("users.main.wt", 2))
            .unwrap();
        fixture
            .store
            .insert(&MetadataEntry::table("users", vec!["name".to_string()]))
            .unwrap();

        assert_eq!(
            fixture.store.all_store_names().unwrap(),
            vec!["users.main.wt".to_string()]
        );
    }
}
