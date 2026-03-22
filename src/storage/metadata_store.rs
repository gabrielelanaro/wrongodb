use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::core::errors::StorageError;
use crate::storage::api::Session;
use crate::storage::btree::BTreeCursor;
use crate::storage::handle_cache::HandleCache;
use crate::storage::reserved_store::{METADATA_STORE_NAME, METADATA_URI};
use crate::storage::table::{
    get_version, open_or_create_btree, scan_range, IndexMetadata, TableMetadata,
};
use crate::txn::{TxnId, TXN_NONE};
use crate::WrongoDBError;

// ============================================================================
// Constants
// ============================================================================

pub(crate) const TABLE_URI_PREFIX: &str = "table:";
pub(crate) const INDEX_URI_PREFIX: &str = "index:";

// ============================================================================
// URI Helpers
// ============================================================================

pub(crate) fn table_uri(collection: &str) -> String {
    format!("{TABLE_URI_PREFIX}{collection}")
}

pub(crate) fn index_uri(collection: &str, name: &str) -> String {
    format!("{INDEX_URI_PREFIX}{collection}:{name}")
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
    source: String,
    columns: Vec<String>,
}

impl MetadataEntry {
    fn table(collection: &str) -> Self {
        Self {
            uri: table_uri(collection),
            source: table_store_name(collection),
            columns: Vec::new(),
        }
    }

    fn index(collection: &str, name: &str, columns: Vec<String>) -> Self {
        Self {
            uri: index_uri(collection, name),
            source: index_store_name(collection, name),
            columns,
        }
    }

    pub(crate) fn uri(&self) -> &str {
        &self.uri
    }

    fn source(&self) -> &str {
        &self.source
    }

    fn from_record(uri: String, record: MetadataRecord) -> Self {
        Self {
            uri,
            source: record.source,
            columns: record.columns,
        }
    }

    fn is_table(&self) -> bool {
        self.uri.starts_with(TABLE_URI_PREFIX)
    }

    fn is_index(&self) -> bool {
        self.uri.starts_with(INDEX_URI_PREFIX)
    }

    fn into_index_metadata(self) -> Result<IndexMetadata, WrongoDBError> {
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
        Ok(IndexMetadata::new(name, self.uri, self.columns))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MetadataRecord {
    source: String,
    #[serde(default)]
    columns: Vec<String>,
}

impl From<&MetadataEntry> for MetadataRecord {
    fn from(entry: &MetadataEntry) -> Self {
        Self {
            source: entry.source.clone(),
            columns: entry.columns.clone(),
        }
    }
}

// ============================================================================
// MetadataStore
// ============================================================================

/// Storage-facing catalog of logical URI to physical store mappings.
///
/// `MetadataStore` owns the reserved `metadata.wt` B-tree used by the storage
/// engine to resolve `table:` and `index:` URIs into backing `.wt` files.
#[derive(Debug, Clone)]
pub(crate) struct MetadataStore {
    base_path: PathBuf,
    store_handles: Arc<HandleCache<String, RwLock<BTreeCursor>>>,
}

impl MetadataStore {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    /// Creates the storage metadata service rooted at `metadata.wt`.
    ///
    /// The store shares the connection-wide handle cache so metadata lookups
    /// and ordinary table/index opens observe the same underlying B-tree
    /// instances.
    pub(crate) fn new(
        base_path: PathBuf,
        store_handles: Arc<HandleCache<String, RwLock<BTreeCursor>>>,
    ) -> Self {
        Self {
            base_path,
            store_handles,
        }
    }

    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------

    #[cfg(test)]
    pub(crate) fn lookup_store_name(&self, uri: &str) -> Result<Option<String>, WrongoDBError> {
        self.lookup_store_name_for_txn(uri, TXN_NONE)
    }

    /// Resolves a logical storage URI to the backing `.wt` file visible to
    /// `txn_id`.
    ///
    /// `metadata:` is bootstrapped outside the metadata rows themselves, so it
    /// is the only URI handled without consulting `metadata.wt`.
    pub(crate) fn lookup_store_name_for_txn(
        &self,
        uri: &str,
        txn_id: TxnId,
    ) -> Result<Option<String>, WrongoDBError> {
        if uri == METADATA_URI {
            return Ok(Some(METADATA_STORE_NAME.to_string()));
        }

        Ok(self
            .lookup_entry_for_txn(uri, txn_id)?
            .map(|entry| entry.source))
    }

    /// Ensures that the table URI for `collection` exists in `metadata.wt`.
    ///
    /// Returns the canonical `table:` URI regardless of whether the row was
    /// inserted in this transaction or already existed.
    pub(crate) fn ensure_table_uri_in_transaction(
        &self,
        session: &mut Session,
        collection: &str,
    ) -> Result<String, WrongoDBError> {
        let entry = MetadataEntry::table(collection);
        let uri = entry.uri.clone();
        let _created = self.register_entry_if_missing(session, entry)?;
        Ok(uri)
    }

    /// Ensures that the index URI for `name` exists in `metadata.wt`.
    ///
    /// The returned boolean reports whether the metadata row was newly created
    /// in the current transaction.
    pub(crate) fn ensure_index_uri_in_transaction(
        &self,
        session: &mut Session,
        collection: &str,
        name: &str,
        columns: Vec<String>,
    ) -> Result<(String, bool), WrongoDBError> {
        let entry = MetadataEntry::index(collection, name, columns);
        let uri = entry.uri.clone();
        let created = self.register_entry_if_missing(session, entry)?;
        Ok((uri, created))
    }

    /// Loads the table definition visible to `txn_id`.
    ///
    /// Table metadata is assembled from the primary `table:` row plus all
    /// matching `index:` rows for the same collection.
    pub(crate) fn table_metadata_for_txn(
        &self,
        uri: &str,
        txn_id: TxnId,
    ) -> Result<Option<TableMetadata>, WrongoDBError> {
        let Some(entry) = self.lookup_entry_for_txn(uri, txn_id)? else {
            return Ok(None);
        };

        if !entry.is_table() {
            return Err(StorageError(format!("URI is not a table: {uri}")).into());
        }

        let collection = collection_name_from_table_uri(uri)?;
        let index_prefix = format!("{INDEX_URI_PREFIX}{collection}:");
        let mut indexes = self
            .scan_prefix_for_txn(&index_prefix, txn_id)?
            .into_iter()
            .map(MetadataEntry::into_index_metadata)
            .collect::<Result<Vec<_>, _>>()?;
        indexes.sort_by(|left, right| left.uri().cmp(right.uri()));

        Ok(Some(TableMetadata::with_indexes(uri, indexes)))
    }

    /// Returns all metadata rows whose URI begins with `prefix`.
    pub(crate) fn scan_prefix(&self, prefix: &str) -> Result<Vec<MetadataEntry>, WrongoDBError> {
        self.scan_prefix_for_txn(prefix, TXN_NONE)
    }

    /// Lists the unique backing store names referenced from `metadata.wt`.
    pub(crate) fn all_store_names(&self) -> Result<Vec<String>, WrongoDBError> {
        let mut store_names = self
            .scan_prefix("")?
            .into_iter()
            .map(|entry| entry.source)
            .collect::<Vec<_>>();
        store_names.sort();
        store_names.dedup();
        Ok(store_names)
    }

    // ------------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------------

    fn register_entry_if_missing(
        &self,
        session: &mut Session,
        entry: MetadataEntry,
    ) -> Result<bool, WrongoDBError> {
        if self
            .lookup_store_name_for_txn(entry.uri(), session.current_txn_id())?
            .is_some()
        {
            return Ok(false);
        }

        // The backing file must exist before the metadata row becomes visible,
        // otherwise later URI resolution could point at a store that cannot be
        // opened yet.
        self.open_or_create_store(entry.source())?;

        let value = encode_entry(&entry)?;
        session.insert_into_store(METADATA_URI, entry.uri.as_bytes(), &value)?;
        Ok(true)
    }

    fn scan_prefix_for_txn(
        &self,
        prefix: &str,
        txn_id: TxnId,
    ) -> Result<Vec<MetadataEntry>, WrongoDBError> {
        let metadata_store = self.open_metadata_store()?;
        let start = (!prefix.is_empty()).then(|| prefix.as_bytes().to_vec());
        let end = prefix_upper_bound(prefix);
        let rows = scan_range(
            &mut metadata_store.write(),
            start.as_deref(),
            end.as_deref(),
            txn_id,
        )?;

        rows.into_iter()
            .map(|(key, value)| decode_entry(key, value))
            .collect()
    }

    fn lookup_entry_for_txn(
        &self,
        uri: &str,
        txn_id: TxnId,
    ) -> Result<Option<MetadataEntry>, WrongoDBError> {
        let value = get_version(
            &mut self.open_metadata_store()?.write(),
            uri.as_bytes(),
            txn_id,
        )?;

        value
            .map(|value| decode_entry(uri.as_bytes().to_vec(), value))
            .transpose()
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
    Ok(MetadataEntry::from_record(uri, record))
}

fn collection_name_from_table_uri(uri: &str) -> Result<&str, WrongoDBError> {
    uri.strip_prefix(TABLE_URI_PREFIX).ok_or_else(|| {
        StorageError(format!(
            "table metadata lookup requires table: URI, got: {uri}"
        ))
        .into()
    })
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
    use crate::storage::api::Session;
    use crate::storage::btree::BTreeCursor;
    use crate::storage::handle_cache::HandleCache;
    use crate::storage::log_manager::LogManager;
    use crate::txn::GlobalTxnState;

    fn insert_entries_in_transaction(
        store: &MetadataStore,
        session: &mut Session,
        entries: impl IntoIterator<Item = MetadataEntry>,
    ) -> Result<(), WrongoDBError> {
        for entry in entries {
            let _created = store.register_entry_if_missing(session, entry)?;
        }
        Ok(())
    }

    struct MetadataFixture {
        store: Arc<MetadataStore>,
        session: Session,
    }

    impl MetadataFixture {
        fn new() -> Self {
            let dir = tempdir().unwrap();
            let base_path = dir.path().to_path_buf();
            std::mem::forget(dir);

            let store_handles = Arc::new(HandleCache::<String, RwLock<BTreeCursor>>::new());
            let global_txn = Arc::new(GlobalTxnState::new());
            let store = Arc::new(MetadataStore::new(base_path.clone(), store_handles.clone()));
            let session = Session::new(
                base_path,
                store_handles,
                store.clone(),
                global_txn,
                Arc::new(LogManager::disabled()),
            );

            Self { store, session }
        }

        fn insert_entries(&mut self, entries: impl IntoIterator<Item = MetadataEntry>) {
            let store = self.store.clone();
            self.session
                .with_transaction(|session| {
                    insert_entries_in_transaction(store.as_ref(), session, entries)
                })
                .unwrap();
        }
    }

    #[test]
    fn test_metadata_store_reads_table_entry() {
        let mut fixture = MetadataFixture::new();
        fixture.insert_entries([MetadataEntry::table("users")]);

        assert_eq!(
            fixture.store.lookup_store_name("table:users").unwrap(),
            Some("users.main.wt".to_string())
        );
    }

    #[test]
    fn test_metadata_store_reads_index_entry() {
        let mut fixture = MetadataFixture::new();
        fixture.insert_entries([MetadataEntry::index(
            "users",
            "name_1",
            vec!["name".to_string()],
        )]);

        assert_eq!(
            fixture
                .store
                .lookup_store_name("index:users:name_1")
                .unwrap(),
            Some("users.name_1.idx.wt".to_string())
        );
    }

    #[test]
    fn test_metadata_store_scans_by_prefix() {
        let mut fixture = MetadataFixture::new();
        fixture.insert_entries([
            MetadataEntry::table("users"),
            MetadataEntry::index("users", "name_1", vec!["name".to_string()]),
            MetadataEntry::index("users", "email_1", vec!["email".to_string()]),
        ]);

        let tables = fixture.store.scan_prefix(TABLE_URI_PREFIX).unwrap();
        let indexes = fixture.store.scan_prefix("index:users:").unwrap();

        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].uri(), "table:users");
        assert_eq!(indexes.len(), 2);
    }

    #[test]
    fn test_metadata_store_lists_store_names() {
        let mut fixture = MetadataFixture::new();
        fixture.insert_entries([
            MetadataEntry::table("users"),
            MetadataEntry::index("users", "name_1", vec!["name".to_string()]),
        ]);

        assert_eq!(
            fixture.store.all_store_names().unwrap(),
            vec![
                "users.main.wt".to_string(),
                "users.name_1.idx.wt".to_string()
            ]
        );
    }

    #[test]
    fn test_table_metadata_for_txn_sorts_indexes_by_uri() {
        let mut fixture = MetadataFixture::new();
        let mut table = None;
        let store = fixture.store.clone();
        fixture
            .session
            .with_transaction(|session| {
                insert_entries_in_transaction(
                    store.as_ref(),
                    session,
                    [
                        MetadataEntry::table("users"),
                        MetadataEntry::index("users", "z_last_1", vec!["z_last".to_string()]),
                        MetadataEntry::index("users", "a_first_1", vec!["a_first".to_string()]),
                    ],
                )?;

                table = fixture
                    .store
                    .table_metadata_for_txn("table:users", session.current_txn_id())?;
                Ok(())
            })
            .unwrap();

        let table = table.unwrap();

        assert_eq!(table.uri(), "table:users");
        assert_eq!(
            table
                .indexes()
                .iter()
                .map(|index| index.uri().to_string())
                .collect::<Vec<_>>(),
            vec![
                "index:users:a_first_1".to_string(),
                "index:users:z_last_1".to_string(),
            ]
        );
        assert_eq!(table.indexes()[0].name(), "a_first_1");
        assert_eq!(table.indexes()[0].columns(), &["a_first".to_string()]);
        assert_eq!(table.indexes()[1].name(), "z_last_1");
        assert_eq!(table.indexes()[1].columns(), &["z_last".to_string()]);
    }
}
