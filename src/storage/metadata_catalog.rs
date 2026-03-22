use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::core::errors::StorageError;
use crate::storage::api::Session;
use crate::storage::btree::BTreeCursor;
use crate::storage::handle_cache::HandleCache;
use crate::storage::table::{
    get_version, open_or_create_btree, scan_range, IndexMetadata, TableMetadata,
};
use crate::txn::{TxnId, TXN_NONE};
use crate::WrongoDBError;

// ============================================================================
// Constants
// ============================================================================

pub(crate) const METADATA_STORE_NAME: &str = "metadata.wt";
pub(crate) const METADATA_URI: &str = "metadata:";
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum MetadataKind {
    Table,
    Index,
}

/// One row in the reserved metadata store.
///
/// Each row maps a logical URI such as `table:users` to the backing `.wt`
/// file. Index rows also carry the index definition needed to rebuild
/// [`TableMetadata`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MetadataEntry {
    uri: String,
    kind: MetadataKind,
    store_name: String,
    name: Option<String>,
    columns: Vec<String>,
}

impl MetadataEntry {
    fn table(collection: &str) -> Self {
        Self {
            uri: table_uri(collection),
            kind: MetadataKind::Table,
            store_name: table_store_name(collection),
            name: None,
            columns: Vec::new(),
        }
    }

    fn index(collection: &str, name: &str, columns: Vec<String>) -> Self {
        Self {
            uri: index_uri(collection, name),
            kind: MetadataKind::Index,
            store_name: index_store_name(collection, name),
            name: Some(name.to_string()),
            columns,
        }
    }

    pub(crate) fn uri(&self) -> &str {
        &self.uri
    }

    fn from_record(uri: String, record: MetadataRecord) -> Self {
        Self {
            uri,
            kind: record.kind,
            store_name: record.store_name,
            name: record.name,
            columns: record.columns,
        }
    }

    fn into_index_metadata(self) -> Result<IndexMetadata, WrongoDBError> {
        if self.kind != MetadataKind::Index {
            return Err(StorageError(format!("metadata row is not an index: {}", self.uri)).into());
        }

        let name = self.name.ok_or_else(|| {
            StorageError(format!(
                "legacy index metadata row is unsupported without name: {}",
                self.uri
            ))
        })?;
        if self.columns.is_empty() {
            return Err(StorageError(format!(
                "legacy index metadata row is unsupported without columns: {}",
                self.uri
            ))
            .into());
        }

        Ok(IndexMetadata::new(name, self.uri, self.columns))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MetadataRecord {
    kind: MetadataKind,
    #[serde(rename = "source", alias = "store_name")]
    store_name: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    columns: Vec<String>,
}

impl From<&MetadataEntry> for MetadataRecord {
    fn from(entry: &MetadataEntry) -> Self {
        Self {
            kind: entry.kind,
            store_name: entry.store_name.clone(),
            name: entry.name.clone(),
            columns: entry.columns.clone(),
        }
    }
}

// ============================================================================
// MetadataCatalog
// ============================================================================

/// Storage-facing catalog of logical URI to physical store mappings.
///
/// This catalog reads and writes rows in the reserved `metadata.wt` store, but
/// leaves collection semantics to higher layers.
#[derive(Debug, Clone)]
pub(crate) struct MetadataCatalog {
    base_path: PathBuf,
    store_handles: Arc<HandleCache<String, RwLock<BTreeCursor>>>,
}

impl MetadataCatalog {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

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
            .map(|entry| entry.store_name))
    }

    pub(crate) fn ensure_table_uri_in_transaction(
        &self,
        session: &mut Session,
        collection: &str,
    ) -> Result<String, WrongoDBError> {
        let entry = MetadataEntry::table(collection);
        let uri = entry.uri.clone();
        let _created = self.create_entry_if_missing(session, entry)?;
        Ok(uri)
    }

    pub(crate) fn ensure_index_uri_in_transaction(
        &self,
        session: &mut Session,
        collection: &str,
        name: &str,
        columns: Vec<String>,
    ) -> Result<(String, bool), WrongoDBError> {
        let entry = MetadataEntry::index(collection, name, columns);
        let uri = entry.uri.clone();
        let created = self.create_entry_if_missing(session, entry)?;
        Ok((uri, created))
    }

    pub(crate) fn table_metadata_for_txn(
        &self,
        uri: &str,
        txn_id: TxnId,
    ) -> Result<Option<TableMetadata>, WrongoDBError> {
        let Some(entry) = self.lookup_entry_for_txn(uri, txn_id)? else {
            return Ok(None);
        };

        if entry.kind != MetadataKind::Table {
            return Err(StorageError(format!("URI is not a table: {uri}")).into());
        }

        let collection = collection_from_table_uri(uri)?;
        let index_prefix = format!("{INDEX_URI_PREFIX}{collection}:");
        let mut indexes = self
            .scan_prefix_for_txn(&index_prefix, txn_id)?
            .into_iter()
            .map(MetadataEntry::into_index_metadata)
            .collect::<Result<Vec<_>, _>>()?;
        indexes.sort_by(|left, right| left.uri().cmp(right.uri()));

        Ok(Some(TableMetadata::with_indexes(uri, indexes)))
    }

    pub(crate) fn scan_prefix(&self, prefix: &str) -> Result<Vec<MetadataEntry>, WrongoDBError> {
        self.scan_prefix_for_txn(prefix, TXN_NONE)
    }

    pub(crate) fn all_store_names(&self) -> Result<Vec<String>, WrongoDBError> {
        let mut store_names = self
            .scan_prefix("")?
            .into_iter()
            .map(|entry| entry.store_name)
            .collect::<Vec<_>>();
        store_names.sort();
        store_names.dedup();
        Ok(store_names)
    }

    // ------------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------------

    fn create_entry_if_missing(
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

        self.open_or_create_store(&entry.store_name)?;

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

fn collection_from_table_uri(uri: &str) -> Result<&str, WrongoDBError> {
    uri.strip_prefix(TABLE_URI_PREFIX).ok_or_else(|| {
        StorageError(format!(
            "table metadata lookup requires table: URI, got: {uri}"
        ))
        .into()
    })
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
    use crate::storage::log_manager::LogManager;
    use crate::txn::GlobalTxnState;

    fn insert_entries_in_transaction(
        catalog: &MetadataCatalog,
        session: &mut Session,
        entries: impl IntoIterator<Item = MetadataEntry>,
    ) -> Result<(), WrongoDBError> {
        for entry in entries {
            let _created = catalog.create_entry_if_missing(session, entry)?;
        }
        Ok(())
    }

    struct MetadataFixture {
        catalog: Arc<MetadataCatalog>,
        session: Session,
    }

    impl MetadataFixture {
        fn new() -> Self {
            let dir = tempdir().unwrap();
            let base_path = dir.path().to_path_buf();
            std::mem::forget(dir);

            let store_handles = Arc::new(HandleCache::<String, RwLock<BTreeCursor>>::new());
            let global_txn = Arc::new(GlobalTxnState::new());
            let catalog = Arc::new(MetadataCatalog::new(
                base_path.clone(),
                store_handles.clone(),
            ));
            let session = Session::new(
                base_path,
                store_handles,
                catalog.clone(),
                global_txn,
                Arc::new(LogManager::disabled()),
            );

            Self { catalog, session }
        }

        fn insert_entries(&mut self, entries: impl IntoIterator<Item = MetadataEntry>) {
            let catalog = self.catalog.clone();
            self.session
                .with_transaction(|session| {
                    insert_entries_in_transaction(catalog.as_ref(), session, entries)
                })
                .unwrap();
        }
    }

    #[test]
    fn test_metadata_catalog_reads_table_entry() {
        let mut fixture = MetadataFixture::new();
        fixture.insert_entries([MetadataEntry::table("users")]);

        assert_eq!(
            fixture.catalog.lookup_store_name("table:users").unwrap(),
            Some("users.main.wt".to_string())
        );
    }

    #[test]
    fn test_metadata_catalog_reads_index_entry() {
        let mut fixture = MetadataFixture::new();
        fixture.insert_entries([MetadataEntry::index(
            "users",
            "name",
            vec!["name".to_string()],
        )]);

        assert_eq!(
            fixture
                .catalog
                .lookup_store_name("index:users:name")
                .unwrap(),
            Some("users.name.idx.wt".to_string())
        );
    }

    #[test]
    fn test_metadata_catalog_scans_by_prefix() {
        let mut fixture = MetadataFixture::new();
        fixture.insert_entries([
            MetadataEntry::table("users"),
            MetadataEntry::index("users", "name", vec!["name".to_string()]),
            MetadataEntry::index("users", "email", vec!["email".to_string()]),
        ]);

        let tables = fixture.catalog.scan_prefix(TABLE_URI_PREFIX).unwrap();
        let indexes = fixture.catalog.scan_prefix("index:users:").unwrap();

        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].uri(), "table:users");
        assert_eq!(indexes.len(), 2);
    }

    #[test]
    fn test_metadata_catalog_lists_store_names() {
        let mut fixture = MetadataFixture::new();
        fixture.insert_entries([
            MetadataEntry::table("users"),
            MetadataEntry::index("users", "name", vec!["name".to_string()]),
        ]);

        assert_eq!(
            fixture.catalog.all_store_names().unwrap(),
            vec!["users.main.wt".to_string(), "users.name.idx.wt".to_string()]
        );
    }

    #[test]
    fn test_table_metadata_for_txn_sorts_indexes_by_uri() {
        let mut fixture = MetadataFixture::new();
        let mut table = None;
        let catalog = fixture.catalog.clone();
        fixture
            .session
            .with_transaction(|session| {
                insert_entries_in_transaction(
                    catalog.as_ref(),
                    session,
                    [
                        MetadataEntry::table("users"),
                        MetadataEntry::index("users", "z_last", vec!["z_last".to_string()]),
                        MetadataEntry::index("users", "a_first", vec!["a_first".to_string()]),
                    ],
                )?;

                table = fixture
                    .catalog
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
                "index:users:a_first".to_string(),
                "index:users:z_last".to_string(),
            ]
        );
        assert_eq!(table.indexes()[0].columns(), &["a_first".to_string()]);
        assert_eq!(table.indexes()[1].columns(), &["z_last".to_string()]);
    }
}
