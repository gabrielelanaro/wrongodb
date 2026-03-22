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
// Metadata Types
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum MetadataKind {
    Table,
    Index,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MetadataEntry {
    pub(crate) uri: String,
    pub(crate) kind: MetadataKind,
    pub(crate) source: String,
    pub(crate) name: Option<String>,
    pub(crate) columns: Vec<String>,
}

impl MetadataEntry {
    fn table(uri: impl Into<String>, source: impl Into<String>) -> Self {
        Self {
            uri: uri.into(),
            kind: MetadataKind::Table,
            source: source.into(),
            name: None,
            columns: Vec::new(),
        }
    }

    fn index(
        uri: impl Into<String>,
        source: impl Into<String>,
        name: impl Into<String>,
        columns: Vec<String>,
    ) -> Self {
        Self {
            uri: uri.into(),
            kind: MetadataKind::Index,
            source: source.into(),
            name: Some(name.into()),
            columns,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MetadataRecord {
    kind: MetadataKind,
    source: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    columns: Vec<String>,
}

// ============================================================================
// MetadataCatalog
// ============================================================================

/// Storage-facing catalog of logical URI to physical store mappings.
///
/// This catalog is intentionally generic: it knows how to read and write URI
/// rows in the reserved `metadata.wt` store, but it does not understand
/// collection semantics above those URIs.
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
    pub(crate) fn lookup_source(&self, uri: &str) -> Result<Option<String>, WrongoDBError> {
        self.lookup_source_for_txn(uri, TXN_NONE)
    }

    pub(crate) fn lookup_source_for_txn(
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

    pub(crate) fn ensure_table_uri_in_transaction(
        &self,
        session: &mut Session,
        collection: &str,
    ) -> Result<String, WrongoDBError> {
        let uri = table_uri(collection);
        if self
            .lookup_source_for_txn(&uri, session.current_txn_id())?
            .is_some()
        {
            return Ok(uri);
        }

        let source = table_source_name(collection);
        self.ensure_source_exists(&source)?;
        let _inserted =
            self.put_if_absent_in_transaction(session, MetadataEntry::table(uri.clone(), source))?;
        Ok(uri)
    }

    pub(crate) fn ensure_index_uri_in_transaction(
        &self,
        session: &mut Session,
        collection: &str,
        name: &str,
        columns: Vec<String>,
    ) -> Result<(String, bool), WrongoDBError> {
        let uri = index_uri(collection, name);
        if self
            .lookup_source_for_txn(&uri, session.current_txn_id())?
            .is_some()
        {
            return Ok((uri, false));
        }

        let source = index_source_name(collection, name);
        self.ensure_source_exists(&source)?;
        let inserted = self.put_if_absent_in_transaction(
            session,
            MetadataEntry::index(uri.clone(), source, name, columns),
        )?;
        Ok((uri, inserted))
    }

    pub(crate) fn put_if_absent_in_transaction(
        &self,
        session: &mut Session,
        entry: MetadataEntry,
    ) -> Result<bool, WrongoDBError> {
        if self
            .lookup_source_for_txn(&entry.uri, session.current_txn_id())?
            .is_some()
        {
            return Ok(false);
        }

        let key = encode_metadata_key(&entry.uri);
        let value = encode_metadata_value(&entry)?;
        session.insert_into_store(METADATA_URI, &key, &value)?;
        Ok(true)
    }

    pub(crate) fn scan_prefix(&self, prefix: &str) -> Result<Vec<MetadataEntry>, WrongoDBError> {
        self.scan_prefix_for_txn(prefix, TXN_NONE)
    }

    pub(crate) fn scan_prefix_for_txn(
        &self,
        prefix: &str,
        txn_id: TxnId,
    ) -> Result<Vec<MetadataEntry>, WrongoDBError> {
        let btree = self.open_metadata_table()?;
        let start = prefix_start(prefix);
        let end = prefix_end(prefix);
        let entries = scan_range(&mut btree.write(), start.as_deref(), end.as_deref(), txn_id)?;

        entries
            .into_iter()
            .map(|(key, value)| decode_metadata_entry(key, value))
            .collect()
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

        let collection = uri.strip_prefix(TABLE_URI_PREFIX).ok_or_else(|| {
            StorageError(format!(
                "table metadata lookup requires table: URI, got: {uri}"
            ))
        })?;
        let prefix = format!("{INDEX_URI_PREFIX}{collection}:");
        let mut indexes = self
            .scan_prefix_for_txn(&prefix, txn_id)?
            .into_iter()
            .map(index_metadata_from_entry)
            .collect::<Result<Vec<_>, _>>()?;
        indexes.sort_by(|left, right| left.uri().cmp(right.uri()));
        Ok(Some(TableMetadata::with_indexes(uri, indexes)))
    }

    pub(crate) fn all_sources(&self) -> Result<Vec<String>, WrongoDBError> {
        let mut sources: Vec<String> = self
            .scan_prefix("")?
            .into_iter()
            .map(|entry| entry.source)
            .collect();
        sources.sort();
        sources.dedup();
        Ok(sources)
    }

    // ------------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------------

    fn lookup_entry_for_txn(
        &self,
        uri: &str,
        txn_id: TxnId,
    ) -> Result<Option<MetadataEntry>, WrongoDBError> {
        let value = get_version(
            &mut self.open_metadata_table()?.write(),
            uri.as_bytes(),
            txn_id,
        )?;
        value
            .map(|value| decode_metadata_entry(uri.as_bytes().to_vec(), value))
            .transpose()
    }

    fn ensure_source_exists(
        &self,
        source: &str,
    ) -> Result<Arc<RwLock<BTreeCursor>>, WrongoDBError> {
        self.store_handles
            .get_or_try_insert_with(source.to_string(), |source| {
                let path = self.base_path.join(source);
                Ok(RwLock::new(open_or_create_btree(path)?))
            })
    }

    fn open_metadata_table(&self) -> Result<Arc<RwLock<BTreeCursor>>, WrongoDBError> {
        self.ensure_source_exists(METADATA_STORE_NAME)
    }
}

// ============================================================================
// Encoding Helpers
// ============================================================================

fn encode_metadata_key(uri: &str) -> Vec<u8> {
    uri.as_bytes().to_vec()
}

fn encode_metadata_value(entry: &MetadataEntry) -> Result<Vec<u8>, WrongoDBError> {
    let record = MetadataRecord {
        kind: entry.kind,
        source: entry.source.clone(),
        name: entry.name.clone(),
        columns: entry.columns.clone(),
    };
    Ok(bson::to_vec(&record)?)
}

fn decode_metadata_entry(key: Vec<u8>, value: Vec<u8>) -> Result<MetadataEntry, WrongoDBError> {
    let uri = String::from_utf8(key)
        .map_err(|err| StorageError(format!("metadata row key is not valid UTF-8: {err}")))?;
    let record: MetadataRecord = bson::from_slice(&value)?;
    Ok(MetadataEntry {
        uri,
        kind: record.kind,
        source: record.source,
        name: record.name,
        columns: record.columns,
    })
}

fn index_metadata_from_entry(entry: MetadataEntry) -> Result<IndexMetadata, WrongoDBError> {
    if entry.kind != MetadataKind::Index {
        return Err(StorageError(format!("metadata row is not an index: {}", entry.uri)).into());
    }

    let name = entry.name.ok_or_else(|| {
        StorageError(format!(
            "legacy index metadata row is unsupported without name: {}",
            entry.uri
        ))
    })?;
    if entry.columns.is_empty() {
        return Err(StorageError(format!(
            "legacy index metadata row is unsupported without columns: {}",
            entry.uri
        ))
        .into());
    }

    Ok(IndexMetadata::new(name, entry.uri, entry.columns))
}

fn prefix_start(prefix: &str) -> Option<Vec<u8>> {
    if prefix.is_empty() {
        None
    } else {
        Some(prefix.as_bytes().to_vec())
    }
}

fn prefix_end(prefix: &str) -> Option<Vec<u8>> {
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

pub(crate) fn table_uri(collection: &str) -> String {
    format!("{TABLE_URI_PREFIX}{collection}")
}

pub(crate) fn index_uri(collection: &str, name: &str) -> String {
    format!("{INDEX_URI_PREFIX}{collection}:{name}")
}

fn table_source_name(collection: &str) -> String {
    format!("{collection}.main.wt")
}

fn index_source_name(collection: &str, name: &str) -> String {
    format!("{collection}.{name}.idx.wt")
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
    use crate::storage::durability::DurabilityBackend;
    use crate::txn::GlobalTxnState;

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
                Arc::new(DurabilityBackend::Disabled),
            );

            Self { catalog, session }
        }
    }

    #[test]
    fn metadata_catalog_writes_and_reads_table_row() {
        let mut fixture = MetadataFixture::new();
        fixture
            .session
            .with_transaction(|session| {
                fixture.catalog.put_if_absent_in_transaction(
                    session,
                    MetadataEntry::table("table:users", "users.main.wt"),
                )?;
                Ok(())
            })
            .unwrap();

        assert_eq!(
            fixture.catalog.lookup_source("table:users").unwrap(),
            Some("users.main.wt".to_string())
        );
    }

    #[test]
    fn metadata_catalog_writes_and_reads_index_row() {
        let mut fixture = MetadataFixture::new();
        fixture
            .session
            .with_transaction(|session| {
                fixture.catalog.put_if_absent_in_transaction(
                    session,
                    MetadataEntry::index(
                        "index:users:name",
                        "users.name.idx.wt",
                        "name",
                        vec!["name".to_string()],
                    ),
                )?;
                Ok(())
            })
            .unwrap();

        assert_eq!(
            fixture.catalog.lookup_source("index:users:name").unwrap(),
            Some("users.name.idx.wt".to_string())
        );
    }

    #[test]
    fn metadata_catalog_scans_table_and_index_prefixes() {
        let mut fixture = MetadataFixture::new();
        fixture
            .session
            .with_transaction(|session| {
                for entry in [
                    MetadataEntry::table("table:users", "users.main.wt"),
                    MetadataEntry::index(
                        "index:users:name",
                        "users.name.idx.wt",
                        "name",
                        vec!["name".to_string()],
                    ),
                    MetadataEntry::index(
                        "index:users:email",
                        "users.email.idx.wt",
                        "email",
                        vec!["email".to_string()],
                    ),
                ] {
                    fixture
                        .catalog
                        .put_if_absent_in_transaction(session, entry)?;
                }
                Ok(())
            })
            .unwrap();

        let tables = fixture.catalog.scan_prefix(TABLE_URI_PREFIX).unwrap();
        let indexes = fixture.catalog.scan_prefix("index:users:").unwrap();

        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].uri, "table:users");
        assert_eq!(indexes.len(), 2);
    }

    #[test]
    fn metadata_catalog_all_sources_includes_table_and_index_stores() {
        let mut fixture = MetadataFixture::new();
        fixture
            .session
            .with_transaction(|session| {
                for entry in [
                    MetadataEntry::table("table:users", "users.main.wt"),
                    MetadataEntry::index(
                        "index:users:name",
                        "users.name.idx.wt",
                        "name",
                        vec!["name".to_string()],
                    ),
                ] {
                    fixture
                        .catalog
                        .put_if_absent_in_transaction(session, entry)?;
                }
                Ok(())
            })
            .unwrap();

        assert_eq!(
            fixture.catalog.all_sources().unwrap(),
            vec!["users.main.wt".to_string(), "users.name.idx.wt".to_string()]
        );
    }

    #[test]
    fn table_metadata_for_txn_loads_index_definitions_in_stable_order() {
        let mut fixture = MetadataFixture::new();
        let mut table = None;
        fixture
            .session
            .with_transaction(|session| {
                for entry in [
                    MetadataEntry::table("table:users", "users.main.wt"),
                    MetadataEntry::index(
                        "index:users:z_last",
                        "users.z_last.idx.wt",
                        "z_last",
                        vec!["z_last".to_string()],
                    ),
                    MetadataEntry::index(
                        "index:users:a_first",
                        "users.a_first.idx.wt",
                        "a_first",
                        vec!["a_first".to_string()],
                    ),
                ] {
                    fixture
                        .catalog
                        .put_if_absent_in_transaction(session, entry)?;
                }

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
