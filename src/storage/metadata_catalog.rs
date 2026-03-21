use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::core::errors::StorageError;
use crate::storage::api::WriteUnitOfWork;
use crate::storage::btree::BTreeCursor;
use crate::storage::handle_cache::HandleCache;
use crate::storage::table::{get_version, open_or_create_btree, scan_range};
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MetadataRecord {
    kind: MetadataKind,
    source: String,
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

    pub(crate) fn ensure_table_uri_in_write_unit(
        &self,
        write_unit: &mut WriteUnitOfWork<'_>,
        collection: &str,
    ) -> Result<String, WrongoDBError> {
        let uri = table_uri(collection);
        if self
            .lookup_source_for_txn(&uri, write_unit.txn_id())?
            .is_some()
        {
            return Ok(uri);
        }

        let source = table_source_name(collection);
        self.ensure_source_exists(&source)?;
        let _inserted = self.put_if_absent_in_write_unit(
            write_unit,
            MetadataEntry {
                uri: uri.clone(),
                kind: MetadataKind::Table,
                source,
            },
        )?;
        Ok(uri)
    }

    pub(crate) fn ensure_index_uri_in_write_unit(
        &self,
        write_unit: &mut WriteUnitOfWork<'_>,
        collection: &str,
        name: &str,
    ) -> Result<(String, bool), WrongoDBError> {
        let uri = index_uri(collection, name);
        if self
            .lookup_source_for_txn(&uri, write_unit.txn_id())?
            .is_some()
        {
            return Ok((uri, false));
        }

        let source = index_source_name(collection, name);
        self.ensure_source_exists(&source)?;
        let inserted = self.put_if_absent_in_write_unit(
            write_unit,
            MetadataEntry {
                uri: uri.clone(),
                kind: MetadataKind::Index,
                source,
            },
        )?;
        Ok((uri, inserted))
    }

    pub(crate) fn put_if_absent_in_write_unit(
        &self,
        write_unit: &mut WriteUnitOfWork<'_>,
        entry: MetadataEntry,
    ) -> Result<bool, WrongoDBError> {
        if self
            .lookup_source_for_txn(&entry.uri, write_unit.txn_id())?
            .is_some()
        {
            return Ok(false);
        }

        let mut cursor = write_unit.open_cursor(METADATA_URI)?;
        let key = encode_metadata_key(&entry.uri);
        let value = encode_metadata_value(&entry)?;
        cursor.insert(&key, &value)?;
        Ok(true)
    }

    pub(crate) fn scan_prefix(&self, prefix: &str) -> Result<Vec<MetadataEntry>, WrongoDBError> {
        let btree = self.open_metadata_table()?;
        let start = prefix_start(prefix);
        let end = prefix_end(prefix);
        let entries = scan_range(
            &mut btree.write(),
            start.as_deref(),
            end.as_deref(),
            TXN_NONE,
        )?;

        entries
            .into_iter()
            .map(|(key, value)| decode_metadata_entry(key, value))
            .collect()
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
    })
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
    use crate::txn::{GlobalTxnState, TransactionManager};

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
            let transaction_manager =
                Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())));
            let catalog = Arc::new(MetadataCatalog::new(
                base_path.clone(),
                store_handles.clone(),
            ));
            let session = Session::new(
                base_path,
                store_handles,
                catalog.clone(),
                transaction_manager,
                Arc::new(DurabilityBackend::Disabled),
            );

            Self { catalog, session }
        }
    }

    #[test]
    fn metadata_catalog_writes_and_reads_table_row() {
        let mut fixture = MetadataFixture::new();
        let mut write_unit = fixture.session.transaction().unwrap();
        fixture
            .catalog
            .put_if_absent_in_write_unit(
                &mut write_unit,
                MetadataEntry {
                    uri: "table:users".to_string(),
                    kind: MetadataKind::Table,
                    source: "users.main.wt".to_string(),
                },
            )
            .unwrap();
        write_unit.commit().unwrap();

        assert_eq!(
            fixture.catalog.lookup_source("table:users").unwrap(),
            Some("users.main.wt".to_string())
        );
    }

    #[test]
    fn metadata_catalog_writes_and_reads_index_row() {
        let mut fixture = MetadataFixture::new();
        let mut write_unit = fixture.session.transaction().unwrap();
        fixture
            .catalog
            .put_if_absent_in_write_unit(
                &mut write_unit,
                MetadataEntry {
                    uri: "index:users:name".to_string(),
                    kind: MetadataKind::Index,
                    source: "users.name.idx.wt".to_string(),
                },
            )
            .unwrap();
        write_unit.commit().unwrap();

        assert_eq!(
            fixture.catalog.lookup_source("index:users:name").unwrap(),
            Some("users.name.idx.wt".to_string())
        );
    }

    #[test]
    fn metadata_catalog_scans_table_and_index_prefixes() {
        let mut fixture = MetadataFixture::new();
        let mut write_unit = fixture.session.transaction().unwrap();
        for entry in [
            MetadataEntry {
                uri: "table:users".to_string(),
                kind: MetadataKind::Table,
                source: "users.main.wt".to_string(),
            },
            MetadataEntry {
                uri: "index:users:name".to_string(),
                kind: MetadataKind::Index,
                source: "users.name.idx.wt".to_string(),
            },
            MetadataEntry {
                uri: "index:users:email".to_string(),
                kind: MetadataKind::Index,
                source: "users.email.idx.wt".to_string(),
            },
        ] {
            fixture
                .catalog
                .put_if_absent_in_write_unit(&mut write_unit, entry)
                .unwrap();
        }
        write_unit.commit().unwrap();

        let tables = fixture.catalog.scan_prefix(TABLE_URI_PREFIX).unwrap();
        let indexes = fixture.catalog.scan_prefix("index:users:").unwrap();

        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].uri, "table:users");
        assert_eq!(indexes.len(), 2);
    }

    #[test]
    fn metadata_catalog_all_sources_includes_table_and_index_stores() {
        let mut fixture = MetadataFixture::new();
        let mut write_unit = fixture.session.transaction().unwrap();
        for entry in [
            MetadataEntry {
                uri: "table:users".to_string(),
                kind: MetadataKind::Table,
                source: "users.main.wt".to_string(),
            },
            MetadataEntry {
                uri: "index:users:name".to_string(),
                kind: MetadataKind::Index,
                source: "users.name.idx.wt".to_string(),
            },
        ] {
            fixture
                .catalog
                .put_if_absent_in_write_unit(&mut write_unit, entry)
                .unwrap();
        }
        write_unit.commit().unwrap();

        assert_eq!(
            fixture.catalog.all_sources().unwrap(),
            vec!["users.main.wt".to_string(), "users.name.idx.wt".to_string()]
        );
    }
}
