use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::core::errors::StorageError;
use crate::storage::metadata_catalog::{table_uri, MetadataCatalog, TABLE_URI_PREFIX};
use crate::txn::TxnId;
use crate::WrongoDBError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct IndexDefinition {
    pub(crate) name: String,
    pub(crate) columns: Vec<String>,
    pub(crate) uri: String,
    pub(crate) key_format: Option<String>,
    pub(crate) value_format: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CollectionSchema {
    indexes: BTreeMap<String, IndexDefinition>,
}

impl CollectionSchema {
    pub(crate) fn has_index(&self, name: &str) -> bool {
        self.indexes.contains_key(name)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct CollectionSchemaFile {
    collection: String,
    indexes: Vec<IndexDefinition>,
}

#[derive(Debug, Clone)]
pub(crate) struct SchemaCatalog {
    base_path: PathBuf,
    metadata_catalog: Arc<MetadataCatalog>,
}

impl SchemaCatalog {
    pub(crate) fn new(base_path: PathBuf, metadata_catalog: Arc<MetadataCatalog>) -> Self {
        Self {
            base_path,
            metadata_catalog,
        }
    }

    pub(crate) fn collection_schema_for_txn(
        &self,
        collection: &str,
        txn_id: TxnId,
    ) -> Result<CollectionSchema, WrongoDBError> {
        let uri = table_uri(collection);
        let _primary_store = self
            .metadata_catalog
            .lookup_source_for_txn(&uri, txn_id)?
            .ok_or_else(|| StorageError(format!("unknown collection: {uri}")))?;
        let indexes = self.load_index_definitions(collection)?;
        Ok(CollectionSchema { indexes })
    }

    pub(crate) fn list_indexes(&self, collection: &str) -> Result<Vec<String>, WrongoDBError> {
        Ok(self
            .load_index_definitions(collection)?
            .into_keys()
            .collect::<Vec<_>>())
    }

    pub(crate) fn list_collections(&self) -> Result<Vec<String>, WrongoDBError> {
        let mut names = self
            .metadata_catalog
            .scan_prefix(TABLE_URI_PREFIX)?
            .into_iter()
            .filter_map(|entry| entry.uri.strip_prefix(TABLE_URI_PREFIX).map(str::to_string))
            .collect::<Vec<_>>();
        names.sort();
        names.dedup();
        Ok(names)
    }

    pub(crate) fn collection_exists_in_txn(
        &self,
        collection: &str,
        txn_id: TxnId,
    ) -> Result<bool, WrongoDBError> {
        let uri = table_uri(collection);
        Ok(self
            .metadata_catalog
            .lookup_source_for_txn(&uri, txn_id)?
            .is_some())
    }

    pub(crate) fn add_index(
        &self,
        collection: &str,
        name: &str,
        columns: Vec<String>,
    ) -> Result<Option<String>, WrongoDBError> {
        if columns.len() != 1 {
            return Err(StorageError("composite indexes are not supported yet".to_string()).into());
        }

        let mut indexes = self.load_index_definitions(collection)?;
        if indexes.contains_key(name) {
            return Ok(None);
        }

        let uri = crate::storage::metadata_catalog::index_uri(collection, name);
        indexes.insert(
            name.to_string(),
            IndexDefinition {
                name: name.to_string(),
                columns,
                uri: uri.clone(),
                key_format: None,
                value_format: None,
            },
        );
        self.save_index_definitions(collection, &indexes)?;
        Ok(Some(uri))
    }

    fn load_index_definitions(
        &self,
        collection: &str,
    ) -> Result<BTreeMap<String, IndexDefinition>, WrongoDBError> {
        let meta_path = self.base_path.join(format!("{collection}.meta.json"));
        if !meta_path.exists() {
            return Ok(BTreeMap::new());
        }

        let bytes = fs::read(&meta_path)?;
        let file: CollectionSchemaFile = serde_json::from_slice(&bytes)?;
        let mut indexes = BTreeMap::new();
        for def in file.indexes {
            indexes.insert(def.name.clone(), def);
        }
        Ok(indexes)
    }

    fn save_index_definitions(
        &self,
        collection: &str,
        indexes: &BTreeMap<String, IndexDefinition>,
    ) -> Result<(), WrongoDBError> {
        let file = CollectionSchemaFile {
            collection: collection.to_string(),
            indexes: indexes.values().cloned().collect(),
        };
        let tmp_path = self.base_path.join(format!("{collection}.meta.json.tmp"));
        let meta_path = self.base_path.join(format!("{collection}.meta.json"));
        fs::write(&tmp_path, serde_json::to_vec_pretty(&file)?)?;
        fs::rename(&tmp_path, &meta_path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parking_lot::RwLock;
    use tempfile::tempdir;

    use super::*;
    use crate::storage::api::Session;
    use crate::storage::btree::BTreeCursor;
    use crate::storage::durability::DurabilityBackend;
    use crate::storage::handle_cache::HandleCache;
    use crate::storage::metadata_catalog::MetadataCatalog;
    use crate::txn::GlobalTxnState;

    #[test]
    fn add_index_persists_logical_index_uri() {
        let tmp = tempdir().unwrap();
        let base_path = tmp.path().to_path_buf();
        let global_txn = Arc::new(GlobalTxnState::new());
        let store_handles = Arc::new(HandleCache::<String, RwLock<BTreeCursor>>::new());
        let metadata_catalog = Arc::new(MetadataCatalog::new(
            base_path.clone(),
            store_handles.clone(),
        ));
        let catalog = SchemaCatalog::new(base_path.clone(), metadata_catalog.clone());
        let mut session = Session::new(
            base_path,
            store_handles,
            metadata_catalog,
            global_txn,
            Arc::new(DurabilityBackend::Disabled),
        );
        session.create("table:users").unwrap();

        assert_eq!(
            catalog
                .add_index("users", "name", vec!["name".to_string()])
                .unwrap(),
            Some("index:users:name".to_string())
        );
        assert_eq!(
            catalog
                .load_index_definitions("users")
                .unwrap()
                .into_values()
                .collect::<Vec<_>>()[0]
                .uri,
            "index:users:name"
        );
        assert_eq!(
            catalog.list_indexes("users").unwrap(),
            vec!["name".to_string()]
        );
    }
}
