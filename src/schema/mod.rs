use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::core::errors::StorageError;
use crate::storage::metadata_catalog::{MetadataCatalog, TABLE_URI_PREFIX};
use crate::txn::TxnId;
use crate::WrongoDBError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct IndexDefinition {
    pub(crate) name: String,
    pub(crate) columns: Vec<String>,
    pub(crate) source: String,
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

    pub(crate) fn index_definitions(
        &self,
        collection: &str,
    ) -> Result<Vec<IndexDefinition>, WrongoDBError> {
        Ok(self
            .load_index_definitions(collection)?
            .into_values()
            .collect())
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

        let source = format!("{collection}.{name}.idx.wt");
        indexes.insert(
            name.to_string(),
            IndexDefinition {
                name: name.to_string(),
                columns,
                source: source.clone(),
                key_format: None,
                value_format: None,
            },
        );
        self.save_index_definitions(collection, &indexes)?;
        Ok(Some(source))
    }

    fn load_index_definitions(
        &self,
        collection: &str,
    ) -> Result<BTreeMap<String, IndexDefinition>, WrongoDBError> {
        let meta_path = self.base_path.join(format!("{collection}.meta.json"));
        if meta_path.exists() {
            let bytes = fs::read(&meta_path)?;
            let file: CollectionSchemaFile = serde_json::from_slice(&bytes)?;
            let mut indexes = BTreeMap::new();
            for def in file.indexes {
                indexes.insert(def.name.clone(), def);
            }
            return Ok(indexes);
        }

        let prefix = format!("{collection}.");
        let mut definitions = Vec::new();
        for entry in fs::read_dir(&self.base_path)? {
            let entry = entry?;
            let file_name = entry.file_name();
            let Some(file_name) = file_name.to_str() else {
                continue;
            };
            if !file_name.starts_with(&prefix) || !file_name.ends_with(".idx.wt") {
                continue;
            }
            let Some(name) = file_name
                .strip_prefix(&prefix)
                .and_then(|suffix| suffix.strip_suffix(".idx.wt"))
            else {
                continue;
            };
            definitions.push(IndexDefinition {
                name: name.to_string(),
                columns: vec![name.to_string()],
                source: file_name.to_string(),
                key_format: None,
                value_format: None,
            });
        }

        definitions.sort_by(|left, right| left.name.cmp(&right.name));
        let mut indexes = BTreeMap::new();
        for def in definitions {
            indexes.insert(def.name.clone(), def);
        }

        if !indexes.is_empty() {
            self.save_index_definitions(collection, &indexes)?;
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

fn table_uri(collection: &str) -> String {
    format!("{TABLE_URI_PREFIX}{collection}")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parking_lot::RwLock;
    use tempfile::tempdir;

    use super::*;
    use crate::durability::DurabilityBackend;
    use crate::storage::api::Session;
    use crate::storage::handle_cache::HandleCache;
    use crate::storage::metadata_catalog::MetadataCatalog;
    use crate::storage::table::Table;
    use crate::txn::{GlobalTxnState, TransactionManager};

    #[test]
    fn add_index_persists_and_resolves_store_name() {
        let tmp = tempdir().unwrap();
        let base_path = tmp.path().to_path_buf();
        let transaction_manager =
            Arc::new(TransactionManager::new(Arc::new(GlobalTxnState::new())));
        let table_handles = Arc::new(HandleCache::<String, RwLock<Table>>::new());
        let metadata_catalog = Arc::new(MetadataCatalog::new(
            base_path.clone(),
            table_handles.clone(),
            transaction_manager.clone(),
        ));
        let catalog = SchemaCatalog::new(base_path.clone(), metadata_catalog.clone());
        let mut session = Session::new(
            base_path,
            table_handles,
            metadata_catalog,
            transaction_manager,
            Arc::new(DurabilityBackend::Disabled),
        );
        session.create("table:users").unwrap();

        assert_eq!(
            catalog
                .add_index("users", "name", vec!["name".to_string()])
                .unwrap(),
            Some("users.name.idx.wt".to_string())
        );
        assert_eq!(
            catalog.index_definitions("users").unwrap()[0].source,
            "users.name.idx.wt"
        );
        assert_eq!(
            catalog.list_indexes("users").unwrap(),
            vec!["name".to_string()]
        );
    }
}
