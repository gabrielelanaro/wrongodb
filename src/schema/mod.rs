use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::core::errors::StorageError;
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
    collection: String,
    primary_store: String,
    indexes: BTreeMap<String, IndexDefinition>,
}

impl CollectionSchema {
    pub(crate) fn primary_store(&self) -> &str {
        &self.primary_store
    }

    pub(crate) fn index_names(&self) -> Vec<String> {
        self.indexes.keys().cloned().collect()
    }

    pub(crate) fn has_index(&self, name: &str) -> bool {
        self.indexes.contains_key(name)
    }

    pub(crate) fn index_store(&self, name: &str) -> Option<&str> {
        self.indexes.get(name).map(|def| def.source.as_str())
    }

    pub(crate) fn index_definitions(&self) -> Vec<IndexDefinition> {
        self.indexes.values().cloned().collect()
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
}

impl SchemaCatalog {
    pub(crate) fn new(base_path: PathBuf) -> Self {
        Self { base_path }
    }

    pub(crate) fn primary_store_name(&self, collection: &str) -> String {
        format!("{collection}.main.wt")
    }

    pub(crate) fn collection_schema(
        &self,
        collection: &str,
    ) -> Result<CollectionSchema, WrongoDBError> {
        let indexes = self.load_index_definitions(collection)?;
        Ok(CollectionSchema {
            collection: collection.to_string(),
            primary_store: self.primary_store_name(collection),
            indexes,
        })
    }

    pub(crate) fn resolve_uri(&self, uri: &str) -> Result<String, WrongoDBError> {
        if let Some(collection) = uri.strip_prefix("table:") {
            return Ok(self.primary_store_name(collection));
        }

        if let Some(rest) = uri.strip_prefix("index:") {
            let mut parts = rest.splitn(2, ':');
            let collection = parts.next().unwrap_or("");
            let index_name = parts.next().unwrap_or("");
            if collection.is_empty() || index_name.is_empty() {
                return Err(StorageError(format!("invalid index URI: {uri}")).into());
            }
            return self
                .collection_schema(collection)?
                .index_store(index_name)
                .map(ToOwned::to_owned)
                .ok_or_else(|| StorageError(format!("unknown index: {uri}")).into());
        }

        Err(StorageError(format!("unsupported URI: {uri}")).into())
    }

    pub(crate) fn list_indexes(&self, collection: &str) -> Result<Vec<String>, WrongoDBError> {
        Ok(self.collection_schema(collection)?.index_names())
    }

    pub(crate) fn list_collections(&self) -> Result<Vec<String>, WrongoDBError> {
        let mut names = Vec::new();
        for entry in fs::read_dir(&self.base_path)? {
            let entry = entry?;
            let file_name = entry.file_name();
            let Some(file_name) = file_name.to_str() else {
                continue;
            };
            if let Some(name) = file_name.strip_suffix(".main.wt") {
                names.push(name.to_string());
            }
        }
        names.sort();
        names.dedup();
        Ok(names)
    }

    pub(crate) fn all_store_names(&self) -> Result<Vec<String>, WrongoDBError> {
        let mut names = Vec::new();
        for entry in fs::read_dir(&self.base_path)? {
            let entry = entry?;
            let file_name = entry.file_name();
            let Some(file_name) = file_name.to_str() else {
                continue;
            };
            if file_name.ends_with(".wt") {
                names.push(file_name.to_string());
            }
        }
        names.sort();
        names.dedup();
        Ok(names)
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

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn add_index_persists_and_resolves_store_name() {
        let tmp = tempdir().unwrap();
        let catalog = SchemaCatalog::new(tmp.path().to_path_buf());

        assert_eq!(
            catalog
                .add_index("users", "name", vec!["name".to_string()])
                .unwrap(),
            Some("users.name.idx.wt".to_string())
        );
        assert_eq!(
            catalog.resolve_uri("index:users:name").unwrap(),
            "users.name.idx.wt"
        );
        assert_eq!(
            catalog.list_indexes("users").unwrap(),
            vec!["name".to_string()]
        );
    }
}
