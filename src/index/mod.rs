mod key;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::hooks::MutationHooks;
use crate::storage::table::Table;
use crate::txn::TransactionManager;
use crate::txn::TxnId;
use crate::{Document, WrongoDBError};

pub use key::{decode_index_id, encode_index_key, encode_range_bounds, encode_scalar_prefix};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexOpType {
    Add,
    Remove,
}

impl IndexOpType {
    pub fn inverse(self) -> Self {
        match self {
            IndexOpType::Add => IndexOpType::Remove,
            IndexOpType::Remove => IndexOpType::Add,
        }
    }
}

#[derive(Debug, Clone)]
pub struct IndexOpRecord {
    pub index_uri: String,
    pub key: Vec<u8>,
    pub op: IndexOpType,
}

impl IndexOpRecord {
    pub fn add(index_uri: String, key: Vec<u8>) -> Self {
        Self {
            index_uri,
            key,
            op: IndexOpType::Add,
        }
    }

    pub fn remove(index_uri: String, key: Vec<u8>) -> Self {
        Self {
            index_uri,
            key,
            op: IndexOpType::Remove,
        }
    }
}

/// A persistent secondary index backed by a B+tree.
///
/// The index stores composite keys in the format:
///   [scalar_type: 1 byte][scalar_value: variable][id_len: 4 bytes LE][id_bytes]
///
/// The value stored is always empty since the _id is embedded in the key.
/// This allows duplicate values (different _id values = different keys).
#[derive(Debug)]
pub struct PersistentIndex {
    table: Arc<RwLock<Table>>,
}

impl PersistentIndex {
    /// Open an existing index BTree or create a new one if it doesn't exist.
    fn open_or_create(
        path: &Path,
        transaction_manager: Arc<TransactionManager>,
        mutation_hooks: Arc<dyn MutationHooks>,
    ) -> Result<(Self, bool), WrongoDBError> {
        let existed = path.exists();
        let table = Table::open_or_create_index(path, transaction_manager, mutation_hooks)?;
        Ok((
            Self {
                table: Arc::new(RwLock::new(table)),
            },
            !existed,
        ))
    }

    fn table_handle(&self) -> Arc<RwLock<Table>> {
        self.table.clone()
    }

    /// Insert a document's field value into the index.
    fn insert(
        &mut self,
        value: &Value,
        id: &Value,
        txn_id: crate::txn::TxnId,
    ) -> Result<(), WrongoDBError> {
        if let Some(key) = encode_index_key(value, id)? {
            self.insert_raw(&key, txn_id)?;
        }
        Ok(())
    }

    /// Lookup all _id values matching a scalar value using range scan.
    fn lookup(&mut self, value: &Value) -> Result<Vec<Value>, WrongoDBError> {
        let Some((start_key, end_key)) = encode_range_bounds(value) else {
            return Ok(Vec::new());
        };

        let entries = {
            let mut table = self.table.write();
            table.scan_range(Some(&start_key), Some(&end_key), crate::txn::TXN_NONE)?
        };

        let mut ids = Vec::new();
        for (key, _) in entries {
            if let Some(id) = decode_index_id(&key)? {
                ids.push(id);
            }
        }

        Ok(ids)
    }

    fn insert_raw(&mut self, key: &[u8], txn_id: crate::txn::TxnId) -> Result<(), WrongoDBError> {
        let (store_name, mutation_hooks) = {
            let table = self.table.read();
            (table.store_name().to_string(), table.mutation_hooks())
        };
        mutation_hooks.before_put(&store_name, key, &[], txn_id)?;
        if !mutation_hooks.should_apply_locally() {
            return Ok(());
        }
        self.table
            .write()
            .local_apply_put_with_txn(key, &[], txn_id)
    }

    fn remove_raw(&mut self, key: &[u8], txn_id: crate::txn::TxnId) -> Result<(), WrongoDBError> {
        let (store_name, mutation_hooks) = {
            let table = self.table.read();
            (table.store_name().to_string(), table.mutation_hooks())
        };
        mutation_hooks.before_delete(&store_name, key, txn_id)?;
        if !mutation_hooks.should_apply_locally() {
            return Ok(());
        }
        let _ = self
            .table
            .write()
            .local_apply_delete_with_txn(key, txn_id)?;
        Ok(())
    }

    /// Checkpoint the index to durable storage.
    fn checkpoint(&mut self) -> Result<(), WrongoDBError> {
        let mut table = self.table.write();
        table.checkpoint()
    }

    /// Run garbage collection on MVCC update chains.
    /// Returns (chains_cleaned, updates_removed, chains_dropped).
    fn run_gc(&mut self) -> (usize, usize, usize) {
        let mut table = self.table.write();
        table.run_gc()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDefinition {
    pub name: String,
    pub columns: Vec<String>,
    pub source: String,
    pub key_format: Option<String>,
    pub value_format: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct IndexCatalogFile {
    pub collection: String,
    pub indexes: Vec<IndexDefinition>,
}

#[derive(Debug)]
pub struct IndexCatalog {
    collection: String,
    db_dir: PathBuf,
    definitions: BTreeMap<String, IndexDefinition>,
    indexes: BTreeMap<String, PersistentIndex>,
    transaction_manager: Arc<TransactionManager>,
    mutation_hooks: Arc<dyn MutationHooks>,
}

impl IndexCatalog {
    pub fn load_or_init<P: AsRef<Path>>(
        collection: &str,
        db_dir: P,
        transaction_manager: Arc<TransactionManager>,
        mutation_hooks: Arc<dyn MutationHooks>,
    ) -> Result<Self, WrongoDBError> {
        let db_dir = db_dir.as_ref().to_path_buf();
        let meta_path = db_dir.join(format!("{}.meta.json", collection));

        let mut definitions = BTreeMap::new();
        if meta_path.exists() {
            let bytes = fs::read(&meta_path)?;
            let catalog: IndexCatalogFile = serde_json::from_slice(&bytes)?;
            for def in catalog.indexes {
                definitions.insert(def.name.clone(), def);
            }
        } else {
            let prefix = format!("{}.", collection);
            let mut defs = Vec::new();
            for entry in fs::read_dir(&db_dir)? {
                let entry = entry?;
                let file_name = entry.file_name();
                let file_name = match file_name.to_str() {
                    Some(s) => s.to_string(),
                    None => continue,
                };
                if !file_name.starts_with(&prefix) || !file_name.ends_with(".idx.wt") {
                    continue;
                }
                let name = file_name
                    .strip_prefix(&prefix)
                    .and_then(|s| s.strip_suffix(".idx.wt"))
                    .map(|s| s.to_string());
                let Some(name) = name else {
                    continue;
                };
                defs.push(IndexDefinition {
                    name: name.clone(),
                    columns: vec![name],
                    source: file_name,
                    key_format: None,
                    value_format: None,
                });
            }

            defs.sort_by(|a, b| a.name.cmp(&b.name));
            if !defs.is_empty() {
                let file = IndexCatalogFile {
                    collection: collection.to_string(),
                    indexes: defs.clone(),
                };
                let tmp_path = db_dir.join(format!("{}.meta.json.tmp", collection));
                fs::write(&tmp_path, serde_json::to_vec_pretty(&file)?)?;
                fs::rename(&tmp_path, &meta_path)?;
                for def in defs {
                    definitions.insert(def.name.clone(), def);
                }
            }
        }

        let mut indexes = BTreeMap::new();
        for def in definitions.values() {
            let index_path = db_dir.join(&def.source);
            let (index, _created) = PersistentIndex::open_or_create(
                &index_path,
                transaction_manager.clone(),
                mutation_hooks.clone(),
            )?;
            indexes.insert(def.name.clone(), index);
        }

        Ok(Self {
            collection: collection.to_string(),
            db_dir,
            definitions,
            indexes,
            transaction_manager,
            mutation_hooks,
        })
    }

    pub fn empty<P: AsRef<Path>>(
        collection: &str,
        db_dir: P,
        transaction_manager: Arc<TransactionManager>,
        mutation_hooks: Arc<dyn MutationHooks>,
    ) -> Self {
        Self {
            collection: collection.to_string(),
            db_dir: db_dir.as_ref().to_path_buf(),
            definitions: BTreeMap::new(),
            indexes: BTreeMap::new(),
            transaction_manager,
            mutation_hooks,
        }
    }

    pub fn index_defs(&self) -> Vec<IndexDefinition> {
        self.definitions.values().cloned().collect()
    }

    pub fn index_names(&self) -> Vec<String> {
        self.definitions.keys().cloned().collect()
    }

    pub fn index_paths(&self) -> Vec<PathBuf> {
        self.definitions
            .values()
            .map(|d| self.db_dir.join(&d.source))
            .collect()
    }

    pub fn has_index(&self, name: &str) -> bool {
        self.definitions.contains_key(name)
    }

    pub fn index_definition(&self, name: &str) -> Option<&IndexDefinition> {
        self.definitions.get(name)
    }

    pub fn index_handle(&self, name: &str) -> Option<Arc<RwLock<Table>>> {
        self.indexes.get(name).map(|idx| idx.table_handle())
    }

    pub fn add_index(
        &mut self,
        name: &str,
        columns: Vec<String>,
        existing_docs: &[Document],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        if self.definitions.contains_key(name) {
            return Ok(());
        }

        if columns.len() != 1 {
            return Err(crate::core::errors::StorageError(
                "composite indexes are not supported yet".into(),
            )
            .into());
        }

        let index_file = format!("{}.{}.idx.wt", self.collection, name);
        let index_path = self.db_dir.join(&index_file);
        let index_exists = index_path.exists();

        let (mut index, created) = PersistentIndex::open_or_create(
            &index_path,
            self.transaction_manager.clone(),
            self.mutation_hooks.clone(),
        )?;

        if !index_exists || created {
            let field = &columns[0];
            for doc in existing_docs {
                let Some(id) = doc.get("_id") else {
                    continue;
                };
                if let Some(value) = doc.get(field) {
                    index.insert(value, id, txn_id)?;
                }
            }
        }

        let def = IndexDefinition {
            name: name.to_string(),
            columns,
            source: index_file,
            key_format: None,
            value_format: None,
        };
        self.definitions.insert(name.to_string(), def);
        self.indexes.insert(name.to_string(), index);
        self.save()?;
        Ok(())
    }

    pub fn set_mutation_hooks(&mut self, mutation_hooks: Arc<dyn MutationHooks>) {
        self.mutation_hooks = mutation_hooks.clone();
        for index in self.indexes.values_mut() {
            index
                .table
                .write()
                .set_mutation_hooks(mutation_hooks.clone());
        }
    }

    pub fn lookup(&mut self, name: &str, value: &Value) -> Result<Vec<Value>, WrongoDBError> {
        match self.indexes.get_mut(name) {
            Some(index) => index.lookup(value),
            None => Ok(Vec::new()),
        }
    }

    pub fn add_doc(&mut self, doc: &Document, txn_id: TxnId) -> Result<(), WrongoDBError> {
        let Some(id) = doc.get("_id") else {
            return Ok(());
        };

        for (name, def) in &self.definitions {
            let field = match def.columns.first() {
                Some(field) => field,
                None => continue,
            };
            let Some(value) = doc.get(field) else {
                continue;
            };
            let Some(key) = encode_index_key(value, id)? else {
                continue;
            };
            if let Some(index) = self.indexes.get_mut(name) {
                index.insert_raw(&key, txn_id)?;
            }
        }
        Ok(())
    }

    pub fn remove_doc(&mut self, doc: &Document, txn_id: TxnId) -> Result<(), WrongoDBError> {
        let Some(id) = doc.get("_id") else {
            return Ok(());
        };

        for (name, def) in &self.definitions {
            let field = match def.columns.first() {
                Some(field) => field,
                None => continue,
            };
            let Some(value) = doc.get(field) else {
                continue;
            };
            let Some(key) = encode_index_key(value, id)? else {
                continue;
            };
            if let Some(index) = self.indexes.get_mut(name) {
                index.remove_raw(&key, txn_id)?;
            }
        }
        Ok(())
    }

    pub fn apply_key_op(
        &mut self,
        index_name: &str,
        key: &[u8],
        op: IndexOpType,
        txn_id: crate::txn::TxnId,
    ) -> Result<(), WrongoDBError> {
        let Some(index) = self.indexes.get_mut(index_name) else {
            return Ok(());
        };
        match op {
            IndexOpType::Add => index.insert_raw(key, txn_id),
            IndexOpType::Remove => index.remove_raw(key, txn_id),
        }
    }

    pub fn mark_updates_committed(&mut self, txn_id: TxnId) -> Result<(), WrongoDBError> {
        for index in self.indexes.values_mut() {
            let mut table = index.table.write();
            table.mark_updates_committed(txn_id)?;
        }
        Ok(())
    }

    pub fn mark_updates_aborted(&mut self, txn_id: TxnId) -> Result<(), WrongoDBError> {
        for index in self.indexes.values_mut() {
            let mut table = index.table.write();
            table.mark_updates_aborted(txn_id)?;
        }
        Ok(())
    }

    pub fn checkpoint(&mut self) -> Result<(), WrongoDBError> {
        for index in self.indexes.values_mut() {
            index.checkpoint()?;
        }
        Ok(())
    }

    pub fn run_gc(&mut self) -> (usize, usize, usize) {
        let mut total_chains_cleaned = 0;
        let mut total_updates_removed = 0;
        let mut total_chains_dropped = 0;

        for index in self.indexes.values_mut() {
            let (chains_cleaned, updates_removed, chains_dropped) = index.run_gc();
            total_chains_cleaned += chains_cleaned;
            total_updates_removed += updates_removed;
            total_chains_dropped += chains_dropped;
        }

        (
            total_chains_cleaned,
            total_updates_removed,
            total_chains_dropped,
        )
    }

    fn save(&self) -> Result<(), WrongoDBError> {
        let meta_path = self.db_dir.join(format!("{}.meta.json", self.collection));
        let tmp_path = self
            .db_dir
            .join(format!("{}.meta.json.tmp", self.collection));
        let file = IndexCatalogFile {
            collection: self.collection.clone(),
            indexes: self.index_defs(),
        };
        fs::write(&tmp_path, serde_json::to_vec_pretty(&file)?)?;
        fs::rename(&tmp_path, &meta_path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hooks::NoopMutationHooks;
    use crate::txn::TXN_NONE;
    use serde_json::json;
    use tempfile::tempdir;

    #[test]
    fn persistent_index_insert_and_lookup() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("test.idx.wt");
        let transaction_manager = Arc::new(TransactionManager::new(Arc::new(
            crate::txn::GlobalTxnState::new(),
        )));
        let mutation_hooks = Arc::new(NoopMutationHooks) as Arc<dyn MutationHooks>;

        let (mut index, _created) =
            PersistentIndex::open_or_create(&path, transaction_manager, mutation_hooks).unwrap();

        index
            .insert(&json!("alice"), &json!("id1"), TXN_NONE)
            .unwrap();
        index
            .insert(&json!("bob"), &json!("id2"), TXN_NONE)
            .unwrap();
        index
            .insert(&json!("alice"), &json!("id3"), TXN_NONE)
            .unwrap();

        let alice_ids = index.lookup(&json!("alice")).unwrap();
        assert_eq!(alice_ids.len(), 2);
        assert!(alice_ids.contains(&json!("id1")));
        assert!(alice_ids.contains(&json!("id3")));

        let bob_ids = index.lookup(&json!("bob")).unwrap();
        assert_eq!(bob_ids.len(), 1);
        assert!(bob_ids.contains(&json!("id2")));
    }

    #[test]
    fn index_catalog_add_and_lookup() {
        let tmp = tempdir().unwrap();
        let transaction_manager = Arc::new(TransactionManager::new(Arc::new(
            crate::txn::GlobalTxnState::new(),
        )));
        let mutation_hooks = Arc::new(NoopMutationHooks) as Arc<dyn MutationHooks>;
        let mut catalog =
            IndexCatalog::empty("coll", tmp.path(), transaction_manager, mutation_hooks);

        let docs: Vec<Document> = vec![
            serde_json::from_value(json!({"_id": "a1", "name": "alice"})).unwrap(),
            serde_json::from_value(json!({"_id": "b1", "name": "bob"})).unwrap(),
            serde_json::from_value(json!({"_id": "a2", "name": "alice"})).unwrap(),
        ];

        catalog
            .add_index("name", vec!["name".to_string()], &docs, TXN_NONE)
            .unwrap();
        let ids = catalog.lookup("name", &json!("alice")).unwrap();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&json!("a1")));
        assert!(ids.contains(&json!("a2")));
    }
}
