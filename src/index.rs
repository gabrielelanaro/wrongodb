use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::index_key::{decode_index_id, encode_index_key, encode_range_bounds};
use crate::{BTree, Document, WrongoDBError};

/// A persistent secondary index backed by a B+tree.
///
/// The index stores composite keys in the format:
///   [scalar_type: 1 byte][scalar_value: variable][id_len: 4 bytes LE][id_bytes]
///
/// The value stored is always empty since the _id is embedded in the key.
/// This allows duplicate values (different _id values = different keys).
#[derive(Debug)]
pub struct PersistentIndex {
    pub field: String,
    pub btree: BTree,
}

impl PersistentIndex {
    /// Open an existing index BTree or create a new one if it doesn't exist.
    fn open_or_create(
        path: &Path,
        field: &str,
        wal_enabled: bool,
    ) -> Result<(Self, bool), WrongoDBError> {
        let mut created = false;
        let btree = if path.exists() {
            match BTree::open(path, wal_enabled) {
                Ok(tree) => tree,
                Err(_) => {
                    std::fs::remove_file(path)?;
                    created = true;
                    BTree::create(path, 4096, wal_enabled)?
                }
            }
        } else {
            created = true;
            BTree::create(path, 4096, wal_enabled)?
        };

        Ok((
            Self {
            field: field.to_string(),
            btree,
            },
            created,
        ))
    }

    /// Insert a document's field value into the index.
    fn insert(&mut self, value: &Value, id: &Value) -> Result<(), WrongoDBError> {
        if let Some(key) = encode_index_key(value, id)? {
            // Value is empty since _id is embedded in key
            self.btree.put(&key, &[])?;
        }
        Ok(())
    }

    /// Remove a document's field value from the index.
    fn remove(&mut self, value: &Value, id: &Value) -> Result<(), WrongoDBError> {
        if let Some(key) = encode_index_key(value, id)? {
            let _ = self.btree.delete(&key)?;
        }
        Ok(())
    }

    /// Lookup all _id values matching a scalar value using range scan.
    fn lookup(&mut self, value: &Value) -> Result<Vec<Value>, WrongoDBError> {
        let Some((start_key, end_key)) = encode_range_bounds(value) else {
            return Ok(Vec::new());
        };

        let mut ids = Vec::new();
        let iter = self.btree.range(Some(&start_key), Some(&end_key))?;

        for result in iter {
            let (key, _) = result?;
            if let Some(id) = decode_index_id(&key)? {
                ids.push(id);
            }
        }

        Ok(ids)
    }

    /// Checkpoint the index to durable storage.
    fn checkpoint(&mut self) -> Result<(), WrongoDBError> {
        self.btree.checkpoint()
    }
}

/// Manages persistent secondary indexes for a collection.
///
/// Each indexed field has its own B+tree stored in a separate file:
///   {collection_path}.{field_name}.idx.wt
#[derive(Debug)]
pub struct SecondaryIndexManager {
    pub fields: HashSet<String>,
    persistent: HashMap<String, PersistentIndex>,
    collection_path: PathBuf,
    wal_enabled: bool,
}

impl SecondaryIndexManager {
    /// Open existing indexes or create and build new ones from existing documents.
    ///
    /// For each field in `fields`:
    /// - If `{collection_path}.{field}.idx.wt` exists, open it
    /// - Otherwise, create it and build from all non-deleted documents
    pub fn open_or_rebuild<P, I, S>(
        collection_path: P,
        fields: I,
        wal_enabled: bool,
        documents: &[Document],
    ) -> Result<Self, WrongoDBError>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let collection_path = collection_path.as_ref().to_path_buf();
        let field_set: HashSet<String> = fields.into_iter().map(Into::into).collect();
        let mut persistent = HashMap::new();

        for field in &field_set {
            let index_path = Self::index_path(&collection_path, field);

            // Check if index exists BEFORE creating/opening
            let index_exists = index_path.exists();

            let (mut index, created) =
                PersistentIndex::open_or_create(&index_path, field, wal_enabled)?;

            // If the index file was just created, build it from existing documents
            if !index_exists || created {
                for doc in documents {
                    let Some(id) = doc.get("_id") else {
                        continue;
                    };
                    if let Some(value) = doc.get(field) {
                        index.insert(value, id)?;
                    }
                }
            }

            persistent.insert(field.clone(), index);
        }

        Ok(Self {
            fields: field_set,
            persistent,
            collection_path,
            wal_enabled,
        })
    }

    /// Create an empty manager with no indexes (for new collections).
    pub fn empty<P: AsRef<Path>>(collection_path: P, wal_enabled: bool) -> Self {
        Self {
            fields: HashSet::new(),
            persistent: HashMap::new(),
            collection_path: collection_path.as_ref().to_path_buf(),
            wal_enabled,
        }
    }

    /// Get the file path for a field's index.
    fn index_path(collection_path: &Path, field: &str) -> PathBuf {
        PathBuf::from(format!("{}.{field}.idx.wt", collection_path.display()))
    }

    /// Insert a document into all indexes.
    pub fn add(&mut self, doc: &Document) -> Result<(), WrongoDBError> {
        let Some(id) = doc.get("_id") else {
            return Ok(());
        };
        for field in self.fields.iter() {
            if let Some(value) = doc.get(field) {
                if let Some(index) = self.persistent.get_mut(field) {
                    index.insert(value, id)?;
                }
            }
        }
        Ok(())
    }

    /// Remove a document from all indexes.
    pub fn remove(&mut self, doc: &Document) -> Result<(), WrongoDBError> {
        let Some(id) = doc.get("_id") else {
            return Ok(());
        };
        for field in self.fields.iter() {
            if let Some(value) = doc.get(field) {
                if let Some(index) = self.persistent.get_mut(field) {
                    index.remove(value, id)?;
                }
            }
        }
        Ok(())
    }

    /// Lookup _id values for a specific field and value.
    pub fn lookup(&mut self, field: &str, value: &Value) -> Result<Vec<Value>, WrongoDBError> {
        match self.persistent.get_mut(field) {
            Some(index) => index.lookup(value),
            None => Ok(Vec::new()),
        }
    }

    /// Add a new index field and build it from existing documents.
    ///
    /// Creates a new BTree index file and populates it from all documents.
    pub fn add_field(
        &mut self,
        field: &str,
        existing_docs: &[Document],
    ) -> Result<(), WrongoDBError> {
        if self.fields.contains(field) {
            return Ok(()); // Already indexed
        }

        let index_path = Self::index_path(&self.collection_path, field);
        let (mut index, _created) =
            PersistentIndex::open_or_create(&index_path, field, self.wal_enabled)?;

        // Build index from existing documents
        for doc in existing_docs {
            let Some(id) = doc.get("_id") else {
                continue;
            };
            if let Some(value) = doc.get(field) {
                index.insert(value, id)?;
            }
        }

        self.fields.insert(field.to_string());
        self.persistent.insert(field.to_string(), index);

        Ok(())
    }

    /// Checkpoint all indexes to durable storage.
    pub fn checkpoint(&mut self) -> Result<(), WrongoDBError> {
        for index in self.persistent.values_mut() {
            index.checkpoint()?;
        }
        Ok(())
    }

    /// Get the paths of all index files (for cleanup/verification).
    pub fn index_paths(&self) -> Vec<PathBuf> {
        self.fields
            .iter()
            .map(|f| Self::index_path(&self.collection_path, f))
            .collect()
    }
}

/// Legacy in-memory index for backward compatibility during migration.
///
/// This is kept temporarily for any code that might reference it,
/// but new code should use SecondaryIndexManager instead.
#[derive(Debug, Clone)]
pub struct InMemoryIndex {
    pub fields: HashSet<String>,
    index: HashMap<String, HashMap<ScalarKey, HashSet<u64>>>,
}

/// A scalar value that can be used as an index key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScalarKey {
    Null,
    Bool(bool),
    Number(String),
    String(String),
}

impl ScalarKey {
    pub fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Null => Some(ScalarKey::Null),
            Value::Bool(b) => Some(ScalarKey::Bool(*b)),
            Value::Number(n) => Some(ScalarKey::Number(n.to_string())),
            Value::String(s) => Some(ScalarKey::String(s.clone())),
            Value::Array(_) | Value::Object(_) => None,
        }
    }
}

impl InMemoryIndex {
    pub fn new<I, S>(fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let field_set: HashSet<String> = fields.into_iter().map(Into::into).collect();
        let index = field_set
            .iter()
            .map(|f| (f.clone(), HashMap::<ScalarKey, HashSet<u64>>::new()))
            .collect();
        Self {
            fields: field_set,
            index,
        }
    }

    pub fn add(&mut self, doc: &serde_json::Map<String, Value>, offset: u64) {
        for field in self.fields.iter() {
            let Some(value) = doc.get(field) else {
                continue;
            };
            let Some(key) = ScalarKey::from_value(value) else {
                continue;
            };
            self.index
                .entry(field.clone())
                .or_default()
                .entry(key)
                .or_default()
                .insert(offset);
        }
    }

    pub fn lookup(&self, field: &str, value: &Value) -> Vec<u64> {
        let Some(field_index) = self.index.get(field) else {
            return Vec::new();
        };
        let Some(key) = ScalarKey::from_value(value) else {
            return Vec::new();
        };
        field_index
            .get(&key)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn remove(&mut self, doc: &serde_json::Map<String, Value>, offset: u64) {
        for field in self.fields.iter() {
            let Some(value) = doc.get(field) else {
                continue;
            };
            let Some(key) = ScalarKey::from_value(value) else {
                continue;
            };
            if let Some(field_index) = self.index.get_mut(field) {
                if let Some(offsets) = field_index.get_mut(&key) {
                    offsets.remove(&offset);
                }
            }
        }
    }

    pub fn clear(&mut self) {
        for values in self.index.values_mut() {
            values.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    #[test]
    fn persistent_index_insert_and_lookup() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("test.idx.wt");

        let (mut index, _created) = PersistentIndex::open_or_create(&path, "name", false).unwrap();

        // Insert some values
        index.insert(&json!("alice"), &json!("id1")).unwrap();
        index.insert(&json!("bob"), &json!("id2")).unwrap();
        index.insert(&json!("alice"), &json!("id3")).unwrap(); // Duplicate value

        // Lookup
        let alice_ids = index.lookup(&json!("alice")).unwrap();
        assert_eq!(alice_ids.len(), 2);
        assert!(alice_ids.contains(&json!("id1")));
        assert!(alice_ids.contains(&json!("id3")));

        let bob_ids = index.lookup(&json!("bob")).unwrap();
        assert_eq!(bob_ids.len(), 1);
        assert!(bob_ids.contains(&json!("id2")));

        let carol_ids = index.lookup(&json!("carol")).unwrap();
        assert!(carol_ids.is_empty());
    }

    #[test]
    fn persistent_index_remove() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("test.idx.wt");

        let (mut index, _created) = PersistentIndex::open_or_create(&path, "name", false).unwrap();

        index.insert(&json!("alice"), &json!("id1")).unwrap();
        index.insert(&json!("alice"), &json!("id2")).unwrap();

        // Remove one
        index.remove(&json!("alice"), &json!("id1")).unwrap();

        let ids = index.lookup(&json!("alice")).unwrap();
        assert_eq!(ids.len(), 1);
        assert!(ids.contains(&json!("id2")));
    }

    #[test]
    fn persistent_index_checkpoint_and_reopen() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("test.idx.wt");

        // Create and populate index
        {
            let (mut index, _created) =
                PersistentIndex::open_or_create(&path, "name", false).unwrap();
            index.insert(&json!("alice"), &json!("id1")).unwrap();
            index.insert(&json!("bob"), &json!("id2")).unwrap();
            index.checkpoint().unwrap();
        }

        // Reopen and verify
        {
            let (mut index, _created) =
                PersistentIndex::open_or_create(&path, "name", false).unwrap();
            let alice_ids = index.lookup(&json!("alice")).unwrap();
            assert_eq!(alice_ids.len(), 1);
            assert!(alice_ids.contains(&json!("id1")));
        }
    }

    #[test]
    fn secondary_index_manager_open_or_rebuild() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("collection.db");

        // Simulate existing documents
        let docs: Vec<Document> = vec![
            serde_json::from_value(json!({"_id": "a1", "name": "alice", "age": 30})).unwrap(),
            serde_json::from_value(json!({"_id": "b1", "name": "bob", "age": 25})).unwrap(),
            serde_json::from_value(json!({"_id": "a2", "name": "alice", "age": 35})).unwrap(),
        ];

        let mut manager =
            SecondaryIndexManager::open_or_rebuild(&path, ["name", "age"], false, &docs).unwrap();

        // Test lookups
        let name_ids = manager.lookup("name", &json!("alice")).unwrap();
        assert_eq!(name_ids.len(), 2);
        assert!(name_ids.contains(&json!("a1")));
        assert!(name_ids.contains(&json!("a2")));

        let age_ids = manager.lookup("age", &json!(25)).unwrap();
        assert_eq!(age_ids.len(), 1);
        assert!(age_ids.contains(&json!("b1")));
    }

    #[test]
    fn secondary_index_manager_add_field() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("collection.db");

        let docs: Vec<Document> = vec![
            serde_json::from_value(json!({"_id": "a1", "name": "alice", "city": "nyc"})).unwrap(),
            serde_json::from_value(json!({"_id": "b1", "name": "bob", "city": "la"})).unwrap(),
        ];

        // Start with just name index
        let mut manager =
            SecondaryIndexManager::open_or_rebuild(&path, ["name"], false, &docs).unwrap();

        // Add city index
        manager.add_field("city", &docs).unwrap();

        // Verify city index works
        let city_ids = manager.lookup("city", &json!("nyc")).unwrap();
        assert_eq!(city_ids.len(), 1);
        assert!(city_ids.contains(&json!("a1")));
    }
}
