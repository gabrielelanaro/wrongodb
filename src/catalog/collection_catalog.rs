use std::collections::BTreeMap;

use bson::{spec::BinarySubtype, Binary, Bson, Document};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::catalog::{CatalogRecord, CatalogStore, CATALOG_FILE_URI};
use crate::core::errors::StorageError;
use crate::core::{DatabaseName, Namespace};
use crate::storage::api::Session;
use crate::storage::metadata_store::{
    table_uri, MetadataStore, INDEX_URI_PREFIX, TABLE_URI_PREFIX,
};
use crate::WrongoDBError;

/// Normalized `createIndexes` request supported by the durable catalog.
///
/// The current implementation accepts only single-field ascending indexes and
/// fills in MongoDB's default index name when the request omits it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CreateIndexRequest {
    name: String,
    spec: Document,
}

impl CreateIndexRequest {
    /// Normalizes a BSON index spec into the subset supported by WrongoDB.
    pub(crate) fn from_bson_spec(spec: &Document) -> Result<Self, WrongoDBError> {
        let mut normalized = spec.clone();
        let key = normalized
            .get_document("key")
            .map_err(|_| StorageError("createIndexes requires a key document".into()))?
            .clone();
        let field = single_field_ascending_key_field(&key)?;
        let name = normalized
            .get_str("name")
            .map(str::to_string)
            .unwrap_or_else(|_| default_index_name(&field));
        normalized.insert("name", name.clone());

        Ok(Self {
            name,
            spec: normalized,
        })
    }

    #[cfg(test)]
    pub(crate) fn single_field_ascending(field: &str) -> Self {
        Self::from_bson_spec(&bson::doc! { "key": { field: 1 }, "name": default_index_name(field) })
            .expect("single-field test index request")
    }

    /// Builds the normalized single-field ascending request used by oplog apply.
    pub(crate) fn single_field_ascending_named(field: &str, name: &str) -> Self {
        Self::from_bson_spec(&bson::doc! { "key": { field: 1 }, "name": name })
            .expect("single-field ascending oplog request")
    }

    /// Returns the canonical Mongo-visible index name.
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// Returns the single indexed field declared by the request.
    pub(crate) fn indexed_field(&self) -> Result<String, WrongoDBError> {
        let key = self
            .spec
            .get_document("key")
            .map_err(|_| StorageError(format!("index {} is missing a key document", self.name)))?;
        single_field_ascending_key_field(key)
    }
}

/// Server-facing definition of a secondary index persisted in `file:_catalog.wt`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IndexDefinition {
    name: String,
    uri: String,
    spec: Document,
    ready: bool,
}

impl IndexDefinition {
    /// Creates a durable index definition from fully normalized fields.
    pub(crate) fn new(
        name: impl Into<String>,
        uri: impl Into<String>,
        spec: Document,
        ready: bool,
    ) -> Self {
        Self {
            name: name.into(),
            uri: uri.into(),
            spec,
            ready,
        }
    }

    pub(crate) fn from_request_with_ready(
        request: &CreateIndexRequest,
        uri: String,
        ready: bool,
    ) -> Self {
        Self::new(request.name.clone(), uri, request.spec.clone(), ready)
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// Returns the storage-layer `index:` URI for the index.
    pub(crate) fn uri(&self) -> &str {
        &self.uri
    }

    /// Returns the persisted Mongo-visible index specification.
    pub(crate) fn spec(&self) -> &Document {
        &self.spec
    }

    /// Returns the single indexed field declared by the persisted spec.
    pub(crate) fn indexed_field(&self) -> Result<String, WrongoDBError> {
        let key = self
            .spec
            .get_document("key")
            .map_err(|_| StorageError(format!("index {} is missing a key document", self.name)))?;
        single_field_ascending_key_field(key)
    }

    pub(crate) fn ready(&self) -> bool {
        self.ready
    }
}

/// Durable server-side definition of one collection.
///
/// This is the parsed view over one `file:_catalog.wt` row. It carries server-facing
/// collection state such as the UUID, options, and secondary index specs while
/// pointing back to the storage-layer `table:` and `index:` URIs.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CollectionDefinition {
    namespace: Namespace,
    table_uri: String,
    uuid: Binary,
    options: Document,
    storage_columns: Vec<String>,
    indexes: BTreeMap<String, IndexDefinition>,
}

impl CollectionDefinition {
    /// Creates a new collection definition with an empty index set.
    pub(crate) fn new(
        namespace: Namespace,
        table_uri: impl Into<String>,
        storage_columns: Vec<String>,
    ) -> Self {
        Self::new_with_uuid(namespace, table_uri, storage_columns, uuid_binary())
    }

    /// Creates a new collection definition with an explicit UUID.
    pub(crate) fn new_with_uuid(
        namespace: Namespace,
        table_uri: impl Into<String>,
        storage_columns: Vec<String>,
        uuid: Binary,
    ) -> Self {
        Self {
            namespace,
            table_uri: table_uri.into(),
            uuid,
            options: collection_options(&storage_columns),
            storage_columns,
            indexes: BTreeMap::new(),
        }
    }

    /// Creates a new collection definition with a generated storage ident.
    pub(crate) fn new_generated(namespace: Namespace, storage_columns: Vec<String>) -> Self {
        Self::new(
            namespace,
            table_uri(&generated_table_ident()),
            storage_columns,
        )
    }

    /// Creates a new collection definition with an explicit UUID and generated storage ident.
    pub(crate) fn new_generated_with_uuid(
        namespace: Namespace,
        storage_columns: Vec<String>,
        uuid: Binary,
    ) -> Self {
        Self::new_with_uuid(
            namespace,
            table_uri(&generated_table_ident()),
            storage_columns,
            uuid,
        )
    }

    pub(crate) fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    pub(crate) fn db_name(&self) -> &DatabaseName {
        self.namespace.db_name()
    }

    pub(crate) fn collection_name(&self) -> &str {
        self.namespace.collection_name()
    }

    /// Returns the primary storage-layer `table:` URI for the collection.
    pub(crate) fn table_uri(&self) -> &str {
        &self.table_uri
    }

    /// Returns the persisted UUID for the collection.
    pub(crate) fn uuid(&self) -> &Binary {
        &self.uuid
    }

    /// Returns the persisted collection options document.
    pub(crate) fn options(&self) -> &Document {
        &self.options
    }

    pub(crate) fn storage_columns(&self) -> &[String] {
        &self.storage_columns
    }

    /// Returns the durable secondary indexes keyed by index name.
    pub(crate) fn indexes(&self) -> &BTreeMap<String, IndexDefinition> {
        &self.indexes
    }

    /// Adds a secondary index definition when it does not already exist.
    pub(crate) fn add_index(&mut self, index: IndexDefinition) -> bool {
        let name = index.name.clone();
        if self.indexes.contains_key(&name) {
            return false;
        }

        self.indexes.insert(name, index);
        true
    }

    /// Updates the ready state for one named secondary index.
    pub(crate) fn mark_index_ready(
        &mut self,
        index_name: &str,
        ready: bool,
    ) -> Result<(), WrongoDBError> {
        let Some(index) = self.indexes.get_mut(index_name) else {
            return Err(StorageError(format!(
                "unknown index {index_name} on collection {}",
                self.namespace.full_name()
            ))
            .into());
        };

        index.ready = ready;
        Ok(())
    }
}

/// Collection catalog with durable storage and in-memory cache.
///
/// This struct combines:
/// - Durable storage operations on `file:_catalog.wt`
/// - In-memory cache for fast lookups
/// - Auto-refresh after successful writes
#[derive(Debug, Default)]
pub(crate) struct CollectionCatalog {
    store: CatalogStore,
    collections: RwLock<BTreeMap<Namespace, CollectionDefinition>>,
}

impl CollectionCatalog {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    /// Creates the collection catalog over the raw `file:_catalog.wt` store.
    pub(crate) fn new(store: CatalogStore) -> Self {
        Self {
            store,
            collections: RwLock::new(BTreeMap::new()),
        }
    }

    // ------------------------------------------------------------------------
    // Bootstrap
    // ------------------------------------------------------------------------

    /// Ensures that the metadata-managed `file:_catalog.wt` object exists.
    pub(crate) fn ensure_store_exists(&self, session: &mut Session) -> Result<(), WrongoDBError> {
        session.create_file(CATALOG_FILE_URI)
    }

    // ------------------------------------------------------------------------
    // Cache Operations
    // ------------------------------------------------------------------------

    /// Loads the in-memory cache from durable storage.
    pub(crate) fn load_cache(&self, session: &Session) -> Result<(), WrongoDBError> {
        let collections = self
            .list_collections(session)?
            .into_iter()
            .map(|collection| (collection.namespace().clone(), collection))
            .collect();
        *self.collections.write() = collections;
        Ok(())
    }

    /// Refreshes one cache entry after a successful write.
    fn refresh_cache_entry(
        &self,
        session: &Session,
        namespace: &Namespace,
    ) -> Result<(), WrongoDBError> {
        let mut cache = self.collections.write();
        match self.get_collection(session, namespace)? {
            Some(definition) => {
                cache.insert(namespace.clone(), definition);
            }
            None => {
                cache.remove(namespace);
            }
        }
        Ok(())
    }

    /// Lists the collection names from cache for one database.
    pub(crate) fn list_collection_names(&self, db_name: &DatabaseName) -> Vec<String> {
        self.collections
            .read()
            .values()
            .filter(|definition| definition.db_name() == db_name)
            .map(|definition| definition.collection_name().to_string())
            .collect()
    }

    /// Looks up a collection definition from cache.
    pub(crate) fn lookup_collection(&self, namespace: &Namespace) -> Option<CollectionDefinition> {
        self.collections.read().get(namespace).cloned()
    }

    /// Returns the index definitions for a collection from cache.
    pub(crate) fn list_index_definitions(&self, namespace: &Namespace) -> Vec<IndexDefinition> {
        self.collections
            .read()
            .get(namespace)
            .map(|definition| definition.indexes().values().cloned().collect())
            .unwrap_or_default()
    }

    /// Lists the known databases from cache.
    pub(crate) fn list_database_names(&self) -> Vec<DatabaseName> {
        let mut names = self
            .collections
            .read()
            .keys()
            .map(|namespace| namespace.db_name().clone())
            .collect::<Vec<_>>();
        names.sort();
        names.dedup();
        names
    }

    // ------------------------------------------------------------------------
    // Read API
    // ------------------------------------------------------------------------

    /// Loads a collection definition from the catalog.
    pub(crate) fn get_collection(
        &self,
        session: &Session,
        namespace: &Namespace,
    ) -> Result<Option<CollectionDefinition>, WrongoDBError> {
        self.store
            .get_record(session, &namespace.full_name())?
            .map(|record| collection_definition_from_record(namespace.full_name(), record))
            .transpose()
    }

    /// Lists all collection definitions from the catalog.
    pub(crate) fn list_collections(
        &self,
        session: &Session,
    ) -> Result<Vec<CollectionDefinition>, WrongoDBError> {
        self.store
            .list_records(session)?
            .into_iter()
            .map(|(collection, record)| collection_definition_from_record(collection, record))
            .collect()
    }

    // ------------------------------------------------------------------------
    // Write API
    // ------------------------------------------------------------------------

    /// Creates one durable collection entry if it does not already exist.
    pub(crate) fn create_collection(
        &self,
        session: &Session,
        definition: &CollectionDefinition,
    ) -> Result<bool, WrongoDBError> {
        let namespace = definition.namespace();
        if self.get_collection(session, namespace)?.is_some() {
            return Ok(false);
        }

        self.put_collection(session, definition)?;
        Ok(true)
    }

    /// Persists one collection entry as the current catalog state.
    pub(crate) fn put_collection(
        &self,
        session: &Session,
        definition: &CollectionDefinition,
    ) -> Result<(), WrongoDBError> {
        let namespace = definition.namespace();
        let record = catalog_record_from_collection_definition(definition)?;
        self.store
            .put_record(session, &namespace.full_name(), &record)?;
        self.refresh_cache_entry(session, namespace)?;
        Ok(())
    }

    // ------------------------------------------------------------------------
    // Validation
    // ------------------------------------------------------------------------

    /// Verifies that every durable catalog reference points at a storage metadata row.
    pub(crate) fn validate_storage_references(
        &self,
        session: &Session,
        metadata_store: &MetadataStore,
    ) -> Result<(), WrongoDBError> {
        for collection in self.list_collections(session)? {
            validate_collection_references(&collection, metadata_store)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableMetadataRecord {
    uuid: Binary,
    #[serde(default)]
    options: Document,
    storage_columns: Vec<String>,
    #[serde(default)]
    indexes: Vec<DurableIndexMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableIndexMetadata {
    spec: Document,
    #[serde(default)]
    ready: bool,
}

fn collection_definition_from_record(
    namespace_key: String,
    record: CatalogRecord,
) -> Result<CollectionDefinition, WrongoDBError> {
    let namespace = Namespace::parse(&namespace_key)?;
    if !record.table_uri.starts_with(TABLE_URI_PREFIX) {
        return Err(StorageError(format!(
            "catalog row for {namespace_key} has invalid table URI: {}",
            record.table_uri
        ))
        .into());
    }

    let metadata: DurableMetadataRecord = bson::from_document(record.md)?;
    let mut indexes = BTreeMap::new();
    for index_metadata in metadata.indexes {
        let name = index_spec_name(&index_metadata.spec)?;
        let uri = record.index_uris.get(&name).cloned().ok_or_else(|| {
            StorageError(format!(
                "catalog row for {namespace_key} is missing index URI for {name}"
            ))
        })?;
        if !uri.starts_with(INDEX_URI_PREFIX) {
            return Err(StorageError(format!(
                "catalog row for {namespace_key} has invalid index URI: {uri}"
            ))
            .into());
        }
        indexes.insert(
            name.clone(),
            IndexDefinition::new(name, uri, index_metadata.spec, index_metadata.ready),
        );
    }

    for (name, uri) in &record.index_uris {
        if !uri.starts_with(INDEX_URI_PREFIX) {
            return Err(StorageError(format!(
                "catalog row for {namespace_key} has invalid index URI: {uri}"
            ))
            .into());
        }
        if !indexes.contains_key(name) {
            return Err(StorageError(format!(
                "catalog row for {namespace_key} has orphan index URI mapping for {name}"
            ))
            .into());
        }
    }

    Ok(CollectionDefinition {
        namespace,
        table_uri: record.table_uri,
        uuid: metadata.uuid,
        options: metadata.options,
        storage_columns: metadata.storage_columns,
        indexes,
    })
}

fn catalog_record_from_collection_definition(
    definition: &CollectionDefinition,
) -> Result<CatalogRecord, WrongoDBError> {
    let metadata = DurableMetadataRecord {
        uuid: definition.uuid.clone(),
        options: definition.options.clone(),
        storage_columns: definition.storage_columns.clone(),
        indexes: definition
            .indexes
            .values()
            .map(|index| DurableIndexMetadata {
                spec: index.spec.clone(),
                ready: index.ready,
            })
            .collect(),
    };

    Ok(CatalogRecord {
        table_uri: definition.table_uri.clone(),
        index_uris: definition
            .indexes
            .iter()
            .map(|(name, index)| (name.clone(), index.uri.clone()))
            .collect(),
        md: bson::to_document(&metadata)?,
    })
}

fn validate_collection_references(
    collection: &CollectionDefinition,
    metadata_store: &MetadataStore,
) -> Result<(), WrongoDBError> {
    if metadata_store.get(collection.table_uri())?.is_none() {
        return Err(StorageError(format!(
            "catalog collection {} references missing table URI {}",
            collection.namespace().full_name(),
            collection.table_uri()
        ))
        .into());
    }

    for index in collection.indexes().values() {
        if metadata_store.get(index.uri())?.is_none() {
            return Err(StorageError(format!(
                "catalog collection {} references missing index URI {}",
                collection.namespace().full_name(),
                index.uri()
            ))
            .into());
        }
    }

    Ok(())
}

fn uuid_binary() -> Binary {
    Binary {
        subtype: BinarySubtype::Uuid,
        bytes: Uuid::new_v4().as_bytes().to_vec(),
    }
}

fn index_spec_name(spec: &Document) -> Result<String, WrongoDBError> {
    spec.get_str("name")
        .map(str::to_string)
        .map_err(|_| StorageError("index spec is missing name".into()).into())
}

fn default_index_name(field: &str) -> String {
    format!("{field}_1")
}

fn generated_table_ident() -> String {
    format!("c_{}", Uuid::new_v4().simple())
}

fn collection_options(storage_columns: &[String]) -> Document {
    let mut options = Document::new();
    options.insert(
        "storageColumns",
        Bson::Array(
            storage_columns
                .iter()
                .map(|column| Bson::String(column.clone()))
                .collect(),
        ),
    );
    options
}

fn single_field_ascending_key_field(key: &Document) -> Result<String, WrongoDBError> {
    if key.len() != 1 {
        return Err(
            StorageError("only single-field ascending indexes are supported".into()).into(),
        );
    }

    let (field, direction) = key.iter().next().expect("checked length");
    if !is_ascending_one(direction) {
        return Err(StorageError(format!(
            "only single-field ascending indexes are supported, got key: {:?}",
            key
        ))
        .into());
    }

    Ok(field.to_string())
}

#[cfg(test)]
mod request_tests {
    use tempfile::tempdir;

    use super::{CollectionCatalog, CollectionDefinition, CreateIndexRequest, IndexDefinition};
    use crate::catalog::CatalogStore;
    use crate::core::Namespace;
    use crate::storage::api::{Connection, ConnectionConfig};

    fn namespace(name: &str) -> Namespace {
        Namespace::parse(&format!("test.{name}")).expect("valid test namespace")
    }

    // EARS: When the caller creates a durable collection entry inside its own
    // transaction, the catalog shall create it if absent and report false on a
    // second create without opening its own transaction.
    #[test]
    fn create_collection_is_create_if_absent() {
        let dir = tempdir().unwrap();
        let connection = Connection::open(dir.path(), ConnectionConfig::default()).unwrap();
        let catalog = CollectionCatalog::new(CatalogStore::new());
        let mut session = connection.open_session();
        catalog.ensure_store_exists(&mut session).unwrap();

        let definition =
            CollectionDefinition::new_generated(namespace("users"), vec!["name".to_string()]);
        session
            .create_table(definition.table_uri(), vec!["name".to_string()])
            .unwrap();
        session
            .with_transaction(|session| catalog.create_collection(session, &definition))
            .unwrap();

        let created_again = session
            .with_transaction(|session| catalog.create_collection(session, &definition))
            .unwrap();

        assert!(!created_again);
    }

    // EARS: When the caller persists an updated collection state, the catalog
    // shall replace the durable row and refresh cache-visible index readiness.
    #[test]
    fn put_collection_replaces_current_state() {
        let dir = tempdir().unwrap();
        let connection = Connection::open(dir.path(), ConnectionConfig::default()).unwrap();
        let catalog = CollectionCatalog::new(CatalogStore::new());
        let mut session = connection.open_session();
        catalog.ensure_store_exists(&mut session).unwrap();

        let mut definition =
            CollectionDefinition::new_generated(namespace("users"), vec!["name".to_string()]);
        session
            .create_table(definition.table_uri(), vec!["name".to_string()])
            .unwrap();
        session
            .with_transaction(|session| catalog.create_collection(session, &definition))
            .unwrap();

        let request = CreateIndexRequest::single_field_ascending("name");
        let index = IndexDefinition::from_request_with_ready(
            &request,
            "index:users:name_1".to_string(),
            false,
        );
        assert!(definition.add_index(index));
        definition.mark_index_ready("name_1", true).unwrap();

        session
            .with_transaction(|session| catalog.put_collection(session, &definition))
            .unwrap();

        let stored = catalog
            .get_collection(&session, &namespace("users"))
            .unwrap()
            .unwrap();
        assert!(stored.indexes().get("name_1").unwrap().ready());
        assert!(catalog
            .lookup_collection(&namespace("users"))
            .unwrap()
            .indexes()
            .get("name_1")
            .unwrap()
            .ready());
    }
}

fn is_ascending_one(value: &Bson) -> bool {
    match value {
        Bson::Int32(v) => *v == 1,
        Bson::Int64(v) => *v == 1,
        Bson::Double(v) => (*v - 1.0).abs() < f64::EPSILON,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use bson::doc;

    use super::*;

    // EARS: When `createIndexes` omits the index name, catalog normalization
    // shall synthesize MongoDB's default `<field>_1` index name.
    #[test]
    fn create_index_request_normalizes_missing_name() {
        let request = CreateIndexRequest::from_bson_spec(&doc! { "key": { "name": 1 } }).unwrap();

        assert_eq!(request.name(), "name_1");
        assert_eq!(request.spec.get_str("name").unwrap(), "name_1");
    }

    // EARS: When `createIndexes` requests an unsupported key shape, catalog
    // normalization shall reject the request.
    #[test]
    fn create_index_request_rejects_unsupported_specs() {
        let err =
            CreateIndexRequest::from_bson_spec(&doc! { "key": { "a": 1, "b": 1 } }).unwrap_err();
        assert!(err
            .to_string()
            .contains("only single-field ascending indexes are supported"));

        let err = CreateIndexRequest::from_bson_spec(&doc! { "key": { "a": -1 } }).unwrap_err();
        assert!(err
            .to_string()
            .contains("only single-field ascending indexes are supported"));
    }
}
