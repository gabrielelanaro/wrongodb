use std::collections::BTreeMap;

use bson::{spec::BinarySubtype, Binary, Bson, Document};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::catalog::{CatalogRecord, CatalogStore};
use crate::core::errors::StorageError;
use crate::storage::api::Session;
use crate::storage::metadata_store::{MetadataStore, INDEX_URI_PREFIX, TABLE_URI_PREFIX};
use crate::txn::{TxnId, TXN_NONE};
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

/// Server-facing definition of a secondary index persisted in `_catalog.wt`.
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
/// This is the parsed view over one `_catalog.wt` row. It carries server-facing
/// collection state such as the UUID, options, and secondary index specs while
/// pointing back to the storage-layer `table:` and `index:` URIs.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CollectionDefinition {
    name: String,
    table_uri: String,
    uuid: Binary,
    options: Document,
    storage_columns: Vec<String>,
    indexes: BTreeMap<String, IndexDefinition>,
}

impl CollectionDefinition {
    /// Creates a new collection definition with an empty index set.
    pub(crate) fn new(
        name: impl Into<String>,
        table_uri: impl Into<String>,
        storage_columns: Vec<String>,
    ) -> Self {
        Self {
            name: name.into(),
            table_uri: table_uri.into(),
            uuid: uuid_binary(),
            options: collection_options(&storage_columns),
            storage_columns,
            indexes: BTreeMap::new(),
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
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
}

/// Parsed durable collection catalog stored in the reserved `_catalog.wt`.
///
/// `DurableCatalog` sits above [`CatalogStore`]. It translates raw catalog rows
/// into collection and index definitions, validates their invariants, and keeps
/// the server-facing catalog separate from the storage metadata in
/// [`MetadataStore`].
#[derive(Debug, Clone, Default)]
pub(crate) struct DurableCatalog {
    store: CatalogStore,
}

impl DurableCatalog {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    /// Creates the durable catalog over the raw `_catalog.wt` store.
    pub(crate) fn new(store: CatalogStore) -> Self {
        Self { store }
    }

    // ------------------------------------------------------------------------
    // Bootstrap
    // ------------------------------------------------------------------------

    /// Ensures that the reserved `_catalog.wt` store exists.
    pub(crate) fn ensure_store_exists(&self, session: &Session) -> Result<(), WrongoDBError> {
        self.store.ensure_store_exists(session)
    }

    // ------------------------------------------------------------------------
    // Read API
    // ------------------------------------------------------------------------

    /// Loads the collection definition visible to `txn_id`.
    pub(crate) fn collection_for_txn(
        &self,
        session: &Session,
        collection: &str,
        txn_id: TxnId,
    ) -> Result<Option<CollectionDefinition>, WrongoDBError> {
        self.store
            .record_for_txn(session, collection, txn_id)?
            .map(|record| collection_definition_from_record(collection.to_string(), record))
            .transpose()
    }

    pub(crate) fn collection(
        &self,
        session: &Session,
        collection: &str,
    ) -> Result<Option<CollectionDefinition>, WrongoDBError> {
        self.collection_for_txn(session, collection, TXN_NONE)
    }

    /// Lists every collection definition visible to `txn_id`.
    pub(crate) fn list_collections_for_txn(
        &self,
        session: &Session,
        txn_id: TxnId,
    ) -> Result<Vec<CollectionDefinition>, WrongoDBError> {
        self.store
            .list_records_for_txn(session, txn_id)?
            .into_iter()
            .map(|(collection, record)| collection_definition_from_record(collection, record))
            .collect()
    }

    // ------------------------------------------------------------------------
    // Write API
    // ------------------------------------------------------------------------

    /// Inserts the durable collection row if it is still missing in this
    /// transaction.
    pub(crate) fn insert_collection_if_missing_in_transaction(
        &self,
        session: &Session,
        collection: &str,
        table_uri: &str,
        storage_columns: &[String],
    ) -> Result<(CollectionDefinition, bool), WrongoDBError> {
        if let Some(existing) =
            self.collection_for_txn(session, collection, session.current_txn_id())?
        {
            return Ok((existing, false));
        }

        let definition = CollectionDefinition::new(collection, table_uri, storage_columns.to_vec());
        let record = catalog_record_from_collection_definition(&definition)?;
        self.store
            .put_record_in_transaction(session, collection, &record)?;
        Ok((definition, true))
    }

    pub(crate) fn insert_collection_if_missing_committed(
        &self,
        session: &mut Session,
        collection: &str,
        table_uri: &str,
        storage_columns: &[String],
    ) -> Result<(CollectionDefinition, bool), WrongoDBError> {
        session.with_transaction(|session| {
            self.insert_collection_if_missing_in_transaction(
                session,
                collection,
                table_uri,
                storage_columns,
            )
        })
    }

    /// Registers `index` on the durable collection row if it is still missing.
    pub(crate) fn ensure_index_in_transaction(
        &self,
        session: &Session,
        collection: &str,
        index: IndexDefinition,
    ) -> Result<bool, WrongoDBError> {
        let mut definition = self
            .collection_for_txn(session, collection, session.current_txn_id())?
            .ok_or_else(|| StorageError(format!("unknown collection: {collection}")))?;
        if definition.indexes.contains_key(index.name()) {
            return Ok(false);
        }

        definition.indexes.insert(index.name.clone(), index);
        let record = catalog_record_from_collection_definition(&definition)?;
        self.store
            .put_record_in_transaction(session, collection, &record)?;
        Ok(true)
    }

    pub(crate) fn ensure_index_committed(
        &self,
        session: &mut Session,
        collection: &str,
        index: IndexDefinition,
    ) -> Result<bool, WrongoDBError> {
        session.with_transaction(|session| {
            self.ensure_index_in_transaction(session, collection, index)
        })
    }

    pub(crate) fn set_index_ready_committed(
        &self,
        session: &mut Session,
        collection: &str,
        index_name: &str,
        ready: bool,
    ) -> Result<(), WrongoDBError> {
        session.with_transaction(|session| {
            self.set_index_ready_in_transaction(session, collection, index_name, ready)
        })
    }

    // ------------------------------------------------------------------------
    // Validation
    // ------------------------------------------------------------------------

    /// Verifies that every durable catalog reference points at a storage
    /// metadata row visible to `txn_id`.
    pub(crate) fn validate_storage_references(
        &self,
        session: &Session,
        metadata_store: &MetadataStore,
    ) -> Result<(), WrongoDBError> {
        for collection in self.list_collections_for_txn(session, TXN_NONE)? {
            validate_collection_references(&collection, metadata_store)?;
        }
        Ok(())
    }

    fn set_index_ready_in_transaction(
        &self,
        session: &Session,
        collection: &str,
        index_name: &str,
        ready: bool,
    ) -> Result<(), WrongoDBError> {
        let mut definition = self
            .collection_for_txn(session, collection, session.current_txn_id())?
            .ok_or_else(|| StorageError(format!("unknown collection: {collection}")))?;
        let Some(index) = definition.indexes.get_mut(index_name) else {
            return Err(StorageError(format!(
                "unknown index {index_name} on collection {collection}"
            ))
            .into());
        };
        index.ready = ready;

        let record = catalog_record_from_collection_definition(&definition)?;
        self.store
            .put_record_in_transaction(session, collection, &record)?;
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
    collection: String,
    record: CatalogRecord,
) -> Result<CollectionDefinition, WrongoDBError> {
    if !record.table_uri.starts_with(TABLE_URI_PREFIX) {
        return Err(StorageError(format!(
            "catalog row for {collection} has invalid table URI: {}",
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
                "catalog row for {collection} is missing index URI for {name}"
            ))
        })?;
        if !uri.starts_with(INDEX_URI_PREFIX) {
            return Err(StorageError(format!(
                "catalog row for {collection} has invalid index URI: {uri}"
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
                "catalog row for {collection} has invalid index URI: {uri}"
            ))
            .into());
        }
        if !indexes.contains_key(name) {
            return Err(StorageError(format!(
                "catalog row for {collection} has orphan index URI mapping for {name}"
            ))
            .into());
        }
    }

    Ok(CollectionDefinition {
        name: collection,
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
            collection.name(),
            collection.table_uri()
        ))
        .into());
    }

    for index in collection.indexes().values() {
        if metadata_store.get(index.uri())?.is_none() {
            return Err(StorageError(format!(
                "catalog collection {} references missing index URI {}",
                collection.name(),
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

    #[test]
    fn create_index_request_normalizes_missing_name() {
        let request = CreateIndexRequest::from_bson_spec(&doc! { "key": { "name": 1 } }).unwrap();

        assert_eq!(request.name(), "name_1");
        assert_eq!(request.spec.get_str("name").unwrap(), "name_1");
    }

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
