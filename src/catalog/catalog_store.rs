use std::collections::BTreeMap;

use bson::Document;
use serde::{Deserialize, Serialize};

use crate::core::errors::StorageError;
use crate::storage::api::Session;
use crate::WrongoDBError;

pub(crate) const CATALOG_FILE_URI: &str = "file:_catalog.wt";

/// Raw `file:_catalog.wt` row persisted for a single collection.
///
/// The top-level fields mirror MongoDB's separation between storage bindings
/// and parsed collection metadata:
///
/// - **`table_uri`**: Primary storage-engine table for the collection
/// - **`index_uris`**: Mapping from index name to storage-engine index URI
/// - **`md`**: Server-facing collection metadata encoded as a BSON document
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct CatalogRecord {
    /// Primary `table:` URI used by the storage layer.
    pub(crate) table_uri: String,
    #[serde(default)]
    /// Secondary index URIs keyed by the Mongo-visible index name.
    pub(crate) index_uris: BTreeMap<String, String>,
    #[serde(default)]
    /// Parsed collection metadata stored opaquely at this raw layer.
    pub(crate) md: Document,
}

/// Thin raw wrapper around the metadata-managed `file:_catalog.wt` object.
///
/// `CatalogStore` only deals with raw collection rows. Higher-level parsing,
/// validation, and server semantics live in [`crate::catalog::DurableCatalog`].
#[derive(Debug, Clone, Default)]
pub(crate) struct CatalogStore;

impl CatalogStore {
    /// Creates the raw catalog store wrapper.
    pub(crate) fn new() -> Self {
        Self
    }

    /// Reads the collection row visible in the session's current transaction context.
    pub(crate) fn record_visible(
        &self,
        session: &Session,
        collection: &str,
    ) -> Result<Option<CatalogRecord>, WrongoDBError> {
        let mut cursor = session.open_file_cursor(CATALOG_FILE_URI)?;
        cursor
            .get(collection.as_bytes())?
            .map(decode_catalog_record)
            .transpose()
    }

    /// Writes the collection row in the caller's transaction.
    pub(crate) fn put_record_in_transaction(
        &self,
        session: &Session,
        collection: &str,
        record: &CatalogRecord,
    ) -> Result<(), WrongoDBError> {
        let value = bson::to_vec(record)?;
        let mut cursor = session.open_file_cursor(CATALOG_FILE_URI)?;
        if cursor.get(collection.as_bytes())?.is_some() {
            cursor.update(collection.as_bytes(), &value)
        } else {
            cursor.insert(collection.as_bytes(), &value)
        }
    }

    /// Reads every collection row visible in the session's current transaction context.
    pub(crate) fn list_records_visible(
        &self,
        session: &Session,
    ) -> Result<Vec<(String, CatalogRecord)>, WrongoDBError> {
        let mut cursor = session.open_file_cursor(CATALOG_FILE_URI)?;
        let mut rows = Vec::new();

        while let Some((key, value)) = cursor.next()? {
            let collection = String::from_utf8(key).map_err(|err| {
                StorageError(format!("catalog row key is not valid UTF-8: {err}"))
            })?;
            rows.push((collection, decode_catalog_record(value)?));
        }

        Ok(rows)
    }
}

fn decode_catalog_record(value: Vec<u8>) -> Result<CatalogRecord, WrongoDBError> {
    Ok(bson::from_slice(&value)?)
}
