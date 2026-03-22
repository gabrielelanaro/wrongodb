use std::collections::BTreeMap;

use bson::Document;
use serde::{Deserialize, Serialize};

use crate::core::errors::StorageError;
use crate::storage::api::Session;
use crate::storage::reserved_store::{CATALOG_STORE_NAME, CATALOG_URI};
use crate::txn::TxnId;
use crate::WrongoDBError;

/// Raw `_catalog.wt` row persisted for a single collection.
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

/// Thin raw wrapper around the reserved `_catalog.wt` store.
///
/// `CatalogStore` only deals with raw collection rows. Higher-level parsing,
/// validation, and server semantics live in [`crate::catalog::DurableCatalog`].
#[derive(Debug, Clone, Default)]
pub(crate) struct CatalogStore;

impl CatalogStore {
    /// Creates the raw `_catalog.wt` store wrapper.
    pub(crate) fn new() -> Self {
        Self
    }

    /// Ensures that the reserved `_catalog.wt` store exists.
    pub(crate) fn ensure_store_exists(&self, session: &Session) -> Result<(), WrongoDBError> {
        session.ensure_named_store(CATALOG_STORE_NAME)
    }

    /// Reads the collection row visible to `txn_id`.
    pub(crate) fn record_for_txn(
        &self,
        session: &Session,
        collection: &str,
        txn_id: TxnId,
    ) -> Result<Option<CatalogRecord>, WrongoDBError> {
        self.ensure_store_exists(session)?;
        session
            .read_from_named_store(CATALOG_STORE_NAME, collection.as_bytes(), txn_id)?
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
        self.ensure_store_exists(session)?;
        let value = bson::to_vec(record)?;
        session.put_into_named_store(
            CATALOG_STORE_NAME,
            CATALOG_URI,
            collection.as_bytes(),
            &value,
        )
    }

    /// Reads every collection row visible to `txn_id`.
    pub(crate) fn list_records_for_txn(
        &self,
        session: &Session,
        txn_id: TxnId,
    ) -> Result<Vec<(String, CatalogRecord)>, WrongoDBError> {
        self.ensure_store_exists(session)?;
        session
            .scan_named_store_range(CATALOG_STORE_NAME, None, None, txn_id)?
            .into_iter()
            .map(|(key, value)| {
                let collection = String::from_utf8(key).map_err(|err| {
                    StorageError(format!("catalog row key is not valid UTF-8: {err}"))
                })?;
                Ok((collection, decode_catalog_record(value)?))
            })
            .collect()
    }
}

fn decode_catalog_record(value: Vec<u8>) -> Result<CatalogRecord, WrongoDBError> {
    Ok(bson::from_slice(&value)?)
}
