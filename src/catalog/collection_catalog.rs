use std::collections::BTreeMap;

use parking_lot::RwLock;

use crate::catalog::{CollectionDefinition, DurableCatalog, IndexDefinition};
use crate::storage::api::Session;
use crate::txn::TXN_NONE;
use crate::WrongoDBError;

/// In-memory committed view of the durable collection catalog.
///
/// This cache is loaded from [`DurableCatalog`] at startup and refreshed after
/// successful metadata-changing writes. Query planning and server metadata
/// commands use it for committed collection/index listings.
#[derive(Debug, Default)]
pub(crate) struct CollectionCatalog {
    collections: RwLock<BTreeMap<String, CollectionDefinition>>,
}

impl CollectionCatalog {
    /// Creates an empty committed collection catalog.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Rebuilds the committed cache from the durable catalog.
    pub(crate) fn load_from_durable(
        &self,
        session: &Session,
        durable_catalog: &DurableCatalog,
    ) -> Result<(), WrongoDBError> {
        let collections = durable_catalog
            .list_collections_for_txn(session, TXN_NONE)?
            .into_iter()
            .map(|collection| (collection.name().to_string(), collection))
            .collect();
        *self.collections.write() = collections;
        Ok(())
    }

    /// Refreshes one committed collection entry after a successful write path.
    pub(crate) fn refresh_collection(
        &self,
        session: &Session,
        durable_catalog: &DurableCatalog,
        collection: &str,
    ) -> Result<(), WrongoDBError> {
        let mut collections = self.collections.write();
        match durable_catalog.collection_for_txn(session, collection, TXN_NONE)? {
            Some(definition) => {
                collections.insert(collection.to_string(), definition);
            }
            None => {
                collections.remove(collection);
            }
        }
        Ok(())
    }

    /// Lists the committed collection names in sorted order.
    pub(crate) fn list_collection_names(&self) -> Vec<String> {
        self.collections.read().keys().cloned().collect()
    }

    /// Looks up one committed collection definition by name.
    pub(crate) fn lookup_collection(&self, collection: &str) -> Option<CollectionDefinition> {
        self.collections.read().get(collection).cloned()
    }

    /// Returns the committed secondary index definitions for `collection`.
    pub(crate) fn list_index_definitions(&self, collection: &str) -> Vec<IndexDefinition> {
        self.collections
            .read()
            .get(collection)
            .map(|definition| definition.indexes().values().cloned().collect())
            .unwrap_or_default()
    }
}
