use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::WrongoDBError;

/// Thread-safe cache of shared opened handles keyed by logical name.
///
/// The cache stores one shared `Arc<V>` per key and creates entries lazily via
/// a caller-provided closure when a key is first requested.
#[derive(Debug)]
pub(crate) struct HandleCache<K, V> {
    handles_by_key: RwLock<HashMap<K, Arc<V>>>,
}

impl<K, V> HandleCache<K, V>
where
    K: Eq + Hash + Clone,
{
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    pub(crate) fn new() -> Self {
        Self {
            handles_by_key: RwLock::new(HashMap::new()),
        }
    }

    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------

    pub(crate) fn get_or_try_insert_with<F>(
        &self,
        key: K,
        create: F,
    ) -> Result<Arc<V>, WrongoDBError>
    where
        F: FnOnce(&K) -> Result<V, WrongoDBError>,
    {
        if let Some(handle) = self.handles_by_key.read().get(&key) {
            return Ok(handle.clone());
        }

        let mut handles = self.handles_by_key.write();
        if let Some(handle) = handles.get(&key) {
            return Ok(handle.clone());
        }

        let handle = Arc::new(create(&key)?);
        handles.insert(key, handle.clone());
        Ok(handle)
    }

    pub(crate) fn all_handles(&self) -> Vec<Arc<V>> {
        self.handles_by_key.read().values().cloned().collect()
    }
}
