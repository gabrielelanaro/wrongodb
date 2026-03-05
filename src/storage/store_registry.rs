use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::hooks::{MutationHooks, NoopMutationHooks};
use crate::storage::table::Table;
use crate::txn::TransactionManager;
use crate::WrongoDBError;

#[derive(Debug)]
pub struct StoreRegistry {
    base_path: PathBuf,
    transaction_manager: Arc<TransactionManager>,
    mutation_hooks: RwLock<Arc<dyn MutationHooks>>,
    apply_mutations_locally: RwLock<bool>,
    handles_by_store: RwLock<HashMap<String, Arc<RwLock<Table>>>>,
    uri_aliases: RwLock<HashMap<String, String>>,
}

impl StoreRegistry {
    pub fn new(base_path: PathBuf, transaction_manager: Arc<TransactionManager>) -> Self {
        Self {
            base_path,
            transaction_manager,
            mutation_hooks: RwLock::new(Arc::new(NoopMutationHooks)),
            apply_mutations_locally: RwLock::new(true),
            handles_by_store: RwLock::new(HashMap::new()),
            uri_aliases: RwLock::new(HashMap::new()),
        }
    }

    pub fn set_mutation_hooks(
        &self,
        mutation_hooks: Arc<dyn MutationHooks>,
        apply_mutations_locally: bool,
    ) {
        *self.mutation_hooks.write() = mutation_hooks.clone();
        *self.apply_mutations_locally.write() = apply_mutations_locally;
        let handles: Vec<_> = self
            .handles_by_store
            .read()
            .values()
            .map(Arc::clone)
            .collect();
        for handle in handles {
            let mut table = handle.write();
            table.set_mutation_hooks(mutation_hooks.clone());
            table.set_apply_mutations_locally(apply_mutations_locally);
        }
    }

    pub fn get_or_open_primary(
        &self,
        uri: &str,
        collection: &str,
    ) -> Result<Arc<RwLock<Table>>, WrongoDBError> {
        if let Some(store_name) = self.uri_aliases.read().get(uri).cloned() {
            if let Some(handle) = self.handles_by_store.read().get(&store_name) {
                return Ok(handle.clone());
            }
        }

        let store_name = format!("{collection}.main.wt");
        let table = self.resolve_or_open_store(&store_name)?;
        self.uri_aliases
            .write()
            .insert(uri.to_string(), store_name.clone());
        self.register_index_handles(&table);
        Ok(table)
    }

    pub fn resolve_or_open_store(
        &self,
        store_name: &str,
    ) -> Result<Arc<RwLock<Table>>, WrongoDBError> {
        if let Some(handle) = self.handles_by_store.read().get(store_name) {
            return Ok(handle.clone());
        }

        let mutation_hooks = self.mutation_hooks.read().clone();
        let apply_mutations_locally = *self.apply_mutations_locally.read();
        let mut handles = self.handles_by_store.write();
        if let Some(handle) = handles.get(store_name) {
            return Ok(handle.clone());
        }

        let table = if let Some(collection) = store_name.strip_suffix(".main.wt") {
            Table::open_or_create_primary(
                collection,
                &self.base_path,
                self.transaction_manager.clone(),
                mutation_hooks,
                apply_mutations_locally,
            )?
        } else {
            let path = self.base_path.join(store_name);
            Table::open_or_create_index(
                path,
                self.transaction_manager.clone(),
                mutation_hooks,
                apply_mutations_locally,
            )?
        };
        let table = Arc::new(RwLock::new(table));
        handles.insert(store_name.to_string(), table.clone());
        let is_primary = store_name.ends_with(".main.wt");
        drop(handles);
        if is_primary {
            self.register_index_handles(&table);
        }
        Ok(table)
    }

    pub fn all_handles(&self) -> Vec<Arc<RwLock<Table>>> {
        self.handles_by_store
            .read()
            .values()
            .map(Arc::clone)
            .collect()
    }

    fn register_index_handles(&self, primary: &Arc<RwLock<Table>>) {
        let mut handles = self.handles_by_store.write();
        let table = primary.read();
        let Some(catalog) = table.index_catalog() else {
            return;
        };
        for def in catalog.index_defs() {
            if let Some(index_handle) = catalog.index_handle(&def.name) {
                handles.entry(def.source).or_insert(index_handle);
            }
        }
    }
}
