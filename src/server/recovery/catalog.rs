use std::fs;
use std::path::Path;

use crate::catalog::{CatalogStore, DurableCatalog};
use crate::core::errors::StorageError;
use crate::storage::api::connection::Connection;
use crate::storage::reserved_store::reserved_store_names;
use crate::WrongoDBError;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CatalogRecoveryReport {
    pub(crate) unreferenced_sources: Vec<String>,
}

pub(crate) struct CatalogRecovery;

impl CatalogRecovery {
    pub(crate) fn reconcile(
        connection: &Connection,
    ) -> Result<CatalogRecoveryReport, WrongoDBError> {
        let metadata_store = connection.metadata_store();
        let session = connection.open_session();
        for store_name in reserved_store_names() {
            session.ensure_named_store(store_name)?;
        }

        let durable_catalog = DurableCatalog::new(CatalogStore::new());
        durable_catalog.validate_storage_references(&session, metadata_store.as_ref())?;

        let mut referenced_store_names = metadata_store.all_store_names()?;
        referenced_store_names.extend(
            reserved_store_names()
                .iter()
                .map(|name| (*name).to_string()),
        );
        referenced_store_names.sort();
        referenced_store_names.dedup();

        let mut on_disk_sources = list_store_files(connection.base_path())?;
        on_disk_sources.sort();
        on_disk_sources.dedup();

        let missing_sources = referenced_store_names
            .iter()
            .filter(|source| !on_disk_sources.contains(source))
            .cloned()
            .collect::<Vec<_>>();
        if !missing_sources.is_empty() {
            return Err(StorageError(format!(
                "catalog recovery found missing referenced sources: {}",
                missing_sources.join(", ")
            ))
            .into());
        }

        let unreferenced_sources = on_disk_sources
            .into_iter()
            .filter(|source| !referenced_store_names.contains(source))
            .collect();
        Ok(CatalogRecoveryReport {
            unreferenced_sources,
        })
    }
}

fn list_store_files(base_path: &Path) -> Result<Vec<String>, WrongoDBError> {
    let mut store_files = Vec::new();
    for entry in fs::read_dir(base_path)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        if file_name.ends_with(".wt") {
            store_files.push(file_name.to_string());
        }
    }
    Ok(store_files)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::storage::api::{Connection, ConnectionConfig};

    #[test]
    fn reconcile_reports_unreferenced_store_files() {
        let dir = tempdir().unwrap();
        let conn = Connection::open(dir.path(), ConnectionConfig::default()).unwrap();
        std::fs::write(dir.path().join("dangling.main.wt"), b"").unwrap();

        let report = CatalogRecovery::reconcile(&conn).unwrap();
        assert_eq!(
            report.unreferenced_sources,
            vec!["dangling.main.wt".to_string()]
        );
    }

    #[test]
    fn reconcile_fails_when_metadata_references_missing_store() {
        let dir = tempdir().unwrap();
        let conn = Connection::open(dir.path(), ConnectionConfig::default()).unwrap();
        let mut session = conn.open_session();
        session.create_table("table:users", Vec::new()).unwrap();
        drop(session);
        std::fs::remove_file(dir.path().join("users.main.wt")).unwrap();

        let err = CatalogRecovery::reconcile(&conn).unwrap_err();
        assert!(err.to_string().contains("missing referenced sources"));
    }
}
