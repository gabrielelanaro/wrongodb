use std::fs;
use std::path::Path;

fn read(path: &str) -> String {
    fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(path)).unwrap()
}

#[test]
fn lib_rs_only_reexports_supported_surface() {
    let lib_rs = read("src/lib.rs");

    assert!(!lib_rs.contains("pub mod commands"));
    assert!(!lib_rs.contains("pub use crate::txn::"));
    assert!(!lib_rs.contains("pub use crate::index::"));
    assert!(!lib_rs.contains("pub use crate::server::commands"));
    assert!(lib_rs.contains("pub use crate::storage::api::{"));
    assert!(lib_rs.contains("pub use crate::core::errors::{"));
    assert!(lib_rs.contains("pub use crate::server::start_server;"));
}

#[test]
fn api_mod_only_exposes_database_context_internally() {
    let api_mod = read("src/api/mod.rs");

    assert!(api_mod.contains("mod database_context;"));
    assert!(api_mod.contains("pub(crate) use database_context::DatabaseContext;"));
    assert!(!api_mod.contains("pub use connection::{Connection, ConnectionConfig};"));
    assert!(!api_mod.contains("pub use cursor::{Cursor, CursorEntry};"));
    assert!(!api_mod.contains("pub use session::{Session, WriteUnitOfWork};"));
}

#[test]
fn storage_api_mod_reexports_storage_surface() {
    let storage_api_mod = read("src/storage/api/mod.rs");

    assert!(!storage_api_mod.contains("pub use crate::replication::{RaftMode, RaftPeerConfig};"));
    assert!(storage_api_mod.contains("pub use connection::{"));
    assert!(storage_api_mod.contains(
        "Connection, ConnectionConfig, LogSyncMethod, LoggingConfig, TransactionSyncConfig,"
    ));
    assert!(storage_api_mod.contains("pub use cursor::{CursorEntry, TableCursor};"));
    assert!(storage_api_mod.contains("pub use session::Session;"));
    assert!(!storage_api_mod.contains("WriteUnitOfWork"));
}

#[test]
fn server_commands_stay_internal() {
    let server_mod = read("src/server/mod.rs");
    assert!(!server_mod.contains("pub mod commands;"));
}
