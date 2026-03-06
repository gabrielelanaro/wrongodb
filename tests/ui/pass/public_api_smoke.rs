use tempfile::tempdir;
use wrongodb::{
    start_server, Connection, ConnectionConfig, Cursor, CursorEntry, DocumentValidationError,
    RaftMode, RaftPeerConfig, Session, StorageError, WriteUnitOfWork, WrongoDBError,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _server = start_server;
    let _session_marker: Option<&Session> = None;
    let _cursor_marker: Option<&Cursor> = None;
    let _entry_marker: Option<CursorEntry> = None;
    let _wuow_marker: Option<&mut WriteUnitOfWork<'_>> = None;
    let _err_marker: Option<WrongoDBError> = None;
    let _storage_marker: Option<StorageError> = None;
    let _validation_marker: Option<DocumentValidationError> = None;
    let _mode_marker = RaftMode::Standalone;
    let _peer_marker = RaftPeerConfig {
        node_id: "n1".to_string(),
        raft_addr: "127.0.0.1:27018".to_string(),
    };

    let tmp = tempdir()?;
    let conn = Connection::open(tmp.path().join("db"), ConnectionConfig::default())?;
    let mut session = conn.open_session();
    session.create("table:users")?;

    let mut cursor = session.open_cursor("table:users")?;
    cursor.insert(b"alice", b"v1")?;
    let _ = cursor.get(b"alice")?;

    let mut write_unit = session.transaction()?;
    let mut txn_cursor = write_unit.open_cursor("table:users")?;
    txn_cursor.insert(b"bob", b"v2")?;
    let _ = txn_cursor.get(b"bob")?;
    write_unit.commit()?;
    session.checkpoint()?;
    Ok(())
}
