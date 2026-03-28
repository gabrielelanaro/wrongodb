use std::fs;

use uuid::Uuid;
use wrongodb::{
    Connection, ConnectionConfig, CursorEntry, LogSyncMethod, LoggingConfig, Session, TableCursor,
    TransactionSyncConfig, WrongoDBError,
};

fn accepts_public_types(
    _config: ConnectionConfig,
    _logging: LoggingConfig,
    _sync: TransactionSyncConfig,
    _method: LogSyncMethod,
    _session: &Session,
    _cursor: &mut TableCursor<'_>,
    _entry: CursorEntry,
) {
}

fn main() -> Result<(), WrongoDBError> {
    let db_path = std::env::temp_dir().join(format!("wrongodb-trybuild-{}", Uuid::new_v4()));
    let conn = Connection::open(&db_path, ConnectionConfig::new())?;
    let mut session = conn.open_session();
    session.create_table("table:users", Vec::new())?;

    let mut cursor = session.open_table_cursor("table:users")?;
    cursor.insert(b"user:1", b"alice")?;

    accepts_public_types(
        ConnectionConfig::new(),
        LoggingConfig::default(),
        TransactionSyncConfig::default(),
        LogSyncMethod::Fsync,
        &session,
        &mut cursor,
        (b"user:1".to_vec(), b"alice".to_vec()),
    );

    fs::remove_dir_all(&db_path)?;
    Ok(())
}
