use wrongodb::{Connection, Session, WrongoDBError};

fn table_uri(collection: &str) -> String {
    format!("table:{collection}")
}

pub fn insert_kv(
    conn: &Connection,
    collection: &str,
    key: &[u8],
    value: &[u8],
) -> Result<(), WrongoDBError> {
    let mut session = conn.open_session();
    let mut txn = session.transaction()?;
    let txn_id = txn.as_ref().id();
    insert_kv_in_session(txn.session_mut(), collection, key, value, txn_id)?;
    txn.commit()
}

pub fn insert_kv_in_session(
    session: &mut Session,
    collection: &str,
    key: &[u8],
    value: &[u8],
    txn_id: u64,
) -> Result<(), WrongoDBError> {
    session.create(&table_uri(collection))?;
    let mut cursor = session.open_cursor(&table_uri(collection))?;
    cursor.insert(key, value, txn_id)
}

pub fn update_kv_in_session(
    session: &mut Session,
    collection: &str,
    key: &[u8],
    value: &[u8],
    txn_id: u64,
) -> Result<(), WrongoDBError> {
    session.create(&table_uri(collection))?;
    let mut cursor = session.open_cursor(&table_uri(collection))?;
    cursor.update(key, value, txn_id)
}

pub fn delete_kv_in_session(
    session: &mut Session,
    collection: &str,
    key: &[u8],
    txn_id: u64,
) -> Result<(), WrongoDBError> {
    session.create(&table_uri(collection))?;
    let mut cursor = session.open_cursor(&table_uri(collection))?;
    cursor.delete(key, txn_id)
}

pub fn get_kv(
    conn: &Connection,
    collection: &str,
    key: &[u8],
) -> Result<Option<Vec<u8>>, WrongoDBError> {
    let mut session = conn.open_session();
    get_kv_in_session(&mut session, collection, key, 0)
}

pub fn get_kv_in_session(
    session: &mut Session,
    collection: &str,
    key: &[u8],
    txn_id: u64,
) -> Result<Option<Vec<u8>>, WrongoDBError> {
    session.create(&table_uri(collection))?;
    let mut cursor = session.open_cursor(&table_uri(collection))?;
    cursor.get(key, txn_id)
}

pub fn scan_kv(
    session: &mut Session,
    collection: &str,
    txn_id: u64,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, WrongoDBError> {
    session.create(&table_uri(collection))?;
    let mut cursor = session.open_cursor(&table_uri(collection))?;
    let mut entries = Vec::new();
    while let Some(entry) = cursor.next(txn_id)? {
        entries.push(entry);
    }
    Ok(entries)
}
