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
    session.create(&table_uri(collection))?;
    session.with_transaction(|session| insert_kv_in_transaction(session, collection, key, value))
}

pub fn insert_kv_in_session(
    session: &mut Session,
    collection: &str,
    key: &[u8],
    value: &[u8],
) -> Result<(), WrongoDBError> {
    session.create(&table_uri(collection))?;
    let mut cursor = session.open_cursor(&table_uri(collection))?;
    cursor.insert(key, value)
}

pub fn insert_kv_in_transaction(
    session: &mut Session,
    collection: &str,
    key: &[u8],
    value: &[u8],
) -> Result<(), WrongoDBError> {
    let mut cursor = session.open_cursor(&table_uri(collection))?;
    cursor.insert(key, value)
}

pub fn update_kv_in_session(
    session: &mut Session,
    collection: &str,
    key: &[u8],
    value: &[u8],
) -> Result<(), WrongoDBError> {
    session.create(&table_uri(collection))?;
    let mut cursor = session.open_cursor(&table_uri(collection))?;
    cursor.update(key, value)
}

pub fn update_kv_in_transaction(
    session: &mut Session,
    collection: &str,
    key: &[u8],
    value: &[u8],
) -> Result<(), WrongoDBError> {
    let mut cursor = session.open_cursor(&table_uri(collection))?;
    cursor.update(key, value)
}

pub fn delete_kv_in_session(
    session: &mut Session,
    collection: &str,
    key: &[u8],
) -> Result<(), WrongoDBError> {
    session.create(&table_uri(collection))?;
    let mut cursor = session.open_cursor(&table_uri(collection))?;
    cursor.delete(key)
}

pub fn delete_kv_in_transaction(
    session: &mut Session,
    collection: &str,
    key: &[u8],
) -> Result<(), WrongoDBError> {
    let mut cursor = session.open_cursor(&table_uri(collection))?;
    cursor.delete(key)
}

pub fn get_kv(
    conn: &Connection,
    collection: &str,
    key: &[u8],
) -> Result<Option<Vec<u8>>, WrongoDBError> {
    let mut session = conn.open_session();
    get_kv_in_session(&mut session, collection, key)
}

pub fn get_kv_in_session(
    session: &mut Session,
    collection: &str,
    key: &[u8],
) -> Result<Option<Vec<u8>>, WrongoDBError> {
    session.create(&table_uri(collection))?;
    let mut cursor = session.open_cursor(&table_uri(collection))?;
    cursor.get(key)
}

pub fn get_kv_in_transaction(
    session: &mut Session,
    collection: &str,
    key: &[u8],
) -> Result<Option<Vec<u8>>, WrongoDBError> {
    let mut cursor = session.open_cursor(&table_uri(collection))?;
    cursor.get(key)
}

pub fn scan_kv(
    session: &mut Session,
    collection: &str,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, WrongoDBError> {
    session.create(&table_uri(collection))?;
    let mut cursor = session.open_cursor(&table_uri(collection))?;
    let mut entries = Vec::new();
    while let Some(entry) = cursor.next()? {
        entries.push(entry);
    }
    Ok(entries)
}
