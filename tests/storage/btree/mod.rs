use std::fs;
use std::path::{Path, PathBuf};

use wrongodb::{Connection, ConnectionConfig, CursorEntry, Session};

pub(super) const TABLE_URI: &str = "table:test";
pub(super) const FILE_URI: &str = "file:test.wt";

mod checkpoint;
mod cow;
mod multi_level_range;
mod split_root;
mod wal;

pub(super) fn open_connection(db_path: &Path, logging_enabled: bool) -> Connection {
    Connection::open(
        db_path,
        ConnectionConfig::new().logging_enabled(logging_enabled),
    )
    .unwrap()
}

pub(super) fn checkpoint(session: &mut Session) {
    session.checkpoint().unwrap();
}

pub(super) fn create_table(session: &mut Session) {
    session.create_table(TABLE_URI, Vec::new()).unwrap();
}

pub(super) fn create_file(session: &mut Session) {
    session.create_file(FILE_URI).unwrap();
}

pub(super) fn insert_table_row(session: &Session, key: &[u8], value: &[u8]) {
    let mut cursor = session.open_table_cursor(TABLE_URI).unwrap();
    cursor.insert(key, value).unwrap();
}

pub(super) fn update_table_row(session: &Session, key: &[u8], value: &[u8]) {
    let mut cursor = session.open_table_cursor(TABLE_URI).unwrap();
    cursor.update(key, value).unwrap();
}

pub(super) fn delete_table_row(session: &Session, key: &[u8]) {
    let mut cursor = session.open_table_cursor(TABLE_URI).unwrap();
    cursor.delete(key).unwrap();
}

pub(super) fn get_table_row(session: &Session, key: &[u8]) -> Option<Vec<u8>> {
    let mut cursor = session.open_table_cursor(TABLE_URI).unwrap();
    cursor.get(key).unwrap()
}

pub(super) fn scan_table_range(
    session: &Session,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
) -> Vec<CursorEntry> {
    let mut cursor = session.open_table_cursor(TABLE_URI).unwrap();
    cursor.set_range(start.map(Vec::from), end.map(Vec::from));
    collect_table_entries(&mut cursor)
}

pub(super) fn insert_file_row(session: &Session, key: &[u8], value: &[u8]) {
    let mut cursor = session.open_file_cursor(FILE_URI).unwrap();
    cursor.insert(key, value).unwrap();
}

pub(super) fn update_file_row(session: &Session, key: &[u8], value: &[u8]) {
    let mut cursor = session.open_file_cursor(FILE_URI).unwrap();
    cursor.update(key, value).unwrap();
}

pub(super) fn get_file_row(session: &Session, key: &[u8]) -> Option<Vec<u8>> {
    let mut cursor = session.open_file_cursor(FILE_URI).unwrap();
    cursor.get(key).unwrap()
}

pub(super) fn scan_file_range(
    session: &Session,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
) -> Vec<CursorEntry> {
    let mut cursor = session.open_file_cursor(FILE_URI).unwrap();
    cursor.set_range(start.map(Vec::from), end.map(Vec::from));
    let mut entries = Vec::new();
    while let Some(entry) = cursor.next().unwrap() {
        entries.push(entry);
    }
    entries
}

pub(super) fn table_store_path(db_path: &Path) -> PathBuf {
    db_path.join("test.main.wt")
}

pub(super) fn file_store_path(db_path: &Path) -> PathBuf {
    db_path.join("test.wt")
}

pub(super) fn file_len(path: &Path) -> u64 {
    fs::metadata(path).unwrap().len()
}

fn collect_table_entries(cursor: &mut wrongodb::TableCursor<'_>) -> Vec<CursorEntry> {
    let mut entries = Vec::new();
    while let Some(entry) = cursor.next().unwrap() {
        entries.push(entry);
    }
    entries
}
