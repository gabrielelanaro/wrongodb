use tempfile::tempdir;
use wrongodb::{Connection, ConnectionConfig};

fn main() {
    let tmp = tempdir().unwrap();
    let conn = Connection::open(tmp.path().join("db"), ConnectionConfig::default()).unwrap();
    let mut session = conn.open_session();
    session.create("table:test").unwrap();
    let mut cursor = session.open_cursor("table:test").unwrap();
    let _ = cursor.insert(b"k", b"v", 0);
}
