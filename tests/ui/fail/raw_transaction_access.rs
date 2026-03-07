use tempfile::tempdir;
use wrongodb::{Connection, ConnectionConfig};

fn main() {
    let tmp = tempdir().unwrap();
    let conn = Connection::open(tmp.path().join("db"), ConnectionConfig::default()).unwrap();
    let mut session = conn.open_session();
    let write_unit = session.transaction().unwrap();
    let _ = write_unit.txn_id();
}
