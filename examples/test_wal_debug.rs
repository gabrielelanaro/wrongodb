use tempfile::tempdir;
use wrongodb::{Connection, ConnectionConfig};

fn main() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("testdb");

    // Create database and insert data
    let conn = Connection::open(&db_path, ConnectionConfig::default().disable_auto_checkpoint())
        .unwrap();

    {
        let mut session = conn.open_session();
        session.create("table:test").unwrap();

        let mut txn = session.transaction().unwrap();
        let txn_id = txn.as_mut().id();
        let mut cursor = txn.session_mut().open_cursor("table:test").unwrap();

        // Insert enough records to cause a split
        for i in 0..10 {
            let key = format!("key{:05}", i);
            let value = format!("value{}", i);
            println!("Inserting: {}", key);
            cursor.insert(key.as_bytes(), value.as_bytes(), txn_id).unwrap();
        }

        txn.commit().unwrap();
        println!("Inserted 10 records");
    }

    // Force a checkpoint to flush WAL to disk
    conn.checkpoint_all().unwrap();
    drop(conn);

    // Check WAL file size
    let wal_path = db_path.join("wrongo.wal");
    let metadata = match std::fs::metadata(&wal_path) {
        Ok(metadata) => {
            println!("WAL file size: {} bytes", metadata.len());
            metadata
        }
        Err(err) => {
            eprintln!("Failed to read WAL file metadata: {}", err);
            return;
        }
    };

    // Try to read WAL records directly
    use std::fs::File;
    use std::io::{Read, Seek, SeekFrom};

    let mut wal_file = File::open(&wal_path).unwrap();
    let mut header = [0u8; 512];
    wal_file.read_exact(&mut header).unwrap();

    println!("WAL header magic: {:?}", &header[0..8]);
    println!(
        "WAL header version: {:?}",
        u16::from_le_bytes(header[8..10].try_into().unwrap())
    );
    println!(
        "WAL header last_lsn: {:?}",
        u64::from_le_bytes(header[26..34].try_into().unwrap())
    );
    println!(
        "WAL header checkpoint_lsn: {:?}",
        u64::from_le_bytes(header[34..42].try_into().unwrap())
    );

    // Count records
    let mut record_count = 0;
    let mut pos = 512;

    while pos < metadata.len() as u64 {
        wal_file.seek(SeekFrom::Start(pos)).unwrap();
        let mut rec_header = [0u8; 32];
        let bytes_read = wal_file.read(&mut rec_header).unwrap();

        if bytes_read < 32 {
            break;
        }

        let record_type = rec_header[0];
        let payload_len = u16::from_le_bytes(rec_header[2..4].try_into().unwrap());

        println!(
            "Record {}: type={}, payload_len={}, pos={}",
            record_count, record_type, payload_len, pos
        );

        pos += 32 + payload_len as u64;
        record_count += 1;

        if record_count > 20 {
            break;
        }
    }

    println!("Total records found: {}", record_count);
}
