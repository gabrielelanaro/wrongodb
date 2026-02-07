use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use serde_json::json;
use tempfile::tempdir;

use wrongodb::WrongoDB;

fn main() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("test.db");

    {
        let db = WrongoDB::open(&db_path).unwrap();
        let coll = db.collection("test");
        let mut session = db.open_session();

        for i in 0..10 {
            coll.insert_one(&mut session, json!({"_id": i, "value": format!("v{i}")}))
                .unwrap();
        }

        coll.checkpoint(&mut session).unwrap();
    }

    let wal_path = db_path.join("global.wal");
    let metadata = std::fs::metadata(&wal_path).unwrap();
    println!("Global WAL file size: {} bytes", metadata.len());

    let mut wal_file = File::open(&wal_path).unwrap();
    let mut header = [0u8; 512];
    wal_file.read_exact(&mut header).unwrap();

    println!("WAL header magic: {:?}", &header[0..8]);
    println!(
        "WAL header version: {:?}",
        u16::from_le_bytes(header[8..10].try_into().unwrap())
    );

    let mut record_count = 0;
    let mut pos = 512;

    while pos < metadata.len() {
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
        if record_count > 50 {
            break;
        }
    }

    println!("Total records found: {}", record_count);
}
