use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};

use tempfile::tempdir;

use wrongodb::{BlockFile, FileHeader};

#[test]
fn create_writes_header_and_one_block() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.wt");

    let mut bf = BlockFile::create(&path, 4096).unwrap();
    assert_eq!(bf.num_blocks().unwrap(), 1);
    let default_header = FileHeader::default();
    assert_eq!(bf.header.magic, default_header.magic);
    assert_eq!(bf.header.version, default_header.version);
    assert_eq!(bf.header.page_size, 4096);
    bf.close().unwrap();

    let mut bf2 = BlockFile::open(&path).unwrap();
    assert_eq!(bf2.page_size, 4096);
    assert_eq!(bf2.num_blocks().unwrap(), 1);
    bf2.close().unwrap();
}

#[test]
fn write_read_roundtrip() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.wt");

    let mut bf = BlockFile::create(&path, 1024).unwrap();
    let b1 = bf.allocate_block().unwrap();
    let b2 = bf.allocate_block().unwrap();
    assert_eq!(b1, 1);
    assert_eq!(b2, 2);
    bf.write_block(b1, b"hello").unwrap();
    bf.write_block(b2, b"world").unwrap();
    assert_eq!(bf.num_blocks().unwrap(), 3);
    bf.close().unwrap();

    let mut bf2 = BlockFile::open(&path).unwrap();
    assert!(bf2.read_block(b1, true).unwrap().starts_with(b"hello"));
    assert!(bf2.read_block(b2, true).unwrap().starts_with(b"world"));
    bf2.close().unwrap();
}

#[test]
fn payload_too_large_raises() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.wt");

    let mut bf = BlockFile::create(&path, 128).unwrap();
    let max_payload = bf.page_size - 4;
    let b1 = bf.allocate_block().unwrap();
    let err = bf
        .write_block(b1, &vec![b'x'; max_payload + 1])
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("payload too large"));
    bf.close().unwrap();
}

#[test]
fn checksum_mismatch_detected() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.wt");

    let mut bf = BlockFile::create(&path, 512).unwrap();
    let b1 = bf.allocate_block().unwrap();
    bf.write_block(b1, b"data").unwrap();
    bf.close().unwrap();

    // Corrupt a byte in the payload of block 1 (known to be block_id==1).
    let mut fh = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    fh.seek(SeekFrom::Start(b1 * 512 + 4)).unwrap();
    let mut b = [0u8; 1];
    let _ = fh.read(&mut b).unwrap();
    fh.seek(SeekFrom::Start(b1 * 512 + 4)).unwrap();
    fh.write_all(&[b[0] ^ 0xFF]).unwrap();
    fh.flush().unwrap();

    let mut bf2 = BlockFile::open(&path).unwrap();
    let err = bf2.read_block(b1, true).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("checksum mismatch"));
    bf2.close().unwrap();
}

#[test]
fn free_list_reuse_survives_reopen() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.wt");

    let mut bf = BlockFile::create(&path, 512).unwrap();
    let b1 = bf.allocate_block().unwrap();
    let b2 = bf.allocate_block().unwrap();
    let b3 = bf.allocate_block().unwrap();
    assert_eq!((b1, b2, b3), (1, 2, 3));
    bf.write_block(b1, b"a").unwrap();
    bf.write_block(b2, b"b").unwrap();
    bf.write_block(b3, b"c").unwrap();
    bf.free_block(b2).unwrap();
    bf.close().unwrap();

    let mut bf2 = BlockFile::open(&path).unwrap();
    let reused = bf2.allocate_block().unwrap();
    assert_eq!(reused, b2);
    bf2.write_block(reused, b"reused").unwrap();
    assert!(bf2.read_block(reused, true).unwrap().starts_with(b"reused"));
    bf2.close().unwrap();
}

#[test]
fn create_on_existing_file_raises() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("test.wt");

    let bf = BlockFile::create(&path, 4096).unwrap();
    bf.close().unwrap();

    let err = BlockFile::create(&path, 4096).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("file already exists"));
}
