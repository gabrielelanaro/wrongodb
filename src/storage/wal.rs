//! Connection-level Write-Ahead Logging (WAL) for crash recovery.
//!
//! A single WAL file (`global.wal`) is shared across all B-trees in the database.
//! Records include the target store name so replay can route operations correctly.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crc32fast::Hasher;

use crate::core::errors::{StorageError, WrongoDBError};
use crate::txn::{Timestamp, TxnId};

/// Global WAL file name.
const GLOBAL_WAL_FILE_NAME: &str = "global.wal";
/// WAL file magic bytes (8 bytes)
const WAL_MAGIC: &[u8; 8] = b"WALG001\0";
/// WAL file format version
const WAL_VERSION: u16 = 1;
/// WAL file header size (512 bytes)
const WAL_HEADER_SIZE: usize = 512;
/// WAL record header size (32 bytes)
const RECORD_HEADER_SIZE: usize = 32;

/// Log Sequence Number - uniquely identifies a position in the WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Lsn {
    pub file_id: u32,
    pub offset: u64,
}

impl Lsn {
    pub fn new(file_id: u32, offset: u64) -> Self {
        Self { file_id, offset }
    }

    pub fn is_valid(&self) -> bool {
        self.file_id != 0 || self.offset != 0
    }
}

/// WAL record type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WalRecordType {
    Put = 1,
    Delete = 2,
    Checkpoint = 3,
    TxnCommit = 4,
    TxnAbort = 5,
}

impl TryFrom<u8> for WalRecordType {
    type Error = WrongoDBError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Put),
            2 => Ok(Self::Delete),
            3 => Ok(Self::Checkpoint),
            4 => Ok(Self::TxnCommit),
            5 => Ok(Self::TxnAbort),
            _ => Err(StorageError(format!("invalid WAL record type: {value}")).into()),
        }
    }
}

/// Global WAL record containing logical operation data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalRecord {
    Put {
        store_name: String,
        key: Vec<u8>,
        value: Vec<u8>,
        txn_id: TxnId,
    },
    Delete {
        store_name: String,
        key: Vec<u8>,
        txn_id: TxnId,
    },
    Checkpoint,
    TxnCommit {
        txn_id: TxnId,
        commit_ts: Timestamp,
    },
    TxnAbort {
        txn_id: TxnId,
    },
}

impl WalRecord {
    pub fn record_type(&self) -> WalRecordType {
        match self {
            WalRecord::Put { .. } => WalRecordType::Put,
            WalRecord::Delete { .. } => WalRecordType::Delete,
            WalRecord::Checkpoint => WalRecordType::Checkpoint,
            WalRecord::TxnCommit { .. } => WalRecordType::TxnCommit,
            WalRecord::TxnAbort { .. } => WalRecordType::TxnAbort,
        }
    }

    fn serialize_payload(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        match self {
            WalRecord::Put {
                store_name,
                key,
                value,
                txn_id,
            } => {
                buf.extend_from_slice(&txn_id.to_le_bytes());
                serialize_string(&mut buf, store_name);
                serialize_bytes(&mut buf, key);
                serialize_bytes(&mut buf, value);
            }
            WalRecord::Delete {
                store_name,
                key,
                txn_id,
            } => {
                buf.extend_from_slice(&txn_id.to_le_bytes());
                serialize_string(&mut buf, store_name);
                serialize_bytes(&mut buf, key);
            }
            WalRecord::Checkpoint => {}
            WalRecord::TxnCommit { txn_id, commit_ts } => {
                buf.extend_from_slice(&txn_id.to_le_bytes());
                buf.extend_from_slice(&commit_ts.to_le_bytes());
            }
            WalRecord::TxnAbort { txn_id } => {
                buf.extend_from_slice(&txn_id.to_le_bytes());
            }
        }

        buf
    }

    fn deserialize_payload(record_type: WalRecordType, data: &[u8]) -> Result<Self, WrongoDBError> {
        let mut cursor = 0usize;

        let read_u64 = |data: &[u8], cursor: &mut usize| -> Result<u64, WrongoDBError> {
            if *cursor + 8 > data.len() {
                return Err(StorageError("unexpected EOF reading u64".into()).into());
            }
            let out = u64::from_le_bytes(
                data[*cursor..*cursor + 8]
                    .try_into()
                    .map_err(|_| StorageError("invalid u64 encoding".into()))?,
            );
            *cursor += 8;
            Ok(out)
        };

        match record_type {
            WalRecordType::Put => {
                let txn_id = read_u64(data, &mut cursor)?;
                let store_name = deserialize_string(data, &mut cursor)?;
                let key = deserialize_bytes(data, &mut cursor)?;
                let value = deserialize_bytes(data, &mut cursor)?;
                Ok(WalRecord::Put {
                    store_name,
                    key,
                    value,
                    txn_id,
                })
            }
            WalRecordType::Delete => {
                let txn_id = read_u64(data, &mut cursor)?;
                let store_name = deserialize_string(data, &mut cursor)?;
                let key = deserialize_bytes(data, &mut cursor)?;
                Ok(WalRecord::Delete {
                    store_name,
                    key,
                    txn_id,
                })
            }
            WalRecordType::Checkpoint => Ok(WalRecord::Checkpoint),
            WalRecordType::TxnCommit => {
                let txn_id = read_u64(data, &mut cursor)?;
                let commit_ts = read_u64(data, &mut cursor)?;
                Ok(WalRecord::TxnCommit { txn_id, commit_ts })
            }
            WalRecordType::TxnAbort => {
                let txn_id = read_u64(data, &mut cursor)?;
                Ok(WalRecord::TxnAbort { txn_id })
            }
        }
    }
}

fn serialize_string(buf: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();
    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(bytes);
}

fn deserialize_string(data: &[u8], cursor: &mut usize) -> Result<String, WrongoDBError> {
    let bytes = deserialize_bytes(data, cursor)?;
    String::from_utf8(bytes)
        .map_err(|e| StorageError(format!("invalid utf8 in store_name: {e}")).into())
}

fn serialize_bytes(buf: &mut Vec<u8>, value: &[u8]) {
    buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
    buf.extend_from_slice(value);
}

fn deserialize_bytes(data: &[u8], cursor: &mut usize) -> Result<Vec<u8>, WrongoDBError> {
    if *cursor + 4 > data.len() {
        return Err(StorageError("unexpected EOF reading length".into()).into());
    }
    let len = u32::from_le_bytes(
        data[*cursor..*cursor + 4]
            .try_into()
            .map_err(|_| StorageError("invalid length encoding".into()))?,
    ) as usize;
    *cursor += 4;
    if *cursor + len > data.len() {
        return Err(StorageError("unexpected EOF reading bytes".into()).into());
    }
    let out = data[*cursor..*cursor + len].to_vec();
    *cursor += len;
    Ok(out)
}

/// WAL file header - written at the start of the WAL file.
#[derive(Debug, Clone)]
struct WalFileHeader {
    magic: [u8; 8],
    version: u16,
    last_lsn: Lsn,
    checkpoint_lsn: Lsn,
    crc32: u32,
}

impl WalFileHeader {
    fn new() -> Self {
        let mut header = Self {
            magic: *WAL_MAGIC,
            version: WAL_VERSION,
            last_lsn: Lsn::new(0, 0),
            checkpoint_lsn: Lsn::new(0, 0),
            crc32: 0,
        };
        header.crc32 = header.compute_crc32();
        header
    }

    fn serialize(&self) -> Vec<u8> {
        let mut buf = vec![0u8; WAL_HEADER_SIZE];
        let mut cursor = 0usize;

        buf[cursor..cursor + 8].copy_from_slice(&self.magic);
        cursor += 8;

        buf[cursor..cursor + 2].copy_from_slice(&self.version.to_le_bytes());
        cursor += 2;

        // reserved
        cursor += 2;

        // reserved
        cursor += 4;

        buf[cursor..cursor + 4].copy_from_slice(&self.last_lsn.file_id.to_le_bytes());
        cursor += 4;
        buf[cursor..cursor + 8].copy_from_slice(&self.last_lsn.offset.to_le_bytes());
        cursor += 8;

        buf[cursor..cursor + 4].copy_from_slice(&self.checkpoint_lsn.file_id.to_le_bytes());
        cursor += 4;
        buf[cursor..cursor + 8].copy_from_slice(&self.checkpoint_lsn.offset.to_le_bytes());
        cursor += 8;

        buf[cursor..cursor + 4].copy_from_slice(&self.crc32.to_le_bytes());

        buf
    }

    fn deserialize(data: &[u8]) -> Result<Self, WrongoDBError> {
        if data.len() < WAL_HEADER_SIZE {
            return Err(StorageError("WAL header too short".into()).into());
        }

        let mut cursor = 0usize;

        let mut magic = [0u8; 8];
        magic.copy_from_slice(&data[cursor..cursor + 8]);
        cursor += 8;

        if magic != *WAL_MAGIC {
            return Err(StorageError(format!("invalid WAL magic: {magic:?}")).into());
        }

        let version = u16::from_le_bytes(
            data[cursor..cursor + 2]
                .try_into()
                .map_err(|_| StorageError("invalid WAL version".into()))?,
        );
        if version != WAL_VERSION {
            return Err(StorageError(format!("unsupported WAL version: {version}")).into());
        }

        cursor += 4; // version + reserved
        cursor += 4; // reserved

        let last_lsn_file_id = u32::from_le_bytes(
            data[cursor..cursor + 4]
                .try_into()
                .map_err(|_| StorageError("invalid last_lsn file_id".into()))?,
        );
        cursor += 4;
        let last_lsn_offset = u64::from_le_bytes(
            data[cursor..cursor + 8]
                .try_into()
                .map_err(|_| StorageError("invalid last_lsn offset".into()))?,
        );
        cursor += 8;

        let checkpoint_lsn_file_id = u32::from_le_bytes(
            data[cursor..cursor + 4]
                .try_into()
                .map_err(|_| StorageError("invalid checkpoint_lsn file_id".into()))?,
        );
        cursor += 4;
        let checkpoint_lsn_offset = u64::from_le_bytes(
            data[cursor..cursor + 8]
                .try_into()
                .map_err(|_| StorageError("invalid checkpoint_lsn offset".into()))?,
        );
        cursor += 8;

        let crc32 = u32::from_le_bytes(
            data[cursor..cursor + 4]
                .try_into()
                .map_err(|_| StorageError("invalid WAL header checksum".into()))?,
        );

        Ok(Self {
            magic,
            version,
            last_lsn: Lsn::new(last_lsn_file_id, last_lsn_offset),
            checkpoint_lsn: Lsn::new(checkpoint_lsn_file_id, checkpoint_lsn_offset),
            crc32,
        })
    }

    fn compute_crc32(&self) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(&self.magic);
        hasher.update(&self.version.to_le_bytes());
        hasher.update(&[0u8, 0u8]);
        hasher.update(&[0u8; 4]);
        hasher.update(&self.last_lsn.file_id.to_le_bytes());
        hasher.update(&self.last_lsn.offset.to_le_bytes());
        hasher.update(&self.checkpoint_lsn.file_id.to_le_bytes());
        hasher.update(&self.checkpoint_lsn.offset.to_le_bytes());
        hasher.finalize()
    }

    fn validate_crc(&self) -> bool {
        self.compute_crc32() == self.crc32
    }
}

/// WAL record header - precedes each record in the WAL.
#[derive(Debug, Clone)]
pub struct WalRecordHeader {
    pub record_type: u8,
    pub flags: u8,
    pub payload_len: u16,
    pub lsn: Lsn,
    pub prev_lsn: Lsn,
    pub crc32: u32,
}

impl WalRecordHeader {
    fn serialize(&self) -> [u8; RECORD_HEADER_SIZE] {
        let mut buf = [0u8; RECORD_HEADER_SIZE];
        let mut cursor = 0usize;

        buf[cursor] = self.record_type;
        cursor += 1;
        buf[cursor] = self.flags;
        cursor += 1;
        buf[cursor..cursor + 2].copy_from_slice(&self.payload_len.to_le_bytes());
        cursor += 2;

        // reserved
        cursor += 2;

        buf[cursor..cursor + 8].copy_from_slice(&self.lsn.offset.to_le_bytes());
        cursor += 8;

        buf[cursor..cursor + 8].copy_from_slice(&self.prev_lsn.offset.to_le_bytes());
        cursor += 8;

        buf[cursor..cursor + 4].copy_from_slice(&self.crc32.to_le_bytes());

        buf
    }

    fn deserialize(data: &[u8]) -> Result<Self, WrongoDBError> {
        if data.len() < RECORD_HEADER_SIZE {
            return Err(StorageError("WAL record header too short".into()).into());
        }

        let mut cursor = 0usize;

        let record_type = data[cursor];
        cursor += 1;
        let flags = data[cursor];
        cursor += 1;
        let payload_len = u16::from_le_bytes(
            data[cursor..cursor + 2]
                .try_into()
                .map_err(|_| StorageError("invalid payload length".into()))?,
        );
        cursor += 2;

        // reserved
        cursor += 2;

        let lsn_offset = u64::from_le_bytes(
            data[cursor..cursor + 8]
                .try_into()
                .map_err(|_| StorageError("invalid record lsn".into()))?,
        );
        cursor += 8;

        let prev_lsn_offset = u64::from_le_bytes(
            data[cursor..cursor + 8]
                .try_into()
                .map_err(|_| StorageError("invalid prev lsn".into()))?,
        );
        cursor += 8;

        let crc32 = u32::from_le_bytes(
            data[cursor..cursor + 4]
                .try_into()
                .map_err(|_| StorageError("invalid record crc".into()))?,
        );

        Ok(Self {
            record_type,
            flags,
            payload_len,
            lsn: Lsn::new(0, lsn_offset),
            prev_lsn: Lsn::new(0, prev_lsn_offset),
            crc32,
        })
    }
}

#[derive(Debug)]
struct WalFile {
    file: File,
    header: WalFileHeader,
    write_buffer: Vec<u8>,
    buffer_capacity: usize,
    last_lsn: Lsn,
    last_record_lsn: Lsn,
}

impl WalFile {
    const DEFAULT_BUFFER_CAPACITY: usize = 64 * 1024;

    fn create<P: AsRef<Path>>(path: P) -> Result<Self, WrongoDBError> {
        let mut file = File::create(path)?;

        let header = WalFileHeader::new();
        let header_bytes = header.serialize();
        file.write_all(&header_bytes)?;
        file.sync_all()?;

        Ok(Self {
            file,
            header,
            write_buffer: Vec::with_capacity(Self::DEFAULT_BUFFER_CAPACITY),
            buffer_capacity: Self::DEFAULT_BUFFER_CAPACITY,
            last_lsn: Lsn::new(0, WAL_HEADER_SIZE as u64),
            last_record_lsn: Lsn::new(0, 0),
        })
    }

    fn open<P: AsRef<Path>>(path: P) -> Result<Self, WrongoDBError> {
        let path = path.as_ref();
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;

        let mut header_bytes = vec![0u8; WAL_HEADER_SIZE];
        file.read_exact(&mut header_bytes)?;
        let mut header = WalFileHeader::deserialize(&header_bytes)?;

        if !header.validate_crc() {
            return Err(StorageError("WAL header CRC32 mismatch".into()).into());
        }

        let (last_record_lsn, mut write_offset, truncate_reason) = scan_wal_tail(path)?;
        let file_len = file.metadata()?.len();
        if write_offset > file_len {
            write_offset = file_len;
        }

        if write_offset < file_len {
            if let Some(reason) = truncate_reason {
                eprintln!("Truncating invalid WAL tail from offset {write_offset}: {reason}");
            }
            file.set_len(write_offset)?;
        }

        if header.last_lsn != last_record_lsn {
            header.last_lsn = last_record_lsn;
            header.crc32 = header.compute_crc32();
            file.seek(SeekFrom::Start(0))?;
            file.write_all(&header.serialize())?;
        }

        file.seek(SeekFrom::Start(write_offset))?;

        Ok(Self {
            file,
            header,
            write_buffer: Vec::with_capacity(Self::DEFAULT_BUFFER_CAPACITY),
            buffer_capacity: Self::DEFAULT_BUFFER_CAPACITY,
            last_lsn: Lsn::new(0, write_offset),
            last_record_lsn,
        })
    }

    fn set_checkpoint_lsn(&mut self, lsn: Lsn) -> Result<(), WrongoDBError> {
        self.header.checkpoint_lsn = lsn;
        self.header.crc32 = self.header.compute_crc32();

        let header_bytes = self.header.serialize();
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&header_bytes)?;
        self.file.sync_all()?;

        Ok(())
    }

    fn truncate_to_checkpoint(&mut self) -> Result<(), WrongoDBError> {
        self.sync()?;
        self.file.set_len(WAL_HEADER_SIZE as u64)?;
        self.last_lsn = Lsn::new(0, WAL_HEADER_SIZE as u64);
        self.last_record_lsn = Lsn::new(0, 0);
        self.header.last_lsn = Lsn::new(0, 0);
        self.header.checkpoint_lsn = Lsn::new(0, 0);
        self.header.crc32 = self.header.compute_crc32();
        let header_bytes = self.header.serialize();
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&header_bytes)?;
        self.file.sync_all()?;
        self.file.seek(SeekFrom::Start(WAL_HEADER_SIZE as u64))?;

        Ok(())
    }

    fn sync(&mut self) -> Result<(), WrongoDBError> {
        self.flush_buffer()?;
        self.file.sync_all()?;
        Ok(())
    }

    fn log_put(
        &mut self,
        store_name: &str,
        key: &[u8],
        value: &[u8],
        txn_id: TxnId,
    ) -> Result<Lsn, WrongoDBError> {
        self.log_put_owned(store_name.to_string(), key.to_vec(), value.to_vec(), txn_id)
    }

    fn log_put_owned(
        &mut self,
        store_name: String,
        key: Vec<u8>,
        value: Vec<u8>,
        txn_id: TxnId,
    ) -> Result<Lsn, WrongoDBError> {
        self.append_record(WalRecord::Put {
            store_name,
            key,
            value,
            txn_id,
        })
    }

    fn log_delete(
        &mut self,
        store_name: &str,
        key: &[u8],
        txn_id: TxnId,
    ) -> Result<Lsn, WrongoDBError> {
        self.log_delete_owned(store_name.to_string(), key.to_vec(), txn_id)
    }

    fn log_delete_owned(
        &mut self,
        store_name: String,
        key: Vec<u8>,
        txn_id: TxnId,
    ) -> Result<Lsn, WrongoDBError> {
        self.append_record(WalRecord::Delete {
            store_name,
            key,
            txn_id,
        })
    }

    fn log_txn_commit(
        &mut self,
        txn_id: TxnId,
        commit_ts: Timestamp,
    ) -> Result<Lsn, WrongoDBError> {
        self.append_record(WalRecord::TxnCommit { txn_id, commit_ts })
    }

    fn log_txn_abort(&mut self, txn_id: TxnId) -> Result<Lsn, WrongoDBError> {
        self.append_record(WalRecord::TxnAbort { txn_id })
    }

    fn log_checkpoint(&mut self) -> Result<Lsn, WrongoDBError> {
        self.append_record(WalRecord::Checkpoint)
    }

    fn append_record(&mut self, record: WalRecord) -> Result<Lsn, WrongoDBError> {
        let payload = record.serialize_payload();
        let record_type = record.record_type() as u8;
        let payload_len = payload.len() as u16;

        let lsn = self.last_lsn;
        let prev_lsn = self.last_record_lsn;

        let mut hasher = Hasher::new();
        hasher.update(&[record_type, 0]);
        hasher.update(&payload_len.to_le_bytes());
        hasher.update(&[0u8, 0u8]);
        hasher.update(&lsn.offset.to_le_bytes());
        hasher.update(&prev_lsn.offset.to_le_bytes());
        hasher.update(&payload);
        let crc32 = hasher.finalize();

        let header = WalRecordHeader {
            record_type,
            flags: 0,
            payload_len,
            lsn,
            prev_lsn,
            crc32,
        };

        self.write_buffered(&header.serialize())?;
        self.write_buffered(&payload)?;

        let record_size = RECORD_HEADER_SIZE + payload.len();
        self.last_lsn = Lsn::new(
            self.last_lsn.file_id,
            self.last_lsn.offset + record_size as u64,
        );
        self.last_record_lsn = lsn;
        self.header.last_lsn = lsn;

        Ok(lsn)
    }

    fn flush_buffer(&mut self) -> Result<(), WrongoDBError> {
        if self.write_buffer.is_empty() {
            return Ok(());
        }

        self.file.write_all(&self.write_buffer)?;
        self.write_buffer.clear();
        Ok(())
    }

    fn write_buffered(&mut self, data: &[u8]) -> Result<(), WrongoDBError> {
        if self.write_buffer.len() + data.len() > self.buffer_capacity {
            self.flush_buffer()?;
        }
        self.write_buffer.extend_from_slice(data);
        Ok(())
    }
}

impl Drop for WalFile {
    fn drop(&mut self) {
        let _ = self.flush_buffer();
    }
}

fn scan_wal_tail(path: &Path) -> Result<(Lsn, u64, Option<String>), WrongoDBError> {
    let mut reader = WalReader::open(path).map_err(|e| {
        StorageError(format!(
            "failed to open WAL reader while scanning tail: {e}"
        ))
    })?;

    let mut last_record_lsn = Lsn::new(0, 0);
    let mut truncate_reason = None;

    let write_offset = loop {
        match reader.read_record() {
            Ok(Some((header, _record))) => {
                last_record_lsn = header.lsn;
            }
            Ok(None) => {
                break reader.current_offset;
            }
            Err(
                err @ (RecoveryError::ChecksumMismatch { .. }
                | RecoveryError::BrokenLsnChain { .. }
                | RecoveryError::CorruptRecordHeader { .. }
                | RecoveryError::CorruptRecordPayload { .. }),
            ) => {
                truncate_reason = Some(err.to_string());
                break reader.current_offset;
            }
            Err(err) => {
                return Err(StorageError(format!("failed while scanning WAL tail: {err}")).into());
            }
        }
    };

    Ok((last_record_lsn, write_offset, truncate_reason))
}

/// Recovery-specific error type for WAL operations.
#[derive(Debug)]
pub enum RecoveryError {
    ChecksumMismatch {
        offset: u64,
        expected: u32,
        actual: u32,
    },
    BrokenLsnChain {
        current_lsn: Lsn,
        expected_prev: Lsn,
        actual_prev: Lsn,
    },
    CorruptRecordHeader {
        offset: u64,
        details: String,
    },
    CorruptRecordPayload {
        record_type: WalRecordType,
        offset: u64,
        details: String,
    },
    InvalidWalFile(String),
    Io(std::io::Error),
}

impl std::fmt::Display for RecoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecoveryError::ChecksumMismatch {
                offset,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "WAL checksum mismatch at offset {offset}: expected {expected:08x}, got {actual:08x}"
                )
            }
            RecoveryError::BrokenLsnChain {
                current_lsn,
                expected_prev,
                actual_prev,
            } => {
                write!(
                    f,
                    "WAL LSN chain broken at LSN {current_lsn:?}: expected prev LSN {expected_prev:?}, got {actual_prev:?}"
                )
            }
            RecoveryError::CorruptRecordHeader { offset, details } => {
                write!(f, "WAL corrupt record header at offset {offset}: {details}")
            }
            RecoveryError::CorruptRecordPayload {
                record_type,
                offset,
                details,
            } => {
                write!(
                    f,
                    "WAL corrupt payload for record type {record_type:?} at offset {offset}: {details}"
                )
            }
            RecoveryError::InvalidWalFile(msg) => write!(f, "Invalid WAL file: {msg}"),
            RecoveryError::Io(err) => write!(f, "WAL IO error: {err}"),
        }
    }
}

impl std::error::Error for RecoveryError {}

impl From<std::io::Error> for RecoveryError {
    fn from(value: std::io::Error) -> Self {
        RecoveryError::Io(value)
    }
}

pub struct WalReader {
    file: File,
    current_offset: u64,
    last_valid_lsn: Lsn,
}

impl WalReader {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, RecoveryError> {
        let path = path.as_ref();
        if !path.exists() {
            return Err(RecoveryError::InvalidWalFile(format!(
                "WAL file does not exist: {}",
                path.display()
            )));
        }

        let mut file = File::open(path)?;

        let mut header_bytes = vec![0u8; WAL_HEADER_SIZE];
        file.read_exact(&mut header_bytes)?;
        let header = WalFileHeader::deserialize(&header_bytes)
            .map_err(|e| RecoveryError::InvalidWalFile(format!("invalid header: {e}")))?;

        if !header.validate_crc() {
            return Err(RecoveryError::InvalidWalFile(
                "WAL header CRC32 mismatch".to_string(),
            ));
        }

        let current_offset = if header.checkpoint_lsn.is_valid() {
            header.checkpoint_lsn.offset
        } else {
            WAL_HEADER_SIZE as u64
        };

        Ok(Self {
            file,
            current_offset,
            last_valid_lsn: Lsn::new(0, 0),
        })
    }

    pub fn read_record(&mut self) -> Result<Option<(WalRecordHeader, WalRecord)>, RecoveryError> {
        self.file.seek(SeekFrom::Start(self.current_offset))?;

        let mut header_bytes = vec![0u8; RECORD_HEADER_SIZE];
        let bytes_read = self.file.read(&mut header_bytes)?;

        if bytes_read == 0 {
            return Ok(None);
        }

        if bytes_read < RECORD_HEADER_SIZE {
            return Ok(None);
        }

        let header = WalRecordHeader::deserialize(&header_bytes).map_err(|e| {
            RecoveryError::CorruptRecordHeader {
                offset: self.current_offset,
                details: e.to_string(),
            }
        })?;

        let payload_len = header.payload_len as usize;
        let mut payload = vec![0u8; payload_len];
        let payload_bytes_read = self.file.read(&mut payload)?;

        if payload_bytes_read < payload_len {
            return Ok(None);
        }

        let crc_header_len = 1 + 1 + 2 + 2 + 8 + 8;
        let mut hasher = Hasher::new();
        hasher.update(&header_bytes[0..crc_header_len]);
        hasher.update(&payload);
        let computed_crc = hasher.finalize();

        if computed_crc != header.crc32 {
            return Err(RecoveryError::ChecksumMismatch {
                offset: self.current_offset,
                expected: header.crc32,
                actual: computed_crc,
            });
        }

        if self.last_valid_lsn.is_valid() && header.prev_lsn != self.last_valid_lsn {
            return Err(RecoveryError::BrokenLsnChain {
                current_lsn: header.lsn,
                expected_prev: self.last_valid_lsn,
                actual_prev: header.prev_lsn,
            });
        }

        let record_type = WalRecordType::try_from(header.record_type).map_err(|_| {
            RecoveryError::CorruptRecordHeader {
                offset: self.current_offset,
                details: format!("invalid record type: {}", header.record_type),
            }
        })?;

        let record = WalRecord::deserialize_payload(record_type, &payload).map_err(|e| {
            RecoveryError::CorruptRecordPayload {
                record_type,
                offset: self.current_offset + RECORD_HEADER_SIZE as u64,
                details: e.to_string(),
            }
        })?;

        self.last_valid_lsn = header.lsn;
        self.current_offset += (RECORD_HEADER_SIZE + payload_len) as u64;

        Ok(Some((header, record)))
    }
}

/// Global WAL handle with batching/sync policy.
#[derive(Debug)]
pub struct GlobalWal {
    file: WalFile,
}

impl GlobalWal {
    pub fn path_for_db<P: AsRef<Path>>(db_dir: P) -> PathBuf {
        db_dir.as_ref().join(GLOBAL_WAL_FILE_NAME)
    }

    pub fn create<P: AsRef<Path>>(db_dir: P) -> Result<Self, WrongoDBError> {
        let path = Self::path_for_db(db_dir);
        let file = WalFile::create(path)?;
        Ok(Self { file })
    }

    pub fn open_or_create<P: AsRef<Path>>(db_dir: P) -> Result<Self, WrongoDBError> {
        let path = Self::path_for_db(db_dir);
        let file = if path.exists() {
            WalFile::open(path)?
        } else {
            WalFile::create(path)?
        };
        Ok(Self { file })
    }

    pub fn log_put(
        &mut self,
        store_name: &str,
        key: &[u8],
        value: &[u8],
        txn_id: TxnId,
    ) -> Result<Lsn, WrongoDBError> {
        self.file.log_put(store_name, key, value, txn_id)
    }

    pub fn log_put_owned(
        &mut self,
        store_name: String,
        key: Vec<u8>,
        value: Vec<u8>,
        txn_id: TxnId,
    ) -> Result<Lsn, WrongoDBError> {
        self.file.log_put_owned(store_name, key, value, txn_id)
    }

    pub fn log_delete(
        &mut self,
        store_name: &str,
        key: &[u8],
        txn_id: TxnId,
    ) -> Result<Lsn, WrongoDBError> {
        self.file.log_delete(store_name, key, txn_id)
    }

    pub fn log_delete_owned(
        &mut self,
        store_name: String,
        key: Vec<u8>,
        txn_id: TxnId,
    ) -> Result<Lsn, WrongoDBError> {
        self.file.log_delete_owned(store_name, key, txn_id)
    }

    pub fn log_txn_commit(
        &mut self,
        txn_id: TxnId,
        commit_ts: Timestamp,
    ) -> Result<Lsn, WrongoDBError> {
        self.file.log_txn_commit(txn_id, commit_ts)
    }

    pub fn log_txn_abort(&mut self, txn_id: TxnId) -> Result<Lsn, WrongoDBError> {
        self.file.log_txn_abort(txn_id)
    }

    pub fn log_checkpoint(&mut self) -> Result<Lsn, WrongoDBError> {
        self.file.log_checkpoint()
    }

    pub fn set_checkpoint_lsn(&mut self, lsn: Lsn) -> Result<(), WrongoDBError> {
        self.file.set_checkpoint_lsn(lsn)
    }

    pub fn truncate_to_checkpoint(&mut self) -> Result<(), WrongoDBError> {
        self.file.truncate_to_checkpoint()
    }

    pub fn sync(&mut self) -> Result<(), WrongoDBError> {
        self.file.sync()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn wal_record_round_trip() {
        let input = WalRecord::Put {
            store_name: "users.main.wt".to_string(),
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            txn_id: 42,
        };

        let payload = input.serialize_payload();
        let out = WalRecord::deserialize_payload(WalRecordType::Put, &payload).unwrap();

        assert_eq!(input, out);
    }

    #[test]
    fn create_log_and_read() {
        let dir = tempdir().unwrap();
        let mut wal = GlobalWal::create(dir.path()).unwrap();

        wal.log_put("users.main.wt", b"k1", b"v1", 1).unwrap();
        wal.log_txn_commit(1, 1).unwrap();
        wal.sync().unwrap();

        let mut reader = WalReader::open(GlobalWal::path_for_db(dir.path())).unwrap();
        let (_header, record) = reader.read_record().unwrap().unwrap();
        match record {
            WalRecord::Put {
                store_name,
                key,
                value,
                txn_id,
            } => {
                assert_eq!(store_name, "users.main.wt");
                assert_eq!(key, b"k1");
                assert_eq!(value, b"v1");
                assert_eq!(txn_id, 1);
            }
            other => panic!("unexpected record: {other:?}"),
        }
    }
}
