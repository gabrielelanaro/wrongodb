use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crc32fast::Hasher;

use crate::core::errors::StorageError;
use crate::WrongoDBError;

const REPLICATION_CONSISTENCY_FILE: &str = "raft_consistency.bin";
const REPLICATION_CONSISTENCY_MAGIC: &[u8; 8] = b"RFTCONS1";
const REPLICATION_CONSISTENCY_VERSION: u16 = 1;
const FLAG_TRUNCATE_AFTER_PRESENT: u16 = 1;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ReplicationConsistencyState {
    pub(crate) applied_through_index: u64,
    pub(crate) oplog_truncate_after: Option<u64>,
}

#[derive(Debug)]
pub(crate) struct ReplicationConsistencyStore {
    path: PathBuf,
    state: ReplicationConsistencyState,
}

impl ReplicationConsistencyStore {
    pub(crate) fn path_for_db<P: AsRef<Path>>(db_dir: P) -> PathBuf {
        db_dir.as_ref().join(REPLICATION_CONSISTENCY_FILE)
    }

    pub(crate) fn open_or_create<P: AsRef<Path>>(db_dir: P) -> Result<Self, WrongoDBError> {
        let path = Self::path_for_db(db_dir);
        if path.exists() {
            let state = read_state(&path)?;
            return Ok(Self { path, state });
        }

        let state = ReplicationConsistencyState::default();
        persist_state(&path, &state)?;
        Ok(Self { path, state })
    }

    pub(crate) fn state(&self) -> ReplicationConsistencyState {
        self.state
    }

    pub(crate) fn set_applied_through_index(&mut self, index: u64) -> Result<(), WrongoDBError> {
        if index < self.state.applied_through_index {
            return Err(StorageError(format!(
                "replication applied_through_index cannot decrease: current={}, new={index}",
                self.state.applied_through_index,
            ))
            .into());
        }

        if index == self.state.applied_through_index {
            return Ok(());
        }

        self.state.applied_through_index = index;
        persist_state(&self.path, &self.state)
    }
}

fn read_state(path: &Path) -> Result<ReplicationConsistencyState, WrongoDBError> {
    let bytes = fs::read(path)?;
    decode_state(&bytes)
}

fn decode_state(bytes: &[u8]) -> Result<ReplicationConsistencyState, WrongoDBError> {
    if bytes.len() != 8 + 2 + 2 + 8 + 8 + 4 {
        return Err(StorageError("replication consistency file has invalid length".into()).into());
    }

    let payload_len = bytes.len() - 4;
    let expected_crc = u32::from_le_bytes(
        bytes[payload_len..]
            .try_into()
            .map_err(|_| StorageError("invalid replication consistency checksum".into()))?,
    );
    let mut hasher = Hasher::new();
    hasher.update(&bytes[..payload_len]);
    let actual_crc = hasher.finalize();
    if expected_crc != actual_crc {
        return Err(StorageError(format!(
            "replication consistency CRC32 mismatch: expected {expected_crc:08x}, got {actual_crc:08x}"
        ))
        .into());
    }

    let mut cursor = 0usize;

    let mut magic = [0u8; 8];
    magic.copy_from_slice(&bytes[cursor..cursor + 8]);
    cursor += 8;
    if magic != *REPLICATION_CONSISTENCY_MAGIC {
        return Err(
            StorageError(format!("invalid replication consistency magic: {magic:?}")).into(),
        );
    }

    let version = u16::from_le_bytes(
        bytes[cursor..cursor + 2]
            .try_into()
            .map_err(|_| StorageError("invalid replication consistency version".into()))?,
    );
    cursor += 2;
    if version != REPLICATION_CONSISTENCY_VERSION {
        return Err(StorageError(format!(
            "unsupported replication consistency version: {version}"
        ))
        .into());
    }

    let flags = u16::from_le_bytes(
        bytes[cursor..cursor + 2]
            .try_into()
            .map_err(|_| StorageError("invalid replication consistency flags".into()))?,
    );
    cursor += 2;
    if flags & !FLAG_TRUNCATE_AFTER_PRESENT != 0 {
        return Err(
            StorageError(format!("invalid replication consistency flags: {flags:#x}")).into(),
        );
    }

    let applied_through_index = u64::from_le_bytes(
        bytes[cursor..cursor + 8]
            .try_into()
            .map_err(|_| StorageError("invalid applied_through_index".into()))?,
    );
    cursor += 8;

    let truncate_after_raw = u64::from_le_bytes(
        bytes[cursor..cursor + 8]
            .try_into()
            .map_err(|_| StorageError("invalid oplog_truncate_after".into()))?,
    );

    let oplog_truncate_after = if (flags & FLAG_TRUNCATE_AFTER_PRESENT) != 0 {
        Some(truncate_after_raw)
    } else {
        None
    };

    Ok(ReplicationConsistencyState {
        applied_through_index,
        oplog_truncate_after,
    })
}

fn encode_state(state: &ReplicationConsistencyState) -> Vec<u8> {
    let mut payload = Vec::with_capacity(8 + 2 + 2 + 8 + 8 + 4);
    payload.extend_from_slice(REPLICATION_CONSISTENCY_MAGIC);
    payload.extend_from_slice(&REPLICATION_CONSISTENCY_VERSION.to_le_bytes());

    let mut flags = 0u16;
    if state.oplog_truncate_after.is_some() {
        flags |= FLAG_TRUNCATE_AFTER_PRESENT;
    }
    payload.extend_from_slice(&flags.to_le_bytes());
    payload.extend_from_slice(&state.applied_through_index.to_le_bytes());
    payload.extend_from_slice(&state.oplog_truncate_after.unwrap_or(0).to_le_bytes());

    let mut hasher = Hasher::new();
    hasher.update(&payload);
    let crc32 = hasher.finalize();
    payload.extend_from_slice(&crc32.to_le_bytes());
    payload
}

fn persist_state(path: &Path, state: &ReplicationConsistencyState) -> Result<(), WrongoDBError> {
    let bytes = encode_state(state);

    let mut tmp_name: OsString = path.as_os_str().to_os_string();
    tmp_name.push(".tmp");
    let tmp_path = PathBuf::from(tmp_name);

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp_path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);

    fs::rename(&tmp_path, path)?;

    let file = File::open(path)?;
    file.sync_all()?;
    sync_parent_dir(path);
    Ok(())
}

fn sync_parent_dir(path: &Path) {
    #[cfg(unix)]
    {
        if let Some(parent) = path.parent() {
            if let Ok(dir) = File::open(parent) {
                let _ = dir.sync_all();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn missing_file_initializes_default_state() {
        let dir = tempdir().unwrap();
        let store = ReplicationConsistencyStore::open_or_create(dir.path()).unwrap();

        assert_eq!(store.state(), ReplicationConsistencyState::default());
    }

    #[test]
    fn state_round_trips_applied_through_index() {
        let dir = tempdir().unwrap();
        let mut store = ReplicationConsistencyStore::open_or_create(dir.path()).unwrap();
        store.set_applied_through_index(7).unwrap();
        drop(store);

        let reopened = ReplicationConsistencyStore::open_or_create(dir.path()).unwrap();
        assert_eq!(
            reopened.state(),
            ReplicationConsistencyState {
                applied_through_index: 7,
                oplog_truncate_after: None,
            }
        );
    }

    #[test]
    fn corrupted_file_fails_open() {
        let dir = tempdir().unwrap();
        let path = ReplicationConsistencyStore::path_for_db(dir.path());
        let mut store = ReplicationConsistencyStore::open_or_create(dir.path()).unwrap();
        store.set_applied_through_index(3).unwrap();
        drop(store);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&[0xFF]).unwrap();
        file.sync_all().unwrap();

        let err = ReplicationConsistencyStore::open_or_create(dir.path()).unwrap_err();
        assert!(err.to_string().contains("replication consistency"));
    }

    #[test]
    fn unsupported_version_fails_open() {
        let dir = tempdir().unwrap();
        let path = ReplicationConsistencyStore::path_for_db(dir.path());
        let _store = ReplicationConsistencyStore::open_or_create(dir.path()).unwrap();

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::Start(8)).unwrap();
        file.write_all(&99u16.to_le_bytes()).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        let payload_len = bytes.len() - 4;
        let mut hasher = Hasher::new();
        hasher.update(&bytes[..payload_len]);
        let crc32 = hasher.finalize();
        bytes[payload_len..].copy_from_slice(&crc32.to_le_bytes());
        file.write_all(&bytes).unwrap();
        file.sync_all().unwrap();

        let err = ReplicationConsistencyStore::open_or_create(dir.path()).unwrap_err();
        assert!(err
            .to_string()
            .contains("unsupported replication consistency version"));
    }
}
