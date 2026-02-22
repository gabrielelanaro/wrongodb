use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crc32fast::Hasher;

use crate::core::errors::StorageError;
use crate::WrongoDBError;

const RAFT_HARD_STATE_FILE: &str = "raft_state.bin";
const RAFT_HARD_STATE_MAGIC: &[u8; 8] = b"RFTHS01\0";
const RAFT_HARD_STATE_VERSION: u16 = 1;
const FLAG_VOTED_FOR_PRESENT: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RaftHardState {
    pub current_term: u64,
    pub voted_for: Option<String>,
}

impl Default for RaftHardState {
    fn default() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
        }
    }
}

#[derive(Debug)]
pub(crate) struct RaftHardStateStore {
    #[allow(dead_code)]
    path: PathBuf,
    state: RaftHardState,
}

impl RaftHardStateStore {
    pub(crate) fn path_for_db<P: AsRef<Path>>(db_dir: P) -> PathBuf {
        db_dir.as_ref().join(RAFT_HARD_STATE_FILE)
    }

    pub(crate) fn open_or_create<P: AsRef<Path>>(db_dir: P) -> Result<Self, WrongoDBError> {
        let path = Self::path_for_db(db_dir);

        if path.exists() {
            let state = read_state(&path)?;
            return Ok(Self { path, state });
        }

        let state = RaftHardState::default();
        persist_state(&path, &state)?;
        Ok(Self { path, state })
    }

    pub(crate) fn state(&self) -> &RaftHardState {
        &self.state
    }

    #[allow(dead_code)]
    pub(crate) fn set_current_term(&mut self, new_term: u64) -> Result<(), WrongoDBError> {
        if new_term < self.state.current_term {
            return Err(StorageError(format!(
                "raft term cannot decrease: current={}, new={}",
                self.state.current_term, new_term
            ))
            .into());
        }

        if new_term == self.state.current_term {
            return Ok(());
        }

        self.state.current_term = new_term;
        self.state.voted_for = None;
        persist_state(&self.path, &self.state)
    }

    #[allow(dead_code)]
    pub(crate) fn set_voted_for(&mut self, candidate: Option<&str>) -> Result<(), WrongoDBError> {
        if let Some(candidate) = candidate {
            if candidate.is_empty() {
                return Err(StorageError("raft voted_for cannot be empty".into()).into());
            }
        }

        let next = candidate.map(ToString::to_string);
        if self.state.voted_for == next {
            return Ok(());
        }

        self.state.voted_for = next;
        persist_state(&self.path, &self.state)
    }
}

fn read_state(path: &Path) -> Result<RaftHardState, WrongoDBError> {
    let bytes = fs::read(path)?;
    decode_state(&bytes)
}

fn decode_state(bytes: &[u8]) -> Result<RaftHardState, WrongoDBError> {
    if bytes.len() < 8 + 2 + 2 + 8 + 4 + 4 {
        return Err(StorageError("raft hard state file too short".into()).into());
    }

    let payload_len = bytes.len() - 4;
    let expected_crc = u32::from_le_bytes(
        bytes[payload_len..]
            .try_into()
            .map_err(|_| StorageError("invalid raft hard state checksum".into()))?,
    );
    let mut hasher = Hasher::new();
    hasher.update(&bytes[..payload_len]);
    let actual_crc = hasher.finalize();
    if expected_crc != actual_crc {
        return Err(StorageError(format!(
            "raft hard state CRC32 mismatch: expected {expected_crc:08x}, got {actual_crc:08x}"
        ))
        .into());
    }

    let mut cursor = 0usize;

    let mut magic = [0u8; 8];
    magic.copy_from_slice(&bytes[cursor..cursor + 8]);
    cursor += 8;
    if magic != *RAFT_HARD_STATE_MAGIC {
        return Err(StorageError(format!("invalid raft hard state magic: {magic:?}")).into());
    }

    let version = u16::from_le_bytes(
        bytes[cursor..cursor + 2]
            .try_into()
            .map_err(|_| StorageError("invalid raft hard state version".into()))?,
    );
    cursor += 2;
    if version != RAFT_HARD_STATE_VERSION {
        return Err(StorageError(format!("unsupported raft hard state version: {version}")).into());
    }

    let flags = u16::from_le_bytes(
        bytes[cursor..cursor + 2]
            .try_into()
            .map_err(|_| StorageError("invalid raft hard state flags".into()))?,
    );
    cursor += 2;
    if flags & !FLAG_VOTED_FOR_PRESENT != 0 {
        return Err(StorageError(format!("invalid raft hard state flags: {flags:#x}")).into());
    }

    let current_term = u64::from_le_bytes(
        bytes[cursor..cursor + 8]
            .try_into()
            .map_err(|_| StorageError("invalid raft hard state term".into()))?,
    );
    cursor += 8;

    let voted_for_len = u32::from_le_bytes(
        bytes[cursor..cursor + 4]
            .try_into()
            .map_err(|_| StorageError("invalid raft hard state voted_for length".into()))?,
    ) as usize;
    cursor += 4;

    if cursor + voted_for_len != payload_len {
        return Err(StorageError("raft hard state length mismatch".into()).into());
    }

    let voted_for_present = (flags & FLAG_VOTED_FOR_PRESENT) != 0;
    let voted_for = if voted_for_present {
        if voted_for_len == 0 {
            return Err(
                StorageError("raft hard state voted_for flag set with empty value".into()).into(),
            );
        }
        let bytes = &bytes[cursor..cursor + voted_for_len];
        Some(
            String::from_utf8(bytes.to_vec())
                .map_err(|e| StorageError(format!("invalid UTF-8 in raft voted_for: {e}")))?,
        )
    } else {
        if voted_for_len != 0 {
            return Err(StorageError(
                "raft hard state voted_for length must be zero when flag is not set".into(),
            )
            .into());
        }
        None
    };

    Ok(RaftHardState {
        current_term,
        voted_for,
    })
}

fn encode_state(state: &RaftHardState) -> Vec<u8> {
    let voted_for_bytes = state.voted_for.as_deref().unwrap_or("").as_bytes();
    let mut payload = Vec::with_capacity(8 + 2 + 2 + 8 + 4 + voted_for_bytes.len());

    payload.extend_from_slice(RAFT_HARD_STATE_MAGIC);
    payload.extend_from_slice(&RAFT_HARD_STATE_VERSION.to_le_bytes());

    let mut flags = 0u16;
    if state.voted_for.is_some() {
        flags |= FLAG_VOTED_FOR_PRESENT;
    }
    payload.extend_from_slice(&flags.to_le_bytes());

    payload.extend_from_slice(&state.current_term.to_le_bytes());
    payload.extend_from_slice(&(voted_for_bytes.len() as u32).to_le_bytes());
    payload.extend_from_slice(voted_for_bytes);

    let mut hasher = Hasher::new();
    hasher.update(&payload);
    let crc32 = hasher.finalize();

    payload.extend_from_slice(&crc32.to_le_bytes());
    payload
}

fn persist_state(path: &Path, state: &RaftHardState) -> Result<(), WrongoDBError> {
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
            match File::open(parent) {
                Ok(dir) => {
                    if let Err(e) = dir.sync_all() {
                        eprintln!("raft hard-state parent dir sync failed: {e}");
                    }
                }
                Err(e) => {
                    eprintln!("raft hard-state parent dir open failed: {e}");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{self, OpenOptions};
    use std::io::{Seek, SeekFrom, Write};

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn missing_file_initializes_default_state() {
        let dir = tempdir().unwrap();
        let path = RaftHardStateStore::path_for_db(dir.path());
        assert!(!path.exists());

        let store = RaftHardStateStore::open_or_create(dir.path()).unwrap();
        assert_eq!(store.state().current_term, 0);
        assert_eq!(store.state().voted_for, None);
        assert!(path.exists());
    }

    #[test]
    fn persist_and_reload_round_trip() {
        let dir = tempdir().unwrap();
        let mut store = RaftHardStateStore::open_or_create(dir.path()).unwrap();

        store.set_current_term(7).unwrap();
        store.set_voted_for(Some("node-a")).unwrap();

        let reloaded = RaftHardStateStore::open_or_create(dir.path()).unwrap();
        assert_eq!(reloaded.state().current_term, 7);
        assert_eq!(reloaded.state().voted_for.as_deref(), Some("node-a"));
    }

    #[test]
    fn crc_mismatch_fails_open() {
        let dir = tempdir().unwrap();
        let path = RaftHardStateStore::path_for_db(dir.path());
        let _store = RaftHardStateStore::open_or_create(dir.path()).unwrap();

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::End(-1)).unwrap();
        file.write_all(&[0x5A]).unwrap();
        file.sync_all().unwrap();

        let err = RaftHardStateStore::open_or_create(dir.path()).unwrap_err();
        assert!(err.to_string().contains("CRC32 mismatch"));
    }

    #[test]
    fn bad_magic_or_version_fails_open() {
        let dir = tempdir().unwrap();
        let path = RaftHardStateStore::path_for_db(dir.path());

        let mut bytes = encode_state(&RaftHardState::default());
        bytes[0] = b'X';
        let mut hasher = Hasher::new();
        hasher.update(&bytes[..bytes.len() - 4]);
        let crc = hasher.finalize();
        let tail = bytes.len() - 4;
        bytes[tail..].copy_from_slice(&crc.to_le_bytes());
        fs::write(&path, &bytes).unwrap();
        let err = RaftHardStateStore::open_or_create(dir.path()).unwrap_err();
        assert!(err.to_string().contains("invalid raft hard state magic"));

        let mut bytes = encode_state(&RaftHardState::default());
        bytes[8..10].copy_from_slice(&2u16.to_le_bytes());
        let mut hasher = Hasher::new();
        hasher.update(&bytes[..bytes.len() - 4]);
        let crc = hasher.finalize();
        let tail = bytes.len() - 4;
        bytes[tail..].copy_from_slice(&crc.to_le_bytes());
        fs::write(&path, &bytes).unwrap();
        let err = RaftHardStateStore::open_or_create(dir.path()).unwrap_err();
        assert!(err
            .to_string()
            .contains("unsupported raft hard state version"));
    }

    #[test]
    fn non_utf8_voted_for_fails_open() {
        let dir = tempdir().unwrap();
        let path = RaftHardStateStore::path_for_db(dir.path());

        let mut payload = Vec::new();
        payload.extend_from_slice(RAFT_HARD_STATE_MAGIC);
        payload.extend_from_slice(&RAFT_HARD_STATE_VERSION.to_le_bytes());
        payload.extend_from_slice(&FLAG_VOTED_FOR_PRESENT.to_le_bytes());
        payload.extend_from_slice(&9u64.to_le_bytes());
        payload.extend_from_slice(&2u32.to_le_bytes());
        payload.extend_from_slice(&[0xFF, 0xFF]);

        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let crc32 = hasher.finalize();
        payload.extend_from_slice(&crc32.to_le_bytes());

        fs::write(&path, payload).unwrap();
        let err = RaftHardStateStore::open_or_create(dir.path()).unwrap_err();
        assert!(err.to_string().contains("invalid UTF-8"));
    }
}
