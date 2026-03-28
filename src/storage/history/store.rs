use std::path::Path;

use super::encoding::{
    decode_key, decode_value, encode_key, encode_scan_end, encode_scan_start, encode_value,
};

use crate::core::errors::StorageError;
use crate::storage::btree::BTreeCursor;
use crate::storage::mvcc::UpdateType;
use crate::storage::reserved_store::{StoreId, HS_STORE_ID};
use crate::storage::table::{checkpoint_store, open_or_create_btree, scan_range};
use crate::txn::{GlobalTxnState, Timestamp, TXN_NONE};
use crate::WrongoDBError;

#[cfg(test)]
use crate::txn::TS_MAX;

/// One historical version persisted outside the page-local MVCC chain.
///
/// History store entries are an internal storage-engine artifact produced by
/// reconciliation. They carry the source store id, user key, and the version's
/// time window so the older committed value can be recovered later.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::storage) struct HistoryEntry {
    /// User store that owns the original row.
    pub(in crate::storage) store_id: StoreId,
    /// Original row key inside that store.
    pub(in crate::storage) key: Vec<u8>,
    /// Commit timestamp at which this version became visible.
    pub(in crate::storage) start_ts: Timestamp,
    /// Timestamp at which this version stopped being current.
    pub(in crate::storage) stop_ts: Timestamp,
    /// Stored row shape. Reserve updates are never allowed here.
    pub(in crate::storage) update_type: UpdateType,
    /// Full value bytes for standard updates, empty for tombstones.
    pub(in crate::storage) data: Vec<u8>,
}

/// Thin wrapper around the dedicated history-store B-tree.
///
/// The history store is internal-only. It bypasses MVCC update chains and
/// writes reconciled history rows directly into its own page image.
pub(in crate::storage) struct HistoryStore {
    btree: BTreeCursor,
}

impl HistoryStore {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    /// Open or create the history store file at `path`.
    pub(in crate::storage) fn open_or_create<P: AsRef<Path>>(
        path: P,
    ) -> Result<Self, WrongoDBError> {
        Ok(Self {
            btree: open_or_create_btree(path)?,
        })
    }

    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------

    /// Persist an old version that reconciliation is about to discard.
    pub(in crate::storage) fn save_version(
        &mut self,
        entry: &HistoryEntry,
    ) -> Result<(), WrongoDBError> {
        if entry.update_type == UpdateType::Reserve {
            return Err(StorageError(
                "reserve updates cannot be written to the history store".into(),
            )
            .into());
        }
        if entry.update_type == UpdateType::Tombstone && !entry.data.is_empty() {
            return Err(
                StorageError("history store tombstones must not carry value bytes".into()).into(),
            );
        }

        let key = encode_key(entry.store_id, &entry.key, entry.start_ts);
        let value = encode_value(entry.stop_ts, entry.update_type, &entry.data);
        self.btree.write_base_row(&key, &value)
    }

    /// Find the version of `key` in `store_id` that was current at `as_of`.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::storage) fn find_version(
        &mut self,
        store_id: StoreId,
        key: &[u8],
        as_of: Timestamp,
    ) -> Result<Option<HistoryEntry>, WrongoDBError> {
        for (_, entry) in self.scan_versions_for_key(store_id, key)? {
            if entry.start_ts <= as_of && as_of < entry.stop_ts {
                return Ok(Some(entry));
            }
        }

        Ok(None)
    }

    /// Remove all history rows for a `(store_id, key)` pair.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::storage) fn remove_all_for_key(
        &mut self,
        store_id: StoreId,
        key: &[u8],
    ) -> Result<(), WrongoDBError> {
        for (encoded_key, _) in self.scan_versions_for_key(store_id, key)? {
            let _ = self.btree.delete_base_row(&encoded_key)?;
        }

        Ok(())
    }

    /// Reconcile in-memory updates and flush the history store itself.
    pub(in crate::storage) fn checkpoint(
        &mut self,
        global_txn: &GlobalTxnState,
    ) -> Result<(), WrongoDBError> {
        checkpoint_store(&mut self.btree, global_txn, HS_STORE_ID, None)
    }

    // ------------------------------------------------------------------------
    // Private Helpers
    // ------------------------------------------------------------------------

    fn scan_versions_for_key(
        &mut self,
        store_id: StoreId,
        key: &[u8],
    ) -> Result<Vec<(Vec<u8>, HistoryEntry)>, WrongoDBError> {
        let start = encode_scan_start(store_id, key);
        let end = encode_scan_end(store_id, key);
        scan_range(&mut self.btree, Some(&start), Some(&end), TXN_NONE)?
            .into_iter()
            .map(|(encoded_key, encoded_value)| decode_row(encoded_key, encoded_value))
            .collect()
    }
}

fn decode_row(
    encoded_key: Vec<u8>,
    encoded_value: Vec<u8>,
) -> Result<(Vec<u8>, HistoryEntry), WrongoDBError> {
    let (store_id, key, start_ts) = decode_key(&encoded_key)?;
    let (stop_ts, update_type, data) = decode_value(&encoded_value)?;

    Ok((
        encoded_key,
        HistoryEntry {
            store_id,
            key,
            start_ts,
            stop_ts,
            update_type,
            data,
        },
    ))
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::storage::reserved_store::FIRST_DYNAMIC_STORE_ID;

    const TEST_STORE_ID: StoreId = FIRST_DYNAMIC_STORE_ID + 7;

    fn sample_entry(start_ts: Timestamp, stop_ts: Timestamp, data: &[u8]) -> HistoryEntry {
        HistoryEntry {
            store_id: TEST_STORE_ID,
            key: b"k1".to_vec(),
            start_ts,
            stop_ts,
            update_type: UpdateType::Standard,
            data: data.to_vec(),
        }
    }

    #[test]
    fn save_and_find_history_versions_round_trip() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("history.wt");
        let mut history_store = HistoryStore::open_or_create(&path).unwrap();

        history_store
            .save_version(&sample_entry(10, 20, b"v1"))
            .unwrap();

        assert_eq!(
            history_store
                .find_version(TEST_STORE_ID, b"k1", 15)
                .unwrap(),
            Some(sample_entry(10, 20, b"v1"))
        );
        assert_eq!(
            history_store
                .find_version(TEST_STORE_ID, b"k1", 25)
                .unwrap(),
            None
        );
    }

    #[test]
    fn find_version_returns_the_matching_window() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("history_find.wt");
        let mut history_store = HistoryStore::open_or_create(&path).unwrap();

        history_store
            .save_version(&sample_entry(20, TS_MAX, b"v3"))
            .unwrap();
        history_store
            .save_version(&sample_entry(10, 20, b"v2"))
            .unwrap();
        history_store
            .save_version(&sample_entry(1, 10, b"v1"))
            .unwrap();

        assert_eq!(
            history_store
                .find_version(TEST_STORE_ID, b"k1", 19)
                .unwrap(),
            Some(sample_entry(10, 20, b"v2"))
        );
        assert_eq!(
            history_store
                .find_version(TEST_STORE_ID, b"k1", 21)
                .unwrap(),
            Some(sample_entry(20, TS_MAX, b"v3"))
        );
        assert_eq!(
            history_store.find_version(TEST_STORE_ID, b"k1", 5).unwrap(),
            Some(sample_entry(1, 10, b"v1"))
        );
    }

    #[test]
    fn scan_range_returns_versions_in_newest_first_order() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("history_scan.wt");
        let mut history_store = HistoryStore::open_or_create(&path).unwrap();

        history_store
            .save_version(&sample_entry(20, TS_MAX, b"v3"))
            .unwrap();
        history_store
            .save_version(&sample_entry(10, 20, b"v2"))
            .unwrap();
        history_store
            .save_version(&sample_entry(1, 10, b"v1"))
            .unwrap();

        let start = encode_scan_start(TEST_STORE_ID, b"k1");
        let end = encode_scan_end(TEST_STORE_ID, b"k1");
        let entries =
            scan_range(&mut history_store.btree, Some(&start), Some(&end), TXN_NONE).unwrap();
        let ordered_start_ts = entries
            .into_iter()
            .map(|(key, _)| decode_key(&key).unwrap().2)
            .collect::<Vec<_>>();

        assert_eq!(ordered_start_ts, vec![20, 10, 1]);
    }

    #[test]
    fn remove_all_for_key_deletes_all_versions() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("history_remove.wt");
        let mut history_store = HistoryStore::open_or_create(&path).unwrap();

        history_store
            .save_version(&sample_entry(1, 10, b"v1"))
            .unwrap();
        history_store
            .save_version(&HistoryEntry {
                key: b"k2".to_vec(),
                ..sample_entry(10, TS_MAX, b"other")
            })
            .unwrap();

        history_store
            .remove_all_for_key(TEST_STORE_ID, b"k1")
            .unwrap();

        assert_eq!(
            history_store.find_version(TEST_STORE_ID, b"k1", 5).unwrap(),
            None
        );
        assert!(history_store
            .find_version(TEST_STORE_ID, b"k2", 11)
            .unwrap()
            .is_some());
    }
}
