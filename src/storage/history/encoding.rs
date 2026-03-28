#![cfg_attr(not(test), allow(dead_code))]

use byteorder::{BigEndian, ByteOrder};

use crate::core::errors::StorageError;
use crate::storage::mvcc::UpdateType;
use crate::storage::reserved_store::StoreId;
use crate::txn::{Timestamp, TS_MAX};
use crate::WrongoDBError;

const STORE_ID_LEN: usize = 4;
const KEY_LEN_LEN: usize = 2;
const START_TS_LEN: usize = 8;
const STOP_TS_LEN: usize = 8;
const UPDATE_TYPE_LEN: usize = 1;
const DATA_LEN_LEN: usize = 4;

// ============================================================================
// History Key Encoding
// ============================================================================

// History keys sort by `(store_id, user_key, start_ts desc)` so a forward scan
// over one key sees the newest stored version first.

pub(super) fn encode_key(store_id: StoreId, user_key: &[u8], start_ts: Timestamp) -> Vec<u8> {
    let store_id = u32::try_from(store_id).expect("history store ids must fit in u32");
    let key_len = u16::try_from(user_key.len()).expect("history store user keys must fit in u16");
    let mut buf = Vec::with_capacity(STORE_ID_LEN + KEY_LEN_LEN + user_key.len() + START_TS_LEN);
    let mut store_id_buf = [0; STORE_ID_LEN];
    let mut key_len_buf = [0; KEY_LEN_LEN];
    let mut start_ts_buf = [0; START_TS_LEN];

    BigEndian::write_u32(&mut store_id_buf, store_id);
    BigEndian::write_u16(&mut key_len_buf, key_len);
    BigEndian::write_u64(&mut start_ts_buf, descending_start_ts(start_ts));

    buf.extend_from_slice(&store_id_buf);
    buf.extend_from_slice(&key_len_buf);
    buf.extend_from_slice(user_key);
    buf.extend_from_slice(&start_ts_buf);
    buf
}

pub(super) fn decode_key(buf: &[u8]) -> Result<(StoreId, Vec<u8>, Timestamp), WrongoDBError> {
    if buf.len() < STORE_ID_LEN + KEY_LEN_LEN + START_TS_LEN {
        return Err(StorageError("history store key too short".into()).into());
    }

    let store_id = u64::from(BigEndian::read_u32(&buf[..STORE_ID_LEN]));
    let key_len = usize::from(BigEndian::read_u16(
        &buf[STORE_ID_LEN..STORE_ID_LEN + KEY_LEN_LEN],
    ));
    let key_end = STORE_ID_LEN + KEY_LEN_LEN + key_len;
    if buf.len() != key_end + START_TS_LEN {
        return Err(StorageError("history store key has an invalid length".into()).into());
    }

    let start_ts = ascending_start_ts(BigEndian::read_u64(&buf[key_end..]));
    Ok((
        store_id,
        buf[STORE_ID_LEN + KEY_LEN_LEN..key_end].to_vec(),
        start_ts,
    ))
}

// ============================================================================
// History Value Encoding
// ============================================================================

pub(super) fn encode_value(stop_ts: Timestamp, update_type: UpdateType, data: &[u8]) -> Vec<u8> {
    let data_len = u32::try_from(data.len()).expect("history store values must fit in u32");
    let mut buf = Vec::with_capacity(STOP_TS_LEN + UPDATE_TYPE_LEN + DATA_LEN_LEN + data.len());
    let mut stop_ts_buf = [0; STOP_TS_LEN];
    let mut data_len_buf = [0; DATA_LEN_LEN];

    BigEndian::write_u64(&mut stop_ts_buf, stop_ts);
    BigEndian::write_u32(&mut data_len_buf, data_len);

    buf.extend_from_slice(&stop_ts_buf);
    buf.push(encode_update_type(update_type));
    buf.extend_from_slice(&data_len_buf);
    buf.extend_from_slice(data);
    buf
}

pub(super) fn decode_value(buf: &[u8]) -> Result<(Timestamp, UpdateType, Vec<u8>), WrongoDBError> {
    let header_len = STOP_TS_LEN + UPDATE_TYPE_LEN + DATA_LEN_LEN;
    if buf.len() < header_len {
        return Err(StorageError("history store value too short".into()).into());
    }

    let stop_ts = BigEndian::read_u64(&buf[..STOP_TS_LEN]);
    let update_type = decode_update_type(buf[STOP_TS_LEN])?;
    let data_len = usize::try_from(BigEndian::read_u32(
        &buf[STOP_TS_LEN + UPDATE_TYPE_LEN..header_len],
    ))
    .expect("u32 always fits in usize");
    if buf.len() != header_len + data_len {
        return Err(StorageError("history store value has an invalid length".into()).into());
    }

    let data = buf[header_len..].to_vec();
    Ok((stop_ts, update_type, data))
}

// ============================================================================
// Scan Bounds
// ============================================================================

// These bounds cover all history entries for one `(store_id, user_key)` pair.

pub(super) fn encode_scan_start(store_id: StoreId, user_key: &[u8]) -> Vec<u8> {
    let store_id = u32::try_from(store_id).expect("history store ids must fit in u32");
    let key_len = u16::try_from(user_key.len()).expect("history store user keys must fit in u16");
    let mut buf = Vec::with_capacity(STORE_ID_LEN + KEY_LEN_LEN + user_key.len());
    let mut store_id_buf = [0; STORE_ID_LEN];
    let mut key_len_buf = [0; KEY_LEN_LEN];

    BigEndian::write_u32(&mut store_id_buf, store_id);
    BigEndian::write_u16(&mut key_len_buf, key_len);

    buf.extend_from_slice(&store_id_buf);
    buf.extend_from_slice(&key_len_buf);
    buf.extend_from_slice(user_key);
    buf
}

pub(super) fn encode_scan_end(store_id: StoreId, user_key: &[u8]) -> Vec<u8> {
    prefix_upper_bound(&encode_scan_start(store_id, user_key))
}

// ============================================================================
// Helper Functions
// ============================================================================

fn encode_update_type(update_type: UpdateType) -> u8 {
    match update_type {
        UpdateType::Standard => 1,
        UpdateType::Tombstone => 2,
        UpdateType::Reserve => 3,
    }
}

fn decode_update_type(tag: u8) -> Result<UpdateType, WrongoDBError> {
    match tag {
        1 => Ok(UpdateType::Standard),
        2 => Ok(UpdateType::Tombstone),
        3 => Ok(UpdateType::Reserve),
        _ => Err(StorageError(format!("unknown history store update type tag: {tag}")).into()),
    }
}

fn descending_start_ts(start_ts: Timestamp) -> Timestamp {
    TS_MAX - start_ts
}

fn ascending_start_ts(encoded: Timestamp) -> Timestamp {
    TS_MAX - encoded
}

fn prefix_upper_bound(prefix: &[u8]) -> Vec<u8> {
    let mut bound = prefix.to_vec();
    for index in (0..bound.len()).rev() {
        if bound[index] != u8::MAX {
            bound[index] += 1;
            bound.truncate(index + 1);
            return bound;
        }
    }

    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_store_key_round_trips() {
        let encoded = encode_key(7, b"user-key", 42);
        let decoded = decode_key(&encoded).unwrap();
        assert_eq!(decoded, (7, b"user-key".to_vec(), 42));
    }

    #[test]
    fn history_store_value_round_trips() {
        let encoded = encode_value(99, UpdateType::Standard, b"value");
        let decoded = decode_value(&encoded).unwrap();
        assert_eq!(decoded, (99, UpdateType::Standard, b"value".to_vec()));
    }

    #[test]
    fn history_store_key_orders_newest_first() {
        let newer = encode_key(7, b"user-key", 100);
        let older = encode_key(7, b"user-key", 10);
        assert!(newer < older);
    }

    #[test]
    fn history_store_scan_bounds_cover_one_key_prefix() {
        let start = encode_scan_start(7, b"user-key");
        let end = encode_scan_end(7, b"user-key");
        let key = encode_key(7, b"user-key", 42);
        let other = encode_key(7, b"user-key-2", 42);

        assert!(start.as_slice() <= key.as_slice());
        assert!(key.as_slice() < end.as_slice());
        assert!(other.as_slice() >= end.as_slice());
    }
}
