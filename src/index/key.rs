//! Index key encoding for persistent secondary indexes.
//!
//! This module provides composite key encoding for B+tree secondary indexes.
//! Keys are encoded as:
//!   [scalar_type: 1 byte][scalar_value: variable][id_len: 4 bytes LE][id_bytes]
//!
//! The `_id` bytes are appended to the scalar value to ensure uniqueness
//! when multiple documents have the same indexed value (duplicate support).

use serde_json::Value;

use crate::core::bson::{decode_id_value, encode_id_value};
use crate::WrongoDBError;

/// Scalar type tags for key encoding.
/// These are single-byte encodings that also encode the value for fixed-size types.
const TAG_NULL: u8 = 0x00;
const TAG_BOOL_FALSE: u8 = 0x01;
const TAG_BOOL_TRUE: u8 = 0x02;
const TAG_NUMBER: u8 = 0x03;
const TAG_STRING: u8 = 0x04;

/// Encode f64 into bytes that sort naturally (negative < positive).
///
/// IEEE 754 f64 bits don't sort naturally. This transformation:
/// - For positive numbers: flip the sign bit (0x8000...)
/// - For negative numbers: flip all bits
///
/// This produces a byte representation where lexicographic ordering
/// matches numeric ordering.
fn encode_f64_for_sort(f: f64) -> [u8; 8] {
    let bits = f.to_bits();
    // Transform: if positive, flip sign bit; if negative, flip all bits
    let transformed = if bits & 0x8000_0000_0000_0000 == 0 {
        // Positive: flip sign bit to make it larger than negatives
        bits | 0x8000_0000_0000_0000
    } else {
        // Negative: flip all bits so they sort in reverse
        !bits
    };
    transformed.to_be_bytes() // Use big-endian so lexicographic = numeric
}

/// Encode a scalar value and document _id into a composite index key.
///
/// The resulting key can be stored in a B+tree and supports:
/// - Natural sort order by scalar value
/// - Duplicate values (different _id values = different keys)
/// - Range scans by scalar value prefix
///
/// # Format
/// ```text
/// [scalar_type: 1 byte][scalar_value: variable][id_len: 4 bytes LE][id_bytes]
/// ```
///
/// # Type Encodings
/// - Null: `0x00` (no additional bytes)
/// - Bool false: `0x01` (type byte itself encodes false)
/// - Bool true: `0x02` (type byte itself encodes true)
/// - Number: `0x03` followed by 8-byte LE f64
/// - String: `0x04` followed by 4-byte LE length + UTF-8 bytes
pub fn encode_index_key(scalar: &Value, id: &Value) -> Result<Option<Vec<u8>>, WrongoDBError> {
    let mut result = match encode_scalar_prefix(scalar) {
        Some(prefix) => prefix,
        None => return Ok(None),
    };
    let id_bytes = encode_id_value(id)?;
    let id_len: u32 = id_bytes
        .len()
        .try_into()
        .map_err(|_| crate::core::errors::StorageError("id encoding too large".into()))?;
    result.extend_from_slice(&id_len.to_le_bytes());
    result.extend_from_slice(&id_bytes);
    Ok(Some(result))
}

/// Encode just the scalar value prefix (without _id).
///
/// This is useful for range scans where you want to find all records
/// matching a particular scalar value regardless of _id.
pub fn encode_scalar_prefix(scalar: &Value) -> Option<Vec<u8>> {
    match scalar {
        Value::Null => Some(vec![TAG_NULL]),
        Value::Bool(false) => Some(vec![TAG_BOOL_FALSE]),
        Value::Bool(true) => Some(vec![TAG_BOOL_TRUE]),
        Value::Number(n) => {
            let f = n.as_f64()?;
            let mut result = vec![TAG_NUMBER];
            result.extend_from_slice(&encode_f64_for_sort(f));
            Some(result)
        }
        Value::String(s) => {
            let bytes = s.as_bytes();
            let len = bytes.len() as u32;
            let mut result = vec![TAG_STRING];
            result.extend_from_slice(&len.to_le_bytes());
            result.extend_from_slice(bytes);
            Some(result)
        }
        Value::Array(_) | Value::Object(_) => None,
    }
}

/// Encode a range scan prefix for finding all records with a given scalar value.
///
/// Returns (start_key, end_key) where:
/// - start_key includes the scalar value with id_len = 0
/// - end_key includes the scalar value with id_len = u32::MAX
///
/// This allows a range scan to find all matching records.
pub fn encode_range_bounds(scalar: &Value) -> Option<(Vec<u8>, Vec<u8>)> {
    let prefix = encode_scalar_prefix(scalar)?;

    let mut start = prefix.clone();
    start.extend_from_slice(&0u32.to_le_bytes());

    let mut end = prefix;
    end.extend_from_slice(&u32::MAX.to_le_bytes());

    Some((start, end))
}

/// Decode the document _id from a composite index key.
///
/// Returns Ok(None) if the key is too short or malformed.
pub fn decode_index_id(key: &[u8]) -> Result<Option<Value>, WrongoDBError> {
    let prefix_len = match scalar_prefix_len(key) {
        Some(len) => len,
        None => return Ok(None),
    };
    if key.len() < prefix_len + 4 {
        return Ok(None);
    }
    let mut len_bytes = [0u8; 4];
    len_bytes.copy_from_slice(&key[prefix_len..prefix_len + 4]);
    let id_len = u32::from_le_bytes(len_bytes) as usize;
    let start = prefix_len + 4;
    let end = start + id_len;
    if key.len() < end {
        return Ok(None);
    }
    let id_bytes = &key[start..end];
    Ok(Some(decode_id_value(id_bytes)?))
}

fn scalar_prefix_len(key: &[u8]) -> Option<usize> {
    if key.is_empty() {
        return None;
    }
    match key[0] {
        TAG_NULL | TAG_BOOL_FALSE | TAG_BOOL_TRUE => Some(1),
        TAG_NUMBER => {
            if key.len() < 1 + 8 {
                None
            } else {
                Some(1 + 8)
            }
        }
        TAG_STRING => {
            if key.len() < 1 + 4 {
                return None;
            }
            let mut len_bytes = [0u8; 4];
            len_bytes.copy_from_slice(&key[1..5]);
            let len = u32::from_le_bytes(len_bytes) as usize;
            if key.len() < 1 + 4 + len {
                None
            } else {
                Some(1 + 4 + len)
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::bson::encode_id_value;

    #[test]
    fn encode_null() {
        let id = json!("id");
        let key = encode_index_key(&Value::Null, &id).unwrap().unwrap();
        assert!(key.len() >= 5); // type + id length
        assert_eq!(key[0], TAG_NULL);
        assert_eq!(decode_index_id(&key).unwrap(), Some(id));
    }

    #[test]
    fn encode_bool_false() {
        let id = json!(100);
        let key = encode_index_key(&Value::Bool(false), &id).unwrap().unwrap();
        assert!(key.len() >= 5);
        assert_eq!(key[0], TAG_BOOL_FALSE);
        assert_eq!(decode_index_id(&key).unwrap(), Some(id));
    }

    #[test]
    fn encode_bool_true() {
        let id = json!(200);
        let key = encode_index_key(&Value::Bool(true), &id).unwrap().unwrap();
        assert!(key.len() >= 5);
        assert_eq!(key[0], TAG_BOOL_TRUE);
        assert_eq!(decode_index_id(&key).unwrap(), Some(id));
    }

    #[test]
    fn encode_number() {
        let id = json!("abc");
        let key = encode_index_key(&json!(3.14159), &id).unwrap().unwrap();
        assert!(key.len() > 9); // type + f64 + len + id bytes
        assert_eq!(key[0], TAG_NUMBER);
        assert_eq!(decode_index_id(&key).unwrap(), Some(id));
    }

    #[test]
    fn encode_string() {
        let id = json!(1234);
        let key = encode_index_key(&json!("hello"), &id).unwrap().unwrap();
        let id_bytes = encode_id_value(&id).unwrap();
        assert_eq!(key.len(), 1 + 4 + 5 + 4 + id_bytes.len());
        assert_eq!(key[0], TAG_STRING);
        assert_eq!(&key[1..5], &[5, 0, 0, 0]); // little-endian length
        assert_eq!(&key[5..10], b"hello");
        assert_eq!(decode_index_id(&key).unwrap(), Some(id));
    }

    #[test]
    fn encode_empty_string() {
        let id = json!(0);
        let key = encode_index_key(&json!(""), &id).unwrap().unwrap();
        let id_bytes = encode_id_value(&id).unwrap();
        assert_eq!(key.len(), 1 + 4 + 4 + id_bytes.len());
        assert_eq!(key[0], TAG_STRING);
        assert_eq!(&key[1..5], &[0, 0, 0, 0]);
        assert_eq!(decode_index_id(&key).unwrap(), Some(id));
    }

    #[test]
    fn rejects_arrays_and_objects() {
        assert!(encode_index_key(&json!([1, 2, 3]), &json!(1))
            .unwrap()
            .is_none());
        assert!(encode_index_key(&json!({"a": 1}), &json!(1))
            .unwrap()
            .is_none());
    }

    #[test]
    fn range_bounds() {
        let (start, end) = encode_range_bounds(&json!("test")).unwrap();

        // Both should have the same prefix
        assert_eq!(&start[..start.len() - 4], &end[..end.len() - 4]);
    }

    #[test]
    fn sort_order_bool() {
        // false (0x01) should sort before true (0x02)
        let false_key = encode_scalar_prefix(&Value::Bool(false)).unwrap();
        let true_key = encode_scalar_prefix(&Value::Bool(true)).unwrap();
        assert!(false_key < true_key);
    }

    #[test]
    fn sort_order_numbers() {
        let a = encode_scalar_prefix(&json!(1.0)).unwrap();
        let b = encode_scalar_prefix(&json!(2.0)).unwrap();
        let c = encode_scalar_prefix(&json!(-1.0)).unwrap();
        let d = encode_scalar_prefix(&json!(-2.0)).unwrap();

        // Basic ordering: 1.0 < 2.0
        assert!(a < b);

        // Negative numbers sort before positive
        assert!(c < a);
        assert!(d < c); // -2.0 < -1.0

        // Full ordering: -2.0 < -1.0 < 1.0 < 2.0
        assert!(d < c && c < a && a < b);
    }

    #[test]
    fn sort_order_strings() {
        let a = encode_scalar_prefix(&json!("a")).unwrap();
        let b = encode_scalar_prefix(&json!("b")).unwrap();
        let aa = encode_scalar_prefix(&json!("aa")).unwrap();

        assert!(a < b);
        assert!(a < aa);
    }

    use serde_json::json;
}
