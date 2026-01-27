//! Index key encoding for persistent secondary indexes.
//!
//! This module provides composite key encoding for B+tree secondary indexes.
//! Keys are encoded as:
//!   [scalar_type: 1 byte][scalar_value: variable][record_offset: 8 bytes LE]
//!
//! The record_offset is appended to the scalar value to ensure uniqueness
//! when multiple documents have the same indexed value (duplicate support).

use serde_json::Value;

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

/// Encode a scalar value and record offset into a composite index key.
///
/// The resulting key can be stored in a B+tree and supports:
/// - Natural sort order by scalar value
/// - Duplicate values (different offsets = different keys)
/// - Range scans by scalar value prefix
///
/// # Format
/// ```text
/// [scalar_type: 1 byte][scalar_value: variable][record_offset: 8 bytes LE]
/// ```
///
/// # Type Encodings
/// - Null: `0x00` (no additional bytes)
/// - Bool false: `0x01` (type byte itself encodes false)
/// - Bool true: `0x02` (type byte itself encodes true)
/// - Number: `0x03` followed by 8-byte LE f64
/// - String: `0x04` followed by 4-byte LE length + UTF-8 bytes
pub fn encode_index_key(scalar: &Value, offset: u64) -> Option<Vec<u8>> {
    let mut result = encode_scalar_prefix(scalar)?;
    result.extend_from_slice(&offset.to_le_bytes());
    Some(result)
}

/// Encode just the scalar value prefix (without offset).
///
/// This is useful for range scans where you want to find all records
/// matching a particular scalar value regardless of offset.
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
/// - start_key includes the scalar value with offset 0
/// - end_key includes the scalar value with offset u64::MAX
///
/// This allows a range scan to find all matching records.
pub fn encode_range_bounds(scalar: &Value) -> Option<(Vec<u8>, Vec<u8>)> {
    let prefix = encode_scalar_prefix(scalar)?;

    let mut start = prefix.clone();
    start.extend_from_slice(&0u64.to_le_bytes());

    let mut end = prefix;
    end.extend_from_slice(&u64::MAX.to_le_bytes());

    Some((start, end))
}

/// Extract the record offset from a composite index key.
///
/// Returns None if the key is too short to contain an offset.
#[allow(dead_code)]
pub fn extract_offset(key: &[u8]) -> Option<u64> {
    if key.len() < 8 {
        return None;
    }
    let offset_bytes = &key[key.len() - 8..];
    Some(u64::from_le_bytes([
        offset_bytes[0],
        offset_bytes[1],
        offset_bytes[2],
        offset_bytes[3],
        offset_bytes[4],
        offset_bytes[5],
        offset_bytes[6],
        offset_bytes[7],
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_null() {
        let key = encode_index_key(&Value::Null, 42).unwrap();
        assert_eq!(key.len(), 9); // 1 byte type + 8 bytes offset
        assert_eq!(key[0], TAG_NULL);
        assert_eq!(extract_offset(&key), Some(42));
    }

    #[test]
    fn encode_bool_false() {
        let key = encode_index_key(&Value::Bool(false), 100).unwrap();
        assert_eq!(key.len(), 9);
        assert_eq!(key[0], TAG_BOOL_FALSE);
        assert_eq!(extract_offset(&key), Some(100));
    }

    #[test]
    fn encode_bool_true() {
        let key = encode_index_key(&Value::Bool(true), 200).unwrap();
        assert_eq!(key.len(), 9);
        assert_eq!(key[0], TAG_BOOL_TRUE);
        assert_eq!(extract_offset(&key), Some(200));
    }

    #[test]
    fn encode_number() {
        let key = encode_index_key(&json!(3.14159), 999).unwrap();
        assert_eq!(key.len(), 17); // 1 byte type + 8 bytes f64 + 8 bytes offset
        assert_eq!(key[0], TAG_NUMBER);
        assert_eq!(extract_offset(&key), Some(999));
    }

    #[test]
    fn encode_string() {
        let key = encode_index_key(&json!("hello"), 1234).unwrap();
        assert_eq!(key.len(), 18); // 1 byte type + 4 bytes len + 5 bytes string + 8 bytes offset
        assert_eq!(key[0], TAG_STRING);
        assert_eq!(&key[1..5], &[5, 0, 0, 0]); // little-endian length
        assert_eq!(&key[5..10], b"hello");
        assert_eq!(extract_offset(&key), Some(1234));
    }

    #[test]
    fn encode_empty_string() {
        let key = encode_index_key(&json!(""), 0).unwrap();
        assert_eq!(key.len(), 13); // 1 byte type + 4 bytes len + 0 bytes string + 8 bytes offset
        assert_eq!(key[0], TAG_STRING);
        assert_eq!(&key[1..5], &[0, 0, 0, 0]);
        assert_eq!(extract_offset(&key), Some(0));
    }

    #[test]
    fn rejects_arrays_and_objects() {
        assert!(encode_index_key(&json!([1, 2, 3]), 0).is_none());
        assert!(encode_index_key(&json!({"a": 1}), 0).is_none());
    }

    #[test]
    fn range_bounds() {
        let (start, end) = encode_range_bounds(&json!("test")).unwrap();

        // Both should have the same prefix
        assert_eq!(&start[..start.len() - 8], &end[..end.len() - 8]);

        // Start should have offset 0
        assert_eq!(extract_offset(&start), Some(0));

        // End should have offset MAX
        assert_eq!(extract_offset(&end), Some(u64::MAX));
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
