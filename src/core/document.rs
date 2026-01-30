use crate::core::errors::DocumentValidationError;
use crate::{Document, WrongoDBError};
use serde_json::Value;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

pub fn normalize_document(doc: &Document) -> Result<Document, WrongoDBError> {
    let mut normalized = doc.clone();

    let id_key = "_id".to_string();
    if !normalized.contains_key(&id_key) {
        normalized.insert(id_key, Value::String(generate_object_id_hex()));
    }

    // Keys are already String in Document; keep a lightweight validation for values:
    // ensure the document is JSON-serializable (Value always is).
    Ok(normalized)
}

pub fn validate_is_object(value: &Value) -> Result<(), WrongoDBError> {
    if !value.is_object() {
        return Err(DocumentValidationError("document must be a JSON object".into()).into());
    }
    Ok(())
}

fn generate_object_id_hex() -> String {
    // MongoDB ObjectId is 12 bytes, typically rendered as 24 lower-case hex characters:
    // - 4 bytes: timestamp (seconds since epoch), big-endian
    // - 5 bytes: random value
    // - 3 bytes: incrementing counter, big-endian (mod 2^24)
    static COUNTER: AtomicU32 = AtomicU32::new(0);

    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as u32;
    let ts = now_secs.to_be_bytes();

    let rand = Uuid::new_v4();
    let rand_bytes = rand.as_bytes();

    let c = COUNTER.fetch_add(1, Ordering::Relaxed) & 0x00FF_FFFF;
    let counter_bytes = c.to_be_bytes();

    let mut bytes = [0u8; 12];
    bytes[0..4].copy_from_slice(&ts);
    bytes[4..9].copy_from_slice(&rand_bytes[0..5]);
    bytes[9..12].copy_from_slice(&counter_bytes[1..4]);

    hex_lower(&bytes)
}

fn hex_lower(bytes: &[u8]) -> String {
    const LUT: &[u8; 16] = b"0123456789abcdef";
    let mut out = Vec::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(LUT[(b >> 4) as usize]);
        out.push(LUT[(b & 0x0F) as usize]);
    }
    // SAFETY: LUT only contains ASCII hex digits.
    unsafe { String::from_utf8_unchecked(out) }
}
