use serde_json::Value;

use crate::core::errors::StorageError;
use crate::WrongoDBError;

pub fn encode_id_value(id: &Value) -> Result<Vec<u8>, WrongoDBError> {
    let bson_id = bson::to_bson(id)?;
    let mut doc = bson::Document::new();
    doc.insert("_id", bson_id);
    Ok(bson::to_vec(&doc)?)
}

pub fn decode_id_value(bytes: &[u8]) -> Result<Value, WrongoDBError> {
    let doc: bson::Document = bson::from_slice(bytes)?;
    let bson_id = doc
        .get("_id")
        .ok_or_else(|| StorageError("missing _id in encoded key".into()))?
        .clone();
    let value: Value = bson::from_bson(bson_id)?;
    Ok(value)
}
