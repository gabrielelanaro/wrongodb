use bson::Bson;
use serde_json::Value;

use crate::errors::StorageError;
use crate::{Document, WrongoDBError};

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

pub fn encode_document(doc: &Document) -> Result<Vec<u8>, WrongoDBError> {
    let bson_doc = match bson::to_bson(doc)? {
        Bson::Document(doc) => doc,
        _ => {
            return Err(StorageError(
                "document did not serialize to a BSON document".into(),
            )
            .into())
        }
    };
    Ok(bson::to_vec(&bson_doc)?)
}

pub fn decode_document(bytes: &[u8]) -> Result<Document, WrongoDBError> {
    let bson_doc: bson::Document = bson::from_slice(bytes)?;
    let value: Value = bson::from_bson(Bson::Document(bson_doc))?;
    match value {
        Value::Object(map) => Ok(map),
        _ => Err(StorageError("decoded BSON document was not an object".into()).into()),
    }
}
