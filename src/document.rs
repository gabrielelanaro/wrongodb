use crate::{Document, WrongoDBError};
use crate::errors::DocumentValidationError;
use serde_json::Value;
use uuid::Uuid;

pub fn normalize_document(doc: &Document) -> Result<Document, WrongoDBError> {
    let mut normalized = doc.clone();

    let id_key = "_id".to_string();
    if !normalized.contains_key(&id_key) {
        normalized.insert(id_key, Value::String(Uuid::new_v4().to_string()));
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

