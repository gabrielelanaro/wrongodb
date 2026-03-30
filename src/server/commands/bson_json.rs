use bson::{Bson, Document};
use serde_json::{Map, Value};

/// Convert one BSON document into WrongoDB's JSON document representation.
pub(crate) fn bson_document_to_json_document(doc: &Document) -> Map<String, Value> {
    let mut map = Map::new();
    for (key, value) in doc {
        map.insert(key.clone(), bson_value_to_json_value(value));
    }
    map
}

/// Convert one BSON document into a JSON value.
pub(crate) fn bson_document_to_json_value(doc: &Document) -> Value {
    Value::Object(bson_document_to_json_document(doc))
}

/// Convert one BSON value into the closest JSON value WrongoDB currently supports.
///
/// Unsupported BSON types still collapse to `null` because the command layer
/// only needs the subset already handled by the document model.
pub(crate) fn bson_value_to_json_value(bson: &Bson) -> Value {
    match bson {
        Bson::Double(value) => Value::Number(
            serde_json::Number::from_f64(*value).unwrap_or_else(|| serde_json::Number::from(0)),
        ),
        Bson::String(value) => Value::String(value.clone()),
        Bson::Document(value) => bson_document_to_json_value(value),
        Bson::Array(values) => Value::Array(values.iter().map(bson_value_to_json_value).collect()),
        Bson::Boolean(value) => Value::Bool(*value),
        Bson::Null => Value::Null,
        Bson::Int32(value) => Value::Number((*value).into()),
        Bson::Int64(value) => Value::Number((*value).into()),
        Bson::ObjectId(value) => Value::String(value.to_hex()),
        _ => Value::Null,
    }
}

/// Convert one JSON value into a BSON document when the value is an object.
pub(crate) fn json_value_to_bson_document(value: &Value) -> Document {
    match value {
        Value::Object(map) => {
            let mut doc = Document::new();
            for (key, value) in map {
                doc.insert(key.clone(), json_value_to_bson_value(value));
            }
            doc
        }
        _ => Document::new(),
    }
}

/// Convert one JSON value into the closest BSON value WrongoDB's command layer emits.
pub(crate) fn json_value_to_bson_value(value: &Value) -> Bson {
    match value {
        Value::Null => Bson::Null,
        Value::Bool(value) => Bson::Boolean(*value),
        Value::Number(value) => {
            if let Some(integer) = value.as_i64() {
                Bson::Int64(integer)
            } else if let Some(float) = value.as_f64() {
                Bson::Double(float)
            } else {
                Bson::Null
            }
        }
        Value::String(value) => Bson::String(value.clone()),
        Value::Array(values) => Bson::Array(values.iter().map(json_value_to_bson_value).collect()),
        Value::Object(_) => Bson::Document(json_value_to_bson_document(value)),
    }
}
