use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};

use crate::core::bson::decode_id_value;
use crate::core::errors::{DocumentValidationError, StorageError};
use crate::index::encode_index_key;
use crate::storage::table::{IndexMetadata, TableMetadata};
use crate::{Document, WrongoDBError};

// ============================================================================
// Constants
// ============================================================================

const ROW_MAGIC_WT_ROW_V1: u8 = 0x01;

const CELL_MISSING: u8 = 0x00;
const CELL_NULL: u8 = 0x01;
const CELL_BOOL_FALSE: u8 = 0x02;
const CELL_BOOL_TRUE: u8 = 0x03;
const CELL_INT64: u8 = 0x04;
const CELL_DOUBLE: u8 = 0x05;
const CELL_STRING: u8 = 0x06;

// ============================================================================
// Row Format
// ============================================================================

/// Physical row encoding persisted for primary table values.
///
/// WrongoDB now mirrors the WT schema split more literally: table metadata
/// declares named value columns and the row bytes hold a typed tuple in that
/// declared order instead of an opaque BSON document.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum RowFormat {
    #[serde(rename = "wt_row_v1")]
    WtRowV1,
}

impl RowFormat {
    // ------------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------------

    fn magic_byte(self) -> u8 {
        match self {
            Self::WtRowV1 => ROW_MAGIC_WT_ROW_V1,
        }
    }
}

/// Decoded structured row values in declared table-column order.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DecodedRow {
    fields: Vec<Option<Value>>,
}

impl DecodedRow {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    pub(crate) fn from_bytes(
        row_format: RowFormat,
        value_columns: &[String],
        bytes: &[u8],
    ) -> Result<Self, WrongoDBError> {
        match row_format {
            RowFormat::WtRowV1 => decode_wt_row_v1(value_columns, bytes),
        }
    }

    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------

    pub(crate) fn into_document(
        self,
        primary_key: &[u8],
        table: &TableMetadata,
    ) -> Result<Document, WrongoDBError> {
        let mut document = Document::new();
        document.insert("_id".to_string(), decode_id_value(primary_key)?);

        for (column, value) in table.value_columns().iter().zip(self.fields.into_iter()) {
            if let Some(value) = value {
                document.insert(column.clone(), value);
            }
        }

        Ok(document)
    }

    pub(crate) fn value<'a>(
        &'a self,
        table: &TableMetadata,
        column: &str,
    ) -> Result<Option<&'a Value>, WrongoDBError> {
        let position = table.value_column_position(column).ok_or_else(|| {
            StorageError(format!(
                "table {} does not declare storage column {column}",
                table.uri()
            ))
        })?;
        Ok(self.fields[position].as_ref())
    }
}

// ============================================================================
// Validation
// ============================================================================

pub(crate) fn validate_storage_columns(value_columns: &[String]) -> Result<(), WrongoDBError> {
    let mut seen = BTreeSet::new();

    for column in value_columns {
        if column.is_empty() {
            return Err(
                DocumentValidationError("storageColumns entries must not be empty".into()).into(),
            );
        }
        if column == "_id" {
            return Err(DocumentValidationError(
                "_id is implicit and must not appear in storageColumns".into(),
            )
            .into());
        }
        if column.contains('.') {
            return Err(DocumentValidationError(format!(
                "nested storage column paths are not supported: {column}"
            ))
            .into());
        }
        if !seen.insert(column.clone()) {
            return Err(
                DocumentValidationError(format!("duplicate storage column: {column}")).into(),
            );
        }
    }

    Ok(())
}

pub(crate) fn validate_document_for_storage(
    doc: &Document,
    value_columns: &[String],
) -> Result<(), WrongoDBError> {
    validate_storage_columns(value_columns)?;

    let Some(id) = doc.get("_id") else {
        return Err(DocumentValidationError("missing _id".into()).into());
    };
    validate_scalar_value("_id", id)?;

    for (field, value) in doc {
        if field == "_id" {
            continue;
        }
        if !value_columns.iter().any(|column| column == field) {
            return Err(DocumentValidationError(format!(
                "field {field} is not declared in storageColumns"
            ))
            .into());
        }
        validate_scalar_value(field, value)?;
    }

    Ok(())
}

// ============================================================================
// Encoding
// ============================================================================

pub(crate) fn encode_row_value(
    row_format: RowFormat,
    value_columns: &[String],
    doc: &Document,
) -> Result<Vec<u8>, WrongoDBError> {
    validate_document_for_storage(doc, value_columns)?;

    match row_format {
        RowFormat::WtRowV1 => encode_wt_row_v1(value_columns, doc),
    }
}

pub(crate) fn decode_row_value(
    table: &TableMetadata,
    primary_key: &[u8],
    primary_value: &[u8],
) -> Result<Document, WrongoDBError> {
    DecodedRow::from_bytes(table.row_format(), table.value_columns(), primary_value)?
        .into_document(primary_key, table)
}

pub(crate) fn index_key_from_row(
    table: &TableMetadata,
    index: &IndexMetadata,
    primary_key: &[u8],
    primary_value: &[u8],
) -> Result<Option<Vec<u8>>, WrongoDBError> {
    let row = DecodedRow::from_bytes(table.row_format(), table.value_columns(), primary_value)?;
    index_key_from_decoded_row(table, index, primary_key, &row)
}

pub(crate) fn index_key_from_decoded_row(
    table: &TableMetadata,
    index: &IndexMetadata,
    primary_key: &[u8],
    row: &DecodedRow,
) -> Result<Option<Vec<u8>>, WrongoDBError> {
    let field = single_index_column(index)?;
    let Some(value) = row.value(table, field)? else {
        return Ok(None);
    };
    let id = decode_id_value(primary_key)?;
    encode_index_key(value, &id)
}

// ============================================================================
// WT Row V1
// ============================================================================

fn encode_wt_row_v1(value_columns: &[String], doc: &Document) -> Result<Vec<u8>, WrongoDBError> {
    let mut encoded = Vec::new();
    encoded.push(RowFormat::WtRowV1.magic_byte());

    for column in value_columns {
        match doc.get(column) {
            None => encoded.push(CELL_MISSING),
            Some(Value::Null) => encoded.push(CELL_NULL),
            Some(Value::Bool(false)) => encoded.push(CELL_BOOL_FALSE),
            Some(Value::Bool(true)) => encoded.push(CELL_BOOL_TRUE),
            Some(Value::Number(number)) => encode_number_cell(&mut encoded, number)?,
            Some(Value::String(value)) => encode_string_cell(&mut encoded, value)?,
            Some(Value::Array(_)) | Some(Value::Object(_)) => {
                return Err(DocumentValidationError(format!(
                    "field {column} must be a scalar value"
                ))
                .into());
            }
        }
    }

    Ok(encoded)
}

fn decode_wt_row_v1(value_columns: &[String], bytes: &[u8]) -> Result<DecodedRow, WrongoDBError> {
    if bytes.first().copied() != Some(ROW_MAGIC_WT_ROW_V1) {
        return Err(StorageError("row value does not use wt_row_v1 encoding".into()).into());
    }

    let mut offset = 1usize;
    let mut fields = Vec::with_capacity(value_columns.len());

    for column in value_columns {
        let tag = *bytes.get(offset).ok_or_else(|| {
            StorageError(format!(
                "row value ended before column {column} could be decoded"
            ))
        })?;
        offset += 1;

        let value = match tag {
            CELL_MISSING => None,
            CELL_NULL => Some(Value::Null),
            CELL_BOOL_FALSE => Some(Value::Bool(false)),
            CELL_BOOL_TRUE => Some(Value::Bool(true)),
            CELL_INT64 => {
                let raw = take_array::<8>(bytes, &mut offset, column)?;
                Some(Value::Number(i64::from_le_bytes(raw).into()))
            }
            CELL_DOUBLE => {
                let raw = take_array::<8>(bytes, &mut offset, column)?;
                let value = f64::from_bits(u64::from_le_bytes(raw));
                let number = Number::from_f64(value).ok_or_else(|| {
                    StorageError(format!(
                        "column {column} contains a non-finite floating-point value"
                    ))
                })?;
                Some(Value::Number(number))
            }
            CELL_STRING => {
                let raw_len = take_array::<4>(bytes, &mut offset, column)?;
                let len = u32::from_le_bytes(raw_len) as usize;
                let string_bytes = take_slice(bytes, &mut offset, len, column)?;
                let string = String::from_utf8(string_bytes.to_vec()).map_err(|err| {
                    StorageError(format!(
                        "column {column} contains invalid UTF-8 in wt_row_v1: {err}"
                    ))
                })?;
                Some(Value::String(string))
            }
            other => {
                return Err(StorageError(format!(
                    "column {column} uses unsupported wt_row_v1 tag {other:#x}"
                ))
                .into());
            }
        };

        fields.push(value);
    }

    if offset != bytes.len() {
        return Err(StorageError(format!(
            "row value has {} trailing bytes after wt_row_v1 decode",
            bytes.len() - offset
        ))
        .into());
    }

    Ok(DecodedRow { fields })
}

fn encode_number_cell(encoded: &mut Vec<u8>, number: &Number) -> Result<(), WrongoDBError> {
    if let Some(value) = json_number_to_i64(number) {
        encoded.push(CELL_INT64);
        encoded.extend_from_slice(&value.to_le_bytes());
        return Ok(());
    }

    let Some(value) = number.as_f64() else {
        return Err(DocumentValidationError("numbers must fit in i64 or f64".into()).into());
    };
    encoded.push(CELL_DOUBLE);
    encoded.extend_from_slice(&value.to_bits().to_le_bytes());
    Ok(())
}

fn encode_string_cell(encoded: &mut Vec<u8>, value: &str) -> Result<(), WrongoDBError> {
    let len: u32 = value
        .len()
        .try_into()
        .map_err(|_| DocumentValidationError("string value is too large".into()))?;
    encoded.push(CELL_STRING);
    encoded.extend_from_slice(&len.to_le_bytes());
    encoded.extend_from_slice(value.as_bytes());
    Ok(())
}

// ============================================================================
// Helper Functions
// ============================================================================

fn validate_scalar_value(field: &str, value: &Value) -> Result<(), WrongoDBError> {
    match value {
        Value::Null | Value::Bool(_) | Value::String(_) => Ok(()),
        Value::Number(number) => {
            if json_number_to_i64(number).is_some() || number.as_f64().is_some() {
                return Ok(());
            }
            Err(DocumentValidationError(format!(
                "field {field} uses a numeric value that cannot be stored"
            ))
            .into())
        }
        Value::Array(_) | Value::Object(_) => Err(DocumentValidationError(format!(
            "field {field} must be a top-level scalar value"
        ))
        .into()),
    }
}

fn json_number_to_i64(number: &Number) -> Option<i64> {
    number
        .as_i64()
        .or_else(|| number.as_u64().and_then(|value| i64::try_from(value).ok()))
}

fn single_index_column(index: &IndexMetadata) -> Result<&str, WrongoDBError> {
    if index.columns().len() != 1 {
        return Err(StorageError(format!(
            "index {} has unsupported composite definition with {} columns",
            index.uri(),
            index.columns().len()
        ))
        .into());
    }

    Ok(index.columns()[0].as_str())
}

fn take_array<const N: usize>(
    bytes: &[u8],
    offset: &mut usize,
    column: &str,
) -> Result<[u8; N], WrongoDBError> {
    let slice = take_slice(bytes, offset, N, column)?;
    let mut out = [0u8; N];
    out.copy_from_slice(slice);
    Ok(out)
}

fn take_slice<'a>(
    bytes: &'a [u8],
    offset: &mut usize,
    len: usize,
    column: &str,
) -> Result<&'a [u8], WrongoDBError> {
    if bytes.len().saturating_sub(*offset) < len {
        return Err(StorageError(format!(
            "row value ended before column {column} could be decoded"
        ))
        .into());
    }

    let start = *offset;
    let end = start + len;
    *offset = end;
    Ok(&bytes[start..end])
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::core::bson::encode_id_value;
    use crate::storage::table::TableMetadata;

    fn test_table() -> TableMetadata {
        TableMetadata::new("table:users", vec!["name".to_string(), "age".to_string()])
    }

    #[test]
    fn round_trip_wt_row_value() {
        let mut doc = Document::new();
        doc.insert("_id".to_string(), json!(1));
        doc.insert("name".to_string(), json!("alice"));
        doc.insert("age".to_string(), json!(30));

        let encoded =
            encode_row_value(RowFormat::WtRowV1, test_table().value_columns(), &doc).unwrap();
        let primary_key = encode_id_value(&json!(1)).unwrap();
        let decoded = decode_row_value(&test_table(), &primary_key, &encoded).unwrap();

        assert_eq!(decoded.get("_id"), Some(&json!(1)));
        assert_eq!(decoded.get("name"), Some(&json!("alice")));
        assert_eq!(decoded.get("age"), Some(&json!(30)));
    }

    #[test]
    fn missing_columns_are_omitted_on_decode() {
        let mut doc = Document::new();
        doc.insert("_id".to_string(), json!(1));
        doc.insert("name".to_string(), json!("alice"));

        let encoded =
            encode_row_value(RowFormat::WtRowV1, test_table().value_columns(), &doc).unwrap();
        let primary_key = encode_id_value(&json!(1)).unwrap();
        let decoded = decode_row_value(&test_table(), &primary_key, &encoded).unwrap();

        assert_eq!(decoded.get("name"), Some(&json!("alice")));
        assert!(!decoded.contains_key("age"));
    }

    #[test]
    fn rejects_undeclared_fields() {
        let mut doc = Document::new();
        doc.insert("_id".to_string(), json!(1));
        doc.insert("name".to_string(), json!("alice"));
        doc.insert("city".to_string(), json!("rome"));

        let err =
            encode_row_value(RowFormat::WtRowV1, test_table().value_columns(), &doc).unwrap_err();
        assert!(err.to_string().contains("field city is not declared"));
    }

    #[test]
    fn rejects_nested_field_names_in_storage_schema() {
        let err = validate_storage_columns(&["user.name".to_string()]).unwrap_err();
        assert!(err
            .to_string()
            .contains("nested storage column paths are not supported"));
    }
}
