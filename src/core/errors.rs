use thiserror::Error;

/// Top-level error type returned by the supported public API.
///
/// This type exists so callers can handle one error surface while still
/// preserving the distinction between validation failures, storage failures,
/// protocol problems, and transaction-state errors.
#[derive(Debug, Error)]
pub enum WrongoDBError {
    #[error("document validation error: {0}")]
    DocumentValidation(#[from] DocumentValidationError),

    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("bson serialization error: {0}")]
    Bson(#[from] bson::ser::Error),

    #[error("bson deserialization error: {0}")]
    BsonDe(#[from] bson::de::Error),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("invalid transaction state: {0}")]
    InvalidTransactionState(String),

    #[error("transaction already active")]
    TransactionAlreadyActive,

    #[error("no active transaction")]
    NoActiveTransaction,

    #[error("not writable primary")]
    NotLeader { leader_hint: Option<String> },
}

/// Error returned when user data violates a database rule.
///
/// This is separate from [`StorageError`] so callers can distinguish invalid
/// input from engine or persistence failures.
#[derive(Debug, Error)]
#[error("{0}")]
pub struct DocumentValidationError(pub String);

/// Error returned when the storage engine cannot carry out a requested action.
///
/// This is the low-level operational error surface for the public API.
#[derive(Debug, Error)]
#[error("{0}")]
pub struct StorageError(pub String);
