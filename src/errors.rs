use thiserror::Error;

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
}

#[derive(Debug, Error)]
#[error("{0}")]
pub struct DocumentValidationError(pub String);

#[derive(Debug, Error)]
#[error("{0}")]
pub struct StorageError(pub String);

