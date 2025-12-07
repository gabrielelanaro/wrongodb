class MiniMongoError(Exception):
    """Base error for the project."""


class DocumentValidationError(MiniMongoError):
    """Raised when input documents fail basic validation."""


class StorageError(MiniMongoError):
    """Raised for persistence-level issues."""
