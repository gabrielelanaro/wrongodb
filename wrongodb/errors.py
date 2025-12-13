class WrongoDBError(Exception):
    """Base error for the project."""


class DocumentValidationError(WrongoDBError):
    """Raised when input documents fail basic validation."""


class StorageError(WrongoDBError):
    """Raised for persistence-level issues."""


# Backward-compat alias.
MiniMongoError = WrongoDBError
