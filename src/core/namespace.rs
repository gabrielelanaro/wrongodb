use std::fmt;

use crate::core::errors::DocumentValidationError;
use crate::WrongoDBError;

const MAX_DATABASE_NAME_LENGTH: usize = 63;

/// Validated Mongo-style database name.
///
/// WrongoDB keeps the database component separate from the collection
/// component so command parsing, catalog lookups, and cursor replies can carry
/// the same identity model MongoDB uses with `DatabaseName`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct DatabaseName(String);

impl DatabaseName {
    /// Build a validated database name.
    pub(crate) fn new(name: impl Into<String>) -> Result<Self, WrongoDBError> {
        let name = name.into();
        validate_database_name(&name)?;
        Ok(Self(name))
    }

    /// Return the database name as a string slice.
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    /// Return whether this is the `local` database.
    pub(crate) fn is_local(&self) -> bool {
        self.as_str() == "local"
    }
}

impl fmt::Display for DatabaseName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Fully qualified Mongo-style namespace.
///
/// A namespace is the logical identity of a collection-like object. It is
/// intentionally distinct from physical storage identifiers so the catalog can
/// map one logical namespace onto opaque storage idents.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct Namespace {
    db_name: DatabaseName,
    collection_name: String,
}

impl Namespace {
    /// Build a validated namespace from a database and collection name.
    pub(crate) fn new(
        db_name: DatabaseName,
        collection_name: impl Into<String>,
    ) -> Result<Self, WrongoDBError> {
        let collection_name = collection_name.into();
        validate_collection_name(&collection_name)?;
        Ok(Self {
            db_name,
            collection_name,
        })
    }

    /// Parse a fully qualified namespace string such as `db.collection`.
    pub(crate) fn parse(full_name: &str) -> Result<Self, WrongoDBError> {
        let Some((db_name, collection_name)) = full_name.split_once('.') else {
            return Err(DocumentValidationError(format!(
                "namespace must be in the form <db>.<collection>: {full_name}"
            ))
            .into());
        };

        Self::new(DatabaseName::new(db_name)?, collection_name)
    }

    /// Build the synthetic cursor namespace for `listCollections`.
    pub(crate) fn list_collections_cursor(db_name: DatabaseName) -> Result<Self, WrongoDBError> {
        Self::new(db_name, "$cmd.listCollections")
    }

    /// Return the database component of this namespace.
    pub(crate) fn db_name(&self) -> &DatabaseName {
        &self.db_name
    }

    /// Return the collection component of this namespace.
    pub(crate) fn collection_name(&self) -> &str {
        &self.collection_name
    }

    /// Return the fully qualified namespace string.
    pub(crate) fn full_name(&self) -> String {
        format!("{}.{}", self.db_name, self.collection_name)
    }

    /// Return whether this is the synthetic `<db>.$cmd` namespace.
    pub(crate) fn is_command_namespace(&self) -> bool {
        self.collection_name() == "$cmd"
    }
}

impl fmt::Display for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.full_name())
    }
}

fn validate_database_name(name: &str) -> Result<(), WrongoDBError> {
    if name.is_empty() {
        return Err(DocumentValidationError("database name must not be empty".into()).into());
    }
    if name.len() > MAX_DATABASE_NAME_LENGTH {
        return Err(DocumentValidationError(format!(
            "database name exceeds {} characters: {name}",
            MAX_DATABASE_NAME_LENGTH
        ))
        .into());
    }
    if name
        .chars()
        .any(|ch| matches!(ch, '\0' | ' ' | '.' | '/' | '\\' | '"' | '$'))
    {
        return Err(DocumentValidationError(format!("invalid database name: {name}")).into());
    }
    Ok(())
}

fn validate_collection_name(name: &str) -> Result<(), WrongoDBError> {
    if name.is_empty() {
        return Err(DocumentValidationError("collection name must not be empty".into()).into());
    }
    if name.contains('\0') {
        return Err(DocumentValidationError(format!(
            "collection name must not contain NUL bytes: {name}"
        ))
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{DatabaseName, Namespace};

    // EARS: When the server parses a fully qualified namespace, it shall split
    // the database from the collection at the first `.` only.
    #[test]
    fn parse_namespace_preserves_collection_suffix() {
        let namespace = Namespace::parse("analytics.events.daily").unwrap();

        assert_eq!(namespace.db_name().as_str(), "analytics");
        assert_eq!(namespace.collection_name(), "events.daily");
    }

    // EARS: When a database name violates MongoDB naming rules, namespace
    // construction shall reject it.
    #[test]
    fn rejects_invalid_database_name() {
        let error = DatabaseName::new("bad.db").unwrap_err();

        assert!(error.to_string().contains("invalid database name"));
    }
}
