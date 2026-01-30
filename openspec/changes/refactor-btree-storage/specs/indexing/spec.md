## MODIFIED Requirements

### Requirement: Persistent secondary index storage
The system SHALL persist secondary index data to disk so it can be reopened without rebuilding from the data log. Index entries SHALL reference documents by `_id` rather than file offset.

#### Scenario: Index stores _id reference
- **WHEN** a document is added to a secondary index
- **THEN** the index entry SHALL contain the document's `_id` value, not a file offset

#### Scenario: Index lookup returns _id
- **WHEN** a secondary index lookup is performed
- **THEN** the result SHALL be `_id` values used to look up documents in the main table

## ADDED Requirements

### Requirement: Index migration from offset to _id
The system SHALL rebuild existing secondary indexes to use `_id` references when migrating from the old append-only format.

#### Scenario: Migrate existing index
- **WHEN** opening a database with offset-based indexes
- **THEN** the indexes SHALL be rebuilt to use `_id` references
