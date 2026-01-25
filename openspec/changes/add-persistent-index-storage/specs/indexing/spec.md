## ADDED Requirements
### Requirement: Persistent secondary index storage
The system SHALL persist secondary index data to disk so it can be reopened without rebuilding from the data log.

#### Scenario: Open after restart
- **WHEN** a collection with a secondary index is reopened
- **THEN** index data SHALL be loaded from disk and made available for queries

### Requirement: Index maintenance on writes
The system SHALL update secondary indexes on insert, update, and delete operations affecting indexed fields.

#### Scenario: Insert updates index
- **WHEN** a document with an indexed field is inserted
- **THEN** the index SHALL include the document’s key so it can be retrieved by equality query

#### Scenario: Update moves index entry
- **WHEN** an update changes the value of an indexed field
- **THEN** the old key SHALL be removed and the new key SHALL be added

#### Scenario: Delete removes index entry
- **WHEN** a document is deleted
- **THEN** the index SHALL no longer return that document

### Requirement: Equality queries use secondary indexes
The query engine SHALL use a secondary index for top-level equality filters when an index exists for the field.

#### Scenario: Indexed equality lookup
- **WHEN** a filter includes an indexed field with an equality predicate
- **THEN** the engine SHALL use the index to narrow candidate documents

### Requirement: Index creation builds persistent data
The system SHALL build and persist index data when createIndexes is called for an existing collection.

#### Scenario: Create index on existing data
- **WHEN** createIndexes is called for a field in a collection with existing documents
- **THEN** the index SHALL be built from existing data and persisted to disk

### Requirement: Index metadata is discoverable
The system SHALL list secondary indexes created for a collection via listIndexes.

#### Scenario: List indexes
- **WHEN** listIndexes is executed for a collection
- **THEN** the response SHALL include all user-created secondary indexes
