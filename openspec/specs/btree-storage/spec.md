# btree-storage Specification

## Purpose
TBD - created by archiving change refactor-btree-storage. Update Purpose after archive.
## Requirements
### Requirement: Main table B-tree storage
The system SHALL provide a main table B-tree that maps document `_id` values to full document content, replacing the append-only log storage.

#### Scenario: Insert document into main table
- **WHEN** a document with an `_id` is inserted
- **THEN** the document SHALL be stored in the main table B-tree with `_id` as key

#### Scenario: Retrieve document by _id
- **WHEN** a document is retrieved by `_id` from the main table
- **THEN** the full document content SHALL be returned

#### Scenario: Update document in main table
- **WHEN** an existing document is updated
- **THEN** the new content SHALL be stored via COW without appending to a log

#### Scenario: Delete document from main table
- **WHEN** a document is deleted
- **THEN** the `_id` entry SHALL be removed and space reclaimed after checkpoint

### Requirement: Extent-based block allocation
The BlockManager SHALL manage free space using extent lists with skiplists for efficient allocation and coalescing.

#### Scenario: Allocate extent for new page
- **WHEN** a new page needs to be allocated
- **THEN** the BlockManager SHALL find the best-fit extent from the avail list

#### Scenario: Free extent on COW update
- **WHEN** a page is replaced via COW
- **THEN** the old extent SHALL be moved to the discard list

#### Scenario: Reclaim space after checkpoint
- **WHEN** a checkpoint completes successfully
- **THEN** extents in the discard list SHALL be moved to the avail list for reuse

#### Scenario: Coalesce adjacent free extents
- **WHEN** an extent is freed and checkpointed
- **THEN** it SHALL be coalesced with adjacent free extents if possible

### Requirement: Document serialization format
The system SHALL encode document keys and values using BSON for consistent binary representation.

#### Scenario: Encode _id as B-tree key
- **WHEN** an `_id` value is used as a B-tree key
- **THEN** it SHALL be encoded as BSON for proper ordering

#### Scenario: Encode document as B-tree value
- **WHEN** a document is stored in the B-tree
- **THEN** it SHALL be serialized as BSON

