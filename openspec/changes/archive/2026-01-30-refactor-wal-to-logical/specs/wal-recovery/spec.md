## ADDED Requirements
### Requirement: Logical WAL records for BTree writes
The storage engine SHALL log BTree mutations as logical operations (key/value puts and optional deletes), not
page-level changes.

#### Scenario: Logical put logged
- **WHEN** a key/value is inserted or updated with WAL enabled
- **THEN** the WAL SHALL contain a logical put record with that key/value

### Requirement: Recovery replays logical operations via BTree API
The storage engine SHALL replay WAL records by calling the BTree write API with WAL disabled, allowing normal
split and root update handling.

#### Scenario: Replay rebuilds data
- **WHEN** the database is reopened after a crash
- **THEN** logical WAL records after the last checkpoint SHALL be replayed and data SHALL be readable

### Requirement: Recovery is idempotent for logical records
The storage engine SHALL tolerate reapplying logical records without corrupting state (put is upsert; delete of
missing keys is OK).

#### Scenario: Second recovery pass
- **WHEN** recovery is run twice on the same WAL
- **THEN** results SHALL be identical

### Requirement: Checkpoint LSN bounds recovery
The storage engine SHALL start WAL replay at the checkpoint LSN and ignore earlier records.

#### Scenario: Post-checkpoint writes only
- **WHEN** a checkpoint is taken and additional writes occur
- **THEN** recovery SHALL apply only the writes after the checkpoint

### Requirement: Replay does not append to WAL
The storage engine SHALL disable WAL logging while replaying WAL records.

#### Scenario: Replay does not grow WAL
- **WHEN** recovery replays logical records
- **THEN** the WAL size SHALL NOT increase due to replay
