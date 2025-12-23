## ADDED Requirements
### Requirement: Checkpoint creates a durable root snapshot
The storage engine SHALL provide a checkpoint operation that persists a durable root pointer representing a consistent snapshot of the B+tree.

#### Scenario: Checkpoint commit selects new root
- **WHEN** a checkpoint completes successfully after writes
- **THEN** reopening the database SHALL use the new root pointer

### Requirement: Copy-on-write updates preserve the last checkpoint
The storage engine SHALL write modified pages to new blocks rather than overwriting blocks reachable from the last durable checkpoint.

#### Scenario: Crash before checkpoint
- **WHEN** a page is modified and the process crashes before a checkpoint
- **THEN** reopening SHALL be able to traverse the last checkpoint without touching the modified (uncheckpointed) blocks

### Requirement: Atomic root metadata selection
The storage engine SHALL persist checkpoint metadata in multiple slots with generation numbers and CRCs, and on open SHALL select the latest valid slot.

#### Scenario: Torn header write
- **WHEN** the newest slot fails checksum validation
- **THEN** the engine SHALL fall back to the previous valid slot

### Requirement: Retired block reuse only after checkpoint
The storage engine SHALL NOT recycle blocks that are still reachable from the last durable checkpoint; blocks retired by copy-on-write MAY be reused only after a successful checkpoint.

#### Scenario: Replaced page before checkpoint
- **WHEN** a leaf page is replaced by a copy-on-write update
- **THEN** the old page’s block SHALL NOT appear in the free list until after checkpoint completion
