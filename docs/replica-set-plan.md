# Replica Set Management Plan

## Overview

This plan describes how to evolve WrongoDB's current static single-primary/single-secondary replication into a **full production-grade replica set** with election-based failover, topology management, write concerns, read preferences, oplog durability, and initial sync.

### Current State Summary

WrongoDB already has a solid replication foundation:
- Oplog stored as `local.oplog.rs` table with term+index ordering
- `ReplicationCoordinator` for role/term/oplog-index management
- `ReplicationObserver` appends oplog inside same transaction as user data
- `SecondaryReplicator` background loop: fetch via tailable cursor, persist, apply, report progress
- `OplogApplier` with idempotent CRUD + DDL replay
- Durable `lastApplied` marker in `local.repl_state`
- Crash recovery: restarted secondary catches up from persisted state
- Integration tests covering DDL/CRUD replication, write rejection, restart recovery

### Design Philosophy

1. **Build on existing primitives** - The oplog-as-table, tailable cursor, and transactional oplog+durability model are correct. Extend rather than replace.
2. **MongoDB-compatible wire protocol** - Use the same command names (`replSetInitiate`, `replSetGetStatus`, `replSetHeartbeat`, etc.) and response shapes so standard MongoDB drivers and tooling work.
3. **Raft-inspired, not Raft-implementing** - Use Raft's conceptual model (terms, elections, majority quorum, log replication) but implement it using WrongoDB's existing storage primitives rather than importing a Raft library.
4. **Incremental phases** - Each phase is independently testable and adds user-visible value.

---

## Phase 1: Replica Set Topology Configuration

### Goal
Replace the static `--role primary/secondary` CLI model with a **replica set configuration document** that supports N members, voting, priorities, and persistent topology state.

### New Types

```rust
/// Unique identifier for one replica set.
pub struct ReplicaSetName(pub String);

/// One member in the replica set configuration.
pub struct ReplicaSetMember {
    /// Stable member identifier (e.g., "node-1", "node-2").
    pub id: u64,
    /// Host:port address used for inter-node communication.
    pub host: String,
    /// Voting priority (higher = more likely to become primary).
    pub priority: u64,
    /// Whether this member participates in elections.
    pub votes: bool,
    /// Whether this member is a passive data-only node (no vote, no become primary).
    pub hidden: bool,
    /// Member-level tags for read preference targeting.
    pub tags: HashMap<String, String>,
}

/// The persisted replica set configuration.
pub struct ReplicaSetConfig {
    /// Unique name for this replica set.
    pub name: ReplicaSetName,
    /// Monotonically increasing config version.
    pub version: u64,
    /// All members in the set.
    pub members: Vec<ReplicaSetMember>,
    /// Election timeout in milliseconds.
    pub election_timeout_ms: u64,
    /// Heartbeat interval in milliseconds.
    pub heartbeat_interval_ms: u64,
}

/// Runtime role derived from replica set state.
pub enum ReplicaSetRole {
    /// Not yet part of a replica set (startup before rs.initiate()).
    Startup,
    /// Currently the elected primary.
    Primary { term: u64 },
    /// Following the primary, applying oplog.
    Secondary { term: u64, sync_source: String },
    /// Lost election or stepped down; waiting before next election.
    Candidate { term: u64 },
}
```

### Persistent Storage

**New reserved namespace: `local.system.replset`**

This single-document collection stores the current replica set configuration. It is:
- Created during `rs.initiate()`
- Read by every node at startup to determine topology
- Updated (version incremented) by `rs.add()`, `rs.remove()`, `rs.reconfig()`
- Replicated through the oplog like any other write (so all nodes have the config)

```json
{
  "_id": "my-replica-set",
  "version": 3,
  "members": [
    { "_id": 0, "host": "127.0.0.1:27017", "priority": 1, "votes": true },
    { "_id": 1, "host": "127.0.0.1:27018", "priority": 1, "votes": true },
    { "_id": 2, "host": "127.0.0.1:27019", "priority": 0, "votes": false, "hidden": true }
  ],
  "settings": {
    "electionTimeoutMs": 10000,
    "heartbeatIntervalMs": 2000
  }
}
```

### Startup Flow Change

**Before (current):**
```
CLI --role primary  ->  ReplicationCoordinator { role: Primary }
CLI --role secondary --sync-source mongodb://node-1  ->  ReplicationCoordinator { role: Secondary }
```

**After:**
```
1. Connection::open() -> WAL recovery
2. Read local.system.replset
   - If absent -> Startup role (awaiting rs.initiate())
   - If present -> Determine role from config:
     - Find self by host in members list
     - If no config exists yet -> Startup
     - If config exists, start heartbeat/election runtime
3. If primary was elected in previous term, start in Secondary role
   (must step down and re-participate in election on restart)
```

### CLI Changes

The `--role` and `--sync-source` flags are **deprecated** in favor of:

```bash
wrongodb-server --db-path ./data --repl-set-name my-rs --repl-self 127.0.0.1:27017
```

- `--repl-set-name`: The replica set name (all members must agree)
- `--repl-self`: This node's host:port string for member identification

If neither flag is provided, the node runs in standalone mode (current behavior).

### Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `src/replication/config.rs` | **New** | `ReplicaSetConfig`, `ReplicaSetMember`, parsing, validation |
| `src/replication/replset_store.rs` | **New** | Durable read/write of config from `local.system.replset` |
| `src/replication/coordinator.rs` | **Modify** | Replace `ReplicationConfig` with config-driven role resolution |
| `src/replication/mod.rs` | **Modify** | Export new types |
| `src/replication/bootstrap.rs` | **Modify** | Remove oplog pre-creation; let `rs.initiate()` create it |
| `src/bin/server.rs` | **Modify** | Add `--repl-set-name`, `--repl-self` flags |
| `src/lib.rs` | **Modify** | Export `ReplicaSetConfig` |
| `docs/decisions.md` | **Modify** | Record design decision |

---

## Phase 2: Heartbeat and Member Discovery

### Goal
Implement inter-node heartbeat (`replSetHeartbeat`) so every node knows the liveness and role of every other member.

### Wire Protocol: `replSetHeartbeat`

```json
// Request
{
  "replSetHeartbeat": 1,
  "setName": "my-replica-set",
  "from": "127.0.0.1:27017",
  "fromId": 0,
  "checkEmpty": true,
  "pv": 1,
  "v": 5,
  "term": 2
}

// Response (from primary)
{
  "setName": "my-replica-set",
  "primary": "127.0.0.1:27017",
  "isWritablePrimary": true,
  "term": 2,
  "electionId": { "$oid": "..." },
  "hosts": ["127.0.0.1:27017", "127.0.0.1:27018"],
  "ok": 1.0
}

// Response (from secondary)
{
  "setName": "my-replica-set",
  "primary": "127.0.0.1:27017",
  "isWritablePrimary": false,
  "term": 2,
  "hosts": ["127.0.0.1:27017", "127.0.0.1:27018"],
  "ok": 1.0
}
```

### Heartbeat Runtime

Each node runs a **heartbeat sender** that:
1. Every `heartbeat_interval_ms` (default 2s), sends `replSetHeartbeat` to every other member
2. Records response: last heartbeat time, role, term, primary hint
3. If a member fails to respond for `election_timeout_ms` (default 10s), mark it unreachable

Each node also handles **incoming heartbeats**:
1. Validate `setName` matches
2. Return current role, term, primary hint
3. If heartbeat has higher term, update local term and step down if currently primary

### Member State Tracking

```rust
struct MemberHealth {
    member_id: u64,
    host: String,
    last_heartbeat: Instant,
    last_heartbeat_status: Option<HeartbeatResponse>,
    is_reachable: bool,
    consecutive_failures: u32,
}

struct TopologyView {
    config: ReplicaSetConfig,
    members: HashMap<u64, MemberHealth>,
    known_primary: Option<String>,
    known_term: u64,
}
```

### Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `src/replication/heartbeat.rs` | **New** | `replSetHeartbeat` command handler |
| `src/replication/topology.rs` | **New** | `TopologyView`, member health tracking |
| `src/replication/heartbeat_sender.rs` | **New** | Background task sending heartbeats to peers |
| `src/server/commands/handlers/replication.rs` | **Modify** | Add `ReplSetHeartbeatCommand` |
| `src/replication/coordinator.rs` | **Modify** | Integrate topology view, term updates from heartbeats |

---

## Phase 3: Election Protocol

### Goal
Implement Raft-style leader election so the replica set can automatically elect a new primary when the current one becomes unreachable.

### Election Algorithm (Simplified Raft)

```
States: Follower, Candidate, Primary

Follower:
  - If heartbeat timeout expires without hearing from primary:
    -> Increment term, become Candidate, start election

Candidate:
  - Increment term
  - Vote for self
  - Send RequestVote to all voting members
  - If majority votes received:
    -> Become Primary, start sending heartbeats as leader
  - If hear heartbeat from higher term:
    -> Become Follower, reset timeout
  - If election timeout expires:
    -> Start new election (increment term again)

Primary:
  - Send heartbeats with current term
  - If hear heartbeat from higher term:
    -> Step down to Follower
```

### Wire Protocol: `replSetRequestVotes`

```json
// Request (candidate -> voter)
{
  "replSetRequestVotes": 1,
  "setName": "my-replica-set",
  "term": 3,
  "candidate": "127.0.0.1:27018",
  "dryRun": false
}

// Response
{
  "term": 3,
  "voteGranted": true,
  "reason": "",
  "ok": 1.0
}
```

### Voting Rules

A member grants a vote if:
1. `term >= local_term` (update local term if higher)
2. Has not voted in this term (or already voted for this candidate)
3. Candidate's config version >= local config version
4. Candidate is not hidden
5. Candidate has priority > 0

### Primary Behavior After Winning

1. Write an **election oplog entry** to `local.oplog.rs`:
   ```json
   { "_id": 42, "term": 3, "op": "n", "ns": "", "o": { "msg": "initiating election" } }
   ```
2. Update `ReplicationCoordinator` term
3. Begin accepting writes
4. Start heartbeat as leader

### Step-Down Behavior

When a primary steps down (higher term discovered, or `rs.stepDown()` called):
1. Stop accepting new writes immediately
2. Wait for any in-flight transactions to complete (short timeout)
3. Flush oplog to ensure durability
4. Transition to Secondary role
5. Begin following the new primary

### Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `src/replication/election.rs` | **New** | Election state machine, vote request/handler |
| `src/replication/vote_store.rs` | **New** | Durable vote tracking per term (survives restart) |
| `src/replication/coordinator.rs` | **Modify** | Add election-driven role transitions, step-down logic |
| `src/replication/observer.rs` | **Modify** | Emit election noop oplog entry |
| `src/server/commands/handlers/replication.rs` | **Modify** | Add `ReplSetRequestVotesCommand` |

---

## Phase 4: Write Concern

### Goal
Allow clients to specify durability guarantees via `writeConcern`, including `w: "majority"` acknowledgment.

### Write Concern Model

```rust
pub enum WriteConcern {
    /// Acknowledge after primary applies (default).
    Acknowledged,
    /// Wait for acknowledgment from N members (including primary).
    N(u32),
    /// Wait for majority of voting members.
    Majority,
    /// Fire-and-forget (no acknowledgment).
    Unacknowledged,
}
```

### Wire Protocol Integration

Write commands (`insert`, `update`, `delete`) carry `writeConcern`:

```json
{
  "insert": "users",
  "documents": [{ "_id": 1, "name": "alice" }],
  "writeConcern": { "w": "majority", "wtimeout": 5000 },
  "ordered": true
}
```

### Majority Acknowledgment Flow

```
1. Primary applies write + oplog in local transaction (existing behavior)
2. Return immediate acknowledgment to client for w:1
3. For w:"majority":
   a. Track the oplog index of this write
   b. Monitor follower progress (already tracked via replSetUpdatePosition)
   c. When majority of voting members report lastWritten >= this index:
      -> Send majority acknowledgment to client
   d. If wtimeout expires:
      -> Return error with partial acknowledgment status
```

### Write Concern Response

```json
{
  "n": 1,
  "ok": 1.0,
  "writeConcernError": {
    "code": 64,
    "codeName": "WriteConcernFailed",
    "errmsg": "waiting for replication timed out",
    "errInfo": {
      "wtimeout": true,
      "writeConcern": { "w": "majority", "wtimeout": 5000, "provenance": "clientSupplied" }
    }
  }
}
```

### Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `src/replication/write_concern.rs` | **New** | `WriteConcern` parsing, majority tracking |
| `src/replication/majority_tracker.rs` | **New** | Tracks which oplog indices have reached majority |
| `src/server/commands/handlers/crud.rs` | **Modify** | Parse writeConcern, wait for majority if needed |
| `src/write_ops/executor.rs` | **Modify** | Accept writeConcern parameter |

---

## Phase 5: Read Preferences

### Goal
Allow clients to read from secondaries with various consistency guarantees.

### Read Preference Modes

```rust
pub enum ReadPreference {
    /// Read only from primary (default).
    Primary,
    /// Read from primary if available, otherwise secondary.
    PrimaryPreferred,
    /// Read from any secondary.
    Secondary,
    /// Read from secondary if available, otherwise primary.
    SecondaryPreferred,
    /// Read from any member (used for nearest).
    Nearest,
}
```

### Wire Protocol: `secondaryOk` Flag

The MongoDB wire protocol carries `secondaryOk` (bit 4) in OP_QUERY and OP_MSG flags. Commands should check this flag:

```rust
fn check_read_preference(
    coordinator: &ReplicationCoordinator,
    secondary_ok: bool,
) -> Result<(), WrongoDBError> {
    match coordinator.role() {
        ReplicationRole::Primary => Ok(()),
        ReplicationRole::Secondary if secondary_ok => Ok(()),
        ReplicationRole::Secondary => Err(WrongoDBError::NotWritablePrimary { ... }),
    }
}
```

### Tag-Based Read Preference

Clients can specify tag sets to target specific secondaries:

```json
{
  "find": "users",
  "filter": { "name": "alice" },
  "$readPreference": {
    "mode": "secondary",
    "tags": [{ "dc": "us-east" }, { "dc": "us-west" }]
  }
}
```

The secondary uses its own tags to determine if it matches the preference.

### Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `src/replication/read_preference.rs` | **New** | `ReadPreference` parsing, tag matching |
| `src/server/mod.rs` | **Modify** | Extract secondaryOk flag from wire protocol |
| `src/server/commands/context.rs` | **Modify** | Carry read preference through command context |
| `src/server/commands/handlers/crud.rs` | **Modify** | Check read preference before executing find |

---

## Phase 6: Oplog Durability and Capped Behavior

### Goal
Prevent unbounded oplog growth and support rollback when a former primary has unreplicated writes.

### Capped Oplog

The oplog should behave like a **capped collection** with a configurable maximum size:

```rust
pub struct OplogConfig {
    /// Maximum oplog size in bytes (default: 5% of disk, min 990MB).
    pub max_size_bytes: u64,
    /// Minimum age of oldest oplog entry before truncation (default: 24h).
    pub min_retention_hours: u64,
}
```

**Truncation policy:**
1. Oplog can only be truncated up to the **oldest follower's lastWritten** position
2. Never truncate entries newer than `min_retention_hours`
3. When oplog reaches `max_size_bytes`, truncate oldest entries that satisfy both constraints

### Truncation Implementation

Since the oplog is a regular table with `_id` (oplog index) as key:

```rust
impl OplogStore {
    pub fn truncate_before(
        &self,
        session: &mut Session,
        before_index: u64,
    ) -> Result<(), WrongoDBError> {
        let mut cursor = session.open_table_cursor(&self.table_uri)?;
        // Delete entries with index < before_index
        // WiredTiger-style: iterate and delete
        while let Some((key, _)) = cursor.next()? {
            let entry_index = decode_oplog_index(&key)?;
            if entry_index >= before_index {
                break;
            }
            cursor.delete()?;
        }
        Ok(())
    }
}
```

A background task runs periodically to check oplog size and truncate.

### Rollback

When a former primary restarts and discovers it has writes that were never replicated:

```
1. Compare local oplog tail with primary's oplog
2. Identify unreplicated entries (higher index than any follower reported)
3. Write these entries to `local.rollback` collection
4. Delete unreplicated entries from user collections
5. Transition to Secondary and begin following new primary
```

**Rollback file format:**
Entries in `local.rollback` contain the original oplog entry plus a timestamp:

```json
{
  "_id": ObjectId("..."),
  "op": "i",
  "ns": "test.users",
  "o": { "_id": 42, "name": "alice" },
  "op_time": { "term": 2, "index": 100 },
  "rollback_time": ISODate("2026-04-02T...")
}
```

### Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `src/replication/oplog_config.rs` | **New** | `OplogConfig`, size calculation |
| `src/replication/oplog_truncator.rs` | **New** | Background task for oplog size management |
| `src/replication/oplog.rs` | **Modify** | Add `truncate_before()`, size tracking |
| `src/replication/rollback.rs` | **New** | Rollback detection and execution |
| `src/replication/bootstrap.rs` | **Modify** | Check for rollback state on startup |

---

## Phase 7: Initial Sync

### Goal
Allow a new member to join the replica set and catch up to current state without replaying the entire oplog history.

### Initial Sync Algorithm

```
1. Clone: Copy all data from sync source
   a. For each collection (except local.*):
      - Open cursor on sync source
      - Copy all documents to local storage
   b. For each index:
      - Rebuild from copied documents locally

2. Apply oplog delta:
   - Record start timestamp at beginning of clone
   - After clone completes, apply all oplog entries from start timestamp to now
   - This catches up writes that happened during the clone

3. Verify:
   - Check document counts match
   - Check index counts match

4. Transition to normal secondary replication
```

### Initial Sync State

```rust
enum InitialSyncState {
    NotStarted,
    Cloning { progress: CloneProgress },
    ApplyingOplogDelta { from: OpTime, to: OpTime },
    Verifying,
    Complete,
    Failed { error: String },
}

struct CloneProgress {
    current_namespace: Option<Namespace>,
    documents_copied: u64,
    estimated_total_documents: u64,
}
```

### Wire Protocol

Initial sync uses the existing MongoDB client protocol (same as normal secondary replication):
- `find` on each collection to enumerate documents
- `listIndexes` to get index definitions
- Tailable cursor on `local.oplog.rs` for delta application

No special protocol is needed - the initial sync is an orchestration layer on top of existing primitives.

### Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `src/replication/initial_sync.rs` | **New** | Initial sync state machine and orchestration |
| `src/replication/secondary.rs` | **Modify** | Detect empty state + non-empty primary, trigger initial sync |
| `src/replication/coordinator.rs` | **Modify** | Track initial sync state, expose in `replSetGetStatus` |

---

## Phase 8: Replica Set Management Commands

### Goal
Implement the administrative commands for managing the replica set at runtime.

### Commands

#### `replSetInitiate`

```json
// Request
{
  "replSetInitiate": 1,
  "cmdLineOpts": {
    "_id": "my-replica-set",
    "members": [
      { "_id": 0, "host": "127.0.0.1:27017" }
    ]
  }
}

// Response
{ "ok": 1.0 }
```

**Behavior:**
1. Validate config (at least 1 voting member, valid hosts)
2. Persist config to `local.system.replset`
3. Create `local.oplog.rs` and `local.repl_state` namespaces
4. Start heartbeat and election runtime
5. First node with highest priority begins election immediately

#### `replSetGetStatus`

```json
// Response
{
  "set": "my-replica-set",
  "date": ISODate("2026-04-02T..."),
  "myState": 1,
  "term": 3,
  "syncSourceHost": "127.0.0.1:27017",
  "heartbeatIntervalMillis": 2000,
  "members": [
    {
      "_id": 0,
      "name": "127.0.0.1:27017",
      "health": 1,
      "state": 1,
      "stateStr": "PRIMARY",
      "uptime": 3600,
      "optime": { "ts": { "term": 3, "index": 150 }, "t": 3 },
      "optimeDurable": { "ts": { "term": 3, "index": 150 }, "t": 3 },
      "electionTime": { "term": 3, "index": 100 },
      "self": true
    },
    {
      "_id": 1,
      "name": "127.0.0.1:27018",
      "health": 1,
      "state": 2,
      "stateStr": "SECONDARY",
      "uptime": 3500,
      "optime": { "ts": { "term": 3, "index": 148 }, "t": 3 },
      "optimeDurable": { "ts": { "term": 3, "index": 148 }, "t": 3 },
      "lastApplied": { "term": 3, "index": 148 },
      "syncSourceHost": "127.0.0.1:27017",
      "configVersion": 3
    }
  ],
  "ok": 1.0
}
```

#### `replSetReconfig`

```json
// Request
{
  "replSetReconfig": 1,
  "protocolVersion": 1,
  "force": false,
  "config": {
    "_id": "my-replica-set",
    "version": 4,
    "members": [
      { "_id": 0, "host": "127.0.0.1:27017" },
      { "_id": 1, "host": "127.0.0.1:27018" },
      { "_id": 2, "host": "127.0.0.1:27019" }
    ]
  }
}
```

**Behavior:**
1. Validate new config
2. Increment version
3. Persist to `local.system.replset` via oplog (so all nodes see it)
4. Update in-memory topology
5. If current node is removed, transition to Startup

#### `replSetStepDown`

```json
// Request
{
  "replSetStepDown": 1,
  "stepDownSecs": 60,
  "secondaryCatchUpPeriodSecs": 10
}
```

**Behavior:**
1. Wait for `secondaryCatchUpPeriodSecs` for secondaries to catch up
2. Step down: stop accepting writes, flush oplog
3. Remain in Secondary state for `stepDownSecs` (don't re-run for election)
4. After timeout, re-enter election as eligible candidate

#### `replSetFreeze`

```json
// Request
{
  "replSetFreeze": 1,
  "seconds": 120
}
```

**Behavior:**
1. Prevent this node from running for election for `seconds`
2. Useful for maintenance without triggering elections

### Member State Strings

| State | Value | Description |
|-------|-------|-------------|
| `STARTUP` | 0 | Node has not yet been configured |
| `PRIMARY` | 1 | Elected leader, accepting writes |
| `SECONDARY` | 2 | Following primary, applying oplog |
| `RECOVERING` | 3 | Startup sync, rollback, or resync |
| `STARTUP2` | 5 | Initial sync in progress |
| `UNKNOWN` | 6 | Unreachable or not yet contacted |
| `ARBITER` | 7 | Voting-only member (future) |
| `DOWN` | 8 | Former member that is now unreachable |
| `ROLLBACK` | 9 | Rolling back unreplicated writes |

### Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `src/server/commands/handlers/repl_initiate.rs` | **New** | `replSetInitiate` handler |
| `src/server/commands/handlers/repl_get_status.rs` | **New** | `replSetGetStatus` handler |
| `src/server/commands/handlers/repl_reconfig.rs` | **New** | `replSetReconfig` handler |
| `src/server/commands/handlers/repl_step_down.rs` | **New** | `replSetStepDown` handler |
| `src/server/commands/handlers/repl_freeze.rs` | **New** | `replSetFreeze` handler |
| `src/server/commands/handlers/repl_is_master.rs` | **New** | `isMaster`/`hello` with full replica set info |
| `src/server/commands/registry.rs` | **Modify** | Register new commands |
| `src/replication/coordinator.rs` | **Modify** | Add status aggregation, step-down, freeze |
| `src/replication/status.rs` | **New** | `MemberStatus`, `ReplicaSetStatus` types |

---

## Implementation Order and Dependencies

```
Phase 1: Topology Config
    |
    v
Phase 2: Heartbeat
    |
    v
Phase 3: Election Protocol
    |
    v
Phase 4: Write Concern  ---- Phase 5: Read Preferences
    |                              |
    v                              v
Phase 6: Oplog Durability ---- Phase 7: Initial Sync
    |
    v
Phase 8: Management Commands (can be built in parallel with 4-7)
```

### Phase Grouping for PRs

| PR | Phases | Estimated Complexity |
|----|--------|---------------------|
| PR 1 | Phase 1 | Medium - Config types, storage, CLI changes |
| PR 2 | Phase 2 | Medium - Heartbeat command, topology view |
| PR 3 | Phase 3 | High - Election state machine, vote protocol |
| PR 4 | Phase 4 | Medium - Write concern parsing, majority tracking |
| PR 5 | Phase 5 | Low - Read preference checks, wire protocol flag |
| PR 6 | Phase 6 | High - Capped oplog, truncation, rollback |
| PR 7 | Phase 7 | Medium - Initial sync orchestration |
| PR 8 | Phase 8 | Medium - Management command handlers |

---

## Testing Strategy

### Unit Tests

Each new module gets unit tests following the existing EARS pattern:

```rust
// EARS: When a replica set config is persisted and reloaded, the store
// shall preserve the member list and config version.
#[test]
fn replset_config_roundtrips() { ... }

// EARS: When a candidate requests votes from a majority, it shall
// transition to primary.
#[test]
fn election_wins_with_majority_votes() { ... }
```

### Integration Tests

Extend `tests/replication/mod.rs`:

```rust
// 3-node replica set election
#[tokio::test]
async fn test_elects_new_primary_after_primary_shutdown() {
    // Start 3 nodes, initiate replica set
    // Kill primary
    // Wait for new election
    // Verify one of the remaining nodes becomes primary
    // Verify writes succeed to new primary
}

// Write concern majority
#[tokio::test]
async fn test_write_concern_majority_waits_for_replication() {
    // Start 3-node replica set
    // Write with w:"majority"
    // Verify write returns only after 2 nodes have the data
}

// Rollback after primary step-down
#[tokio::test]
async fn test_former_primary_rolls_back_unreplicated_writes() {
    // Start 3-node replica set
    // Write to primary, kill before replication completes
    // Elect new primary
    // Restart old primary
    // Verify unreplicated writes are in rollback collection
}

// Initial sync
#[tokio::test]
async fn test_new_member_initial_syncs_from_primary() {
    // Start 2-node replica set with data
    // Add 3rd node
    // Verify 3rd node catches up via initial sync
}
```

### Test Infrastructure

Add a `ReplicaSetTestHarness` helper:

```rust
struct ReplicaSetTestHarness {
    nodes: Vec<TestNode>,
    repl_set_name: String,
}

impl ReplicaSetTestHarness {
    async fn new(num_nodes: usize) -> Self { ... }
    async fn initiate(&self) { ... }
    async fn wait_for_election(&self) { ... }
    fn primary(&self) -> &TestNode { ... }
    fn secondaries(&self) -> Vec<&TestNode> { ... }
}
```

---

## Migration from Current Behavior

### Backward Compatibility

The current `--role` / `--sync-source` flags will be **deprecated but functional** for one release cycle:

```
--role primary --node-name node-1
```

Maps internally to:
```rust
ReplicaSetConfig {
    name: ReplicaSetName("implicit-singleton"),
    version: 1,
    members: vec![ReplicaSetMember {
        id: 0,
        host: "127.0.0.1:27017",
        priority: 1,
        votes: true,
        hidden: false,
        tags: {},
    }],
    // ...
}
```

This creates a **single-node replica set** that is immediately primary (no election needed for a single voting member). This preserves current behavior while moving to the new model.

### Breaking Changes

- Existing data directories with the old `local.oplog.rs` pre-created format will need to be migrated or recreated
- The `local.repl_state` format changes (from simple term+index to include election state)
- Wire protocol: `hello` response shape changes to include full replica set topology

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Split brain during network partition | Medium | High | Strict majority quorum for elections; single-node partitions cannot elect |
| Oplog truncation removes entries needed by slow follower | Low | High | Truncation blocked by oldest follower's lastWritten position |
| Election storms (repeated elections) | Medium | Medium | Randomized election timeout; freeze mechanism |
| Initial sync never completes on busy primary | Low | Medium | Oplog delta application retries; fallback to full resync |
| Rollback loses client data | Low | High | Rollback entries preserved in `local.rollback`; manual recovery possible |

---

## Future Work (Out of Scope)

These are intentionally deferred to later phases:

- **Arbiters** - Voting-only members with no data
- **Priority elections** - Weighted voting based on member priority
- **Chained replication** - Secondary following another secondary instead of primary
- **Oplog compression** - Compressing older oplog entries to save space
- **Cross-datacenter replica sets** - Tag-aware placement for geo-distribution
- **Snapshot reads** - Causal consistent reads with `afterClusterTime`
- **Transactions across replica sets** - Distributed transactions with two-phase commit
- **Change streams** - Real-time change notifications built on oplog tailing
