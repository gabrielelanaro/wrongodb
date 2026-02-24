# Distributed Replication and Fault Tolerance Plan for WrongoDB

## Overview

This document outlines a comprehensive plan for adding replication and fault tolerance to WrongoDB, transforming it from a single-node database into a distributed system capable of surviving node failures while maintaining data consistency.

## Current Architecture Analysis

### Key Components Relevant to Replication

| Component | Current State | Replication Relevance |
|-----------|---------------|----------------------|
| **WAL** ([`GlobalWal`](../src/storage/wal.rs:918)) | Single-node, file-based | Primary vehicle for replication - already logs all mutations |
| **Transaction Manager** ([`TransactionManager`](../src/txn/transaction_manager.rs:14)) | MVCC with snapshot isolation | Needs distributed coordination for cross-node transactions |
| **BTree Storage** ([`BTree`](../src/storage/btree/mod.rs)) | Copy-on-write with checkpoints | Replicas apply WAL entries to reach same state |
| **Server** ([`start_server`](../src/server/mod.rs:50)) | MongoDB wire protocol | Client-facing API remains unchanged |

### Current Write Path

```
Client Request
     │
     ▼
┌─────────────────┐
│  MongoDB Wire   │
│    Protocol     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Collection    │
│   CRUD Ops      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐     ┌─────────────────┐
│   Transaction   │────▶│   MVCC Chains   │
│    Manager      │     └─────────────────┘
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Global WAL    │◀── Durability boundary
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  BTree + COW    │
│   Checkpoint    │
└─────────────────┘
```

---

## Replication Architecture Options

### Option 1: Primary-Secondary (Leader-Follower)

**Description**: One node accepts writes, replicates to followers synchronously or asynchronously.

```
┌─────────────────────────────────────────────────────────┐
│                    Client Writes                         │
└─────────────────────────┬───────────────────────────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │      PRIMARY          │
              │   Accepts Writes      │
              │                       │
              │  ┌─────────────────┐  │
              │  │   Global WAL    │  │
              │  └────────┬────────┘  │
              └───────────┼───────────┘
                          │
          ┌───────────────┼───────────────┐
          │               │               │
          ▼               ▼               ▼
    ┌───────────┐   ┌───────────┐   ┌───────────┐
    │SECONDARY 1│   │SECONDARY 2│   │SECONDARY 3│
    │  Apply    │   │  Apply    │   │  Apply    │
    │   WAL     │   │   WAL     │   │   WAL     │
    └───────────┘   └───────────┘   └───────────┘
```

**Pros**:
- Simple to implement and understand
- Strong consistency with synchronous replication
- Natural fit with existing WAL architecture
- Read scaling via secondary reads

**Cons**:
- Primary is single point of failure during failover
- Write throughput limited to single primary
- Failover requires consensus mechanism

### Option 2: Multi-Primary (Multi-Leader)

**Description**: Multiple nodes accept writes, conflicts resolved via last-write-wins or application-level resolution.

```
                    ┌───────────────────────────────┐
                    │       Conflict Resolution     │
                    │    Last-Write-Wins / Vector   │
                    │          Clocks               │
                    └───────────────┬───────────────┘
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        │                           │                           │
        ▼                           ▼                           ▼
  ┌───────────┐               ┌───────────┐               ┌───────────┐
  │ PRIMARY A │◀─────────────▶│ PRIMARY B │◀─────────────▶│ PRIMARY C │
  │ Region US │               │ Region EU │               │ Region AP │
  └─────┬─────┘               └─────┬─────┘               └─────┬─────┘
        │                           │                           │
        ▼                           ▼                           ▼
  Local Reads               Local Reads               Local Reads
  Local Writes              Local Writes              Local Writes
```

**Pros**:
- Write scaling across regions
- Lower latency for geo-distributed clients
- No single point of failure

**Cons**:
- Conflict resolution is complex
- May lose strong consistency guarantees
- Significantly more complex to implement

### Option 3: Quorum-Based (Leaderless)

**Description**: No distinguished leader, any node can accept writes if quorum agrees.

```
                    Client Write Request
                          │
                          ▼
              ┌───────────────────────┐
              │   Send to ALL nodes   │
              └───────────┬───────────┘
                          │
        ┌─────────────────┼─────────────────┐
        │                 │                 │
        ▼                 ▼                 ▼
  ┌───────────┐     ┌───────────┐     ┌───────────┐
  │  NODE 1   │     │  NODE 2   │     │  NODE 3   │
  │  Accept   │     │  Accept   │     │  Reject   │
  └───────────┘     └───────────┘     └───────────┘
        │                 │
        └────────┬────────┘
                 │
                 ▼
        Quorum Reached (2/3)
        Write Committed
```

**Pros**:
- No leader election needed
- Strong fault tolerance
- Natural load balancing

**Cons**:
- Higher latency for writes (need quorum)
- Read repair and anti-entropy complexity
- Session consistency challenges

---

## Recommended Approach: Raft-Based Primary-Secondary

Given WrongoDBs existing architecture and learning-oriented goals, I recommend implementing **Raft-based primary-secondary replication** because:

1. **WAL is already log-based** - natural fit for Rafts log replication
2. **COW + Checkpoints** - replicas can apply logs deterministically
3. **Single writer model** - matches existing transaction semantics
4. **Well-documented** - Raft is designed for understandability

### Raft Consensus Protocol Overview

Raft provides:
- **Leader Election**: Automatic promotion of follower to leader
- **Log Replication**: Leader replicates log entries to followers
- **Safety**: Committed entries are never lost

```
┌─────────────────────────────────────────────────────────────────────┐
│                         RAFT CLUSTER                                 │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │                    LEADER (Term 5)                            │   │
│  │  ┌─────────────────────────────────────────────────────────┐ │   │
│  │  │                    Log Entries                          │ │   │
│  │  │  [1] [2] [3] [4] [5] [6] [7] [8] [9] [10] [11] [12]    │ │   │
│  │  │                              ▲                          │ │   │
│  │  │                              │ commit index             │ │   │
│  │  └─────────────────────────────────────────────────────────┘ │   │
│  └───────────────────────────┬──────────────────────────────────┘   │
│                              │                                       │
│              AppendEntries RPC                                       │
│                              │                                       │
│         ┌────────────────────┼────────────────────┐                  │
│         │                    │                    │                  │
│         ▼                    ▼                    ▼                  │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐         │
│  │   FOLLOWER     │  │   FOLLOWER     │  │   FOLLOWER     │         │
│  │  Log: [1-10]   │  │  Log: [1-11]   │  │  Log: [1-9]    │         │
│  │  match: 10     │  │  match: 11     │  │  match: 9      │         │
│  └────────────────┘  └────────────────┘  └────────────────┘         │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Detailed Design: Log-Based Replication

### Phase 1: WAL Enhancement for Replication

The existing [`GlobalWal`](../src/storage/wal.rs:918) needs enhancements:

```rust
// Proposed: Replicated WAL Entry
pub struct ReplicatedWalEntry {
    pub term: u64,           // Raft term when entry was created
    pub index: u64,          // Log index (monotonic)
    pub record: WalRecord,   // Existing WAL record
    pub checksum: u32,       // Integrity check
}
```

**Changes needed**:

1. **Add term and index to WAL records**
   - Modify [`WalRecordHeader`](../src/storage/wal.rs:384) to include `term: u64` and `index: u64`
   - Update serialization/deserialization

2. **Create ReplicationState module**
   ```
   src/replication/
   ├── mod.rs           # Public API
   ├── raft_node.rs     # Raft state machine
   ├── log.rs           # Replicated log (wraps WAL)
   ├── state.rs         # Persistent state (term, votedFor)
   └── peer.rs          # Peer communication
   ```

### Phase 2: Raft State Machine

```
┌─────────────────────────────────────────────────────────────────────┐
│                      RAFT STATE MACHINE                              │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │                    Persistent State                           │   │
│  │  ┌─────────────────────────────────────────────────────────┐ │   │
│  │  │  current_term: u64                                      │ │   │
│  │  │  voted_for: Option<NodeId>                              │ │   │
│  │  │  log: Vec<LogEntry>                                     │ │   │
│  │  └─────────────────────────────────────────────────────────┘ │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │                    Volatile State                             │   │
│  │  ┌─────────────────────────────────────────────────────────┐ │   │
│  │  │  commit_index: u64                                      │ │   │
│  │  │  last_applied: u64                                      │ │   │
│  │  │  state: Follower | Candidate | Leader                   │ │   │
│  │  └─────────────────────────────────────────────────────────┘ │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │                  Leader-Only State                            │   │
│  │  ┌─────────────────────────────────────────────────────────┐ │   │
│  │  │  next_index: HashMap<NodeId, u64>                       │ │   │
│  │  │  match_index: HashMap<NodeId, u64>                      │ │   │
│  │  └─────────────────────────────────────────────────────────┘ │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Phase 3: Replication Flow

#### Write Path with Replication

```
┌──────────────────────────────────────────────────────────────────────┐
│                    REPLICATED WRITE FLOW                              │
│                                                                       │
│  Client                                                               │
│    │                                                                  │
│    │ Insert/Update/Delete                                             │
│    ▼                                                                  │
│  ┌────────────────┐                                                   │
│  │ MongoDB Wire   │                                                   │
│  │ Protocol       │                                                   │
│  └───────┬────────┘                                                   │
│          │                                                            │
│          ▼                                                            │
│  ┌────────────────┐      ┌─────────────────┐                         │
│  │   Session +    │      │  Raft State     │                         │
│  │   Transaction  │─────▶│  Check: Am I    │                         │
│  └───────┬────────┘      │  Leader?        │                         │
│          │               └────────┬────────┘                         │
│          │                        │                                  │
│          │                        │ Yes                              │
│          │                        ▼                                  │
│          │               ┌─────────────────┐                         │
│          │               │ Append to       │                         │
│          │               │ Raft Log        │                         │
│          │               └────────┬────────┘                         │
│          │                        │                                  │
│          │                        ▼                                  │
│          │               ┌─────────────────┐                         │
│          │               │ Replicate to    │                         │
│          │               │ Followers via   │                         │
│          │               │ AppendEntries   │                         │
│          │               └────────┬────────┘                         │
│          │                        │                                  │
│          │                        ▼                                  │
│          │               ┌─────────────────┐                         │
│          │               │ Wait for        │                         │
│          │               │ Majority ACK    │                         │
│          │               └────────┬────────┘                         │
│          │                        │                                  │
│          │                        ▼                                  │
│          │               ┌─────────────────┐                         │
│          │               │ Advance         │                         │
│          │               │ Commit Index    │                         │
│          │               └────────┬────────┘                         │
│          │                        │                                  │
│          ▼                        ▼                                  │
│  ┌────────────────┐      ┌─────────────────┐                         │
│  │ Apply to       │      │ Apply to        │                         │
│  │ Local WAL      │◀─────│ State Machine   │                         │
│  └───────┬────────┘      └─────────────────┘                         │
│          │                                                            │
│          ▼                                                            │
│  ┌────────────────┐                                                   │
│  │ Apply to       │                                                   │
│  │ BTree + MVCC   │                                                   │
│  └───────┬────────┘                                                   │
│          │                                                            │
│          ▼                                                            │
│  ┌────────────────┐                                                   │
│  │ Response to    │                                                   │
│  │ Client         │                                                   │
│  └────────────────┘                                                   │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

#### Follower Apply Loop

```
┌──────────────────────────────────────────────────────────────────────┐
│                    FOLLOWER APPLY LOOP                                │
│                                                                       │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │                    Background Task                            │    │
│  │                                                               │    │
│  │  loop {                                                       │    │
│  │      while last_applied < commit_index {                      │    │
│  │          last_applied += 1;                                   │    │
│  │          entry = log[last_applied];                           │    │
│  │                                                               │    │
│  │          // Apply to local state machine                      │    │
│  │          match entry.record {                                 │    │
│  │              WalRecord::Put { .. } => apply_put(..),          │    │
│  │              WalRecord::Delete { .. } => apply_delete(..),    │    │
│  │              WalRecord::TxnCommit { .. } => apply_commit(..), │    │
│  │              ...                                              │    │
│  │          }                                                    │    │
│  │      }                                                        │    │
│  │  }                                                            │    │
│  │                                                               │    │
│  └──────────────────────────────────────────────────────────────┘    │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Failover and Leader Election

### Election Process

```
┌──────────────────────────────────────────────────────────────────────┐
│                    RAFT LEADER ELECTION                               │
│                                                                       │
│  Timeline: Leader fails at T=0                                        │
│                                                                       │
│  T=0        T=150-300ms        T=300-450ms        T=450ms+           │
│    │            │                   │                 │               │
│    ▼            ▼                   ▼                 ▼               │
│  ┌─────┐    ┌─────────┐       ┌───────────┐     ┌─────────┐          │
│  │Leader│    │Follower │       │Candidate  │     │ New     │          │
│  │ FAIL │───▶│timeout  │──────▶│starts     │────▶│ LEADER  │          │
│  │      │    │expires  │       │election   │     │         │          │
│  └─────┘    └─────────┘       └───────────┘     └─────────┘          │
│                                                                       │
│  Election Steps:                                                      │
│                                                                       │
│  1. Follower election timeout (150-300ms random)                     │
│  2. Increment term, transition to Candidate                          │
│  3. Vote for self, send RequestVote to all peers                     │
│ 4. Wait for majority votes                                            │
│  5. If majority: become Leader, send heartbeats                      │
│  6. If timeout or higher term discovered: retry or become Follower   │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

### Failover Scenario

```
┌──────────────────────────────────────────────────────────────────────┐
│                    FAILOVER SCENARIO                                  │
│                                                                       │
│  Before: 3-node cluster, Node A is leader                            │
│                                                                       │
│  ┌───────────┐     ┌───────────┐     ┌───────────┐                  │
│  │  Node A   │────▶│  Node B   │     │  Node C   │                  │
│  │  LEADER   │     │ FOLLOWER  │     │ FOLLOWER  │                  │
│  │  Term: 5  │     │ Term: 5   │     │ Term: 5   │                  │
│  └───────────┘     └───────────┘     └───────────┘                  │
│                                                                       │
│  Node A crashes!                                                      │
│                                                                       │
│  After: Node B becomes leader                                        │
│                                                                       │
│  ┌───────────┐     ┌───────────┐     ┌───────────┐                  │
│  │  Node A   │     │  Node B   │────▶│  Node C   │                  │
│  │  DOWN     │     │ LEADER    │     │ FOLLOWER  │                  │
│  │  Term: 5  │     │ Term: 6   │     │ Term: 6   │                  │
│  └───────────┘     └───────────┘     └───────────┘                  │
│                                                                       │
│  Uncommitted entries from Term 5 are preserved and committed         │
│  in Term 6 by new leader.                                            │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Consistency Levels

### Supported Consistency Levels

| Level | Description | Use Case |
|-------|-------------|----------|
| **Strong** | Read from leader only, wait for commit | Financial transactions |
| **Eventual** | Read from any replica, may be stale | Analytics, caching |
| **Read-Your-Writes** | Read from leader or synced replica | User sessions |
| **Monotonic** | Session sticks to one replica | Pagination |

### Implementation

```rust
pub enum ReadConsistency {
    /// Read only from leader, wait for commit index
    Strong,
    /// Read from any replica, may return stale data
    Eventual,
    /// Read from leader or replica with commit_index >= last_write
    ReadYourWrites,
}
```

### Read Path with Consistency

```
┌──────────────────────────────────────────────────────────────────────┐
│                    CONSISTENT READ PATH                               │
│                                                                       │
│  Strong Read:                                                         │
│  ┌────────────────┐                                                   │
│  │ Route to       │                                                   │
│  │ LEADER only    │                                                   │
│  │                │                                                   │
│  │ Wait for       │                                                   │
│  │ commit_index   │                                                   │
│  └────────────────┘                                                   │
│                                                                       │
│  Eventual Read:                                                       │
│  ┌────────────────┐                                                   │
│  │ Route to ANY   │                                                   │
│  │ replica        │                                                   │
│  │                │                                                   │
│  │ Read local     │                                                   │
│  │ state directly │                                                   │
│  └────────────────┘                                                   │
│                                                                       │
│  Read-Your-Writes:                                                    │
│  ┌────────────────┐                                                   │
│  │ Track session  │                                                   │
│  │ last_write_idx │                                                   │
│  │                │                                                   │
│  │ Route to       │                                                   │
│  │ replica where  │                                                   │
│  │ applied >=     │                                                   │
│  │ last_write_idx │                                                   │
│  └────────────────┘                                                   │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Implementation Roadmap

### Completed (as of 2026-02-22)

1. **Raft core and durable protocol state**
   - Hard state sidecar (`raft_state.bin`) with corruption checks
   - Durable protocol log (`raft_log.bin`) with truncate/append semantics
   - Deterministic RequestVote/AppendEntries protocol handlers
   - Deterministic role/timer engine (follower/candidate/leader, election + heartbeat ticks)

2. **Durability boundary integration**
   - WAL v2 includes `raft_term` and `raft_index`
   - WAL APIs route through Raft proposals (`Put/Delete/TxnCommit/TxnAbort/Checkpoint`)
   - Leader-gated writes with wire-protocol `NotWritablePrimary` mapping

3. **Runtime bridge and multi-node transport MVP**
   - `RaftRuntime` envelope API for inbound/outbound RPC messages
   - Dedicated Raft TCP transport (`u32_le` frame + bincode envelope)
   - Actor-owned Raft service with:
     - 50ms tick cadence
     - proposal commit waiting (5s timeout)
     - outbound send + inbound listener integration

4. **Static cluster topology config**
   - `RaftMode::Cluster { local_node_id, local_raft_addr, peers }`
   - per-peer `{ node_id, raft_addr }` configuration
   - validation for address parseability and uniqueness invariants

### In Progress / Remaining

1. **Apply-path parity for live follower reads**
   - Followers currently persist replicated WAL entries; replay-to-table visibility is recovery-driven.
   - Add incremental apply into live table state for immediate follower-read freshness.

2. **Operational hardening**
   - Connection pooling/backoff for outbound Raft RPC
   - Structured metrics/logging (election churn, commit latency, replication lag)
   - Better shutdown/startup orchestration for large clusters

3. **Consensus feature completeness**
   - Read-index / linearizable read option
   - Snapshot installation and log compaction workflow
   - Dynamic membership (joint consensus)

4. **Fault-injection and soak coverage**
   - Partition simulation harness
   - Duplicate/out-of-order frame stress
   - Long-running churn/failover soak tests

---

## Code Changes Summary

### New Files

| File | Purpose |
|------|---------|
| `src/raft/protocol.rs` | Pure Raft protocol core (vote + append rules) |
| `src/raft/role_engine.rs` | Deterministic role/timer and replication effects |
| `src/raft/runtime.rs` | Transport-agnostic runtime envelopes and stepping |
| `src/raft/transport.rs` | Framed wire transport + bincode envelope encoding |
| `src/raft/service.rs` | Actor-owned runtime with tick loop + proposal waiting |
| `src/raft/log_store.rs` | Durable protocol log storage |
| `src/raft/hard_state.rs` | Durable hard state store |

### Modified Files

| File | Changes |
|------|---------|
| [`src/storage/wal.rs`](../src/storage/wal.rs) | WAL v2 raft identity, snapshot boundary metadata |
| [`src/recovery/manager.rs`](../src/recovery/manager.rs) | Raft proposal-driven WAL mutation path + service integration |
| [`src/engine/database.rs`](../src/engine/database.rs) | Public `RaftMode` cluster topology shape |
| [`src/bin/server.rs`](../src/bin/server.rs) | CLI flags for raft node id/address/peers |
| [`src/server/mod.rs`](../src/server/mod.rs) | `NotWritablePrimary` error mapping with optional primary hint |

---

## Testing Strategy

### Unit Tests

- Raft state machine transitions
- Log replication logic
- Election timeout handling
- Log matching and repair

### Integration Tests

- Multi-node cluster formation
- Leader election with network delays
- Log replication under load
- Failover and recovery

### Fault Injection Tests

```rust
#[test]
fn test_leader_failure_during_write() {
    let cluster = TestCluster::new(3);
    cluster.start();
    
    // Write in progress
    let handle = spawn(|| cluster.leader().insert(doc));
    
    // Kill leader mid-write
    cluster.kill_leader();
    
    // Verify: write either completes or returns error
    // No data loss, eventual consistency
}
```

---

## References

1. **Raft Paper**: In Search of an Understandable Consensus Algorithm (Diego Ongaro, John Ousterhout)
2. **MongoDB Replication**: https://www.mongodb.com/docs/manual/replication/
3. **WiredTiger**: https://source.wiredtiger.com/develop/arch-replication.html
4. **etcd Raft**: https://github.com/etcd-io/raft (reference implementation)

---

## Questions for Discussion

1. **Synchronous vs Asynchronous Replication**: Should we wait for majority ACK before responding to client (strong consistency) or acknowledge immediately (higher throughput)?

2. **Read from Followers**: Should secondary reads be enabled by default, or require explicit opt-in?

3. **Snapshotting**: How should we handle log compaction? Periodic snapshots + log truncation?

4. **Membership Changes**: Single-node changes (Raft) or joint consensus for bulk changes?
