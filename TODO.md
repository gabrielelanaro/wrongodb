# TODO

- Re-align WrongoDB with WiredTiger by moving document-aware indexing logic up into the server layer, keeping storage/index maintenance separate from document field extraction and query semantics.
- Implement storage-level rollback-to-stable as a prerequisite for replication. The replication path needs this when background sync detects that a node diverged after a new primary/master is elected: the old primary must roll back local writes newer than the stable point before it can resume as a follower and continue applying replicated log entries from the new leader.
