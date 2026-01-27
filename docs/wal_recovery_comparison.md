# WiredTiger vs Minimongo Recovery: Logical WAL Alignment

## Executive Summary

Minimongo now aligns with WiredTiger’s recovery strategy by logging **logical operations** (put/delete) and replaying
those operations through the normal BTree API. This removes the previous COW page-id drift problem and eliminates the
need for split logging or page-allocation mapping during recovery.

## WiredTiger’s Approach (Reference)

WiredTiger logs row/column operations (key-based) and replays them using cursor inserts/removes. Splits and root
changes happen inside the normal B-tree code path, not in the log format.

```
WAL log:  row_put(fileid, key, value)
recovery: cursor->insert()
```

## Minimongo’s Updated Approach

```
WAL log:  put(key, value) / delete(key)
recovery: BTree::put / BTree::delete   (WAL disabled)
```

Key properties:
- No WAL page IDs.
- No split records.
- No recovery-specific page mapping.
- Recovery reuses normal B-tree logic (splits, root updates, COW).

## What This Fixes

### Previous failure mode (removed)
```
WAL: LeafInsert(page_id=1)
COW: page 1 -> page 15 during recovery
Root still points to page 1
Result: recovered data invisible
```

### New behavior
```
WAL: Put(key="foo", value="bar")
Recovery: BTree::put("foo", "bar")
Normal insert path handles splits + root updates
```

## Comparison Table

| Aspect | WiredTiger | Minimongo (now) | Notes |
|--------|------------|-----------------|-------|
| WAL granularity | Logical row/col ops | Logical put/delete | No page IDs |
| Recovery API | Cursor insert/remove | BTree::put/delete | Normal code path |
| Split handling | Normal B-tree logic | Normal B-tree logic | Not logged |
| COW concerns | None (logical replay) | None (logical replay) | Page-id drift removed |
| Recovery idempotence | Yes | Yes | Put = upsert; delete missing ok |

## Remaining Differences

- Minimongo does not implement full transactional logging, timestamps, or rollback-to-stable.
- WAL versioning is strict (no backward compatibility for unreleased format changes).
