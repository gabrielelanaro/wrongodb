# WrongoDB blog series plan (working draft)

Format target:
- 5–10 minutes each (~1,200–2,000 words, depending on density).
- Story first, then concepts, then “what I shipped”.
- Each post ends with **Editing notes** so I can tune voice and add details.

## Series arc

The narrative thread:
1) “I can store and query documents” (toy but real)
2) “I need predictable IO units” (pages)
3) “I need structures, not scans” (B+tree)
4) “I need a durability story” (WAL + checkpoints)

## Proposed posts

### 00 — Introduction
File: `blog/00-introduction.md`
Goal: set expectations, define “learning-first”, explain AI-as-pair approach.

### 01 — Thin slices
File: `blog/01-thin-slices.md`
Goal: the first shippable DB: JSONL log + in-memory index rebuilt at startup.

### 02 — Naming + boundaries
File: `blog/02-rename-and-boundaries.md`
Goal: why “WrongoDB” exists, guardrails, and how constraints keep AI help useful.

### 03 — BlockFile + durability anxiety
File: `blog/03-blockfile-and-durability.md`
Goal: fixed-size pages, sparse files, explicit allocation, CRC as a sanity alarm.

### 04 — Slotted pages (leaf KV)
File: `blog/04-slots-are-not-scary.md`
Goal: slotted layout, deletes, compaction, `PageFull` as a boundary.

### 05 — The first B+tree
Status: draft later
Goal: root+leaves, splits, separator keys, “height ≤ 2” as a deliberate slice.
Note: another agent is actively working in the repo on Slice D; treat the code as moving.

### 06 — Watching the OS write
Status: draft later
Goal: `fs_usage`, `FULLFSYNC`, the difference between “written” and “durable”.

### 07 — What “WAL + checkpoints” will mean here
Status: outline later
Goal: set expectations: not production durability, but real invariants and recovery flow.

## Reusable post template

Suggested structure (not mandatory):
1) Hook (the moment of confusion or a concrete question)
2) The smallest concept that resolves it
3) The thin slice I shipped (and what I did *not* ship)
4) What broke / what I learned
5) What’s next
6) Editing notes

