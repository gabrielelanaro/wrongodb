# WrongoDB devlog (drafts)

This folder contains draft posts for a short “build-a-MongoDB-ish thing with AI” series.

Conventions:
- Filenames are numbered so the series stays ordered.
- Each post aims for ~5–10 minutes reading time.
- I’m optimizing for *story + learnings*, not completeness.

Planned posts (subject to change):
1. `00-introduction.md` — what I’m building and why, and what “AI-driven” means in practice.
2. `01-blockfile-and-durability.md` — fixed-size pages, checksums, and the moment I started caring about corruption.
3. `02-slots-are-not-scary.md` — slotted pages: why “a slot” is just a tiny pointer, and how deletes become possible.
4. `03-watching-the-os-write.md` — `fs_usage`, `FULLFSYNC`, and why “it wrote” doesn’t mean “it’s durable”.
5. `04-first-btree.md` — turning one page into a 2-level B+tree (root + leaves), splits, and separator keys.
6. `05-what-next.md` — WAL, checkpoints, MVCC: the roadmap and the “reasonable next slice”.
