# WrongoDB devlog (drafts)

This folder contains draft posts for a short “build-a-MongoDB-ish thing with AI” series.

Conventions:
- Filenames are numbered so the series stays ordered.
- Each post aims for ~5–10 minutes reading time.
- I’m optimizing for *clear explanations + learnings*, not completeness.

Current post order:
1. `00-introduction/post.md` — what I’m building and why, and what “AI-driven” means in practice.
2. `01-blockfile-and-durability/post.md` — fixed-size pages, checksums, and the moment I started caring about corruption.
3. `02-watching-the-os-write/post.md` — `fs_usage`, `FULLFSYNC`, and why “it wrote” doesn’t mean “it’s durable”.
4. `03-slots-are-not-scary/post.md` — slotted pages: why “a slot” is just a tiny pointer, and how deletes become possible.
5. `04-first-btree/post.md` — turning one page into a 2-level B+tree (root + leaves), splits, and separator keys.
6. `05-multi-level-btree/post.md` — internal-page splits, separator propagation, and the first real growth step.
7. `06-checkpoints-and-cow/post.md` — stable root vs working root, atomic checkpoint slots, and why copy-on-write makes the tree crash-safe.
8. `07-write-ahead-log/post.md` — why the WAL exists, what recovery replays, and where the durability boundary actually is.
