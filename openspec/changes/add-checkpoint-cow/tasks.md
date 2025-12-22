## 1. Implementation
- [x] 1.1 Define checkpoint metadata format (dual root slots + generation + CRC) and bump file version.
- [ ] 1.2 Add root-slot selection on open; write-next-slot on checkpoint commit.
- [ ] 1.3 Add copy-on-write page writes for BTree mutations (new blocks for modified pages).
- [ ] 1.4 Introduce "stable root" vs "working root" and a `checkpoint()` API that swaps roots.
- [ ] 1.5 Track retired blocks and only recycle them after successful checkpoint (initially allow leaks on crash).
- [ ] 1.6 Tests: checkpoint commit selects new root; crash before commit uses old root; retired blocks not reused before checkpoint.
