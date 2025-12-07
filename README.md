# MiniMongo

A tiny, learning-oriented MongoDB-like store written in Python.

### Thin-slice scope
- Append-only log on disk (newline-delimited JSON).
- In-memory index rebuilt at startup.
- API: `insert_one`, `find_one`, `find` with top-level equality filters.
- Single-process, no concurrency controls in the first iteration.

This repo evolves step by step with small, focused commits to make the design easy to follow.
