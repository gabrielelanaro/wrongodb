# MiniMongo

A tiny, learning-oriented MongoDB-like store written in Python.

### Thin-slice scope
- Append-only log on disk (newline-delimited JSON).
- In-memory index rebuilt at startup.
- API: `insert_one`, `find_one`, `find` with top-level equality filters.
- Single-process, no concurrency controls in the first iteration.

This repo evolves step by step with small, focused commits to make the design easy to follow.

### Quickstart (current state)
```python
from minimongo import MiniMongo

db = MiniMongo(path="data/db.log", index_fields=["name"])
db.insert_one({"name": "alice", "age": 30})
db.insert_one({"name": "bob", "age": 25})

print(db.find())               # all docs
print(db.find({"name": "bob"}))  # equality on indexed field
```
