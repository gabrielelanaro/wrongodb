# Thin slices, but make it Mongo: shipping the tiniest possible DB

The fastest way to never ship a database is to try to build a database.

So I did the opposite: I shipped the tiniest thing that *behaves* like a database in a way I can poke, break, and understand.

This post is about that first slice: an **append-only log on disk** (newline-delimited JSON) and an **in-memory index** rebuilt at startup. That’s it. No networking. No query planner. No concurrency. Just enough “Mongo-ish” surface area to make the next decision concrete.

## The first question: what is the smallest thing that feels like Mongo?

MongoDB is “documents + CRUD + filters”. If I’m honest, I only needed a sliver of that to begin:

- `insert_one(document)` that persists something and gives me back a stable identifier.
- `find(filter)` that returns matching documents.
- A very restricted filter language: **top-level equality**.

Everything else can wait.

This is the key trick: the API doesn’t need to be complete, it needs to be **consistent**. If I can rely on a handful of semantics, I can change the storage engine under it without rewriting the entire project.

## The core idea: a log you can always append to

My first storage format was deliberately boring:

- serialize each document as one JSON line
- append it to a file
- remember the byte offset where it was written

That’s a database in the same sense that a notebook is a calendar: technically it works, but you’re going to feel pain the moment you need to find anything.

And that pain is the point. I want the next slice to be motivated by real friction, not vibes.

Here’s a tiny diagram of what that looks like:

```
append-only log file (JSONL)

  [0]  {"_id": 1, "name": "alice", "age": 30}\n
  [42] {"_id": 2, "name": "bob",   "age": 25}\n
  [83] {"_id": 3, "name": "cory",  "age": 40}\n

in-memory index (rebuilt on startup)

  "name=alice" -> [0]
  "name=bob"   -> [42]
  "name=cory"  -> [83]
```

If you’re thinking “that’s basically grep + a hashmap”, yes. Exactly.

## A tiny API sketch (the thing I can explain to myself)

I like having an example that fits on screen because it forces the design to have a shape.

Here’s the vibe (this is intentionally small and aligned with the repo’s quickstart in `README.md`):

```rust
use serde_json::json;
use wrongodb::WrongoDB;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = WrongoDB::open("data/db.log", ["name"], false)?;

    db.insert_one(json!({"name": "alice", "age": 30}))?;
    db.insert_one(json!({"name": "bob",   "age": 25}))?;

    // full scan (no filter)
    let all = db.find(None)?;

    // top-level equality filter (uses an index if available)
    let bob = db.find_one(Some(json!({"name": "bob"})))?;

    println!("all={:?}", all.len());
    println!("bob={:?}", bob);
    Ok(())
}
```

That is enough for the rest of the project to exist.

## Why the “dumb” design is actually the smart first move

There are a bunch of reasons to start with a log:

- It’s easy to reason about. Appends are simple.
- It’s easy to debug. You can literally open the file.
- It forces the next storage abstraction to be explicit.
- It creates a baseline for correctness tests.

The biggest reason, though:

It gives you a controlled place to ask questions like:

- What is my document normalization policy? (`_id`, types, etc.)
- What does a “filter” mean, exactly?
- What should be indexed, and what should be scanned?

If you skip this, you end up building a storage engine without knowing what you need it to *do*. That’s backwards.

## The moment the log stops being cute

The log approach collapses the moment you want any of these:

- updates (rewrite-in-place? tombstones? copy-on-write?)
- deletes (same story)
- more complex filters
- range scans
- stable performance as the file grows

And the “rebuild the index at startup” trick is also temporary. It works for the learning stage, but it’s not a database story. It’s a prototype story.

Which is why the series moves from:

1) log-only
2) pages (BlockFile)
3) a B+tree
4) later: WAL + checkpoints

In other words: “how do I stop scanning a growing file forever?”

## AI as a pair

This slice is also where I learned the best way (for me) to use an assistant: not for big architecture, but for **scope discipline**.

The pattern that keeps working:

- I describe what I want emotionally (“make it more like Mongo”).
- The assistant turns that into a set of axes (API behavior vs. storage engine vs. networking).
- Then I pick the smallest axis that moves the project forward *today*.

I also use AI to ask extremely naive questions without feeling bad about it.

Like: “If I only implement equality filters, what do I regret later?”

It’s surprisingly good at answering that kind of question, because it can enumerate the failure modes and trade-offs quickly. And then I can decide what I actually care about in this toy.

## What I consider “done” for Slice 1

I like having a definition of done that’s not “it feels done”.

For this slice, it’s something like:

- a single file stores appended documents
- I can restart the program and still query what I inserted
- `find_one` behaves consistently for simple filters
- tests exist for these behaviors (even if they’re boring)

Once those are true, I’m allowed to move on.

Because the next slice is where the interesting storage questions start.

## Editing notes

- Add a short anecdote about the very first “it worked!” moment (the first persisted find after restart).
- Decide whether to mention the earlier Python scaffolding explicitly or keep this post Rust-centric.
- Include one screenshot/snippet of a real `data/*.log` file for texture.
- Tighten the definition of the filter language (what exactly counts as “top-level equality”?).
- Add a short section on `_id` semantics (generated vs. provided).

