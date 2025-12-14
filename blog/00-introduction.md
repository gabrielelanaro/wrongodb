# WrongoDB: a tiny wrong database (on purpose)

So. I’m building a small MongoDB-like store in Rust.

Is it going to replace MongoDB? No.
Is it going to teach me a lot about storage engines? That’s the bet.

This repository is my learning journal in code form. I’m calling the project **WrongoDB** because it’s intentionally “wrong” in the early stages: simplified, incomplete, occasionally naive, and very honest about it. The goal isn’t to ship a production database. The goal is to understand the moving parts well enough that I can explain them, refactor them, and eventually build something that *feels* like a tiny, simplified cousin of a real engine.

And yes, I’m slowly steering it toward a **WiredTiger-inspired** design. Not a clone. Not even close. More like: “what are the big ideas, and what happens if I re-implement the smallest possible version of them?”

## Why a MongoDB-like store?

Because MongoDB is approachable from the outside. You think in documents. You think in CRUD. You can sketch an API in five minutes. That’s a nice on-ramp.

Then you hit the fun questions:

- How do I store documents on disk without making a mess?
- What happens when updates are smaller than a page? Bigger than a page?
- How do I index keys efficiently?
- What does “durable” even mean if I crash mid-write?
- When do I rewrite pages? When do I keep a log? When do I checkpoint?

If you squint, these are storage-engine questions wearing a friendly document-database hoodie.

## The real topic: storage, the hard way (but small)

WrongoDB starts with “store documents somewhere.” Then it progresses, one thin slice at a time, toward something that looks like:

- a **log-ish** view of writes (append is easy, right?),
- **pages** as a unit of IO (because disks and OS caches don’t love random tiny writes),
- a **B+tree** (because scanning a log forever is… not great),
- and later: **WAL + checkpoints** (because “I hope it doesn’t crash” isn’t a durability strategy).

Here’s the rough mental model I’m carrying right now:

```
   writes
     |
     v
  +-------+      +--------+      +--------+      +----------------------+
  |  log  | ---> | pages  | ---> | B+tree | ---> | (future) WAL/checkpt |
  +-------+      +--------+      +--------+      +----------------------+
```

That diagram is deliberately “too neat”. Real designs loop back. They have background work. They have compaction, reconciliation, eviction, and all the machinery you only notice once you’re in trouble. But as a learning scaffold? It’s perfect.

## “WiredTiger-inspired” without the cargo cult

When people say “WiredTiger,” they often mean: fast, durable, concurrent, battle-tested, and full of sharp edges you only learn by living there.

What I mean is simpler:

- **Page-oriented storage** as the baseline abstraction.
- **B+tree** organization for primary/index access patterns.
- **Write-ahead logging and checkpoints** as the story for crash safety.
- A bias toward designs that separate “how you change data” from “how you read data.”

The point is not to recreate WiredTiger. The point is to steal the right *questions*.

If you’re reading this and thinking “this sounds like reinventing a wheel,” you’re right. But I’m not trying to avoid wheels. I’m trying to learn how spokes, rims, and axles fight each other when you hit a pothole.

## Learning-first rules (and why they matter)

I’m optimizing for understanding. That changes the trade-offs.

- I’ll pick a clean design over a fast design.
- I’ll pick an explicit invariant over a clever trick.
- I’ll accept “toy” constraints if they keep the next concept teachable.

The best sign that this is working is when I can answer: “why is it built like this?” without hand-waving. If I can’t, it means I moved too fast.

So expect refactors. Expect dead ends. Expect me to rename things once I finally understand what they are.

## How AI-driven development fits in here

This project is also a sandbox for **AI-assisted development**. Not “autopilot.” More like: a very patient pair programmer who doesn’t get tired when I ask the same basic question three different ways.

The workflow that seems to work for me looks like this:

1. **Thin slice first.** Pick the smallest feature that changes behavior in a visible way.
2. **Ask naive questions.** “If I had pages, what’s the minimum metadata I need?” “How do I test a B+tree split without building a whole DB?”
3. **Let the assistant propose options.** I want trade-offs, not commandments.
4. **I choose and implement.** The code should still feel like mine.
5. **I verify with tests and tiny experiments.** If I can’t test it, I probably don’t understand it yet.

The assistant is great at:

- sketching a design space quickly,
- pointing out common foot-guns (alignment, torn writes, off-by-one splits),
- turning “vague idea” into “list of structs + invariants + tests”.

And it’s also great at being wrong with confidence. So I treat it like a collaborator, not an oracle. If something feels too smooth, I slow down and read the code twice. Ok?

## What’s in scope (and what isn’t)

WrongoDB is intentionally small. I’m not trying to solve everything at once.

In scope:

- a minimal document/record model,
- basic CRUD semantics,
- a storage layer that becomes progressively more realistic,
- the data-structure work (B+tree, pages, free lists, etc.),
- crash-safety ideas once the basics are in place.

Out of scope (for now):

- distributed anything,
- complex query planning,
- full MongoDB compatibility,
- “it must be faster than X” benchmarks,
- rock-solid durability guarantees from day one.

If you want a production database, you already know where to look. If you want to watch one being *understood*, built, broken, and rebuilt in public… that’s what this is.

## The journey format

I’m planning to write this as a series. Each post should answer three questions:

1. What problem am I trying to solve *today*?
2. What’s the smallest thing that could work?
3. What did I learn once I tried it?

Sometimes the answer will be “I realized my abstraction was wrong.” That’s not failure. That’s the whole point.

## What’s next

Near-term, the direction is:

- Nail down a clear on-disk layout story: pages, headers, checksums (maybe), and how to evolve formats.
- Build a small B+tree that can survive inserts, splits, and basic iteration.
- Make writes less magical: introduce an explicit WAL and define what “durable” means in this toy world.
- Add checkpoints so startup doesn’t require replaying forever.
- Start measuring: even basic counters for reads/writes/splits are surprisingly enlightening.

Longer-term (still learning-first):

- Background maintenance (compaction-ish / reconciliation-ish ideas).
- Concurrency boundaries that are understandable and testable.
- A tiny “debugger view” of the file format (because I want to *see* the pages).

## If you’re following along

If you’re here to learn with me: welcome.
If you’re here to nitpick: also welcome (politely).

I’ll try to keep the code readable, the commits small, and the writeups honest. And when something breaks, I’ll try to explain the breakage instead of hiding it behind a “fixed stuff” commit message.

Ok, enough meta. Let’s go build something wrong until it becomes less wrong.

## Editing notes

- Add a short personal origin story (why I started this, and why *now*).
- Link to a few posts/resources that influenced the direction (WiredTiger papers/talks, database textbooks, etc.).
- Confirm the exact intended “MongoDB-like” surface area (collections? indexes? update operators?).
- Decide whether to include a tiny code snippet here (maybe a 10-line API sketch).
- Add a brief disclaimer about data loss and “do not use for real data”.
