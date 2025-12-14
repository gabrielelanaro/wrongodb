# When `seek()` feels like a lie: pages, sparse files, and why I started caring about corruption

I had a moment of mild panic while reading my own code.

It was basically:

> How does `file.seek(block_id * page_size)` work?
> Does it just… make the file huge for no reason?
> Do we write all the pages before that one?

If you’ve never built a “page file” before, this question hits hard. Because it *feels* like cheating.

And it’s also the moment where the project stops being “store some JSON” and starts being “ok, we’re doing storage now”.

This post is about the transition from an append-only log to a fixed-page **BlockFile**, and why it made me care about things like **checksums**, **corruption**, and **explicit allocation**.

## The problem: logs don’t want to be databases forever

Append-only logs are amazing until you need:

- random access by key
- updates/deletes without rewriting the whole world
- predictable performance as the dataset grows

So the next abstraction I wanted was: a file that I can treat like an array of fixed-size pages.

That gives me a stable unit of IO and a place to put “real” structures (like B+tree nodes).

## The BlockFile mental model

The model is simple:

- choose a page size (I started with 4KB)
- page `0` is special: it’s the **header**
- pages `1..N` are payload pages

And each page has:

- a checksum (CRC32)
- a payload (padded to page size)

ASCII time:

```
========================== one file ==========================+
| page 0 | page 1 | page 2 | page 3 | ... | page N            |
+--------+--------+--------+--------+-----+-------------------+

page 0 (header):
  [ CRC32 | magic | version | page_size | root_block_id | ... ]

page k (k > 0):
  [ CRC32 | payload bytes...                               ... ]
```

That’s not WiredTiger. It’s not even close. It’s just enough structure to stop pretending that “a file” is a magical blob.

## So what *does* `seek()` do? (and why it’s not actually magic)

When you do:

```
seek(block_id * page_size)
write(page_bytes)
```

you are telling the OS: “write these bytes at this offset”.

If the offset is past the current end of file, most filesystems will extend the file. But they don’t necessarily allocate all the intermediate data eagerly.

That’s where **sparse files** show up: the file *claims* to be large, but the “holes” don’t necessarily consume disk blocks until you actually write data there.

So the scary scenario (“it increases the file size like crazy for no reason”) is partially true:

- the **logical size** can jump
- but the **physical storage** can stay smaller (until written)

This matters because a storage engine can accidentally create sparse holes and think it “allocated” space when it didn’t. Or it can expose uninitialized bytes if you’re sloppy.

The only sane approach is: don’t let random callers pick arbitrary block ids.

Which leads to the next decision.

## The key design decision: don’t auto-allocate on write

I made (and wrote down) a rule:

`write_block(block_id, payload)` must **not** implicitly allocate.

If you want a new block, you call `allocate_block()` (or similar) and you get back an id that is known to exist.

This is documented in `docs/decisions.md`.

Why I like this rule:

- it prevents accidental sparse growth via “oops wrong id”
- it forces allocation to be explicit (which is required later for checkpointing / copy-on-write patterns)
- it creates a clean boundary between “block manager” and “data structure”

This boundary is one of those “boring” design choices that pays you back later.

## Checksums: the first time I admitted disks can lie

I also asked myself:

> Why do I need to worry about corruption? I never do this with JSON.

Same. For years I basically trusted the filesystem like a nice person.

But the moment you start treating a file as a structured database, the failure modes become part of the design:

- partial writes
- torn pages
- stale data after crashes
- bit flips

And the minimum “learning-safe” response is: add a checksum and verify it on read.

CRC32 isn’t cryptographic. It’s not “secure”. It’s just a cheap “this page is not what you think it is” alarm.

And honestly, just having that alarm changes how you debug. You stop guessing.

## Page size: why 4KB is a default (and why you might pick differently)

If you’ve ever asked “why fixed page size?”, you’re in good company. I asked the same thing.

4KB is a reasonable default because it matches common OS page sizes and aligns nicely with filesystem block sizes in many setups. It’s not magic. It’s a starting point.

In a learning project, the real reason to pick a size is:

- you want pages small enough to force splits in tests
- but not so small that everything is overhead

So: 4KB for default realism, and much smaller sizes in tests when I want to trigger interesting behavior quickly.

## The artifacts that keep me honest

Two files became “project infrastructure” for me:

- `PLAN.md`: the roadmap of slices (what’s implemented, what’s next)
- `docs/decisions.md`: the “why” behind file formats and invariants

This is part of the AI-driven dev loop too. It’s easy for an assistant (or me) to produce a plausible design. It’s harder to keep the design coherent over time.

Writing decisions down is how I stop future-me from rebuilding the same confusion.

## Editing notes

- Add an explicit note about sparse files on APFS (and a link/reference if I want to go deeper).
- Include one real “before/after” example of an accidental sparse file bug (if I have one).
- Mention where the header stores `root_block_id` and why “page 0 is special”.
- Add one paragraph on “verify on read” vs “verify on write” (and why I picked the simplest).

