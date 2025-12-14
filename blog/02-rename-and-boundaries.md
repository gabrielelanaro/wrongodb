# Naming it WrongoDB: the power of a deliberately wrong project

I renamed the project to **WrongoDB** and, weirdly, it made everything easier.

Not because the name is clever (it’s not). But because it’s a promise:

This thing is going to be wrong for a while. On purpose. And that’s fine.

## Why naming matters more than it should

When a project is called something like “minimongo” or “mongodb-clone”, you immediately inherit expectations:

- “Will it support MongoDB drivers?”
- “Is it wire-compatible?”
- “Does it do replication?”
- “Is it safe?”

And you can answer those questions. But you’ll answer them constantly. And you’ll start designing to satisfy imaginary users instead of designing to learn.

The WrongoDB name is me writing a giant disclaimer across the repo:

> this is a learning project
> and it’s going to cut corners loudly

That disclaimer doesn’t lower standards. It changes the standards.

The metric stops being “feature completeness” and becomes “clarity per line of code”.

## The boundary I needed: “Mongo-ish behavior” vs “Mongo clone”

At some point I said out loud: “I want to build a clone here.”

And then I had to immediately ask: “clone of what?”

There are at least two different projects hiding behind that word:

1) **Surface clone**: the API feels like MongoDB.
2) **Engine clone**: the storage internals resemble a real engine (WiredTiger-like ideas).

Trying to do both early is a guaranteed way to do neither.

So I’m choosing a sequence:

- First: a tiny DB with Mongo-ish behavior (documents, basic filters)
- Then: evolve the storage underneath toward a simplified WiredTiger-inspired engine
- Maybe later: widen the API surface

This ordering matters because a storage engine without a workload is just a data structure zoo.

## Guardrails (aka “how I keep this fun”)

If I’m publishing a series, I need rules. Otherwise every post becomes “and then I implemented… everything”.

Here are the guardrails I keep repeating to myself:

- **Single-process** first. No networking. No drivers.
- **Readable invariants** beat micro-optimizations.
- **Small commits** that correspond to one conceptual change.
- **Thin slices** that change behavior in a testable way.
- **Write it down** when I make a real decision (see `docs/decisions.md`).

That last one is big. If I can’t explain a persistence or file-format decision in plain language, it’s probably a bad decision (or at least premature).

## How constraints make AI help *better*

Here’s something I didn’t expect: constraints don’t just help *me* stay focused.

They help the assistant be useful.

If I ask:

> “How do I make this more like Mongo?”

The answer is either a 10,000-foot overview or a multi-month backlog.

But if I ask:

> “Given single-process and no networking, what is the smallest storage slice that makes the next slice possible?”

Now we’re talking.

The best AI-driven dev work I’ve had on this project looks like:

- the assistant proposes a few options with trade-offs
- I pick one that matches the guardrails
- I commit a thin slice and write down the decision

It’s not autopilot. It’s iterative prompting with a clear target.

## The psychological trick: “wrong” is a feature

When you’re learning, you need to be allowed to do bad versions of things.

WrongoDB is basically my permission slip to:

- implement the simplest allocator (even if it’s not what real engines do)
- pick an absurdly small page size in tests to force splits
- write a B+tree that’s intentionally limited (height ≤ 2) just to learn splits

If I named it “MongoClone9000”, every corner cut would feel like failure.

But I’m not failing. I’m sequencing.

Ok, so. That’s the name.

Now the actual work is to keep earning it by being honest in the writeups and strict about the slices.

## Editing notes

- Add the exact date/commit where I did the rename (and maybe why the earlier name was bugging me).
- Add a short “what this project is NOT” paragraph with bullet points.
- Decide how much to say about “WiredTiger-inspired” this early (some readers will interpret it too literally).
- Include one concrete example of scope creep I avoided because of the guardrails.

