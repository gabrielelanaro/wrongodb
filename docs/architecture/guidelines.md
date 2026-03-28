# Architecture Doc Guidelines

These rules are meant to keep the architecture docs useful as WrongoDB evolves.

## 1. Keep one maintained "current state" path

The maintained architecture source of truth is:

- `docs/architecture/`
- `docs/decisions.md`

Everything else is secondary:

- `NOTES.md` for investigations
- `notebooks/` for research
- `blog/` for historical narrative

Do not rely on README prose, blog posts, or notebook notes as the canonical description of today's architecture.

## 2. Update docs in the same change as the code

Any change that does one of these should update the relevant architecture doc in the same PR:

- moves a major responsibility across module boundaries
- adds or removes a persistent metadata source
- changes transaction, checkpoint, or recovery semantics
- changes how collections or indexes are represented
- introduces a new top-level subsystem

If the change is non-trivial, add a dated entry to `docs/decisions.md` as well.

## 3. Document boundaries, not every implementation detail

Architecture docs should answer:

- what this layer owns
- what it explicitly does not own
- which files and types are the entry points
- which invariants other code should rely on

They should not duplicate low-level code walkthroughs that are already obvious from the implementation.

## 4. Prefer current behavior over proposals

Do not leave proposal docs mixed with current docs unless they are explicitly marked as historical or draft material.

If a proposal becomes real:

- fold the final result into `docs/architecture/`
- record the rationale in `docs/decisions.md`
- remove or clearly demote the proposal from the main doc entry points

## 5. Keep docs aligned with the real module tree

When modules are moved or removed:

- update file paths in docs immediately
- update README summaries immediately
- remove references to deleted docs instead of leaving dead links behind

The architecture docs should make the current source tree easier to navigate, not preserve old names.

## 6. Separate "what" from "why"

Use:

- `docs/architecture/` for what exists and how it is organized
- `docs/decisions.md` for why a non-trivial choice was made

That separation makes it easier to evolve the structure without turning overview docs into a decision log.

## 7. Mark historical material explicitly

If older material remains useful, keep it, but make its status obvious:

- historical
- investigation
- benchmark note
- external reference

Do not let readers guess whether a document describes the current implementation.
