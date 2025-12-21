<!-- OPENSPEC:START -->
# OpenSpec Instructions

These instructions are for AI assistants working in this project.

Always open `@/openspec/AGENTS.md` when the request:
- Mentions planning or proposals (words like proposal, spec, change, plan)
- Introduces new capabilities, breaking changes, architecture shifts, or big performance/security work
- Sounds ambiguous and you need the authoritative spec before coding

Use `@/openspec/AGENTS.md` to learn:
- How to create and apply change proposals
- Spec format and conventions
- Project structure and guidelines

Keep this managed block so 'openspec update' can refresh the instructions.

<!-- OPENSPEC:END -->

# Repo agent guidelines

## Decisions log
- When making a non-trivial design/behavior decision (API semantics, file formats, invariants), record it in `docs/decisions.md`.
- Add an entry even if the change is “small” but affects persistence, crash-safety, or public APIs.

## Communication style
- Use normal paragraph style for explanations and reviews unless asked otherwise.
