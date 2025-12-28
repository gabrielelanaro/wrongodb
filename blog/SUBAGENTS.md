# Sub-agent prompts (Codex CLI)

If I want Codex to draft a post without touching the repo, I run it in read-only mode and write the output to `/tmp`, then copy the text into `blog/<post-dir>/post.md` manually.

Examples (copy/paste):

## Post 05 — first B+tree (Slice D story)

```sh
codex -a never -s read-only -C /Users/gabrielelanaro/workspace/minimongo exec -o /tmp/wrongodb-05-first-btree.md <<'PROMPT'
Write ONLY the markdown content for a blog post. Do not write or modify any files.

Title: "From one page to a tree: my first B+tree split"

Voice:
- First person.
- Curious, slightly informal, short punchy sentences.
- Occasional “ok” / “so”. No heavy profanity.

Content:
- 5–10 minute read.
- Explain the goal of a 2-level B+tree (root + leaves) and why it's a good thin slice.
- Define "separator key" in plain language with a tiny diagram.
- Explain the split sequence at a high level (extract entries -> insert -> split -> write left/right -> update root).
- Mention constraints: height ≤ 2; root can fill; that's the handoff to the next slice.
- Include one ASCII diagram and one short pseudocode snippet (≤12 lines).
- End with **Editing notes** bullets.
PROMPT
```

## Post 06 — watching the OS write (`fs_usage`, `FULLFSYNC`)

```sh
codex -a never -s read-only -C /Users/gabrielelanaro/workspace/minimongo exec -o /tmp/wrongodb-06-watch-the-os.md <<'PROMPT'
Write ONLY the markdown content for a blog post. Do not write or modify any files.

Title: "I watched my database write to disk (and learned to fear `fsync`)"

Voice:
- First person.
- Curious, slightly informal.
- Occasional “ok” / “so”. Keep it professional.

Content:
- 5–10 minute read.
- Explain why you used `fs_usage` on macOS, and what you looked for.
- Discuss `FULLFSYNC` vs normal `fsync` at a conceptual level (no need to be exhaustive).
- Tie it back to your BlockFile design and the future WAL/checkpoint story.
- Include one "what surprised me" section and one "what I'd measure next" section.
- End with **Editing notes** bullets.
PROMPT
```
