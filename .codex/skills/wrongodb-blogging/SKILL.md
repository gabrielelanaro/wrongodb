---
name: wrongodb-blogging
description: Plan and write WrongoDB devlog posts in this repo. Use when asked to plan, outline, draft, or revise posts under blog/, generate blog images, or follow the series structure for WrongoDB. This skill uses blog/prompts/plan.md and blog/prompts/write.md as the canonical workflow and blog/generate_image.py for image generation.
---

# WrongoDB Blogging

## Overview
Create blog post plans and drafts for the WrongoDB series without re-reading or re-deriving the series structure. Use the existing prompts and the image generator script in this repo.

## Workflow (plan -> write -> images)

### 1) Planning a post
- Open `blog/prompts/plan.md` and follow it exactly.
- Before locking the topic, grab inspiration from recent work:
  - Scan git history beyond the last 20 commits and pinpoint the relevant change:
    - `git log --oneline --reverse --since="2025-12-01"` (widen/narrow dates as needed)
    - `git log --oneline -- src/blockfile.rs src/leaf_page.rs src/btree.rs docs/decisions.md` (file-focused)
    - `git log -S "BlockFile" -S "FULLFSYNC" -S "checkpoint" -S "slot" --oneline` (string-focused)
  - Cross-check `PLAN.md`, `docs/decisions.md`, `blog/SERIES.md`
  - Codex session logs for narrative hooks: `~/.codex/sessions` and `~/.codex/history.jsonl` (use `rg` for keywords like `blockfile`, `fs_usage`, `BTree`, `checkpoint`)
- Do not reread or re-discover prior posts; the prompt already encodes the structure and voice.
- Produce a plan with the required sections (Title + hook, scope, 7 beats, decisions, artifact, images/ASCII, verification).
- If details are uncertain, tag as **TO VERIFY**.
- Image planning lessons:
  - Each image prompt must state the **story purpose** (e.g., “show the durability boundary” or “map trace lines to meaning”), not just the subject.
  - Prefer narrative structures (before/after, timelines, mappings) over generic box-and-arrow diagrams.
  - Specify labels and icons that reinforce the story (e.g., crash bolt, shield, timeline bands).
  - After generation, validate files are real images (`file blog/images/*.png`); if invalid or dull, revise prompts and regenerate.
- Story/structure lessons:
  - After any significant change anywhere, re-check that the arc still flows.
  - Introduce new concepts inline before using jargon; keep definitions direct (no metaphors).
  - Use sections for readability; include explicit transitions or “lightbulb” moments where they help.
  - Diagrams must be narrated in the text and placed near the concept they illustrate.
  - Include a brief “how I ran the tool” line in the post when a tool is central; mention other options you did not use.
  - Keep series continuity: add a short recap/link to the previous post when relevant; ensure numbering/order is updated.

### 2) Writing a post
- Open `blog/prompts/write.md` and follow it exactly.
- Use the plan as the single source of truth; do not add new slices.
- Keep the voice candid, playful, learning-first, “wrong on purpose.”
- Keep the body tight (5–10 minute read).
- Include the images specified in the plan.

### 3) Generating images
- For each image prompt, run:
  `python blog/generate_image.py "<prompt>" --out blog/images/<filename>.png`
- Prefer sizes/aspects by intent:
  - Hero: `--aspect 16:9 --size 2K`
  - Diagrams: `--aspect 4:3 --size 1K` or `--aspect 1:1 --size 1K`
- If an image prompt is unclear, revise the prompt text first (do not guess).

## QA checklist
- Keep **TO VERIFY** tags until verified against code or notes.
- Confirm the plan matches the thin-slice scope.
- Ensure images were generated with `blog/generate_image.py` and filenames match the post markdown.
