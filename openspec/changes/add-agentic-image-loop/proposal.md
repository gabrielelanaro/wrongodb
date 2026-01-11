# Change: Agentic loop for blog image generation

## Why
Single-shot prompts are producing inconsistent diagrams; we need a loop that drafts, critiques, and refines images for story clarity and visual quality.

## What Changes
- Add an optional agentic loop to `blog/generate_image.py` that drafts prompts, critiques images, and iterates with a cap.
- Define critique rubric output (story effectiveness, visual consistency, pleasing aesthetics) and use it to refine prompts.
- Emit a sidecar JSON file in agentic mode with prompts and critique summaries.
- Update the `wrongodb-blogging` skill to document the loop and new CLI flags.

## Impact
- Affected specs: blog-image-generation (new)
- Affected code: blog/generate_image.py, .codex/skills/wrongodb-blogging/SKILL.md
