## Context
We want `blog/generate_image.py` to run a self-contained agentic loop that drafts prompts and critiques generated images to improve story effectiveness and visual quality.

## Goals / Non-Goals
- Goals:
  - Add an opt-in agentic loop with a clear iteration cap.
  - Use structured critique output to drive revisions.
  - Use gemini-3-pro-image-preview for draft, critique, and image generation.
- Non-Goals:
  - No persistent storage of critique history beyond console output (except a sidecar summary file).

## Decisions
- Decision: Add `--agentic` and `--iterations` flags.
- Decision: Use a structured JSON critique response to decide pass/fail and produce a revised prompt.
  - Rationale: Stable parsing and easier automation vs free-form text.
- Decision: Emit a sidecar JSON file in agentic mode with prompts and critique summaries.
  - Rationale: Preserve the iteration trail for later tuning without extra manual logging.

## Flow (proposed)

seed prompt
  ↓
DRAFT PROMPT
  ↓
IMAGE GENERATION
  ↓
CRITIQUE (multimodal model)
  ↓
PASS? ── yes → write image + print summary + sidecar
  └─ no → revise prompt → repeat (cap)

## Critique schema (draft)
```
{
  "pass": true|false,
  "story_effectiveness": {"pass": bool, "notes": "..."},
  "visual_consistency": {"pass": bool, "notes": "..."},
  "pleasing": {"pass": bool, "notes": "..."},
  "revision_prompt": "..."
}
```

## Open Questions
- None.
