# Blog Post Planning Prompt (WrongoDB series)

Use this prompt to plan a new post in the WrongoDB devlog series.

---

## Goal
Produce a tight, publish-ready plan for a single new post (5–10 minute read) that advances the series by **one thin slice**.

## Known series DNA (do NOT re-derive)
- Voice: candid, playful, learning-first
- Structure rhythm: hook → context → mental model → one key decision → concrete artifact → why it matters → what’s next.
- Teaching moves: rhetorical questions, crisp definitions, zoom from concept to bytes, explicit layer separation.
- Visual rhythm: 2–4 images per post + optional ASCII diagrams.
- Scope: one slice only, no roadmap dumps.

## Planning rules
- Plan the next post using the structure above
- Pick **one** core concept and **one** key decision to explain, if not provided by the user.
- Include **one** concrete artifact to anchor the explanation (code, struct, layout, file header, algorithm step, etc.).
- Prefer examples that can be verified against the repo if needed.
- If details are uncertain, mark as **TO VERIFY** (do not invent).

## Output format
Return a plan with the following sections, in order:

### 1) Title + One-line hook
- Proposed title
- A single-sentence hook that sounds like the existing voice

### 2) Thin-slice scope
- One sentence: “This post explains … and stops before …”

### 3) Outline (7 beats)
Use exactly these beats:
1. Hook
2. Context / Why this exists
3. Mental model (diagram candidate)
4. Key decision (trade-off + rationale)
5. Concrete artifact (code/struct/layout)
6. Why it matters (behavior + invariants)
7. What’s next (2–3 bullets)

### 4) Key decisions
- Decision: …
- Alternatives considered: … (2–3 options max)
- Trade-offs: …

### 5) Concrete artifact
- Name the artifact and where it lives (file path or conceptual object)
- Bullet list of the 2–4 elements you will show or explain

### 6) Images
- 2–4 image prompts (short, literal) and each must include:
  - The **purpose** of the image (what it teaches or clarifies)
  - Any required labels or visual metaphors (e.g., crash bolt, shield, before/after split)
  - A hint about structure (timeline, mapping, split panel) if important

### 7) Verification checklist
- 3–6 bullets of facts to verify against code/notes
- If anything is speculative, tag it **TO VERIFY**

---

## Style constraints for the plan
- Keep each section short; avoid narrative prose.

## Example: acceptable brevity (mini)
- “Key decision: explicit allocation vs implicit append; trade-off: clarity vs convenience.”
