# Blog Post Writing Prompt (WrongoDB series)

Use this prompt to write a full post **from an existing outline** created with `blog/prompts/plan.md`.
**Do not reread or re-discover the existing posts.** The structure and voice are already known and summarized here.

---

## Inputs
You will be given a plan that follows the 7-beat outline and includes:
- Title + One-line hook
- Thin-slice scope
- Outline beats
- Key decisions
- Concrete artifact
- Images & ASCII
- Verification checklist

## Goal
Produce a complete markdown post that follows the plan exactly, in the established voice and structure, and is ready to drop into `blog/NN-title.md`.

## Known series DNA (do NOT re-derive)
- Voice: candid, playful, learning-first, “wrong on purpose.”
- Structure rhythm: hook → context → mental model → one key decision → concrete artifact → why it matters → what’s next.
- Teaching moves: rhetorical questions, crisp definitions, zoom from concept to bytes, explicit layer separation.
- Visual rhythm: 2–4 images per post + optional ASCII diagrams.
- Scope: one slice only, no roadmap dumps.

## Writing rules
- Follow the plan’s 7 beats in order.
- Do not add new topics or extra slices.
- Include 2–4 inline images as markdown: `![Alt](images/<filename>.png)`.
- Generate images by running `python blog/generate_image.py "<prompt>" --out blog/images/<filename>.png`.
- If an image looks generic or confusing, revise the prompt to emphasize **purpose + narrative structure** and regenerate.
- After any significant change anywhere, re-check flow.
- Introduce new concepts inline before jargon appears; keep definitions direct (no metaphors).
- If the plan includes an ASCII diagram, include it in a fenced code block.
- If a fact is uncertain, mark it inline as **TO VERIFY** and keep going.
- Do not invent code details; only reference file paths or structs if the plan says they exist.
- Use plain ASCII only.
- Avoid markdown links unless the plan explicitly provides them.

## Output format
Return a single markdown document with:
1) `# Title`
2) Optional hero image (if the plan calls for it)
3) Body sections in a natural flow (no numbered headings required)
4) “What’s next” as a bullet list

## Style constraints
- Tight paragraphs (3–6 lines each)
- Occasional rhetorical questions
- Definitions in short, punchy sentences
- Avoid marketing language

## Post template (skeleton)

# <Title>

![<Hero Alt>](images/<hero_filename>.png)

<Hook paragraph>

<Context / why this exists>

<Mental model + image + optional ASCII>

<Key decision + trade-off>

<Concrete artifact explanation>

<Why it matters>

## What’s next
- <bullet>
- <bullet>
- <bullet>

---

## After writing
- Ensure all **TO VERIFY** flags are still present (do not resolve them).
- Ensure all images mentioned in the plan are included.
- Ensure each image was generated with `blog/generate_image.py` and matches the prompt.
- Validate image files are real images (not empty or corrupt); regenerate if needed.
- Ensure the post matches the thin-slice scope.
- Ensure diagrams are introduced in the text and placed near the concept they illustrate.
