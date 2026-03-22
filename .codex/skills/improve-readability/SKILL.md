---
name: improve-readability
description: Improve readability of some code
---

Improve the code for readability, optimize for clear English and a short mental path through the module.

- Make the module tell one clear story from top to bottom.
- Put the main public operations first, then the supporting details below.
- Use simple names that describe intent, not plumbing.
- Use one word for one concept consistently across the file.
- Inline or merge helpers that only forward calls and add no meaning.
- Keep helpers that name an important domain concept or invariant.
- Prefer small public interfaces with explicit names over generic ones.
- Write comments to explain behavior and important constraints.
- Remove comments that defend the code structure instead of clarifying it.
- If a type is doing too many jobs, call that out and reduce the mixing when possible.

The outcome is simple, easy to grasp code, where the excess has been removed, terminology is consistent, and the intent is obvious.