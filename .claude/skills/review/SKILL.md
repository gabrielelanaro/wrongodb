---
name: review
description: Review code or changes, and suggest improvements
---

# Review

## When to use this skill
Use this skill when you need to review changes and improve quality

## How to use

Review EACH individual file that is likely in scope to be reviewed (either uncommitted or in the PR, diff to main, depending on context, or the user specified file or directory)

Really focus on the review on what the user intend to get out of it.

At the end select the top 2-3 aspects that need to be addressed and a short suggestion. The user doesn't want to read a whole essay on the PR. Concise-focused suggestions are best. Like a skilled code reviewer would do.

Limit strictly to the following aspects and criteria:

### Single responsibility principle (SRP)

- Does the current file contains a single isolated functionality or is it a mix? Consider splitting it into multiple files if it's not the case.

- Does the current class contain a single isolated functionality or is it expanding into a god class? Think again on the overall purpose and evaluate how the responsibility can be split.

## Dependency injection (DI)

- Are we doing some weird initialization in the main constructor? Consider accepting dependencies directly by simplifying the main constructor and moving the initialization of its to a separate factory method. Or whether we need the factory method at all (maybe this is internally initialized class and we don't care)

### Naming

- Files: does each file has a clear name that reflects its purpose? Or does it look like a leftover or a hack?

### Layout

- Does the file belong to its location? Does the file belong to this module or specific directory? If not or it's weird consider renames, or repositioning the file in another place.

### Simplicity

- Are we having unnecessary indirection? If a function is just calling another function, consider inlining it.

## Consistency

- Are we duplicating logic that already exists elsewhere in the codebase? Search for similar patterns, helper methods, or services that we should be using instead.
- Are there other places in the codebase with similar situations where this same logic/fix should be applied? We don't want inconsistency.
- Check for opportunities to consolidate with existing utilities, concerns, or services.

### Comments

- Do comments explain the "why" and not the "what"? Comments should only be added in confusing parts and explain why this particular decision was made. If the code is clear, it should not need comments, so consider removing them.

### Meaningful, tight interfaces

- Are the interfaces defined meaningful and tight, are they a pleasure to use?
- Are they too broad or too narrow?
- Are they redundant?
- Are they all used outside Are there too many publid methods? 3–7 public methods is a sweet spot.
- Does it follow ISP, the interface segregation principle?