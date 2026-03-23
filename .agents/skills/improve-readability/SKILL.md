---
name: improve-readability
description: Improve readability of some code
---

Improve the code for readability, optimize for clear English and a short mental path through the module.

<improvement id="clear-story" applies-to="module">
Make the module tell one clear story from top to bottom.
</improvement>
<improvement id="clean-code-layout" applies-to="module,class">
Put the main public operations first, then the supporting details below.
</improvement>
<improvement id="intent-revealing-names" applies-to="function,method,class,filename,directory,module">
Use simple names that describe intent, not plumbing.
</improvement>
<improvement id="remove-empty-indirection" applies-to="function,method,class,module">
Inline or merge helpers that only forward calls and add no meaning.
</improvement>
<improvement id="consistent-terms" applies-to="module,class,function,method,filename,directory">
Use one word for one concept consistently across the file.
</improvement>
<improvement id="keep-meaningful-helpers" applies-to="function,method,module,class">
Keep helpers that name an important domain concept or invariant.
</improvement>
<improvement id="tight-interfaces" applies-to="class,module">
Prefer small public interfaces with explicit names over generic ones.
</improvement>
<improvement id="fully-documented-interfaces" applies-to="class,function,method,module">
If an interface is used by other modules, fully document it. Document the type or module itself, all functions and methods that form the interface, and all fields on structs that are part of that interface.
</improvement>
<improvement id="explain-behavior-and-constraints" applies-to="module,function,method,class">
Write comments to explain behavior and important constraints.
</improvement>
<improvement id="keep-why-comments" applies-to="module,function,method,class">
Remove comments that defend the code structure instead of clarifying it. But keep comments about the "why" this is invaluable context that cannot be recovered later.
</improvement>
<improvement id="reduce-mixed-responsibilities" applies-to="class,module">
If a type is doing too many jobs, call that out and reduce the mixing when possible.
</improvement>
<improvement id="align-module-name-and-content" applies-to="module,filename,directory">
Look out for a drift between the directory layout + file name and the content. The name of the module should represent what is inside.
</improvement>
<improvement id="proper-file-positioning" applies-to="module,filename,directory">
Review whether the file belongs in its current module and directory at all. Inspect the current module and its parent module, then consider whether the file should stay where it is, move to a sibling module, move up a level, or be split so the layout matches the responsibilities.
</improvement>
<improvement id="keep-mod-rs-thin" applies-to="filename,module">
Do not put logic in mod.rs, use that just to organize the exports.
</improvement>

<improvement id="tighten-visibility" applies-to="class,function,method,module">
Use the minimum visibility required. Search for external usages of each type/function, then set visibility to the narrowest scope that covers all callers. A type cannot be less visible than the interfaces that expose it.
</improvement>

The outcome is simple, easy to grasp code, where the excess has been removed, terminology is consistent, and the intent is obvious.

Identify the list of module, classes, functions, filenames, directory analize each one of them individually for the improvements that applies to them.

Review the directory layout of the file's current module and its parent module before making changes. Be explicit about whether the file still belongs in the current module or should be repositioned somewhere else.

Work thorugh this checklist and summarize for each item the improvement you intend to make (or None, but be explicit with each one of these.)

Example summary:

- <improvement-id>:
    classes:
       <class-1>: ok
       <class-2>: improve ... so that ...
       ...
    methods:
       <meth-1>: ...
    modules:
       <mod-1>: ...
    functions:
       <func-1>: ...
    filenames:
       <fn-1>: ...
    directories:
       <dir-1>: ...

Only after this careful analysis, proceed with the implementation.
