---
name: refactor
description: Find opportunities for refactoring and applies refactorings.
---

The objective of refactoring is to reorganize the code so that future changes are easier. Things that make the code easier are:
- reducing coupling
- improving understandability
- remove unneeded abstraction
- simplify complexity when it doesn't serve a purpose

All these activities contribute to make the codebase easier to change.

Violations:

<violation>
  <id>R-0001</id>
  <title>layout and content semantic mismatch</title>
  <description>
  The content of the file (test or implementation) is misaligned with the layout. As the codebase evolve the agent may forget to rethink how to reorganize the file structure for better reusability.
  </description>
  <solution>
  Explore the high level layout to understand where to best move the file. The objective is understandability and reducing surprises. Which are due to the inconsistency. Rename the file when necessary and reposition it in a less surprising position.
  </solution>
</violation>

<violation>
  <id>R-0002</id>
  <title>constructor hides required initialization</title>
  <description>
  A public `new()` should not hide semantic initialization or policy decisions from the caller. If constructing the type requires choosing behavior-affecting defaults, configuration, startup policy, durability mode, or other meaningful semantics, those inputs should be explicit at the call site.
  </description>
  <solution>
  Make `new()` explicit about all non-derivable inputs. Prefer named constructors, builder methods, or `Default` for trivial zero-state objects, and keep policy choices visible where the type is constructed.
  </solution>
</violation>
