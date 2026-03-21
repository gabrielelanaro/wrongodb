---
name: sw-design-review
description: Review software design for violations, must be called explicitly.
---

The objective of software design review is to evaluate the code so that future changes are easier. Things that make the code easier are:
- reducing coupling
- improving understandability
- remove unneeded abstraction
- simplify complexity when it doesn't serve a purpose

Refactoring is one of the main tools used to address these issues. But this skill is only about review.

All these activities contribute to make the codebase easier to change.

Evaluate the following violations individually

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

<violation>
  <id>R-0003</id>
  <title>leaky abstraction</title>
  <description>
  A component is forced to understand another component's internal representation in order to do its job. This often appears as callers inspecting filenames, paths, naming conventions, internal ids, storage layout, or other incidental structure because the owning abstraction does not expose the semantic operation they actually need.
  </description>
  <solution>
  Move the behavior behind the abstraction that owns the knowledge. Expose a semantic API for the real capability instead of leaking representation details upward. If the abstraction cannot answer the question because the underlying state is missing, promote that state to a first-class concept owned and maintained by the correct layer.
  </solution>
</violation>

<violation>
  <id>R-0004</id>
  <title>interface lacks semantic cohesion</title>
  <description>
  A public type exposes methods that do not feel like one coherent capability, which is a sign that the interface is grouping together responsibilities that belong to different abstractions. Callers then have to infer intent from the exposed surface instead of relying on a clear, harmonious API.
  </description>
  <solution>
  Reconsider the boundary of the type and group operations by the semantic concept they serve, not by convenience of implementation. Move outlier methods to the abstraction that naturally owns them, or introduce a higher-level facade whose methods all speak the same language.
  </solution>
</violation>

<violation>
  <id>R-0005</id>
  <title>foreign concepts in tests</title>
  <description>
  A test pulls in higher-level concepts, context objects, or unrelated collaborators that are outside the intended scope of the module under test. This widens the test surface, obscures the behavior being exercised, and makes the tests more coupled than the design boundary itself.
  </description>
  <solution>
  Tighten the tests to the current module. Exercise the public interface of the module and its intended usage and behavior. Build fixtures that assemble only the minimum collaborators needed to run the tests, and remove foreign concepts from the setup. Initialize the fixtures directly from its immediate collaborators.
  </solution>
</violation>

<violation>
  <id>R-0006</id>
  <title>module tests semantic mismatch</title>
  <description>
  A module's test block primarily drives the behavior of a different component than the one the module owns. The tests may use the current module as plumbing, but the assertions are really about another abstraction's behavior. This blurs ownership, hides where behavior is actually specified, and makes test organization drift away from the code's design boundaries.
  </description>
  <solution>
  Search for tests whose assertions directly exercise another owned component and move them to the appropriate module. The appropriate module is the one that owns the components being tested directly. Keep each module's tests focused on driving the behavior of that module's public interface and intended responsibilities.
  </solution>
</violation>
