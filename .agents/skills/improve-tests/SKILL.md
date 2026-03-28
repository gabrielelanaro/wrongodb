---
name: improve-tests
description: Review s tests, find structural and quality violations, and recommend concrete improvements to test placement, coverage, reliability, and layout.
---

Review the current change (infer scope from context) test strategy and test layout. Find violations, explain why they matter, and recommend the smallest clear improvement that would fix each one.

Optimize for test suites that are easy to trust, easy to run, and easy to extend.

<improvement id="correct-test-bucket" applies-to="module,function,method,filename,directory">
Put each test in the right Rust bucket: unit tests in `src/`, integration tests in `tests/`, doctests in public docs, benchmarks in `benches/`, and manual exploration in `examples/`.
</improvement>
<improvement id="wire-all-tests" applies-to="module,filename,directory">
Check that every intended integration test is actually compiled and run. In Rust, files under `tests/` subdirectories do nothing unless a top-level `tests/*.rs` crate includes them.
</improvement>
<improvement id="test-observable-behavior" applies-to="function,method,module,class">
Prefer assertions on externally visible behavior over assertions on incidental implementation details.
</improvement>
<improvement id="keep-unit-tests-local" applies-to="function,method,module">
Keep fast, focused, deterministic tests close to the code they exercise, especially for pure logic, encoding, parsing, validation, and edge cases.
</improvement>
<improvement id="exercise-public-api-integration" applies-to="module,class,function,method">
Use integration tests to validate multi-module flows only through the public API.
</improvement>
<improvement id="cover-failures-and-edges" applies-to="function,method,module,class">
Check not only the happy path, but also boundary conditions, invalid input, persistence/recovery edges, and failure semantics.
</improvement>
<improvement id="make-tests-deterministic" applies-to="function,method,module,class">
Avoid sleeps, timing assumptions, random data without fixed seeds, global mutable state, and order-sensitive assertions when a deterministic setup is possible.
</improvement>
<improvement id="isolate-fixtures" applies-to="function,method,module,directory">
Each test should own its state and clean up after itself. Shared helpers should reduce noise, not couple tests together.
</improvement>
<improvement id="use-clear-test-names" applies-to="function,method">
Name tests after the behavior and scenario being checked so failures are self-explanatory.
</improvement>
<improvement id="avoid-brittle-source-inspection" applies-to="function,method,module">
Prefer compile-time or runtime behavioral checks over tests that grep source files for strings, unless the source text itself is the contract being enforced.
</improvement>
<improvement id="keep-examples-and-benchmarks-honest" applies-to="filename,module,directory">
Examples should demonstrate usage, benchmarks should measure performance, and neither should be silently standing in for missing correctness tests.
</improvement>
<improvement id="use-doctests-for-public-api" applies-to="function,method,class,module">
For public interfaces, add concise doctests when a small executable example would strengthen the contract and documentation.
</improvement>
<improvement id="keep-test-helpers-thin" applies-to="module,function,method">
Test helpers should hide boilerplate, not hide the scenario. If the helper makes the assertion or behavior hard to see, call that out.
</improvement>
<improvement id="right-size-fixture-scope" applies-to="module,directory">
Promote repeated setup into shared helpers only when duplication obscures intent. Do not centralize test setup so aggressively that the test story becomes opaque.
</improvement>
<improvement id="align-suite-layout-and-domain" applies-to="module,filename,directory">
The test directory structure should mirror the product's domains and public surfaces well enough that missing coverage is obvious.
</improvement>
<improvement id="check-runner-ergonomics" applies-to="directory,module,filename">
Make it easy to run focused subsets of the suite. Call out confusing suite entrypoints, hidden tests, or layouts that fight Cargo's model.
</improvement>

Follow this workflow:

1. Map the repository's test buckets:
   - `src/**` unit tests
   - `tests/**` integration tests
   - doctests in public docs
   - `benches/**`
   - `examples/**`
2. Inspect how Cargo will actually run the suite.
3. Verify that every intended integration test is wired into a top-level `tests/*.rs` crate.
4. Identify the main modules, suites, helper modules, filenames, and directories involved in testing.
5. For each improvement above, analyze the applicable items individually.
6. Report violations first, then recommend concrete fixes.
7. Only implement changes after the analysis if the user explicitly asks for code changes.

Useful commands:

- `rg -n "#\\[cfg\\(test\\)\\]|#\\[test\\]" src tests`
- `rg --files tests src benches examples`
- `cargo test -- --list`
- `cargo test --test <suite> -- --list`
- `rg -n '```' src`
- `sed -n '1,200p' Cargo.toml`

When reviewing a Rust application, pay special attention to:

- hidden or unwired integration tests
- tests that assert on source text instead of behavior
- examples named like tests but not executed by `cargo test`
- persistence, recovery, and crash-safety scenarios that belong in integration tests
- public APIs with no executable examples
- sleeps or timing-based synchronization that could make tests flaky
- missing negative tests and edge-case coverage
- helpers that make the test scenario hard to read

Work through this checklist and summarize, for each improvement, what you found and what should change. Be explicit even when the answer is `ok` or `none`.

Example summary:

- <improvement-id>:
    modules:
       <module-1>: ok
       <module-2>: improve ... because ...
    functions:
       <test-fn-1>: improve ... so that ...
    filenames:
       <file-1>: move to `tests/` because ...
    directories:
       <dir-1>: add suite entrypoint or `mod` wiring ...

Present findings ordered by severity:

1. tests that are not running or are in the wrong bucket
2. brittle or misleading tests
3. important missing behavioral coverage
4. layout and ergonomics issues

Keep the recommendations concrete. Prefer "move this test", "wire this module", "replace this assertion with a behavioral check", or "add these two missing scenarios" over generic advice.
