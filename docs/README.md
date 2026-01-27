# Design Documentation

This directory contains design documents and architectural decisions for the minimongo project.

## Documents

### Recovery and WAL

- **`wal_recovery_implementation.md`** - Logical WAL recovery implementation summary
  - Date: 2026-01-27
  - Describes logical WAL record types, replay flow, and recovery behavior
- **`wal_recovery_comparison.md`** - WiredTiger vs minimongo logical WAL comparison
  - Shows why logical replay avoids page-id drift and split logging
- **`wal_checkpoint_lsn_bug.md`** - Checkpoint LSN bug analysis
  - Date: 2025-01-27
  - Explains the off-by-record bug and expected fix
- **`recovery_cow_fix.md`** - WAL Recovery: COW Bypass (deprecated)
  - Superseded by logical WAL replay; kept for historical context

### Other Design Documents

- **`decisions.md`** - Project design decisions and architectural choices
- **`fs_usage_notes.md`** - Notes on file system usage patterns
- **`server.md`** - Server implementation details

## Document Format

Design documents should follow this structure:

1. **Header**: Title, date, status
2. **Problem**: What issue are we solving?
3. **Solution**: How did we solve it?
4. **Implementation**: Code changes and file locations
5. **Results**: Test results and performance impact
6. **Lessons Learned**: What did we learn?
7. **References**: Related documents and source code

## Adding New Documents

When adding a new design document:

1. Use descriptive filenames with underscores: `feature_description.md`
2. Include the date in the header
3. Cross-reference related documents
4. Update this README to include the new document

## Purpose

These documents serve to:
- **Explain why** certain decisions were made
- **Document trade-offs** and alternatives considered
- **Preserve knowledge** for future maintainers
- **Provide context** for code changes
- **Track evolution** of the codebase
