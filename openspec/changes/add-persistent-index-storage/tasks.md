## 1. Implementation
- [ ] 1.1 Define persistent index metadata (per collection) and file naming scheme
- [ ] 1.2 Implement B+tree-backed secondary index storage (encode scalar key + record offset)
- [ ] 1.3 Load secondary indexes on open and fall back to rebuild when missing
- [ ] 1.4 Update insert/update/delete paths to maintain persistent secondary indexes
- [ ] 1.5 Wire equality queries to use persistent index lookups
- [ ] 1.6 Update createIndexes/listIndexes handlers to reflect persistent indexes

## 2. Tests
- [ ] 2.1 Create index on existing data and reopen to confirm persistence
- [ ] 2.2 Verify equality lookups use index and return correct results
- [ ] 2.3 Verify update/delete maintain index entries
