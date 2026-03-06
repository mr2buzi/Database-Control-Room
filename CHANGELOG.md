# Changelog

## v1.3.0

- Added single-column `UPDATE table SET col = literal [WHERE ...]`
- Reused indexed equality and range planning for update candidate selection
- Added pager savepoints so failed updates do not leave partial dirty state inside open transactions
- Added workbench presets and trace coverage for update flows
- Expanded engine tests for update parsing, planning, persistence, rollback, and failure recovery

## v1.2.0

- Added single-bound range predicates: `>`, `>=`, `<`, `<=`
- Added `IndexRangeScan` planning and ordered B+ tree leaf traversal
- Updated the workbench to surface range-scan presets and plan traces
- Expanded engine tests for range parsing, planning, bounds, ordering, and `LIMIT`

## v1.0.0

- Initial public release of the Rust engine and Electron workbench
