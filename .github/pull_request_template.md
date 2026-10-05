<!-- PROJECT_CONTEXT §11.6. Each row needs evidence (NN-8): a CI run, test report, benchmark
record or validation report. Write "not applicable" where a row does not apply. -->

## Summary

What this change does and which round it belongs to.

## Evidence

| Check | Evidence |
|---|---|
| Specification | Section reference; approved? |
| Statistical behavior | "No change" with R0 evidence (NUM-2), or a discrepancy ID with approval |
| Tests | Layers added or updated; failing-test sets before and after |
| Parity | Three-level parity results for affected features |
| Distribution | Layout-invariance results; any new driver materialization complies with DIST-1 |
| Compatibility | `engine` compiles against spark-sql-api only; API check passes; Connect suite status |
| Data protection | No real data; no row values in logs or messages |
| Performance | Benchmark comparison if a kernel, layout or reduction changed |
| Documentation | Specification, user guide, parity matrix, changelog |
| State files | STATUS, OPEN_ITEMS, DECISIONS, DISCREPANCIES, HANDOFF |
