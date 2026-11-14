# Performance phase: results

Two kinds of numbers are kept here, in separate tables:
- **Cluster measurements** are runs of `PerfJob` (`perf-runbook.md`) on the standard cluster, entered from their
  report lines.
- **Sandbox measurements** come from the assistant's one-core sandbox, and say nothing about wall time on a cluster.

Estimates and arithmetic are never entered here; they live in `docs/spec/performance/plan.md`.

## Cluster measurements

None yet. The scale runs reported on 2026-11-13 used round 81's build (OI-103), so they are not entered.

## Sandbox measurements

### Round 149: one-core profile by call site

This was measured with `local[1]`, the `02_scale.py` design, 200,000 rows, 320 providers and 100 features. The full
tables are in plan §2.5 and round 149's evidence log.

| Model | Fit, s | Passes | Their CPU, s | Validation, wall s | Validation, CPU s | Working-set build, wall s |
|---|---|---|---|---|---|---|
| Cox (3 iterations) | 31.5 | 4 evaluations, 15.7 s wall | 12.7 | 5.5 | 1.6 | 7.1 |
| Logistic (4 iterations) | 48.9 | 4 pass A and 4 pass B, 13.5 s wall | 9.3 | 17.3 | 6.8 | 6.0 |
