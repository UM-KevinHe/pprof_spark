# CLAUDE.md — pprof_spark

Read docs/PROJECT_CONTEXT.md before any non-trivial change. It is normative: RFC 2119 keywords
and rule IDs such as NN-4 or DIST-6. Precedence: approved specs (docs/spec/) and DECISIONS.md,
then the pinned reference (reference/REFERENCE.lock), then PROJECT_CONTEXT.md, then code.

## Commands
- `sbt ci`: formatting check, compile, tests (layers T1–T3). `sbt scalafmtAll scalafmtSbt` formats.
- `sbt -Dpprof.linkageCheck=true compile`: compile main code against the runtime's scala-library.
- `bash scripts/check-engine-api.sh [--self-test]`: engine API rules.

## Rules that bite most often
- Evidence only (NN-8): never claim that something compiles, passes, matches, or is faster
  without the run that shows it; say which environment produced the evidence.
- `engine` uses only the shared Classic/Connect API: no `sparkContext`, `rdd`, `checkpoint`,
  `localCheckpoint`, `cache()`, Spark ML, or `org.apache.spark.sql.classic`. The compile
  classpath does not catch all of these (spark-connect-shims); the API check does.
- Kernels are pure top-level functions: `StrictMath` for transcendental functions, no
  iteration over hash-based collections, no per-row allocation, closures capture no `this`.
- Patient-level rows never reach the driver (NN-3); only size-guarded `collect…`/`toLocal…`
  methods materialize anything there.
- Every behavioral difference from pprof_py gets a discrepancy class (§3.5) in DISCREPANCIES.md.
- Statistical and design decisions are proposed, never made: record them in DECISIONS.md.
- No real data anywhere (NN-7): synthetic generators only; no row values in logs or messages.

## Modules
numerics (pure Scala) → engine (spark-sql-api) → ml (spark-mllib, Classic) and app (job runners);
testkit (test harness) and bench are never published.

## End of every round
Update STATUS.md, OPEN_ITEMS.md, DECISIONS.md, DISCREPANCIES.md and HANDOFF.md.
