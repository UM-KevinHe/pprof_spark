# CLAUDE.md — pprof_spark

Read docs/PROJECT_CONTEXT.md (v2.2, which folds in the Phase 0 decisions) before any non-trivial
change. It is normative: RFC 2119 keywords and rule IDs such as NN-4 or DIST-6. No Databricks
deployment work (D-14): pprof_spark is a standalone Apache Spark package. Precedence: approved specs (docs/spec/) and DECISIONS.md,
then the pinned reference (reference/REFERENCE.lock), then PROJECT_CONTEXT.md, then code.

## Commands
- `sbt ci`: formatting check, compile, tests (layers T1–T3). `sbt scalafmtAll scalafmtSbt` formats.
- `sbt -Dpprof.linkageCheck=true compile`: compile main code against the runtime's scala-library.
- `bash scripts/check-engine-api.sh [--self-test]`: engine API rules.
- `sbt "bench/runMain pprof.spark.bench.DeterminismCost"`: cost of deterministic mode (ADR-0006).

## Reference fixtures
`fixtures/` is generated, never edited by hand: `python reference/fixtures/generate.py` (fixture
environment: reference/fixtures/requirements.txt, pprof_py at the pinned commit, R 4.3.3 with
survival 3.5-8), then `python reference/fixtures/calibrate.py`. `FixturesSuite` checks checksums,
the pin and the calibration in CI. Doubles in fixtures are hexadecimal floating-point strings.

## Rules that bite most often
- Evidence only (NN-8): never claim that something compiles, passes, matches, or is faster
  without the run that shows it; say which environment produced the evidence.
- `engine` uses only the shared Classic/Connect API: no `sparkContext`, `rdd`, `checkpoint`,
  `localCheckpoint`, `cache()`, Spark ML, or `org.apache.spark.sql.classic`. The compile
  classpath does not catch all of these (spark-connect-shims); the API check does.
- Kernels are pure top-level functions: `StrictMath` for transcendental functions (the source
  check enforces it), no iteration over hash-based collections, no per-row allocation, closures
  capture no `this`. Sums go through `pprof.spark.numerics.Summation` (ADR-0003).
- Classic test classpaths never contain spark-connect-shims; build.sbt filters them (OI-21).
- Patient-level rows never reach the driver (NN-3); only size-guarded `collect…`/`toLocal…`
  methods materialize anything there.
- Every behavioral difference from pprof_py gets a discrepancy class (§3.5) in DISCREPANCIES.md.
- Statistical and design decisions are proposed, never made: record them in DECISIONS.md.
- No real data anywhere (NN-7): synthetic generators only; no row values in logs or messages.

## Modules
numerics (pure Scala) → engine (spark-sql-api) → ml (spark-mllib, Classic) and app (job runners);
testkit (test harness) and bench are never published. Inside engine: data (input contract), layout
(plans), backend (blocks, working set, ordered reduction) and skeleton (BlockMoments, the path every
model family follows; ADR-0004).

## End of every round
Update STATUS.md, OPEN_ITEMS.md, DECISIONS.md, DISCREPANCIES.md and HANDOFF.md.
