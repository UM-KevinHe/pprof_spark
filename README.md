# pprof_spark

A Spark-native Scala package for large-scale healthcare provider profiling. It reimplements the
validated statistical methods of [pprof_py](https://github.com/UM-KevinHe/pprof_py) as
distributed algorithms for Databricks, preserving their statistical definitions.

**Status: Phase 0 (foundations and spikes).** There are no statistical features yet; every
feature stays `Experimental` until it passes the parity gate (NN-12).

| Document | Purpose |
|---|---|
| [docs/PROJECT_CONTEXT.md](docs/PROJECT_CONTEXT.md) | Normative project context: rules, architecture, roadmap |
| [STATUS.md](STATUS.md) | Current phase, gates and rounds |
| [DECISIONS.md](DECISIONS.md), [docs/adr/](docs/adr/) | Decision register and architecture decision records |
| [OPEN_ITEMS.md](OPEN_ITEMS.md), [DISCREPANCIES.md](DISCREPANCIES.md) | Open work; differences from the reference |
| [HANDOFF.md](HANDOFF.md) | Continuity between working sessions |
| [docs/compatibility.md](docs/compatibility.md) | Spark, Databricks Runtime, Scala and JDK pins |

## Modules

| Module | Purpose | Compiles against |
|---|---|---|
| `numerics` | Pure kernels and linear algebra | Scala standard library only |
| `engine` | Distributed engine, models, inference, measures | `spark-sql-api`, the shared Classic/Connect interface |
| `ml` | Spark ML adapters (Classic only) | `spark-mllib` |
| `app` | Databricks job entry points | `engine` |
| `testkit` | Shared test harness (never published) | `spark-sql` (Classic) |
| `bench` | Benchmark workloads (never published) | `engine`, `testkit` |

## Building

The build targets Databricks Runtime 18 LTS: Spark 4.1.0, Scala 2.13.16, bytecode for Java 17.
CI runs everything. Anywhere with JDK 17+ and sbt (Codespaces, for example):

```
sbt ci                                  # formatting check, compile, all tests
sbt scalafmtAll scalafmtSbt             # format sources and build files
sbt -Dpprof.linkageCheck=true compile   # PLAT-3: compile against the runtime's scala-library
bash scripts/check-engine-api.sh        # ARCH-2 and PLAT-7 engine API rules
```
