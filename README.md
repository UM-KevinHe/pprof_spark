# pprof_spark

A standalone, Spark-native Scala package for large-scale healthcare provider profiling. It
reimplements the validated statistical methods of [pprof_py](https://github.com/UM-KevinHe/pprof_py)
as distributed algorithms for Apache Spark, preserving their statistical definitions. pprof_py is
used only to produce reference results for the test suite.

**Status: Phases 0 to 2 closed at parity-verified; Phase 3 (linear fixed effects) is next.** Stratified
Cox regression (`pprof.spark.engine.cox.CoxPH`): Breslow or Efron ties, case weights, offsets, entry times,
model-based and robust variance, baselines, prediction and residuals; two-stage SMR and SHR
(`CoxMeasures`) and exact and mid-p provider tests (`CoxProviderTests`). Logistic fixed-effect provider
models (`pprof.spark.engine.logistic.LogisticFE`, SerBIN): covariate tests, cluster-robust variances,
prediction and AUC; exact Poisson-binomial, score, Wald and bootstrap provider tests
(`LogisticProviderTests`); indirect and direct standardization (`LogisticStandardization`). The
three-stage SRR model with providers crossed with clusters (`ThreeStagePipeline`): stage 3's tests,
measures and intervals, and σ sensitivity. Job runners for `spark-submit` (`pprof.spark.app.CoxJob`,
`LogisticJob`, `ThreeStageJob`; guides in `docs/guide/`) and Python wrappers through py4j (`python/`,
`docs/guide/python.md`). Everything is at parity with pprof_py v0.7.0, and with R where R is a reference;
every feature stays `Experimental` until the package-level scale test (D-28).

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
| `app` | Spark job entry points | `engine` |
| `testkit` | Shared test harness (never published) | `spark-sql` (Classic) |
| `bench` | Benchmark workloads (never published) | `engine`, `testkit` |

## Building

The build targets Apache Spark 4.1 with Scala 2.13.16 and Java 17 bytecode, so the same JAR runs
on open-source Spark 4.1.x and on Databricks Runtime 18 LTS. CI runs everything, including the
engine suites under Spark Connect. Anywhere with JDK 17+ and sbt (Codespaces, for example):

```
sbt ci                                  # formatting check, compile, all tests
sbt scalafmtAll scalafmtSbt             # format sources and build files
sbt -Dpprof.linkageCheck=true compile   # PLAT-3: compile against the runtime's scala-library
bash scripts/check-engine-api.sh        # ARCH-2 and PLAT-7 engine API rules
```

## Development environment

CI is the build and test environment. For interactive work, `.devcontainer/` sets up a GitHub
codespace with JDK 21, sbt and Metals (ADR-0007); use synthetic data only.

## Reference fixtures

`fixtures/` holds synthetic inputs and reference outputs from pprof_py and R for parity tests;
`reference/fixtures/` holds the generators and the calibration script (ADR-0005).

## License

MIT; see [LICENSE](LICENSE).
