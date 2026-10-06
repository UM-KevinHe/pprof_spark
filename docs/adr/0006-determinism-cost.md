# ADR-0006: Cost of deterministic mode (spike S-05)

- Status: Accepted (delegated; D-18)
- Date: 2026-10-06
- Spike: S-05. Its second half, a benchmark of the real Cox kernel, follows that kernel in Phase 1a.

## Context
§8.1 and §8.2 make bitwise reproducibility the default: `StrictMath` for transcendental functions,
pairwise sums within blocks, and Neumaier sums across blocks in block order (ADR-0003). S-05 asks
what this costs and whether it should stay the default.

## Method
`pprof.spark.bench.DeterminismCost` in the bench module (`sbt "bench/runMain
pprof.spark.bench.DeterminismCost"`; `bench.yml` runs it by hand on GitHub's runners). Medians of 11
timed runs after 7 warm-ups: exp, log, log1p and expm1 over 2^20 inputs; summation of 2^20 values;
a Cox-shaped kernel over 2^16 rows (w = exp(x . beta), then the sums of w, w x and w x x') with
p = 10 and p = 1; and a driver reduction of 10,000 partials of 66 values.

## Evidence (assistant sandbox: one shared x86-64 vCPU; OpenJDK 17.0.20 and 21.0.12; two trials each)

| Measurement | Cost relative to the fast variant |
|---|---|
| `StrictMath.exp` against `Math.exp` | 1.21 to 1.33 |
| `StrictMath.log` against `Math.log` | 1.11 to 1.88 |
| `log1p`, `expm1` | 0.95 to 1.16 |
| Pairwise summation against a plain loop | 0.59 to 0.62 |
| Neumaier summation against a plain loop | 1.5 to 1.7 |
| Cox-shaped kernel, p = 10: `StrictMath`, sequential sums | 1.31 to 1.45 |
| Same, pairwise sums with one scratch column per statistic | 3.1 to 3.5 |
| Same, pairwise sums with a fused cascade (bits identical to the previous line) | 0.99 to 1.19 |
| Cox-shaped kernel, p = 1: every deterministic variant | 0.62 to 0.99 |
| Driver reduction: Neumaier in block order against plain sums | 3.2 to 5.3 (about 1.3 ms per pass) |

## Decision
Deterministic mode stays the default and, for now, the only mode. Kernels that accumulate several
sums per row use a fused cascade: one walk over the rows that builds, for every statistic, the same
addition tree as `Summation.pairwise`, so its bits equal per-statistic pairwise sums while the
row data are read once. A reusable form belongs in `numerics` with the first Cox kernel (OI-34).

## Consequences
- In the measured kernel shapes, full determinism costs at most about 20 percent, and nothing
  measurable at p = 10 once sums are fused. `StrictMath` alone matters only where exp or log
  dominate the work.
- The platform skeleton's toy `Moments` kernel keeps its scratch columns; production kernels use
  the fused pattern.
- Re-measure with the Phase 1a Cox kernel and on a GitHub runner (`bench.yml`).
