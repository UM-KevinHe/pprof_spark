# ADR-0005: Reference fixtures and tolerance calibration

- Status: Accepted (delegated; D-17)
- Date: 2026-10-06
- Decisions: D-05 (pin), D-09 (tolerances), D-17 (this design)

## Context
Parity tests (§9.2) compare pprof_spark with the pinned reference, pprof_py v0.7.0, and with R
where pprof_py is validated against R (PAR-2). §9.3 asks for fixtures generated once from shared
synthetic inputs, with a manifest of versions, options and checksums, and §8.4 asks for
tolerances calibrated against negative controls.

## Decision
- Inputs are synthetic and exact in text: integer event times, covariates on a 1/64 grid, offsets
  on a 1/16 grid, case weights on a 1/2 grid. They are generated once and never regenerated per
  language; a re-pin recomputes outputs only.
- Format: `input.csv` per case, `case.json` (definition), `pprof_py.json` and `r_survival.json`
  (outputs, every double as a hexadecimal floating-point string), and `fixtures/manifest.json`
  (versions, options, SHA-256 of every file). This replaces §9.3's Parquet: the files are exact,
  small, and readable by Scala without Spark, by R without extra packages, and by Python.
- Outputs per case and tie method: production-default and tight fits (eps 1e-11, iteration cap
  100; R warns below its Cholesky tolerance at 1e-12), and log-likelihood, score and information
  at β = 0 and at a fixed β (R: `init = β`, `iter.max = 0`). R uses `robust = FALSE` so that its
  variance is model-based like pprof_py's (X-009).
- Negative controls stored with the outputs: the other tie method, one tied event moved a day
  later, and one case weight dropped. `reference/fixtures/calibrate.py` and testkit's
  `FixturesSuite` require pprof_py-against-R agreement of at most 1 and every negative control at
  least 10, measured in units of the tolerance class (NN-9).
- Environment: Python 3.12 with pinned packages, pprof_py installed from the pinned commit, R
  4.3.3 and survival 3.5-8 from Ubuntu 24.04's archive. `fixtures.yml`, run by hand, recomputes
  the outputs from the committed inputs and compares them under T-part, since numpy's BLAS and
  numba compile for the host CPU.
- No Git LFS while the fixtures stay small (about 113 KB in total so far).

## Evidence (assistant sandbox, 2026-10-06)
- Generation is deterministic: two runs, and a run from the committed inputs, reproduce all 16
  files byte for byte.
- Calibration (docs/parity/cox-calibration.md): every check passes. Worst pprof_py-against-R
  agreement: T-coef 0.995 (tiny-ties, Breslow; see X-010), T-var 0.0015, T-fn 0.025. Weakest
  negative control: T-coef 8.2e3, T-var 509, T-fn 2.4e6 times its tolerance.
- pprof_py's survival suite at v0.7.0 passes with its committed R results (275 passed, 1 skipped
  for an optional package), and R 4.3.3 with survival 3.5-8 regenerates those 112 result files byte
  for byte (X-007, X-008).

## Consequences
- Phase 1a parity tests read `fixtures/cox`. Later phases add families: left truncation and
  baseline hazards (1b), residuals and robust variance (1c), provider measures (1d) (OI-32).
- Changing a tolerance must keep `FixturesSuite` passing, which is the negative-control evidence
  NN-9 asks for.
