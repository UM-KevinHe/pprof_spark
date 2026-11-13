# Specification: cluster terms — persistence, jobs, Python and 4c's stage 2 (slice 4d-3)

- Status: approved with D-74 (2026-11-05); persistence in round 146.
- Plan: [cluster-terms-plan.md](cluster-terms-plan.md) (D-68), slice 4d-3. It builds on slices 4d-1 and 4d-2
  ([../linear/random-effect-clusters.md](../linear/random-effect-clusters.md),
  [../logistic/random-effect-clusters.md](../logistic/random-effect-clusters.md)), on 4a-3's and 4b-3's jobs, and on
  slice 4c-3's persistence with `attach` ([../logistic/fe-random-cluster-job.md](../logistic/fe-random-cluster-job.md)).
  No new statistics.
- One change to D-68's wording. The plan named "format version 2 of both random-effect fits". The cluster-factor
  fits became types of their own (`LinearREClusterFit`, `LogisticREClusterFit`, rounds 137 and 142), so they get kinds
  of their own, each at format version 1. The single-factor formats stay at version 1, unchanged.

## 1. Persistence

### 1.1 `LinearREClusterFitIO` (kind `LinearREClusterFit`, format version 1)

| Part | Content |
|---|---|
| `metadata` | One JSON line, every double as its bit pattern: kind, version, the column roles (the provider and the clusters in order) and options. Then the coefficients with their standard errors and Var(β̂). Then the factors, θ̂ and σ̂, the criterion, rows, the deviance, log-likelihood, AIC, BIC, PWRSS and log det H, the search's record, warnings, software, the fingerprint and the status |
| `effects/<factor>` | Parquet, one directory per factor: key, `records`, `weight`, `blup` |

The fit's methods (`summary`, `fixedEffectsTable`, `confInt`, `fitted`, `predict`) need no records, so a loaded fit
is complete. `load` refuses another kind, an unknown version, and effect tables whose factors or lengths differ from
the metadata.

### 1.2 `LogisticREClusterFitIO` (kind `LogisticREClusterFit`, format version 1)

| Part | Content |
|---|---|
| `metadata` | As 1.1 for the logistic fit: the coefficients and Var(β̂), the factors and σ̂, the deviance, log-likelihood, AIC, BIC, stage 1's deviance, the search's record, the boundary factors, warnings, software, rows and the fingerprint |
| `effects/<factor>` | Parquet: key, `records`, `blup`, `variance`, `se`, and `u`, the spherical mode, kept so that the profile can start from it |

`load` returns the saved fit. Its `summary`, effect tables, `fitted` and `predict` need no records. `attach(df,
saved)` re-collects the training data on the driver in canonical order, as `fit` does, and requires the same rows,
fingerprint and level keys. It then rebuilds the kernel's state from the saved arrays, after which `test`, the
measures, the intervals and the profiles equal the original's bit for bit.

## 2. Jobs

The jobs keep their run specifications at version 1 and gain an optional `columns.clusters`, a list of column names.

### 2.1 `LinearREJob` with `columns.clusters`

It fits `LinearREClusters`. The outputs that apply:
- `fit`, `summary`, `fixedEffects`, `confInt`, `predictions` and `fitted`, as 4a-3 defines them;
- `effects`, new, which writes `effects/<factor>` for every factor;
- `predictions` takes `reVars`, the factors whose BLUPs it adds (none by default, as pprof_py's `use_re` = False).

`providers`, `providerTests`, `alphaIntervals`, `measures` and `measureIntervals` are refused at parse time, naming
pprof_py's rule that provider profiling needs a fit without cluster factors.

### 2.2 `LogisticREJob` with `columns.clusters`

It fits `LogisticREClusters`, with every output of 4b-3. In addition:
- `effects` writes `effects/<factor>`;
- `providerTests` accepts `exact` when there is exactly one cluster factor;
- `profile` takes `factors`, by default every factor, and writes `profile/<factor>`;
- `predictions` takes `reVars` in place of `useRe`.

`fit.stage2: false` is refused with cluster factors, since the cluster fit is the Laplace fit.

### 2.3 Run records

Both jobs' run records name the factors and give θ̂ or σ̂ by factor.

## 3. Python

- `LinearRandomEffect(..., cluster_vars=[...])` fits through `LinearREClusters` and returns
  `LinearRandomEffectClusterModel`. It has:
  - `coefficients`, `theta`, `sigma`, `get_random_effects(var)`, `get_random_effect_sd(var)`;
  - `summary`, `fixed_effects_table`, `conf_int`, `fitted_values`, `predict(df, use_re, re_vars)`;
  - `save`, `load`.
  `test`, `calculate_standardized_measures` and `calculate_confidence_intervals` raise pprof_py's error.
- `LogisticRandomEffect(..., cluster_vars=[...])` fits through `LogisticREClusters` and returns
  `LogisticRandomEffectClusterModel`. It has:
  - `summary`, `test` (`exact` with one cluster factor), `calculate_standardized_measures`,
    `calculate_confidence_intervals`, `profile_sigma(var)`;
  - `get_random_effects(var)`, `fitted_values`, `predict(df, use_re, re_vars)`;
  - `save`, and `load(spark, path, df=None)`. Without `df`, the methods that need the records raise and name the
    remedy, as 4c-3's model does.

## 4. The random-cluster model's stage 2 from a logistic cluster fit

`LogisticFERandomClusterSource.Stages` accepts, besides the three-stage model's stage 2, a `LogisticREClusterFit`
whose factors are exactly the model's provider and cluster columns, as pprof_py's `_values_from_stages` requires:
- σ is its cluster factor's σ̂;
- the start is its provider BLUPs plus its intercept, matched by key, with the provider keys compared as text when
  the types differ, as pprof_py does;
- a fit without an intercept, with other factors, or missing a provider is refused, naming the rule.

Python's `fit(df, stage1=, stage2=)` takes a `LogisticRandomEffectClusterModel`. The job's `fit.stages.stage2` takes
the path of either saved fit, distinguished by the kind in its metadata.

## 5. Validation plan

- `LinearREClusterFitIOSuite` and `LogisticREClusterFitIOSuite`:
  - round trips bit for bit;
  - for the logistic fit, `attach` restoring the inference bit for bit;
  - the refusals: overwriting, another kind, an unknown version, short or missing effect tables, and other or
    changed data.
- The jobs:
  - with `columns.clusters`, every output equals the library's on the saved fit;
  - the refused outputs list their problems;
  - the existing jobs' tests unchanged.
- Python: fits with `cluster_vars` against the fixtures at D-70's and D-72's classes, and save and load.
- 4c's stage 2:
  - the random-cluster fit from a logistic cluster fit equals, bit for bit, the given-source fit with the σ and start
    the rules derive;
  - a new `fixtures/fe-random-cluster` case, frc-stages-re, runs pprof_py's model from its own
    `LogisticFixedEffectModel` and `LogisticRandomEffectModel(cluster_vars=[cluster])`. pprof_spark's result from
    its own two stages is compared with it at T-opt, since the stages agree at their classes.

## 6. Known discrepancies

None new.

## 7. Implementation notes (round 146, persistence; no statistical change)

- `engine.linear.LinearREClusterFitIO` and `engine.logistic.LogisticREClusterFitIO` follow §1.
  - Inside a saved fit, each factor's table is stored under `effects/<factor index>`, its name kept in the metadata,
    so that a column name never becomes a path. The jobs' `effects/<factor>` outputs (§2) keep the names.
  - The metadata records each factor's level count; `load` checks every table against it.
  - The metadata write is the first, in `errorifexists` mode, so nothing is overwritten (ARCH-2).
- `LogisticREClusterSaved` holds the saved fit: its tables and scalars, the level keys, and the kernel's arrays (the
  modes u, the BLUPs and the posterior variances) in level order.
  - `attach` re-validates the data, re-collects the rows on the driver in canonical order, and requires the same
    rows, fingerprint and keys.
  - It then rebuilds the kernel's state, so the full fit's inference runs unchanged. `LogisticREClusters`' validation
    and collection are now package-visible for it.
- Tests:
  - `LinearREClusterFitIOSuite` (3): lrc-two and lrc-text round trips bit for bit, with the loaded fit's tables,
    fitted values and predictions unchanged; the refusals.
  - `LogisticREClusterFitIOSuite` (3): lgc-one and lgc-two saved, loaded and re-attached bit for bit, with the tests,
    measures, option `alpha`, fitted values and, on lgc-one, the cluster factor's profile unchanged; the refusals,
    other and changed data among them.
