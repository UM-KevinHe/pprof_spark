# Performance phase plan: Cox and provider fixed-effect logistic regression at scale

Status: **proposed** as D-75 (round 149, 2026-11-13). The plan follows the algorithmic feasibility study
(`docs/reviews/algorithmic-feasibility-2026-11.md`, read at round 139) and a reading of the code at round 148
(`303ae11`), recorded in §2. Phase 4 is paused while this phase runs (§8).

Every number below says what kind it is:
- **measured (cluster)**: a run on the maintainer's Databricks workspace;
- **measured (sandbox)**: a run in the assistant's one-core sandbox, which says nothing about wall time on a cluster;
- **arithmetic**: derived from the code or from measured rates.

No cluster measurement of the current `main` exists yet.

## 1. Goal and constraints

`CoxPH` and `LogisticFE` must fit tens of millions of rows at about 180 features on the standard cluster: a 32 GB
driver and 30 workers of 32 GB. A larger driver must not be required.

Kept unchanged:
- **Cox:** Breslow and Efron ties, strata, start-stop data and left truncation, weights, offsets, and the
  model-based and robust variances.
- **Logistic:** SerBIN's estimates, provider effects at the bound, and every provider-level output (variances,
  tests, standardized measures).
- **Reproducibility:** R0 per layout, R1 and R2 (PROJECT_CONTEXT §8.2), DIST-7 and DIST-8.
- **Architecture:** ARCH-2, so engine code uses Dataset operations only, with no RDD and no `SparkContext`, and
  passes `scripts/check-engine-api.sh`; Spark Connect compatibility.

The phase ends when both models fit 100 million rows at 180 features and 8,000 providers on the standard cluster.
The bytes collected to the driver per pass must not depend on n, and the three checks of §6.4 must pass at every
grid point that is run.

## 2. Where `main` stands at round 148

### 2.1 The reported failures came from round 81

The scale runs reported on 2026-11-13 used build `b670dbb`, which is round 81, on a 128 GB driver.
`DriverLimitExceededException` is the package's own guard (`DriverGuards`, with `BlockOptions.driverBudgetBytes` of
64 MB), not a JVM or Databricks limit. Round 81 planned fixed blocks of 4 MB and then applied the guard. Round 83
(issues L5 and C3, D-49) added `BudgetedLayout`, which doubles the block size, up to 256 MB, until the partials
fit.

The arithmetic below follows the code, with equal providers as `databricks/02_scale.py` generates them:

| Point | Round 81: blocks, bytes per pass | Reported |
|---|---|---|
| Logistic, 10M rows, 100 features | 1,946 blocks, 77 MB | Failed |
| Cox, 20M rows, 100 features | 3,892 blocks, 153 MB | Failed |
| Both models, 5M rows, 180 features | 1,737 blocks, 218 MB | Failed |

The kernels, Newton and SerBIN are unchanged since round 81, so the study's rates per row still apply to `main`.

### 2.2 Block growth on `main`

The table is arithmetic, for 8,000 equal providers. A packed partial takes 8(p(p+1)/2 + p + 2) bytes: 41 KB at 100
features and 129 KB at 180. Sizes are in binary units, as the code's are.

| Features | 5M rows | 20M | 50M | 100M |
|---|---|---|---|---|
| 100 | 973 blocks of 4 MB; 38 MB per pass | 973 of 16 MB; 38 MB | 1,216 of 32 MB; 48 MB | 1,216 of 64 MB; 48 MB |
| 180 | 434 of 16 MB; 55 MB | 434 of 64 MB; 55 MB | 272 of 256 MB; 34 MB | 543 of 256 MB; 68 MB, so the guard fails |

So the reported failures should not recur up to 50M rows. Growth, however, trades driver bytes for task size. At 50M
rows and 180 features a fit runs 272 tasks over 256 MB blocks. During a pass each block is held in up to three
copies: the cache's decoding, the join that attaches γ in the logistic passes, and the deserializer. This is the
study's n·p³ coupling, and at 100M rows and 180 features the guard still fails.

### 2.3 What reaches the driver

On every pass, one packed p×p partial per block is collected:
- `CoxPH.scala` line 508: each Newton evaluation, halved steps included;
- `LogisticFE.scala` line 808 (pass A) and line 840 (pass B, which also returns Δγ per provider): each SerBIN
  iteration.

Once per fit:
- `CoxPH.scala` 398 (block summaries), and 444 and 462 (robust meat);
- `LogisticFE.scala` 102 (summaries, including each block's p×p co-moments for the correlation warning), 165
  (variance), 186 (provider variances), and 201 and 215 (robust).

The covariate tests collect at 458, 501 and 515. The study's list omitted the two sites that run every iteration.

### 2.4 The kernels

The study's account holds:
- `CoxStratum.add` makes p(p+1)/2 Neumaier additions per row into S₂. It makes the same number again, each with two
  divisions, at every distinct event time.
- Logistic pass A makes p(p+1)/2 Neumaier additions per row, through `Accumulator.add`, which carries a finiteness
  test and a branch.

Reading the code adds two points:
- **The Efron buffer.** `CoxStratum.add` zeroes `tied2`, p(p+1)/2 doubles or 130 KB at 180 features, at every
  distinct time, for Breslow fits too. Skipping it for Breslow changes no bit (OI-104).
- **The γ table.** Every logistic pass builds a γ table on the driver and broadcast-joins it to the cached blocks
  (`withGamma`). This is small today, but it adds a copy of every block per pass, which will matter once the kernel
  is near 5 µs per row (OI-105).

### 2.5 A profile by call site

This profile is **measured (sandbox)**: one core, local mode, with the `02_scale.py` design at 200,000 rows, 320
providers of 625 rows each (the shape of 5M rows and 8,000 providers), and 100 features. A listener attributed
executor CPU, task results and shuffle writes to SQL executions. Round 149's evidence log holds the raw output.

Cox: 3 iterations, 4 evaluations, no halvings, 40 blocks; the fit took 31.5 s.

| Call site | Executions | Wall s | CPU s | Bytes to the driver |
|---|---|---|---|---|
| Newton evaluations (`CoxPH.scala:507`) | 4 | 15.7 | 12.7 | 6.7 MB |
| Working-set build (`CoxBlocks.scala:214`) | 1 | 7.1 | 2.8 | 11 KB |
| Validation (`CoxInput.scala:105`) | 1 | 5.5 | 1.6 | 3 KB |
| Block summaries (`CoxPH.scala:397`) | 1 | 0.8 | 0.2 | 19 KB |
| Group sizes (`GroupSizes.scala:19`) | 1 | 0.5 | 0.1 | 4 KB |

Logistic: 4 iterations; the fit took 48.9 s.

| Call site | Executions | Wall s | CPU s | Bytes to the driver |
|---|---|---|---|---|
| Validation (`LogisticInput.scala:47`) | 1 | 17.3 | 6.8 | 3 KB |
| SerBIN pass A (`LogisticFE.scala:807`) | 4 | 10.7 | 8.1 | 6.7 MB |
| Working-set build (`LogisticBlocks.scala:215`) | 1 | 6.0 | 2.4 | 11 KB |
| Summaries with co-moments (`LogisticFE.scala:101`) | 1 | 4.0 | 2.6 | 1.5 MB |
| Variance pass (`LogisticFE.scala:164`) | 1 | 3.2 | 2.1 | 1.6 MB |
| SerBIN pass B (`LogisticFE.scala:839`) | 4 | 2.8 | 1.2 | 49 KB |
| Area under the curve (`LogisticFE.scala:657`) | 1 | 1.0 | 0.3 | 8 KB |
| Provider variances (`LogisticFE.scala:185`) | 1 | 0.7 | 0.2 | 13 KB |
| Group sizes (`GroupSizes.scala:19`) | 1 | 0.6 | 0.1 | 4 KB |

Pass A costs 10.2 µs of CPU per row at 100 features, or 33 µs scaled to 180, against the study's 29.9. For the
logistic model, however, SerBIN's passes take only 29% of the fit's wall time and 39% of its attributed CPU.
Validation alone takes 35% and 29%, and the co-moment pass for the correlation warning costs 13 µs per row, more than
a pass A at this width.

The Cox evaluation costs 15.9 µs of CPU per row at 100 features. That is the study's 51 µs at 180 features scaled
by p².

On one core, the Newton passes take half the fit's wall time and three quarters of its attributed CPU. Preparation
takes the rest:
- validation's single aggregate of 2p + 10 counts, at 8 µs of CPU per row plus seconds of planning and code
  generation;
- the working set's shuffle and build;
- the block summaries.

Validation is the costliest call site outside the passes, and `explain` confirms its mechanism (evidence log). Both models validate in one aggregate that mixes `countDistinct` on the row identifier with about 2p + 10 counts, and Spark plans that by grouping every count on the row identifier. The aggregation state is therefore n keys of about 2p + 10 counters, shuffled: about 8 GB at 5M rows and 100 features, and 59 GB at 20M rows and 180 features (arithmetic). Splitting the duplicate check from the counts changes no floating-point result (OI-109).

The kernels alone do not explain a 30-worker run being slower than one pprof_py thread. At 5M rows and 100 features, an
evaluation costs about 80 CPU-seconds spread over the cluster, against about 26 seconds on pprof_py's one thread
(arithmetic). The cause lies in what a one-core run cannot show, and step 0 must find it (OI-106). The validation aggregate is the leading candidate.

The prototype listener also taught one lesson for the harness. It mapped stages to SQL executions when their jobs
started, and so missed the working-set build's shuffle stage: it attributed no shuffle bytes and only part of the
build's CPU. The harness therefore maps each stage when it is submitted, from the stage's own properties, and
reports any executor time it cannot attribute.

## 3. Where this plan departs from the study

1. **Step 0 measures the current `main`, not round 81.** Its points include 50M rows at 180 features, where blocks
   have grown to 256 MB, and 100M rows, where the guard fails.
2. **Step 1's reduction groups replace block growth and keep every golden run bitwise.** Blocks keep their target
   size. Groups of G consecutive blocks are placed together when the working set is built, so a pass needs no
   shuffle (§4). G = 1 whenever today's plan fits the budget without growth. The study classes step 1 as "same
   bits"; that holds only where G = 1. G = 1 covers every golden run; elsewhere the order of summation changes.
3. **Engine passes are tagged by operation.** Tags use `SparkSession.addTag`, which spark-sql-api declares and Spark
   Connect supports. PROJECT_CONTEXT §6.13 asks for this, and it was never built. The harness attributes time by
   tag and SQL execution identifier, not by line numbers, which every change moves.
4. **A step 0b, if needed.** If step 0 attributes more than a quarter of a fit's wall time to call sites outside the
   Newton and SerBIN passes, those sites are fixed before step 1. The likely ones are validation (OI-109), the working-set build and the summaries.
5. **Profile-likelihood Newton is built as an option beside SerBIN.** It becomes the default only by a later
   decision on step 5's measurements, since it changes the iterates and needs a discrepancy (X-051).
6. **ℓ-only evaluation of halved Cox steps moves into step 3** and stays low priority: the profile above had no
   halvings.

## 4. Steps

| Step | Work | Class | Exit measurement | Rounds (estimate) |
|---|---|---|---|---|
| 0 | Harness, data generators, golden manifest, operation tags; baseline of `main` | None: tags change no bit | Per call site: wall time, CPU, passes, bytes to the driver per pass, driver heap peak, shuffle bytes, executor memory; both models at 5M and 20M rows | 2 |
| 0b | Only if step 0 calls for it (§3, item 4) | Bit-preserving | Those call sites' share of wall time | 0 to 2 |
| 1 | Executor-side reduction in fixed groups; block growth retired | Bit-preserving where G = 1; summation order elsewhere (§6.3) | Bytes to the driver per pass flat from 5M to 100M rows; 100M rows at 180 features fits; R0 across partition counts | 2 |
| 2 | Chunked compensated accumulation: Cox S₂ and per-time terms; logistic pass A, variance and co-moment passes; the Breslow buffer | Summation order | CPU per row per pass against step 0; the re-baselining evidence (§6.3) | 2 |
| 3 | Cox weighted-Gram information with Efron ties, entry times, weights and offsets: specification, fixtures, build; ℓ-only halvings | Summation order | Cox CPU per row; parity at the three levels | 3 |
| 4 | Cox per-time aggregates for unstratified fits and oversized strata on gridded times: specification, fixtures, build | Summation order | An unstratified 20M-row fit using every worker; agreement with the stratum sweep within T-fn | 3 |
| 5 | Logistic profile-likelihood Newton stepping through SerBIN's reduced system, bounded providers included: specification, fixtures, discrepancy, build as an option | Other iterates (§6.3) | Passes and CPU per fit against step 2; converged estimates within T-coef of SerBIN's | 4 |
| 6 | Internal scaling (OI-66); a sparse Gram if the covariates warrant it; whether a wide-model mode is wanted | Summation order | Conditioning; a measured crossover | 2 |

The study's arithmetic for a fit at 180 features (its §5.2) is:
- **Cox:** 312 µs per row today, 38 after steps 2 and 3;
- **Logistic:** 237 µs per row today, 60 after step 2, and 39 with the profile method.

These are expectations to test, not targets.

### Step 0: harness and baseline

Step 0 builds the following in `bench`, which runs on Classic Spark. Engine code is untouched apart from the tags.
- **`perf.Generator`.** Data are a pure function of (seed, row identifier), so any n is generated without a shuffle
  and written once to Parquet with a manifest and checksum, which `bench.synthetic` supplies.
  - Two designs: `simple`, which is `02_scale.py`'s, and `correlated`, which is the study's (correlated indicators,
    some of them rare, and continuous covariates with standard deviations from 0.1 to 50).
  - Equal providers by default, with an option for skew.
  - Cox data either stratified by provider or unstratified.
- **`perf.Recorder`.** A listener recording, for each SQL execution:
  - its identifier, tags and call site;
  - wall time, executor run time, CPU time and GC time;
  - task result bytes, which are the bytes sent to the driver;
  - shuffle read and write bytes, and peak executor memory;
  - the physical plan.

  It also samples the driver heap's peak during the fit, and reads cached bytes per executor once the working set
  is built.
- **`perf.PerfJob`.** The `spark-submit` entry point:
  - generates or reads the data;
  - fits with the chosen options, and records;
  - writes one JSON record per run, with the commit, the Spark version and the cluster's shape;
  - optionally runs the checks of §6.4.
- **The runbook,** `docs/parity/scale/perf-runbook.md`, with the commands for each grid point and how results come
  back. Results are kept in `docs/parity/scale/perf-results.md`.
- **`GoldenRunsSuite`** and its manifest (§6.1).
- **Operation tags** on every pass of both models, with a test under Spark Connect.

The maintainer runs the baseline points:
- both models at 5M and 20M rows, 180 features and 8,000 providers, with the simple design;
- 20M rows at 100 features;
- 50M and 100M rows at 180 features. The 100M point is expected to fail the guard, which costs only preparation time;
- the correlated design at 5M rows;
- unstratified Cox at 2.5M rows, below `maxStratumBytes`;
- pprof_py at 5M rows on the 32 GB driver.

### Step 1: reduction in fixed groups

Step 1 addresses OI-02 and part of OI-30.
- **Blocks.** Blocks stay at `targetBlockBytes`, and `BudgetedLayout`'s growth is retired for both models.
- **Groups.** There are R groups of G consecutive block identifiers.
  - G = 1 when B partials fit `driverBudgetBytes`, which is today's condition for no growth.
  - Otherwise G is the smallest value for which ⌈B/G⌉ partials fit.
  - G, R and the plan are recorded with the fit.
- **Building the working set.**
  - Rows are repartitioned into R partitions on a key computed on the driver for each group, chosen so that
    Spark's hash partitioning sends group g to partition g.
  - Within partitions, rows are sorted by group and then by block.
  - Blocks are built in a streaming `mapPartitions` with the present canonical sort.
  - There is one shuffle, as now.
- **A pass.**
  - One `mapPartitions` folds each group's block partials in block order and emits one partial per group.
  - The driver folds the group partials in group order.
  - Every fold checks that block identifiers arrive in order, and fails otherwise.
  - No pass shuffles, and none uses an RDD.
- **Partitioning cannot change results.** A group's key is a function of the group, so a group can never be split
  across partitions; if the choice of keys were lost, only balance would suffer. The per-provider vectors (Δγ and
  the variances) still return per provider, m doubles per pass.

### Steps 2 to 6

- **Step 2** addresses OI-38 and OI-104. It brings the study's chunked accumulation into the kernels: plain sums over
  a fixed number of rows, in a layout the JIT vectorizes, with each chunk folded into Neumaier totals. The chunk
  length is fixed in code, so it is not a tuning parameter that could change bits between runs.
- **Step 3** specifies the weighted-Gram identity with Efron ties and entry times before any code is written (NN-2),
  with fixtures for Efron ties with truncation.
- **Step 4** covers gridded times, which addresses OI-03 for them. Continuous times keep the sorted scans planned
  under OI-03.
- **Step 5** follows study §4.3:
  - interior intercepts are solved in the tasks, and bounded providers are held at the bound;
  - the β step comes from SerBIN's reduced system;
  - X-051 registers the reported effects of zero-event providers at the default tolerance.
- **Step 6** takes up OI-66 and OI-61.

## 5. Benchmark grid

The full grid crosses rows {5, 20, 50, 100} million, features {50, 100, 180} and providers {800, 8,000, 80,000}, on
the standard cluster with a fixed worker count. It runs once, at the end of the phase.

Each step's exit runs a core set:
- the sweep in n at 180 features and 8,000 providers;
- 50 and 100 features at 20M rows;
- 800 and 80,000 providers at 20M rows;
- the correlated design at 20M rows.

pprof_py runs on the 32 GB driver up to 5M rows at 180 features. By arithmetic, `X` and a second n×p array take about
14 GB there. Each run records what step 0's recorder collects.

An estimate is never entered in `perf-results.md`. Measurements from the cluster and from the sandbox are kept in
separate tables.

## 6. Correctness protocol

### 6.1 Golden runs

NUM-2 relies on golden runs, but the project has never listed them. This plan defines them:
- every case of `fixtures/cox` (both tie methods) and of `fixtures/logistic`, at the options their suites use;
- two multi-block synthetic cases per model, fitted at partition counts 1 and 7. The Cox cases have several strata,
  weights, offsets and entry times; the logistic cases have trials.

For each run, `GoldenRunsSuite` records the bit patterns of the log-likelihood, the estimates, the information or
Schur complement, the covariance, the provider effects and variances, and the iteration and halving counts. These
go into a committed manifest, and the suite fails on any difference.

A negative control perturbs one recorded value by one ulp and checks that the comparison fails. CI runs the suite on
both JDKs and under Spark Connect. If R2 turns out not to be bitwise there, the suite keeps one manifest per platform
and the finding is recorded.

### 6.2 Bit-preserving changes

A bit-preserving change must leave the manifest unchanged: NUM-2 as written.

### 6.3 Changes to summation order: the proposed re-baselining rule

Some changes reorder floating-point sums without changing what is summed: chunked accumulation, the weighted-Gram
identity, per-time aggregates, and groups with G > 1. Such a change is admitted when the round that makes it shows
all four of the following:
1. **Fixtures.** Every fixture comparison passes in its tolerance class, with tolerances unchanged.
2. **Old manifest against new:**
   - log-likelihoods, scores and information at fixed β agree within 1·10⁻¹³ scaled, a tenth of T-fn;
   - converged estimates and covariances agree within T-part (1·10⁻¹⁰);
   - iteration counts, halving counts and flags are identical. A difference in counts is explained in the evidence
     log and approved.
3. **R0 for the new code.** Bits are identical across partition counts and repeated runs, with the one-ulp
   negative control.
4. **Re-baselining.** The new manifest replaces the old in the same commit, and the evidence log reports the
   largest difference for each quantity.

A change of iterates, as in step 5, is compared at convergence:
- tightened runs must agree with SerBIN and pprof_py within T-coef and T-var;
- production-default behaviour, iteration counts included, is compared separately (§8.4);
- differences are registered as discrepancies.

### 6.4 At scale

Three checks need no reference, or only a sample that fits the driver:
1. Estimates, log-likelihood and information are bitwise identical across two input partition counts.
2. The generating coefficients are recovered: max |β̂ − β|/SE ≤ 4.5.
3. Results agree with pprof_py within T-coef and T-var on a deterministic sample of whole providers that fits the
   driver, about 2M rows at 180 features. The maintainer runs it, with a comparison script the harness provides.

### 6.5 Fixtures

New fixtures are needed for:
- Efron ties with entry times (step 3);
- large unstratified grids (step 4);
- bounded providers under the profile method (step 5).

Regenerating the Cox and logistic families needs R's poibin, which the sandbox cannot install. Rounds that
regenerate those families ask the maintainer to run `fixtures.yml`.

## 7. Decisions requested (D-75)

1. The plan: steps 0 to 6 in this order, with their exit measurements and the departures of §3.
2. The definition of golden runs (§6.1) and the re-baselining rule for summation order (§6.3).
3. Phase 4 paused at round 148 (§8).
4. Profile-likelihood Newton built as an option, its default decided after step 5.

## 8. Phase 4, paused

Phase 4 paused at a clean point on 2026-11-13, after round 148. Nothing is half-built. To resume, in order:
1. Slice 4d-3's last item (`docs/spec/random-effects/cluster-terms-job.md` §4): the random-cluster model's stage 2
   from a `LogisticREClusterFit`, with its fixture case frc-stages-re.
2. OI-102: PIRLS passes over distributed blocks for the logistic cluster fit.
3. The S1 smoke runs and the gate reviews of parts 4a and 4b (OI-97, OI-100).

## 9. Out of scope

- Approximate or alternative estimators for reported measures.
- A wide-model mode, unless step 6 decides otherwise.
- The linear fixed-effect model, which can adopt step 1's reduction later.
- The random-effect models' scale items (OI-101, OI-102).
