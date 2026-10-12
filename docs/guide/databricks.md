# Running pprof_spark on Databricks

Three notebooks in [`databricks/`](../../databricks/) build pprof_spark on a Databricks cluster, check it
against the reference fixtures, and give a first look at its scale. Nothing has to be installed on your
computer, nothing runs on GitHub, and no Unity Catalog volumes are needed: builds live in a folder of
workspace files, from which the cluster installs the JARs.

| Notebook | What it does | Time |
|---|---|---|
| [`00_build`](../../databricks/00_build.py) | Clones a commit on the driver, builds the three JARs with sbt and the Python package, and copies them, with the fixtures and tolerances, to a build folder | About 10 minutes the first time |
| [`01_parity`](../../databricks/01_parity.py) | Checks that the installed JARs are that build, fits every Cox, logistic and linear fixture through the PySpark wrappers, compares with pprof_py and R, and checks bitwise reproducibility (R0) under other partitioning | A few minutes |
| [`02_scale`](../../databricks/02_scale.py) | Generates a synthetic data set of a chosen size, fits one model, times each phase, and checks recovery of the true coefficients | Depends on the size |

## Requirements

- **Classic compute in dedicated (single-user) access mode**, on Databricks Runtime 18 (Apache Spark
  4.1). The Python wrappers call the JARs through py4j, which standard access mode blocks and Spark Connect
  and serverless compute do not offer ([python.md](python.md), ADR-0009).
- **Outbound access from the cluster to github.com and repo1.maven.org**, for `00_build` only: it
  downloads sbt from GitHub and the project's dependencies from Maven Central. The notebook checks both
  before it starts.
- A **fixed-size cluster** when you time anything: autoscaling makes runs hard to compare.

## One-time setup

1. Clone the repository into a Databricks Git folder: Workspace → Create → Git folder, with
   `https://github.com/UM-KevinHe/pprof_spark.git`. The notebooks open from its `databricks/` folder.
2. That is all. The first build creates the builds folder, `/Workspace/Users/<you>/pprof_spark_builds`
   by default.

## For each new version

1. **Pull** the Git folder, so the notebooks are current.
2. **Build:** run `00_build` with the commit to build (default `main`). It writes
   `pprof_spark_builds/<first 12 characters of the commit>/`:

   | Content | Used by |
   |---|---|
   | `pprof-spark-numerics_2.13-…jar`, `pprof-spark-engine_2.13-…jar`, `pprof-spark-app_2.13-…jar` | The cluster, as libraries |
   | `python/pprof_spark/` | `01_parity` and `02_scale`, which import it from here so the Python always matches the JARs |
   | `pprof_spark-…whl` | Anyone who prefers `%pip install --no-deps <wheel>` (without `--no-deps`, pip would replace the runtime's own PySpark) |
   | `fixtures/` (Cox, logistic, linear) and `tolerances.conf` | `01_parity` |
   | `manifest.json` | Every notebook: the commit, the build time, the files and their SHA-256 |

3. **Install the JARs:** Compute → your cluster → Libraries → Install new → Workspace, once for each of
   the three JAR paths `00_build` prints. Uninstall the previous build's `pprof-spark-` JARs, then restart
   the cluster; a JAR replaced without a restart is not picked up. Alternatively, set the widget *Install
   on this cluster* to `yes` before running `00_build`, then restart.
   Keep the JARs in workspace files (or at an `s3://` address, widget *Optional s3:// folder*): libraries
   stored in DBFS, which includes `/dbfs/…` and `/mnt/…` paths, are disabled by default since Databricks
   Runtime 15.1.
4. **Check parity:** run `01_parity`. It stops if the installed JARs come from another commit than the
   build folder's (every build is called `0.1.0-SNAPSHOT`, so only the commit tells builds apart). It then
   makes about 155 comparisons:

   | Model | Fits | Compared with |
   |---|---|---|
   | Cox | Six fixtures, Breslow and Efron, tight fits with 2 KiB blocks (about twenty blocks each) | pprof_py and R survival: coefficients (T-coef), standard errors (T-var), log-likelihood (T-fn); default fits' iteration counts exactly |
   | Logistic fixed effects | Six fixtures, default fits; exact provider tests on `lfe-base` | pprof_py: β and γ (T-coef), variances (T-var), log-likelihood (T-fn), iterations exactly; test statistics (T-test) and flags exactly |
   | Linear fixed effects | Six fixtures, including text provider keys | pprof_py: β and γ (T-coef), standard errors (T-var), σ, AIC and BIC (T-fn) |
   | All three | Refit with `spark.sql.shuffle.partitions` = 7 and the input reversed across 13 partitions | The first fit, bit for bit (R0) |

   The tolerance rule is the CI tests' (PROJECT_CONTEXT §8.4). The table is saved as
   `parity-<time>.csv`, with `parity-<time>-environment.json`, in the build folder. In the assistant's
   sandbox (local mode, four cores) the notebook passed all 155 comparisons in three minutes.
5. **Look at scale:** run `02_scale`. Choose the model, rows, providers and features; start at 10⁶ rows
   and 10³ providers and grow tenfold. Up to about 10⁷ rows the data can stay cached on the executors;
   beyond that, give *Optional folder to write the data to* an S3 path (a mount path such as `/mnt/…` is
   fine for data). Each run appends a row to `scale-first-look.csv`: the size, the cluster, the time of each
   phase, the iterations, and the largest |z| of the coefficients against their true values (above 4.5
   deserves a look).
6. **Send back** the parity CSV and environment file, `scale-first-look.csv`, and, for any failure, the
   output of the failing cell (and the driver log if the error came from the JVM).

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `00_build`: `cannot reach https://repo1.maven.org/...` | No outbound access: see the next section |
| `the pprof_spark JARs are not on the Spark classpath` | The JARs are not installed, or the cluster was not restarted after installing them |
| `the JARs on this cluster were built from X, not Y` | Another build's JARs are installed: uninstall them, install this build's, restart; or point the notebook's *Build folder* widget at build X |
| `Py4JSecurityException: ... is not allowlisted` | Standard access mode: use dedicated compute |
| A library from a `/dbfs/…` or `/mnt/…` path does not install | DBFS library storage is disabled: use the workspace folder or an `s3://` address |
| `02_scale` runs out of memory | Write the data to S3 (*Optional folder*) instead of caching it, or use a larger cluster |

## If the cluster cannot reach the internet

Build elsewhere and upload the result. The repository has a `.devcontainer`, so a GitHub Codespace
(within a personal account's free monthly hours; it is blocked, not billed, when they run out and no
payment method is set) can run `sbt numerics/package engine/package app/package` and
`python -m pip wheel --no-deps --wheel-dir dist ./python`. Then create
`/Workspace/Users/<you>/pprof_spark_builds/<commit>/` by hand with the layout of step 2: the three JARs, the
wheel, `python/pprof_spark/`, `fixtures/{cox,logistic,linear}`, `testkit/src/main/resources/tolerances.conf`,
and a `manifest.json` with at least `commit` (the full SHA), `subject` and `built_at`.

## Limits

- `02_scale` measures wall time only. Spark-level measurements (tasks, shuffle bytes, driver memory),
  the full grid and the decisions they inform come with the scale-test kit
  ([plan](../spec/scale/plan.md)).
- The notebooks have been run in the assistant's sandbox with local Spark and stand-ins for `dbutils`, not
  yet on Databricks (OI-72).
