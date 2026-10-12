# Databricks notebook source
# MAGIC %md
# MAGIC # pprof_spark · 02 · A first look at scale
# MAGIC
# MAGIC Generates a synthetic data set of the chosen size on the cluster, fits one model to it, times each
# MAGIC phase, and checks that the fit recovers the coefficients the data were generated with. One run is one
# MAGIC row of results, appended to `scale-first-look.csv` in the results folder.
# MAGIC
# MAGIC The data are deterministic: every value is a hash of (row identifier, seed, column), so the same widgets
# MAGIC give the same data on any cluster. True coefficients are βₖ = (−1)ᵏ·0.5/√p; provider effects are uniform
# MAGIC on [−0.5, 0.5]; odd-numbered features are binary (P(1) = 0.3), even-numbered ones uniform on [−1, 1] in
# MAGIC steps of 1/64. Cox data have an exponential baseline per stratum (provider), uniform censoring over five
# MAGIC years and times in days, so ties occur.
# MAGIC
# MAGIC Start small (10⁶ rows, 10³ providers), then grow by tenfold steps. Use a fixed-size cluster (no
# MAGIC autoscaling) so timings compare. This is a first look; the scale-test kit (docs/spec/scale/plan.md) adds
# MAGIC Spark-level measurements later.

# COMMAND ----------

dbutils.widgets.dropdown("model", "logistic", ["cox", "logistic", "linear"], "1. Model")
dbutils.widgets.text("rows", "1000000", "2. Rows (n)")
dbutils.widgets.text("providers", "1000", "3. Providers (m)")
dbutils.widgets.text("features", "10", "4. Features (p)")
dbutils.widgets.text("seed", "79", "5. Seed")
dbutils.widgets.text("shuffle_partitions", "", "6. spark.sql.shuffle.partitions (empty: unchanged)")
dbutils.widgets.text("data_path", "", "7. Optional folder to write the data to (e.g. an S3 path)")
dbutils.widgets.text("build", "", "8. Build folder (empty: the newest build)")
dbutils.widgets.text("builds_root", "", "9. Builds folder (empty: /Workspace/Users/<you>/pprof_spark_builds)")
dbutils.widgets.text("results_dir", "", "10. Results folder (empty: the build folder)")

# COMMAND ----------

import datetime
import glob
import json
import math
import os
import sys
import time

import pandas as pd

user = spark.sql("SELECT current_user()").first()[0]
builds_root = dbutils.widgets.get("builds_root").strip() or f"/Workspace/Users/{user}/pprof_spark_builds"
build = dbutils.widgets.get("build").strip()
if not build:
    manifests = glob.glob(os.path.join(builds_root, "*", "manifest.json"))
    if not manifests:
        raise RuntimeError(f"no build under {builds_root}: run 00_build first")
    build = os.path.dirname(max(manifests, key=lambda p: json.load(open(p))["built_at"]))
with open(os.path.join(build, "manifest.json")) as f:
    manifest = json.load(f)
sys.path.insert(0, os.path.join(build, "python"))
for name in [m for m in list(sys.modules) if m == "pprof_spark" or m.startswith("pprof_spark.")]:
    del sys.modules[name]
import pprof_spark

pprof_spark.api(spark)
jar_commit = spark._jvm.pprof.spark.engine.BuildInfo.gitSha()
if jar_commit != manifest["commit"]:
    raise RuntimeError(f"the JARs on this cluster are {jar_commit[:12]}, not this build's {manifest['commit'][:12]}")

model_name = dbutils.widgets.get("model")
n = int(float(dbutils.widgets.get("rows")))
m = int(float(dbutils.widgets.get("providers")))
p = int(dbutils.widgets.get("features"))
seed = int(dbutils.widgets.get("seed"))
shuffle_partitions = dbutils.widgets.get("shuffle_partitions").strip()
data_path = dbutils.widgets.get("data_path").strip()
results_dir = dbutils.widgets.get("results_dir").strip() or build
if not (1 <= m <= n and p >= 1):
    raise ValueError("need 1 <= providers <= rows and features >= 1")
print(f"{model_name}: n = {n:,}, m = {m:,}, p = {p}, seed {seed}; build {manifest['commit'][:12]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Synthetic data

# COMMAND ----------

FEATURES = [f"x{k}" for k in range(1, p + 1)]
TRUTH = [(-1) ** k * 0.5 / math.sqrt(p) for k in range(1, p + 1)]


def uniform(key, tag):
    """A uniform draw in (0, 1), a deterministic function of the key, the seed and the tag."""
    return f"((pmod(hash({key}, {seed}, '{tag}'), 1000003) + 0.5) / 1000003.0)"


def feature(k):
    if k % 2:
        return f"CASE WHEN {uniform('id', f'x{k}')} < 0.3 THEN 1.0 ELSE 0.0 END"
    return f"(pmod(hash(id, {seed}, 'x{k}'), 129) - 64) / 64.0"


def generate():
    base = spark.range(n).selectExpr("id", f"(id * {m}) div {n} AS provider")
    with_features = base.selectExpr("id", "provider", *[f"{feature(k)} AS x{k}" for k in range(1, p + 1)])
    linear_part = " + ".join(f"({beta!r}) * x{k}" for k, beta in enumerate(TRUTH, start=1))
    effect = f"((pmod(hash(provider, {seed}, 'gamma'), 201) - 100) / 200.0)"
    if model_name == "logistic":
        outcome = f"CASE WHEN {uniform('id', 'y')} < 1.0 / (1.0 + exp(-({effect} + {linear_part}))) THEN 1 ELSE 0 END"
        return with_features.selectExpr("*", f"{outcome} AS y")
    if model_name == "linear":
        noise = " + ".join(uniform("id", f"e{j}") for j in range(12)) + " - 6.0"
        return with_features.selectExpr("*", f"{effect} + {linear_part} + ({noise}) AS y")
    hazard = f"0.2 * exp({effect}) * exp({linear_part})"
    return with_features.selectExpr(
        "*", f"-ln({uniform('id', 'event_time')}) / ({hazard}) AS event_years",
        f"5.0 * {uniform('id', 'censoring')} AS censor_years",
    ).selectExpr(
        "id", "provider", *FEATURES,
        "greatest(1.0, ceil(365.0 * least(event_years, censor_years, 5.0))) AS time",
        "CASE WHEN event_years <= least(censor_years, 5.0) THEN 1 ELSE 0 END AS event",
    )


def specification():
    if model_name == "logistic":
        return pprof_spark.LogisticFixedEffect("y", FEATURES, "provider", row_id="id")
    if model_name == "linear":
        return pprof_spark.LinearFixedEffect("y", FEATURES, "provider", row_id="id")
    return pprof_spark.CoxPH("time", "event", FEATURES, strata="provider", row_id="id")


def downstream(fit, df):
    """One follow-up call per model, chosen to stay cheap at large sizes."""
    if model_name == "logistic":
        return "provider tests (score)", lambda: fit.provider_tests(df, method="score").count()
    if model_name == "linear":
        return "provider tests", lambda: fit.test().count()
    return "baseline hazard", lambda: fit.baseline(df).count()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Run and time

# COMMAND ----------

timings = {}


def timed(label, action):
    start = time.perf_counter()
    value = action()
    timings[label] = round(time.perf_counter() - start, 2)
    print(f"{label}: {timings[label]} s")
    return value


key = "spark.sql.shuffle.partitions"
previous_partitions = spark.conf.get(key)
if shuffle_partitions:
    spark.conf.set(key, shuffle_partitions)
persisted = None
try:
    if data_path:
        location = f"{data_path.rstrip('/')}/{model_name}-n{n}-m{m}-p{p}-seed{seed}"
        timed("generate and write", lambda: generate().write.mode("overwrite").parquet(location))
        df = spark.read.parquet(location)
    else:
        persisted = generate().persist()
        timed("generate and cache", persisted.count)
        df = persisted
    fit = timed("fit", lambda: specification().fit(df))
    label, action = downstream(fit, df)
    timed(label, action)
finally:
    spark.conf.set(key, previous_partitions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recovery of the true coefficients, and the results row
# MAGIC z = (β̂ − β)/SE for each coefficient; with correct estimates and standard errors these behave like
# MAGIC standard normal draws, so max |z| above 4.5 deserves a look.

# COMMAND ----------

z = [(c.estimate - beta) / c.standard_error for c, beta in zip(fit.coefficients, TRUTH)]
max_abs_z = max(abs(v) for v in z)
row = {
    "when": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
    "commit": manifest["commit"][:12],
    "model": model_name,
    "rows": n,
    "providers": m,
    "features": p,
    "seed": seed,
    "shuffle_partitions": spark.conf.get(key) if not shuffle_partitions else shuffle_partitions,
    "workers": spark.conf.get("spark.databricks.clusterUsageTags.clusterWorkers", "local"),
    "worker_type": spark.conf.get("spark.databricks.clusterUsageTags.clusterNodeType", "local"),
    "default_parallelism": spark.sparkContext.defaultParallelism,
    "databricks_runtime": spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "unknown"),
    "data": "parquet" if data_path else "cached",
    "iterations": getattr(fit, "iterations", None),
    "converged": getattr(fit, "converged", None),
    "max_abs_z": round(max_abs_z, 3),
    "recovered": max_abs_z <= 4.5,
}
row.update({f"seconds: {k}": v for k, v in timings.items()})
os.makedirs(results_dir, exist_ok=True)
results_path = os.path.join(results_dir, "scale-first-look.csv")
history = pd.read_csv(results_path) if os.path.exists(results_path) else pd.DataFrame()
history = pd.concat([history, pd.DataFrame([row])], ignore_index=True)
history.to_csv(results_path, index=False)
display(pd.DataFrame([row]))
print(f"z by coefficient: {[round(v, 2) for v in z]}\nappended to {results_path}")
if persisted is not None:
    persisted.unpersist()
