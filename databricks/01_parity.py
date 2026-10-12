# Databricks notebook source
# MAGIC %md
# MAGIC # pprof_spark · 01 · Parity on this cluster
# MAGIC
# MAGIC Checks that the JARs installed on this cluster come from the expected build, then fits the reference
# MAGIC fixtures through the PySpark wrappers on the cluster and compares the results with the stored outputs of
# MAGIC pprof_py v0.7.0 and R, under the tolerance classes the CI tests use (`tolerances.conf`, PROJECT_CONTEXT
# MAGIC §8.4). Last, it refits with other partitioning and checks that every bit is unchanged (R0).
# MAGIC
# MAGIC Run `00_build` first and install its JARs. A run takes a few minutes; the summary table at the end is
# MAGIC also saved as a CSV file in the build folder: send it back with the build's commit.

# COMMAND ----------

dbutils.widgets.text("build", "", "Build folder (empty: the newest build)")
dbutils.widgets.text("builds_root", "", "Builds folder (empty: /Workspace/Users/<you>/pprof_spark_builds)")

# COMMAND ----------

import datetime
import glob
import json
import math
import os
import sys

import pandas as pd
from pyspark.sql import types as T

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

# The Python package comes from the build folder, so it always matches the build's JARs.
sys.path.insert(0, os.path.join(build, "python"))
for name in [m for m in list(sys.modules) if m == "pprof_spark" or m.startswith("pprof_spark.")]:
    del sys.modules[name]
import pprof_spark

print(f"build {build}\ncommit {manifest['commit'][:12]}: {manifest['subject']}\nbuilt {manifest['built_at']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The installed JARs are this build

# COMMAND ----------

pprof_spark.api(spark)  # fails clearly without the JARs or on a version mismatch
jar_commit = spark._jvm.pprof.spark.engine.BuildInfo.gitSha()
if jar_commit != manifest["commit"]:
    raise RuntimeError(
        f"the JARs on this cluster were built from {jar_commit[:12]}, not {manifest['commit'][:12]}: install "
        "this build's JARs, remove the others and restart the cluster"
    )
print(f"JARs on the cluster: commit {jar_commit[:12]}, as expected")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers: fixtures, tolerances, results

# COMMAND ----------

FIXTURES = os.path.join(build, "fixtures")


def load_tolerances(path):
    entries = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith(("#", "!")) and "=" in line:
                key, _, value = line.partition("=")
                entries[key.strip()] = float(value.strip())
    return entries


TOLERANCES = load_tolerances(os.path.join(build, "tolerances.conf"))


def worst_ratio(actual, expected, cls):
    """PROJECT_CONTEXT §8.4 and D-09, as in the testkit: element i passes when
    |a - e| <= atol + rtol * max(|e|, s), s the largest finite |e|; returns the worst ratio of observed to
    allowed difference (at most 1 passes)."""
    rtol, atol = TOLERANCES[f"{cls}.rtol"], TOLERANCES[f"{cls}.atol"]
    assert len(actual) == len(expected), f"{len(actual)} values against {len(expected)}"
    finite = [abs(e) for e in expected if math.isfinite(e)]
    scale = max(finite) if finite else 0.0
    worst = 0.0
    for a, e in zip(actual, expected):
        if math.isnan(a) or math.isnan(e):
            return math.inf
        if math.isinf(a) or math.isinf(e):
            if a != e:
                return math.inf
            continue
        difference = abs(a - e)
        if difference:
            worst = max(worst, difference / (atol + rtol * max(abs(e), scale)))
    return worst


def doubles(values):
    """Hexadecimal doubles as the fixtures store them (Python's float.hex, R's %a)."""
    return [float(v) if v in ("inf", "-inf", "nan") else float.fromhex(v) for v in values]


def reference(family, case, source):
    with open(os.path.join(FIXTURES, family, case, f"{source}.json")) as f:
        return json.load(f)


results = []


def check(model, case, quantity, versus, cls, actual, expected):
    ratio = worst_ratio([float(a) for a in actual], [float(e) for e in expected], cls)
    results.append(dict(model=model, case=case, quantity=quantity, versus=versus, rule=cls,
                        worst_ratio=ratio, passed=ratio <= 1.0))


def check_equal(model, case, quantity, versus, actual, expected):
    results.append(dict(model=model, case=case, quantity=quantity, versus=versus, rule="exact",
                        worst_ratio=0.0 if actual == expected else math.inf, passed=actual == expected))


def frame(rows, schema, reverse=False):
    return spark.createDataFrame(list(reversed(rows)) if reverse else rows, schema)


def packed(square):
    """The upper triangle of a symmetric matrix, row by row, as the fixtures store covariances."""
    return [square[i][j] for i in range(len(square)) for j in range(i, len(square))]


def bits(values):
    return [float(v).hex() for v in values]


# COMMAND ----------

# MAGIC %md
# MAGIC ## Cox: tight fits with about twenty blocks per fixture, both tie methods
# MAGIC Coefficients (T-coef), standard errors (T-var) and log-likelihood (T-fn) against pprof_py and R
# MAGIC survival; the default fit's iteration count against pprof_py's.

# COMMAND ----------

COX_CASES = ["tiny-ties", "rc-unstratified", "rc-stratified", "rc-stratified-weights-offset",
             "lt-stratified", "lt-weights-offset"]
MANY_BLOCKS = {"targetBlockBytes": 2048}


def cox_input(case, reverse=False):
    info = reference("cox", case, "case")
    table = pd.read_csv(os.path.join(FIXTURES, "cox", case, "input.csv"))
    extra = ["entry"] if "entry" in table.columns else []
    names = ["id", "stratum", "time", "event", "weight", "offset"] + extra + list(info["features"])
    rows = [(int(r[0]), int(r[1]), float(r[2]), int(r[3])) + tuple(float(v) for v in r[4:])
            for r in table[names].itertuples(index=False)]
    schema = T.StructType(
        [T.StructField("id", T.LongType(), False), T.StructField("stratum", T.LongType(), False),
         T.StructField("time", T.DoubleType(), False), T.StructField("event", T.IntegerType(), False)]
        + [T.StructField(n, T.DoubleType(), False) for n in names[4:]])
    return frame(rows, schema, reverse), info, bool(extra)


def cox_model(info, truncated, ties, tight):
    return pprof_spark.CoxPH(
        "time", "event", info["features"],
        strata="stratum" if info.get("stratified") else None, row_id="id",
        weight="weight" if info.get("weighted") else None, offset="offset" if info.get("offset") else None,
        entry="entry" if truncated else None, ties=ties,
        eps=1e-11 if tight else None, max_iterations=100 if tight else None, blocks=MANY_BLOCKS)


software = None  # the software record of the first fit, for the summary
for case in COX_CASES:
    df, info, truncated = cox_input(case)
    for ties in ["breslow", "efron"]:
        fit = cox_model(info, truncated, ties, tight=True).fit(df)
        software = software or fit.software
        for source in ["pprof_py", "r_survival"]:
            node = reference("cox", case, source)[ties]["tight"]
            versus = f"{source} tight"
            check("cox", f"{case} {ties}", "coefficients", versus, "T-coef", fit.coef_, doubles(node["coef"]))
            check("cox", f"{case} {ties}", "standard errors", versus, "T-var",
                  [c.standard_error for c in fit.coefficients], doubles(node["se"]))
            check("cox", f"{case} {ties}", "log-likelihood", versus, "T-fn", [fit.log_likelihood],
                  doubles(node["loglik"]))
        default = cox_model(info, truncated, ties, tight=False).fit(df)
        check_equal("cox", f"{case} {ties}", "iterations (default fit)", "pprof_py default",
                    default.iterations, reference("cox", case, "pprof_py")[ties]["default"]["iterations"])
print(f"cox: {sum(r['model'] == 'cox' for r in results)} comparisons")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Logistic fixed effects: default fits
# MAGIC β and γ (T-coef), their variances (T-var), log-likelihood (T-fn) and the iteration count against
# MAGIC pprof_py's default fit; exact provider tests on `lfe-base` (T-test, flags exactly).

# COMMAND ----------

LOGISTIC_CASES = ["lfe-base", "lfe-degenerate", "lfe-shifted", "lfe-binomial", "lfe-many", "lfe-clustered"]
LOGISTIC_FEATURES = ["x1", "x2", "x3"]


def logistic_input(case, reverse=False):
    table = pd.read_csv(os.path.join(FIXTURES, "logistic", case, "input.csv"))
    patients, trials = "patient" in table.columns, "n" in table.columns
    names = (["id", "provider", "y"] + (["patient"] if patients else []) + (["n"] if trials else [])
             + LOGISTIC_FEATURES)
    integral = 3 + int(patients) + int(trials)
    rows = [tuple(int(v) for v in r[:integral]) + tuple(float(v) for v in r[integral:])
            for r in table[names].itertuples(index=False)]
    schema = T.StructType(
        [T.StructField("id", T.LongType(), False), T.StructField("provider", T.LongType(), False),
         T.StructField("y", T.IntegerType(), False)]
        + ([T.StructField("patient", T.LongType(), False)] if patients else [])
        + ([T.StructField("n", T.IntegerType(), False)] if trials else [])
        + [T.StructField(n, T.DoubleType(), False) for n in LOGISTIC_FEATURES])
    model = pprof_spark.LogisticFixedEffect("y", LOGISTIC_FEATURES, "provider", row_id="id",
                                            trials="n" if trials else None,
                                            cluster="patient" if patients else None)
    return frame(rows, schema, reverse), model


for case in LOGISTIC_CASES:
    df, model = logistic_input(case)
    fit = model.fit(df)
    ref = reference("logistic", case, "pprof_py")["default"]
    providers = fit.providers().orderBy("provider").collect()
    check_equal("logistic", case, "iterations", "pprof_py default", fit.iterations, ref["iterations"])
    check("logistic", case, "beta", "pprof_py default", "T-coef", fit.coef_, doubles(ref["beta"]))
    check("logistic", case, "gamma", "pprof_py default", "T-coef", [r["gamma"] for r in providers],
          doubles(ref["gamma"]))
    check("logistic", case, "var_beta", "pprof_py default", "T-var", packed(fit.covariance),
          doubles(ref["var_beta"]))
    check("logistic", case, "var_gamma", "pprof_py default", "T-var", [r["var_gamma"] for r in providers],
          doubles(ref["var_gamma"]))
    check("logistic", case, "var_case_mix", "pprof_py default", "T-var",
          [r["var_case_mix"] for r in providers], doubles(ref["var_case_mix"]))
    check("logistic", case, "log-likelihood", "pprof_py default", "T-fn", [fit.log_likelihood],
          doubles(ref["loglik"]))

df, model = logistic_input("lfe-base")
fit = model.fit(df)
tests = fit.provider_tests(df).orderBy("provider").collect()
expected = reference("logistic", "lfe-base", "pprof_py")["provider_tests"]["exact_two_sided_median"]
check("logistic", "lfe-base", "exact test z", "pprof_py", "T-test", [r["z_raw"] for r in tests],
      doubles(expected["z"]))
check_equal("logistic", "lfe-base", "exact test flags", "pprof_py", [int(r["flag"]) for r in tests],
            [int(v) for v in expected["flag"]])
print(f"logistic: {sum(r['model'] == 'logistic' for r in results)} comparisons")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Linear fixed effects: default fits
# MAGIC β and γ (T-coef), standard errors of β (T-var), σ, AIC and BIC (T-fn) against pprof_py's complete
# MAGIC variance option, the engine's default.

# COMMAND ----------

LINEAR_CASES = ["lin-base", "lin-extreme", "lin-many", "lin-shifted", "lin-singletons", "lin-text"]


def linear_input(case, reverse=False):
    table = pd.read_csv(os.path.join(FIXTURES, "linear", case, "input.csv"), dtype={"provider": str})
    text = not all(v.lstrip("-").isdigit() for v in table["provider"])
    rows = [(int(r[0]), r[1] if text else int(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]))
            for r in table[["id", "provider", "y", "x1", "x2", "x3"]].itertuples(index=False)]
    schema = T.StructType(
        [T.StructField("id", T.LongType(), False),
         T.StructField("provider", T.StringType() if text else T.LongType(), False),
         T.StructField("y", T.DoubleType(), False)]
        + [T.StructField(n, T.DoubleType(), False) for n in ["x1", "x2", "x3"]])
    model = pprof_spark.LinearFixedEffect("y", ["x1", "x2", "x3"], "provider", row_id="id")
    return frame(rows, schema, reverse), model


for case in LINEAR_CASES:
    df, model = linear_input(case)
    fit = model.fit(df)
    ref = reference("linear", case, "pprof_py")["complete"]
    p = len(fit.coefficients)
    var_beta = doubles(ref["var_beta"])
    diagonal = ([var_beta[i * p + i] for i in range(p)] if len(var_beta) == p * p
                else [var_beta[i * p - i * (i - 1) // 2] for i in range(p)])
    check("linear", case, "beta", "pprof_py", "T-coef", [c.estimate for c in fit.coefficients],
          doubles(ref["beta"]))
    check("linear", case, "standard errors", "pprof_py", "T-var",
          [c.standard_error for c in fit.coefficients], [math.sqrt(v) for v in diagonal])
    check("linear", case, "sigma, AIC, BIC", "pprof_py", "T-fn", [fit.sigma, fit.aic, fit.bic],
          doubles(ref["sigma"]) + doubles(ref["aic"]) + doubles(ref["bic"]))
    providers = sorted(fit.providers().collect(), key=lambda r: r["provider"])
    effect = "gamma" if "gamma" in providers[0].asDict() else "effect"
    check("linear", case, "gamma", "pprof_py", "T-coef", [r[effect] for r in providers], doubles(ref["gamma"]))
print(f"linear: {sum(r['model'] == 'linear' for r in results)} comparisons")

# COMMAND ----------

# MAGIC %md
# MAGIC ## R0 on the cluster: other partitioning, same bits
# MAGIC Each model is refitted with `spark.sql.shuffle.partitions` = 7 and its input reversed and repartitioned
# MAGIC across 13 partitions; every coefficient and the log-likelihood must be bitwise identical.

# COMMAND ----------


def refit_elsewhere(make, fit_of):
    key = "spark.sql.shuffle.partitions"
    previous = spark.conf.get(key)
    first = fit_of(make(False))
    spark.conf.set(key, "7")
    try:
        second = fit_of(make(True).repartition(13))
    finally:
        spark.conf.set(key, previous)
    return first, second


cox_df, cox_info, cox_truncated = cox_input("rc-stratified-weights-offset")
a, b = refit_elsewhere(lambda r: cox_input("rc-stratified-weights-offset", r)[0],
                       lambda d: cox_model(cox_info, cox_truncated, "efron", tight=True).fit(d))
check_equal("cox", "rc-stratified-weights-offset efron", "R0: coefficients and log-likelihood bits",
            "same fit, other partitioning", bits(b.coef_ + [b.log_likelihood]), bits(a.coef_ + [a.log_likelihood]))

logistic_model = logistic_input("lfe-clustered")[1]
a, b = refit_elsewhere(lambda r: logistic_input("lfe-clustered", r)[0], logistic_model.fit)
check_equal("logistic", "lfe-clustered", "R0: coefficients and log-likelihood bits",
            "same fit, other partitioning", bits(b.coef_ + [b.log_likelihood]), bits(a.coef_ + [a.log_likelihood]))

linear_model = linear_input("lin-many")[1]
a, b = refit_elsewhere(lambda r: linear_input("lin-many", r)[0], linear_model.fit)
check_equal("linear", "lin-many", "R0: coefficients and sigma bits", "same fit, other partitioning",
            bits([c.estimate for c in b.coefficients] + [b.sigma]),
            bits([c.estimate for c in a.coefficients] + [a.sigma]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

summary = pd.DataFrame(results)
failed = summary[~summary["passed"]]
environment = {
    "commit": manifest["commit"],
    "databricks_runtime": spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "unknown"),
    "workers": spark.conf.get("spark.databricks.clusterUsageTags.clusterWorkers", "unknown"),
    "default_parallelism": spark.sparkContext.defaultParallelism,
    "software": software,
}
stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
path = os.path.join(build, f"parity-{stamp}.csv")
summary.assign(commit=manifest["commit"][:12]).to_csv(path, index=False)
with open(os.path.join(build, f"parity-{stamp}-environment.json"), "w") as f:
    json.dump(environment, f, indent=2)
display(summary)
print(json.dumps(environment, indent=2))
print(f"\n{len(summary)} comparisons, {len(failed)} failed; saved to {path}")
if len(failed):
    display(failed)
    raise AssertionError(f"{len(failed)} parity comparisons failed: see the table above")
