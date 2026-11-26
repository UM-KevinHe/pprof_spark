# Databricks notebook source
# MAGIC %md
# MAGIC # pprof_spark · 03 · Performance baseline
# MAGIC
# MAGIC Runs one point of the performance phase's baseline (`docs/parity/scale/perf-runbook.md`) with the bench
# MAGIC JAR's `PerfJob`. It generates the point's data once, under the data folder, and later runs reuse them. It
# MAGIC fits the model with a recorder attached and checks recovery of the true coefficients. Where the runbook
# MAGIC asks, it refits on another partitioning and checks the two fits agree bit for bit (R0). It prints one report
# MAGIC line, also written under the point's folder.
# MAGIC
# MAGIC 1. Run `00_build`, install the four JARs it prints (numerics, engine, app, bench), and restart the cluster.
# MAGIC 2. Set *Data folder* to an `s3://` folder the cluster can write to.
# MAGIC 3. Choose a point, 1 to 13 as in the runbook, and run all. Or choose `custom` and give its arguments.
# MAGIC 4. Send back the report line printed at the end.
# MAGIC
# MAGIC Widget 6 compares the logistic Newton methods (perf-runbook.md, "Comparing the logistic Newton methods"):
# MAGIC `serbin,profile` fits the point once by each, back to back.
# MAGIC
# MAGIC Requirements: dedicated (single-user) compute, as for `01_parity`. Autoscaling is fine: the report records the
# MAGIC cores the cluster offered during the fit, and points compare on CPU per row (D-76).

# COMMAND ----------

POINTS = {
    "1": "--model cox --rows 5e6 --features 180 --providers 8000 --check-partitions 401 --plans true",
    "2": "--model logistic --rows 5e6 --features 180 --providers 8000 --check-partitions 401 --plans true",
    "3": "--model cox --rows 2e7 --features 180 --providers 8000 --check-partitions 401",
    "4": "--model logistic --rows 2e7 --features 180 --providers 8000 --check-partitions 401",
    "5": "--model cox --rows 2e7 --features 100 --providers 8000",
    "6": "--model logistic --rows 2e7 --features 100 --providers 8000",
    "7": "--model cox --rows 5e7 --features 180 --providers 8000",
    "8": "--model logistic --rows 5e7 --features 180 --providers 8000",
    "9": "--model cox --rows 1e8 --features 180 --providers 8000",
    "10": "--model logistic --rows 1e8 --features 180 --providers 8000",
    "11": "--model cox --rows 5e6 --features 180 --providers 8000 --design correlated",
    "12": "--model logistic --rows 5e6 --features 180 --providers 8000 --design correlated",
    "13": "--model cox --rows 2e6 --features 180 --providers 8000 --strata none",
}

dbutils.widgets.dropdown("point", "1", list(POINTS) + ["custom"], "1. Point (perf-runbook.md)")
dbutils.widgets.text("data_root", "", "2. Data folder (s3://...)")
dbutils.widgets.text("custom", "", "3. Arguments of point 'custom', without --path")
dbutils.widgets.text("extra", "", "4. Extra arguments for any point, e.g. --verify false")
dbutils.widgets.text("storage_levels", "", "5. Storage levels to compare, e.g. OFF_HEAP,DISK_ONLY")
dbutils.widgets.text("newton_methods", "", "6. Logistic Newton methods to compare, e.g. serbin,profile")

# COMMAND ----------

import json
import time

import pandas as pd
from py4j.java_gateway import JavaClass

point = dbutils.widgets.get("point")
data_root = dbutils.widgets.get("data_root").strip().rstrip("/")
if not data_root:
    raise ValueError("set the widget 'Data folder' to a folder the cluster can write to, e.g. s3://<bucket>/pprof-spark/perf")
arguments = (dbutils.widgets.get("custom") if point == "custom" else POINTS[point]).split()
arguments += dbutils.widgets.get("extra").split()
if "--path" in arguments:
    raise ValueError("the notebook sets --path itself, from the data folder and the point")


def value(name, default):
    return arguments[arguments.index(name) + 1] if name in arguments else default


folder = "-".join(
    [
        value("--model", "model"),
        value("--design", "simple"),
        "n" + value("--rows", "5e6"),
        "p" + value("--features", "180"),
        "m" + value("--providers", "8000"),
        "s" + value("--seed", "79"),
    ]
    + (["unstratified"] if value("--strata", "provider") == "none" else [])
)
arguments += ["--path", f"{data_root}/{folder}"]

runner = spark._jvm.pprof.spark.bench.perf.PerfJob
if not isinstance(runner, JavaClass):
    raise RuntimeError(
        "the bench JAR is not on the Spark classpath: install the four JARs 00_build printed "
        "(numerics, engine, app, bench) and restart the cluster"
    )
print(f"point {point}: PerfJob", " ".join(arguments))

# COMMAND ----------

levels = [level.strip() for level in dbutils.widgets.get("storage_levels").split(",") if level.strip()]
if levels and "--storage-level" in arguments:
    raise ValueError("give the storage levels either in widget 5 or as --storage-level, not both")
methods = [method.strip() for method in dbutils.widgets.get("newton_methods").split(",") if method.strip()]
if methods and "--newton" in arguments:
    raise ValueError("give the Newton methods either in widget 6 or as --newton, not both")
reports = []
for level in levels or [None]:
    for method in methods or [None]:
        started = time.time()
        line = " ".join(
            arguments + (["--storage-level", level] if level else []) + (["--newton", method] if method else [])
        )
        reports.append(runner.runLine(spark._jsparkSession, line))
        print(f"{level or 'default storage'}, {method or 'default method'}: finished in {time.time() - started:.0f} s")
print(f"{len(reports)} report(s), also under {data_root}/{folder}/reports/, one folder per fit")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The report
# MAGIC First one row per fit, side by side when several storage levels ran; then each fit's operations, slowest
# MAGIC first, by operation tag (such as `pprof.cox.evaluate` for Cox's Newton passes). The last lines are every
# MAGIC report, one per fit: send them all back. `result_bytes` is what tasks sent to the driver, and
# MAGIC `max_result_bytes_per_execution` the largest single pass. Send back the line printed at the end.

# COMMAND ----------

columns = [
    "tags", "site", "executions", "wall_ms", "cpu_ms", "cpu_us_per_row", "gc_ms", "other_ms", "tasks",
    "result_bytes", "max_result_bytes_per_execution", "input_bytes", "shuffle_write_bytes", "spilled_bytes",
]
summary = []
details = []
for line in reports:
    parsed = json.loads(line)
    fit = parsed["fit"]
    recording = fit["recording"]
    rows = parsed["point"]["n"]
    # The level asked for (round 169 on; OFF_HEAP by default, D-77), else the one given, else "default";
    # and, from round 170, the level used: DISK_ONLY for OFF_HEAP where the cluster has no off-heap memory.
    storage = parsed["fit"].get("storage_level") or parsed["options"].get("storage-level", "default")
    storage_used = parsed["fit"].get("storage_level_used", "")
    sites = pd.DataFrame(recording["sites"])
    newton = parsed["options"].get("newton", "serbin") if parsed["point"]["model"] == "logistic" else ""
    row = {
        "storage": storage,
        "storage_used": storage_used,
        "newton": newton,
        "succeeded": fit["succeeded"],
        "fit_s": round(fit["seconds"], 1),
        "iterations": fit.get("iterations"),
        "executors": f"{recording['executors_min']} to {recording['executors_max']}",
        "utilization": round(recording["utilization"], 2),
        "cache_memory_gib": round(recording["cached_memory_peak_bytes"] / 2**30, 2),
        "cache_disk_gib": round(recording["cached_disk_peak_bytes"] / 2**30, 2),
        "max_abs_z": parsed["checks"].get("truth", {}).get("max_abs_z"),
    }
    if len(sites):
        sites["cpu_us_per_row"] = sites["cpu_ms"] * 1000.0 / rows
        heaviest = sites.loc[sites["cpu_ms"].idxmax()]
        row["cpu_us_per_row"] = round(sites["cpu_ms"].sum() * 1000.0 / rows, 1)
        row["heaviest"] = heaviest["tags"]
        row["heaviest_outside_cpu_gc"] = round(heaviest["other_ms"] / max(heaviest["run_ms"], 1), 3)
        details.append((f"{storage} {newton}".strip(), fit, sites))
    summary.append(row)
display(pd.DataFrame(summary))
# Where both Newton methods ran (widget 6): the profile method's estimates against SerBIN's, in SerBIN's standard
# errors. scripts/compare_newton.py makes the full comparison, with the T-coef check, from the report lines.
fits = {}
for line in reports:
    parsed = json.loads(line)
    if parsed["fit"].get("estimates") is not None:
        fits.setdefault(parsed["options"].get("newton", "serbin"), parsed["fit"])
if "serbin" in fits and "profile" in fits:
    s, q = fits["serbin"], fits["profile"]
    shift = max(abs(a - b) / e for a, b, e in zip(q["estimates"], s["estimates"], s["standard_errors"]))
    print(f"profile against SerBIN: largest |Δβ|/SE {shift:.3g}; log-likelihoods {q['loglik']!r} and {s['loglik']!r}")
for storage, fit, sites in details:
    print(f"== {storage}: {fit['seconds']:.0f} s, succeeded {fit['succeeded']}" + (f"; {fit['error']}" if "error" in fit else ""))
    display(sites[[c for c in columns if c in sites.columns]])
print(f"Send back these {len(reports)} report line(s), one per fit:")
for line in reports:
    print(line)
