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
# MAGIC Requirements: dedicated (single-user) compute, as for `01_parity`. For the baseline, use the standard
# MAGIC cluster: a 32 GB driver and 30 workers of 32 GB, without autoscaling.

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

started = time.time()
report = runner.runLine(spark._jsparkSession, " ".join(arguments))
print(f"finished in {time.time() - started:.0f} s; the report is also under {data_root}/{folder}/reports/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The report
# MAGIC The fit's outcome and checks, then its call sites, slowest first. A call site is a SQL execution's
# MAGIC description, such as `collect at CoxPH.scala:507`. `result_bytes` is what tasks sent to the driver, and
# MAGIC `max_result_bytes_per_execution` the largest single pass. Send back the line printed at the end.

# COMMAND ----------

parsed = json.loads(report)
fit = parsed["fit"]
print({key: fit.get(key) for key in ["succeeded", "error", "seconds", "iterations", "converged", "layout"] if key in fit})
print("checks:", json.dumps(parsed["checks"]))
recording = fit["recording"]
print({key: recording[key] for key in recording if key != "sites"})
columns = [
    "site", "executions", "wall_ms", "cpu_ms", "gc_ms", "tasks", "result_bytes",
    "max_result_bytes_per_execution", "shuffle_write_bytes", "spilled_bytes", "single_task_stages",
]
display(pd.DataFrame(recording["sites"])[columns])
print(report)
