# Databricks notebook source
# MAGIC %md
# MAGIC # pprof_spark · 00 · Build on this cluster
# MAGIC
# MAGIC Builds one commit of pprof_spark on this cluster's driver: the three JARs (`numerics`, `engine`, `app`)
# MAGIC and the Python package. Nothing needs to be installed on your computer and nothing runs on GitHub. The
# MAGIC build lands in a folder of workspace files, from which the cluster installs the JARs
# MAGIC (`docs/guide/databricks.md`).
# MAGIC
# MAGIC 1. Set the widgets: the commit to build and, if you like, the builds folder.
# MAGIC 2. Run all. The first build takes about ten minutes, mostly downloading dependencies.
# MAGIC 3. Install the three JARs printed at the end as cluster libraries (Compute → your cluster → Libraries →
# MAGIC    Install new → Workspace), remove older `pprof-spark-` JARs, and restart the cluster.
# MAGIC 4. Run `01_parity`.
# MAGIC
# MAGIC Requirements: dedicated (single-user) compute on Databricks Runtime 18, and outbound access to
# MAGIC github.com and repo1.maven.org (checked below).

# COMMAND ----------

dbutils.widgets.text("commit", "main", "1. Commit, branch or tag")
dbutils.widgets.text("builds_root", "", "2. Builds folder (empty: /Workspace/Users/<you>/pprof_spark_builds)")
dbutils.widgets.text("repository", "https://github.com/UM-KevinHe/pprof_spark.git", "3. Repository")
dbutils.widgets.text("s3_copy", "", "4. Optional s3:// folder for a copy")
dbutils.widgets.dropdown("install_here", "no", ["no", "yes"], "5. Install on this cluster")

# COMMAND ----------

import datetime
import glob
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tarfile
import urllib.request

# Any 1.x launcher works: the build pins its own sbt version in project/build.properties.
SBT_VERSION = "1.12.15"
SBT_URL = f"https://github.com/sbt/sbt/releases/download/v{SBT_VERSION}/sbt-{SBT_VERSION}.tgz"
MODULES = ["numerics", "engine", "app"]
FIXTURE_FAMILIES = ["cox", "logistic", "linear"]

commit = dbutils.widgets.get("commit").strip() or "main"
repository = dbutils.widgets.get("repository").strip()
user = spark.sql("SELECT current_user()").first()[0]
builds_root = dbutils.widgets.get("builds_root").strip() or f"/Workspace/Users/{user}/pprof_spark_builds"
scratch = "/local_disk0/tmp" if os.path.isdir("/local_disk0") else "/tmp"
work = os.path.join(scratch, "pprof_spark_build")
source = os.path.join(work, "pprof_spark")


def run(command, cwd=None, env=None, tail=40):
    """Runs a command, prints the last lines of its output, and raises if it fails."""
    print("$", " ".join(command))
    result = subprocess.run(
        command, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    lines = result.stdout.splitlines()
    print("\n".join(lines[-tail:]))
    if result.returncode != 0:
        raise RuntimeError(f"{os.path.basename(command[0])} failed with exit code {result.returncode}")
    return result.stdout


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


print(f"building '{commit}' of {repository}\ninto {builds_root}\nscratch {work}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Outbound access
# MAGIC The build downloads sbt from GitHub and the project's dependencies from Maven Central.

# COMMAND ----------

for url in ["https://github.com", "https://repo1.maven.org/maven2/"]:
    try:
        urllib.request.urlopen(url, timeout=30).close()
        print(f"reachable: {url}")
    except Exception as error:
        raise RuntimeError(
            f"cannot reach {url} ({error}). The build needs github.com and repo1.maven.org; see "
            "'If the cluster cannot reach the internet' in docs/guide/databricks.md"
        ) from error

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source at the requested commit

# COMMAND ----------

shutil.rmtree(work, ignore_errors=True)
os.makedirs(work)
run(["git", "clone", "--quiet", repository, source])
run(["git", "checkout", "--quiet", commit], cwd=source)
sha = run(["git", "rev-parse", "HEAD"], cwd=source).split()[-1]
subject = subprocess.run(
    ["git", "log", "-1", "--format=%s"], cwd=source, stdout=subprocess.PIPE, text=True
).stdout.strip()
print(f"commit {sha}: {subject}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compile and package
# MAGIC `sbt numerics/package engine/package app/package`, as CI's Python job does. Tests are not run here: CI
# MAGIC runs them on every push, and `01_parity` checks the build on this cluster.

# COMMAND ----------

archive_path = os.path.join(work, "sbt.tgz")
urllib.request.urlretrieve(SBT_URL, archive_path)
with tarfile.open(archive_path) as archive:
    try:
        archive.extractall(work, filter="data")
    except TypeError:  # Python before 3.12
        archive.extractall(work)
sbt = os.path.join(work, "sbt", "bin", "sbt")
os.chmod(sbt, 0o755)

env = dict(os.environ, SBT_OPTS="-Xmx4g -Xss8m")
run([sbt, "-batch", "-no-colors", "numerics/package", "engine/package", "app/package"], cwd=source, env=env)

jars = []
for module in MODULES:
    pattern = os.path.join(source, module, "target", "scala-2.13", f"pprof-spark-{module}_2.13-*.jar")
    found = [p for p in glob.glob(pattern) if not p.endswith(("-sources.jar", "-javadoc.jar"))]
    if len(found) != 1:
        raise RuntimeError(f"expected one {module} JAR, found {found}")
    jars.append(found[0])
print("\n".join(jars))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Python package
# MAGIC A wheel, for those who prefer `%pip install --no-deps`; the other notebooks import the package straight
# MAGIC from the build folder, so its Python always matches its JARs.

# COMMAND ----------

dist = os.path.join(work, "dist")
python_source = os.path.join(source, "python")
try:
    run([sys.executable, "-m", "pip", "wheel", "--no-deps", "--no-build-isolation", "--wheel-dir", dist, python_source])
except RuntimeError:
    run([sys.executable, "-m", "pip", "wheel", "--no-deps", "--wheel-dir", dist, python_source])
wheel = glob.glob(os.path.join(dist, "pprof_spark-*.whl"))[0]
print(wheel)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copy the build to the builds folder

# COMMAND ----------

destination = os.path.join(builds_root, sha[:12])
if os.path.exists(destination):
    shutil.rmtree(destination)
os.makedirs(destination)
for path in jars + [wheel]:
    shutil.copy(path, destination)
shutil.copytree(os.path.join(python_source, "pprof_spark"), os.path.join(destination, "python", "pprof_spark"))
shutil.copy(os.path.join(source, "testkit", "src", "main", "resources", "tolerances.conf"), destination)
for family in FIXTURE_FAMILIES:
    shutil.copytree(os.path.join(source, "fixtures", family), os.path.join(destination, "fixtures", family))

java = subprocess.run(["java", "-version"], stderr=subprocess.PIPE, text=True).stderr.splitlines()[0]
manifest = {
    "commit": sha,
    "subject": subject,
    "requested": commit,
    "repository": repository,
    "built_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
    "built_by": user,
    "sbt_launcher": SBT_VERSION,
    "java": java,
    "python": sys.version.split()[0],
    "databricks_runtime": spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "unknown"),
    "jars": [os.path.join(destination, os.path.basename(p)) for p in jars],
    "wheel": os.path.join(destination, os.path.basename(wheel)),
    "sha256": {os.path.basename(p): sha256(p) for p in jars + [wheel]},
}
with open(os.path.join(destination, "manifest.json"), "w") as f:
    json.dump(manifest, f, indent=2)
print(json.dumps(manifest, indent=2))

s3_copy = dbutils.widgets.get("s3_copy").strip()
if s3_copy:
    target = f"{s3_copy.rstrip('/')}/{sha[:12]}"
    dbutils.fs.cp(f"file:{destination}", target, recurse=True)
    print(f"copied to {target}; JARs there can be installed as cloud-storage libraries")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install the JARs
# MAGIC By hand (recommended the first time): Compute → your cluster → Libraries → Install new → Workspace, once
# MAGIC for each path below; uninstall older `pprof-spark-` JARs; restart the cluster. Or set the widget
# MAGIC *Install on this cluster* to `yes` and rerun this cell, then restart the cluster.

# COMMAND ----------

print("Install these three JARs as cluster libraries, then restart the cluster:")
for path in manifest["jars"]:
    print("  ", path)
print(f"\nThen run 01_parity with build folder {destination}")

if dbutils.widgets.get("install_here") == "yes":
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.compute import Library

        client = WorkspaceClient()
        cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
        statuses = client.libraries.cluster_status(cluster_id)
        statuses = getattr(statuses, "library_statuses", statuses) or []
        old = [
            s.library
            for s in statuses
            if s.library is not None and s.library.jar and "pprof-spark-" in s.library.jar
        ]
        if old:
            client.libraries.uninstall(cluster_id, old)
            print(f"uninstalled {len(old)} older pprof_spark JARs")
        client.libraries.install(cluster_id, [Library(jar=path) for path in manifest["jars"]])
        print(f"installed on cluster {cluster_id}: restart the cluster now (Compute → Restart)")
    except Exception as error:
        print(f"automatic installation failed ({error}); install the JARs by hand as described above")
