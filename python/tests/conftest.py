import os
from pathlib import Path

import pytest

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"


@pytest.fixture(scope="session")
def spark():
    """A local Classic session with the pprof_spark JARs (comma-separated in PPROF_SPARK_JARS)."""
    jars = os.environ.get("PPROF_SPARK_JARS")
    if not jars:
        pytest.skip("set PPROF_SPARK_JARS to the pprof_spark JARs, comma-separated")
    # Spark resolves relative spark.jars entries against the JVM's working directory, which is where
    # pytest runs; resolve them against the repository root instead, and fail early on missing files.
    root = Path(__file__).resolve().parents[2]
    paths = [Path(p) if Path(p).is_absolute() else root / p for p in jars.split(",") if p.strip()]
    missing = [str(p) for p in paths if not p.is_file()]
    if missing:
        pytest.fail("PPROF_SPARK_JARS names files that do not exist: " + ", ".join(missing), pytrace=False)
    jars = ",".join(str(p) for p in paths)
    from pyspark.sql import SparkSession
    session = (SparkSession.builder.master("local[1]").appName("pprof-spark-python-tests")
               .config("spark.jars", jars).config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", "4").config("spark.sql.session.timeZone", "UTC")
               .getOrCreate())
    yield session
    session.stop()
