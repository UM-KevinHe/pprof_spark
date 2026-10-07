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
    from pyspark.sql import SparkSession
    session = (SparkSession.builder.master("local[1]").appName("pprof-spark-python-tests")
               .config("spark.jars", jars).config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", "4").config("spark.sql.session.timeZone", "UTC")
               .getOrCreate())
    yield session
    session.stop()
