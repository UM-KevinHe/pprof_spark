"""The JVM facade `pprof.spark.app.python.PythonApi`, reached through PySpark's py4j gateway."""

__version__ = "0.1.0.dev0"

_CONNECT = (
    "pprof_spark's Python wrappers need a Classic Spark session: Spark Connect clients have no JVM "
    "gateway (ADR-0009). Use the job runner (docs/guide/cox-job.md) instead."
)


def normalized(version: str) -> str:
    """The JVM version in Python's notation: ``0.1.0-SNAPSHOT`` is ``0.1.0.dev0``."""
    return version.replace("-SNAPSHOT", ".dev0")


def api(spark):
    """The facade for `spark`; fails clearly under Spark Connect, without the JARs, or on a version
    mismatch between the JARs and this package."""
    if type(spark).__module__.startswith("pyspark.sql.connect"):
        raise RuntimeError(_CONNECT)
    try:
        facade = spark._jvm.pprof.spark.app.python.PythonApi
        version = facade.version()
    except Exception as error:  # a missing class is a py4j JavaPackage, which cannot be called
        raise RuntimeError(
            "the pprof_spark JARs are not on the Spark classpath; add them with spark.jars (ADR-0009)"
        ) from error
    if normalized(version) != __version__:
        raise RuntimeError(f"the pprof_spark JARs are version {version}; this Python package is {__version__}")
    return facade
