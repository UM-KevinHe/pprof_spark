"""The Python wrappers carry the engine's Cox results unchanged (ADR-0009)."""
import json

import pytest
from pyspark.errors import IllegalArgumentException

import pprof_spark
from pprof_spark import CoxPH, CoxPHModel
from pprof_spark._jvm import normalized

from conftest import FIXTURES

FEATURES = ["x1", "x2", "x3"]


def frame(spark, case):
    return spark.read.csv(str(FIXTURES / "cox" / case / "input.csv"), header=True, inferSchema=True)


def model(**options):
    return CoxPH("time", "event", FEATURES, strata="stratum", row_id="id", **options)


def test_fit_matches_pprof_py_and_carries_the_engine_bits(spark):
    df = frame(spark, "rc-stratified")
    fit = model(ties="breslow", eps=1e-11, max_iterations=100).fit(df)
    expected = [float.fromhex(v) for v in json.loads(
        (FIXTURES / "cox" / "rc-stratified" / "pprof_py.json").read_text())["breslow"]["tight"]["coef"]]
    scale = max(abs(e) for e in expected)
    assert all(abs(a - e) <= 1e-10 + 1e-8 * max(abs(e), scale) for a, e in zip(fit.coef_, expected))
    engine = spark._jvm.scala.jdk.javaapi.CollectionConverters.asJava(fit._handle.estimates())
    assert fit.coef_ == [float(v) for v in engine]
    assert fit.converged and fit.ties == "breslow" and fit.feature_status == "experimental"
    assert fit.covariance[0][1] == fit.covariance[1][0]
    assert fit.columns["strata"] == "stratum" and fit.observations == df.count()


def test_baseline_predictions_residuals_measures_and_tests(spark):
    df = frame(spark, "rc-stratified")
    fit = model().fit(df)
    baseline = fit.baseline(df)
    assert baseline.columns == ["stratum", "time", "hazard_increment", "cumulative_hazard", "survival"]
    assert fit.linear_predictor(df).columns[-1] == "linear_predictor"
    assert fit.relative_hazard(df).columns[-1] == "relative_hazard"
    with_baseline = df.join(baseline.select("stratum").distinct(), "stratum")  # strata with events
    survival = fit.survival(with_baseline, baseline, "time")
    assert {"cumulative_hazard", "survival"} <= set(survival.columns)
    assert survival.count() == with_baseline.count() < df.count()
    residuals = fit.residuals(df)
    assert {"martingale", "score_x1", "dfbeta_x3"} <= set(residuals.columns)
    measures = fit.standardized_measures(df, "stratum", ("indirect", "direct"))
    assert set(measures) == {"indirect", "direct"} and measures["direct"].count() == fit.strata
    tests = fit.provider_tests(df, "stratum", method="exact", level=0.9)
    assert {"z_raw", "p_value", "flag", "ci_lower", "ci_upper"} <= set(tests.columns)


def test_save_and_load_round_trip(spark, tmp_path):
    df = frame(spark, "rc-stratified")
    fit = model(ties="efron").fit(df)
    path = str(tmp_path / "fit")
    fit.save(path, baseline=fit.baseline(df))
    loaded = CoxPHModel.load(spark, path)
    assert loaded.coef_ == fit.coef_ and loaded.covariance == fit.covariance
    assert loaded.ties == "efron" and loaded.fingerprint == fit.fingerprint
    assert CoxPHModel.load_baseline(spark, path).count() == fit.baseline(df).count()


def test_engine_errors_reach_python_with_their_messages(spark):
    df = frame(spark, "rc-stratified").drop("x2")
    with pytest.raises(IllegalArgumentException, match="column x2 is missing"):
        model().fit(df)
    with pytest.raises(IllegalArgumentException, match="fit.ties"):
        model(ties="exact").fit(frame(spark, "rc-stratified"))


def test_spark_connect_sessions_are_refused_with_a_pointer_to_the_job_runner():
    class Session:
        pass

    Session.__module__ = "pyspark.sql.connect.session"
    with pytest.raises(RuntimeError, match="job runner"):
        pprof_spark.api(Session())


def test_versions_are_compared_in_python_notation(spark):
    assert normalized("0.1.0-SNAPSHOT") == "0.1.0.dev0" == pprof_spark.__version__
    assert normalized(pprof_spark.api(spark).version()) == pprof_spark.__version__
