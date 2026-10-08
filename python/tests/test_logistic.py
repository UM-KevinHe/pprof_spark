"""The Python wrappers carry the engine's logistic results unchanged (ADR-0009, slices 2a and 2b)."""
import json

import pytest
from pyspark.errors import IllegalArgumentException

from pprof_spark import LogisticFixedEffect, LogisticFixedEffectModel

from conftest import FIXTURES

FEATURES = ["x1", "x2", "x3"]
CASE = FIXTURES / "logistic" / "lfe-clustered"


def reference():
    return json.loads((CASE / "pprof_py.json").read_text())


def within(actual, expected, rtol, atol=0.0):
    actual, expected = list(actual), [float.fromhex(v) for v in expected]
    scale = max(abs(e) for e in expected)
    return all(abs(a - e) <= atol + rtol * max(abs(e), scale) for a, e in zip(actual, expected))


@pytest.fixture(scope="module")
def data(spark):
    return spark.read.csv(str(CASE / "input.csv"), header=True, inferSchema=True)


@pytest.fixture(scope="module")
def fit(data):
    return LogisticFixedEffect("y", FEATURES, "provider", row_id="id", cluster="patient").fit(data)


def test_fit_auc_and_robust_variances_match_pprof_py(fit):
    py = reference()
    assert within(fit.coef_, py["default"]["beta"], 1e-8, 1e-10)
    inference = py["inference"]["default"]
    assert within([fit.auc], inference["auc"], 1e-8)
    p = len(FEATURES)
    packed = [fit.robust_covariance[i][j] for i in range(p) for j in range(i, p)]
    assert within(packed, inference["robust"]["var_beta"], 1e-7)
    assert fit.converged and fit.clusters > 0 and fit.n_providers == 30 and fit.columns["cluster"] == "patient"


def test_tests_predictions_and_providers(fit, data):
    inference = reference()["inference"]["default"]
    robust = fit.wald_tests(robust=True)
    assert [t.method for t in robust] == ["wald-robust"] * 3
    assert within([t.statistic for t in robust], inference["wald_robust"]["stat"], 1e-8)
    lr = fit.covariate_tests(data, "lr")
    assert within([t.statistic for t in lr], inference["lr"]["stat"], 1e-8)
    greater = fit.wald_tests(null=0.25, alternative="greater")
    assert all(t.upper == float("inf") for t in greater)
    predicted = fit.predict(data)
    assert {"linear_predictor", "probability"} <= set(predicted.columns) and predicted.count() == data.count()
    providers = fit.providers()
    assert providers.count() == fit.n_providers and "robust_var_case_mix" in providers.columns


def test_save_and_load_round_trip(fit, spark, tmp_path):
    path = str(tmp_path / "logistic")
    fit.save(path)
    loaded = LogisticFixedEffectModel.load(spark, path)
    assert loaded.coef_ == fit.coef_ and loaded.auc == fit.auc and loaded.robust_covariance == fit.robust_covariance


def test_unknown_providers_and_invalid_models_raise(fit, data):
    with pytest.raises(IllegalArgumentException, match="not in the fit"):
        fit.predict(data.withColumn("provider", data["provider"] + 1000))
    with pytest.raises(IllegalArgumentException, match="columns.features"):
        LogisticFixedEffect("y", [], "provider").fit(data)


def test_provider_tests_match_pprof_py(fit, data):
    expected = reference()["provider_tests"]["exact_two_sided_median"]
    table = fit.provider_tests(data).orderBy("provider")
    rows = table.collect()
    assert within([row["z_raw"] for row in rows], expected["z"], 1e-8)
    assert [row["flag"] for row in rows] == expected["flag"]
    assert {"p_value", "ci_lower", "ci_upper", "observed", "expected"} <= set(table.columns)
    assert fit.provider_tests(data, method="wald", providers=[1, 2]).count() == 2
